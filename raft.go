package raft

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shrtyk/raft-core/api"
	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
	tester "github.com/shrtyk/raft/tester1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type State = uint32

const (
	_ State = iota
	follower
	candidate
	leader
)

const (
	votedForNone = -1
)

const (
	ElectionTimeoutRand = 300 * time.Millisecond
	ElectionTimeoutBase = 300 * time.Millisecond
	HeartbeatInterval   = 70 * time.Millisecond

	rpcTimeout = 100 * time.Millisecond
)

// A Go object implementing a single Raft peer.
type Raft struct {
	wg          sync.WaitGroup
	mu          sync.RWMutex               // Lock to protect shared access to this peer's state
	peers       []raftpb.RaftServiceClient // gRPC end points of all peers
	persisterMu sync.Mutex
	persister   *tester.Persister // Object to hold this peer's persisted state
	me          int64             // this peer's index into peers[]
	dead        int32             // set by Kill()

	state State

	timerMu         sync.Mutex
	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker

	applyChan         chan *api.ApplyMessage
	signalApplierChan chan struct{}

	// Persistent state:

	curTerm  int64              // latest term server has seen
	votedFor int64              // index of peer in peers
	log      []*raftpb.LogEntry // log entries

	// Volatile state on all servers:

	commitIdx      int64 // index of highest log entry known to be committed
	lastAppliedIdx int64 // index of the highest log entry applied to state machine

	// Volatile state leaders only (reinitialized after election):

	// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	nextIdx []int64
	// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	matchIdx []int64

	lastIncludedIndex int64 // the index of the last entry in the log that the snapshot replaces
	lastIncludedTerm  int64 // the term of the last entry in the log that the snapshot replaces

	raftCtx    context.Context
	raftCancel func()

	raftpb.UnimplementedRaftServiceServer
}

func (rf *Raft) InstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotRequest) (reply *raftpb.InstallSnapshotResponse, err error) {
	reply = &raftpb.InstallSnapshotResponse{}
	var needToPersist, shouldSignalApplier bool
	var snapshotData []byte

	rf.mu.Lock()
	defer func() {
		rf.unlockConditionally(needToPersist, snapshotData)
		if shouldSignalApplier {
			rf.signalApplier()
		}
	}()

	reply.Term = rf.curTerm
	if req.Term < rf.curTerm {
		return
	}

	if req.Term > rf.curTerm {
		needToPersist = rf.becomeFollower(req.Term)
	}

	rf.resetElectionTimer()
	if req.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	needToPersist = true
	snapshotData = req.Data
	shouldSignalApplier = true

	sliceIndex := req.LastIncludedIndex - rf.lastIncludedIndex
	if sliceIndex < int64(len(rf.log)) && rf.getTerm(req.LastIncludedIndex) == req.LastIncludedTerm {
		rf.log = append([]*raftpb.LogEntry(nil), rf.log[sliceIndex:]...)
	} else {
		rf.log = nil
	}

	rf.lastIncludedIndex = req.LastIncludedIndex
	rf.lastIncludedTerm = req.LastIncludedTerm

	if rf.commitIdx < req.LastIncludedIndex {
		rf.commitIdx = req.LastIncludedIndex
	}

	return
}
func (rf *Raft) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (reply *raftpb.AppendEntriesResponse, err error) {
	reply = &raftpb.AppendEntriesResponse{}
	var needToPersist bool
	var shouldSignalApplier bool

	rf.mu.Lock()
	defer func() {
		rf.unlockConditionally(needToPersist, nil)
		if shouldSignalApplier {
			rf.signalApplier()
		}
	}()

	reply.Success = false
	reply.Term = rf.curTerm

	if req.Term < rf.curTerm {
		return
	}

	if req.Term > rf.curTerm {
		needToPersist = rf.becomeFollower(req.Term)
	}

	rf.resetElectionTimer()
	reply.Term = rf.curTerm
	if req.PrevLogIndex < rf.lastIncludedIndex {
		reply.Success = false
		return
	}

	if !rf.isLogConsistent(req.PrevLogIndex, req.PrevLogTerm) {
		rf.fillConflictReply(req, reply)
		return
	}

	if rf.processEntries(req) {
		needToPersist = true
	}

	if req.LeaderCommitIndex > rf.commitIdx {
		lastLogIndex, _ := rf.lastLogIdxAndTerm()
		rf.commitIdx = min(req.LeaderCommitIndex, lastLogIndex)
		shouldSignalApplier = true
	}

	reply.Success = true

	return
}
func (rf *Raft) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (reply *raftpb.RequestVoteResponse, err error) {
	reply = &raftpb.RequestVoteResponse{}
	var needToPersist bool

	rf.mu.Lock()
	defer func() {
		rf.unlockConditionally(needToPersist, nil)
	}()

	reply.VoteGranted = false
	reply.VoterId = rf.me

	if req.Term < rf.curTerm {
		reply.Term = rf.curTerm
		return
	}

	if req.Term > rf.curTerm {
		needToPersist = rf.becomeFollower(req.Term)
	}

	reply.Term = rf.curTerm
	if rf.isCandidateLogUpToDate(req.LastLogIndex, req.LastLogTerm) &&
		(rf.votedFor == votedForNone || rf.votedFor == req.CandidateId) {
		reply.VoteGranted = true
		rf.votedFor = req.CandidateId
		needToPersist = true
		rf.resetElectionTimer()
	}

	return
}

// GetState returns current term and whether this server believes it is the leader
func (rf *Raft) GetState() (int64, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.curTerm, rf.isState(leader)
}

// getPersistentStateBytes helper function for getting bytes of persistent state
//
// Assumes the lock is held when called
func (rf *Raft) getPersistentStateBytes() []byte {
	b, err := proto.Marshal(&raftpb.RaftPersistentState{
		CurrentTerm:       rf.curTerm,
		VotedFor:          rf.votedFor,
		Log:               rf.log,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
	})
	if err != nil {
		log.Printf("failed to marshal state: %s", err)
		return nil
	}

	return b
}

// persistAndUnlock captures the persistent state, locks the persister mutex,
// unlocks the main mutex, and then persists the state
//
// It must be called with rf.mu held, and it will unlock it
func (rf *Raft) persistAndUnlock(snapshot []byte) {
	state := rf.getPersistentStateBytes()
	rf.persisterMu.Lock()
	rf.mu.Unlock()

	defer rf.persisterMu.Unlock()

	if snapshot == nil {
		rf.persister.Save(state, rf.persister.ReadSnapshot())
		return
	}
	rf.persister.Save(state, snapshot)
}

// unlockConditionally unlocks the main mutex, and persists the state if needed
//
// It must be called with rf.mu held, and it will unlock it
func (rf *Raft) unlockConditionally(needToPersist bool, snapshot []byte) {
	if needToPersist {
		rf.persistAndUnlock(snapshot)
	} else {
		rf.mu.Unlock()
	}
}

// readPersist restores previously persisted state
//
// Assumes the lock is held when called
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	state := &raftpb.RaftPersistentState{}
	err := proto.Unmarshal(data, state)
	if err != nil {
		log.Printf("failed to unmarshal data into state struct: %s", err)
		return
	}

	rf.curTerm = state.GetCurrentTerm()
	rf.votedFor = state.GetVotedFor()
	rf.log = state.GetLog()
	rf.lastIncludedIndex = state.GetLastIncludedIndex()
	rf.lastIncludedTerm = state.GetLastIncludedTerm()

	rf.commitIdx = rf.lastIncludedIndex
	rf.lastAppliedIdx = rf.lastIncludedIndex
}

func (rf *Raft) PersistBytes() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.persister.RaftStateSize()
}

func (rf *Raft) Snapshot(index int64, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		return
	}

	term := rf.getTerm(index)
	sliceIndex := index - rf.lastIncludedIndex
	if sliceIndex < int64(len(rf.log)) {
		rf.log = append([]*raftpb.LogEntry(nil), rf.log[sliceIndex:]...)
	} else {
		rf.log = nil
	}

	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = term

	data := rf.getPersistentStateBytes()
	rf.persister.Save(data, snapshot)
}

type InstallSnapshotArgs struct {
	Term              int64
	LeaderId          int64
	LastIncludedIndex int64
	LastIncludedTerm  int64
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int64
}

func (rf *Raft) sendInstallSnapshotRPC(
	server int,
	args *raftpb.InstallSnapshotRequest,
) (*raftpb.InstallSnapshotResponse, error) {
	ctx, cancel := context.WithTimeout(rf.raftCtx, rpcTimeout)
	defer cancel()

	reply, err := rf.peers[server].InstallSnapshot(ctx, args)
	if err != nil {
		return nil, fmt.Errorf("failed to call server #%d InstallSnapshot: %v", server, err)
	}
	return reply, nil
}

// isCandidateLogUpToDate determines if the candidate's log is at least as up-to-date as receiver's log
//
// Assumes the lock is held when called
func (rf *Raft) isCandidateLogUpToDate(candidateLastLogIdx int64, candidateLastLogTerm int64) bool {
	myLastLogIdx, myLastLogTerm := rf.lastLogIdxAndTerm()
	if candidateLastLogTerm != myLastLogTerm {
		return candidateLastLogTerm > myLastLogTerm
	}
	return candidateLastLogIdx >= myLastLogIdx
}

func (rf *Raft) sendRequestVoteRPC(
	server int,
	req *raftpb.RequestVoteRequest,
) (*raftpb.RequestVoteResponse, error) {
	ctx, cancel := context.WithTimeout(rf.raftCtx, rpcTimeout)
	defer cancel()

	reply, err := rf.peers[server].RequestVote(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to call server #%d RequestVote: %v", server, err)
	}
	return reply, nil
}

func (rf *Raft) sendAppendEntriesRPC(
	server int,
	req *raftpb.AppendEntriesRequest,
) (*raftpb.AppendEntriesResponse, error) {
	ctx, cancel := context.WithTimeout(rf.raftCtx, rpcTimeout)
	defer cancel()

	reply, err := rf.peers[server].AppendEntries(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to call server #%d AppendEntries: %v", server, err)
	}
	return reply, nil
}

// Start proposes a new command to be replicated
func (rf *Raft) Start(command []byte) (int64, int64, bool) {
	rf.mu.Lock()

	isLeader := rf.isState(leader)
	term := rf.curTerm
	if !isLeader {
		rf.mu.Unlock()
		return -1, term, false
	}

	rf.log = append(rf.log, &raftpb.LogEntry{
		Term: rf.curTerm,
		Cmd:  command,
	})
	lastLogIdx, _ := rf.lastLogIdxAndTerm()
	rf.matchIdx[rf.me] = lastLogIdx
	rf.nextIdx[rf.me] = lastLogIdx + 1

	rf.persistAndUnlock(nil)

	go rf.sendAppendEntries()

	return lastLogIdx, term, isLeader
}

// Kill sets the peer to a dead state
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.raftCancel()
	rf.wg.Wait()
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// processEntries handles appending/truncating entries to the follower's log
// and returns true if there is need to persist
//
// Assumes the lock is held when called
func (rf *Raft) processEntries(req *raftpb.AppendEntriesRequest) (needToPersist bool) {
	for i, entry := range req.Entries {
		absIdx := req.PrevLogIndex + 1 + int64(i)
		lastAbsIdx, _ := rf.lastLogIdxAndTerm()
		if absIdx > lastAbsIdx {
			rf.log = append(rf.log, req.Entries[i:]...)
			needToPersist = true
			break
		}

		if rf.getTerm(absIdx) != entry.Term {
			sliceIdx := absIdx - rf.lastIncludedIndex - 1
			rf.log = rf.log[:sliceIdx]
			rf.log = append(rf.log, req.Entries[i:]...)
			needToPersist = true
			break
		}
	}
	return
}

// isLogConsistent is a helper function that checks if the log is consistent
// with the leader's AppendEntries request at a given index and term.
//
// Assumes the lock is held when called.
func (rf *Raft) isLogConsistent(prevLogIdx int64, prevLogTerm int64) bool {
	lastLogIdx, _ := rf.lastLogIdxAndTerm()
	if prevLogIdx > lastLogIdx {
		return false
	}
	return rf.getTerm(prevLogIdx) == prevLogTerm
}

// fillConflictReply sets the conflict fields in an AppendEntries reply
//
// Assumes the lock is held when called
func (rf *Raft) fillConflictReply(req *raftpb.AppendEntriesRequest, reply *raftpb.AppendEntriesResponse) {
	lastLogIdx, _ := rf.lastLogIdxAndTerm()
	if req.PrevLogIndex > lastLogIdx {
		reply.ConflictIndex = lastLogIdx + 1
		reply.ConflictTerm = -1
	} else {
		reply.ConflictTerm = rf.getTerm(req.PrevLogIndex)
		firstIndexOfTerm := req.PrevLogIndex
		for firstIndexOfTerm > rf.lastIncludedIndex+1 && rf.getTerm(firstIndexOfTerm-1) == reply.ConflictTerm {
			firstIndexOfTerm--
		}
		reply.ConflictIndex = firstIndexOfTerm
	}
}

// startElection begins a new election
func (rf *Raft) startElection() {
	timeout := randElectionIntervalMs()

	rf.mu.Lock()
	rf.curTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimer()
	lastLogIdx, lastLogTerm := rf.lastLogIdxAndTerm()
	currentTerm := rf.curTerm

	rf.persistAndUnlock(nil)

	repliesChan := make(chan *raftpb.RequestVoteResponse, len(rf.peers)-1)
	args := &raftpb.RequestVoteRequest{
		Term:         currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIdx,
		LastLogTerm:  lastLogTerm,
	}
	for i := range rf.peers {
		if i == int(rf.me) {
			continue
		}
		go func(idx int) {
			reply, err := rf.sendRequestVoteRPC(idx, args)
			if err != nil {
				// TODO: better handling
				log.Printf("failed to get response from server #%d", idx)
				return
			}
			repliesChan <- reply
		}(i)
	}

	rf.countVotes(timeout, repliesChan)
}

func (rf *Raft) countVotes(timeout time.Duration, repliesChan <-chan *raftpb.RequestVoteResponse) {
	votes := make([]bool, len(rf.peers))
	votes[rf.me] = true

	for {
		select {
		case <-time.After(timeout):
			return
		case reply := <-repliesChan:
			rf.mu.Lock()
			if reply.Term > rf.curTerm {
				rf.becomeFollower(reply.Term)
				rf.resetElectionTimer()
				rf.mu.Unlock()
				return
			} else if reply.VoteGranted && rf.isState(candidate) {
				votes[reply.VoterId] = true
				if rf.isEnoughVotes(votes) {
					rf.becomeLeader()
					rf.mu.Unlock()
					rf.sendAppendEntries()
					return
				}
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) isEnoughVotes(votes []bool) bool {
	var vc int
	for _, voted := range votes {
		if voted {
			vc++
		}
	}
	return vc > len(rf.peers)/2
}

func (rf *Raft) sendAppendEntries() {
	rf.mu.RLock()
	curTerm := rf.curTerm
	rf.mu.RUnlock()

	for i := range rf.peers {
		if int64(i) == rf.me {
			continue
		}
		go func(peerIdx int) {
			rf.mu.RLock()
			if rf.curTerm != curTerm || !rf.isState(leader) {
				rf.mu.RUnlock()
				return
			}

			if rf.nextIdx[peerIdx] <= rf.lastIncludedIndex {
				rf.leaderSendSnapshot(peerIdx)
			} else {
				rf.leaderSendEntries(peerIdx)
			}
		}(i)
	}
}

// leaderSendSnapshot handles sending a snapshot to a single peer
//
// Assumes the lock is held when called
func (rf *Raft) leaderSendSnapshot(peerIdx int) {
	req := &raftpb.InstallSnapshotRequest{
		Term:              rf.curTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.RUnlock()

	reply, err := rf.sendInstallSnapshotRPC(peerIdx, req)
	if err != nil {
		// TODO: better handling
		log.Printf("failed to send InstallSnapshot to server #%d: %v", peerIdx, err)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.curTerm != req.Term {
		return
	}

	if reply.Term > rf.curTerm {
		rf.becomeFollower(reply.Term)
		rf.resetElectionTimer()
		return
	}

	rf.matchIdx[peerIdx] = max(rf.matchIdx[peerIdx], req.LastIncludedIndex)
	rf.nextIdx[peerIdx] = rf.matchIdx[peerIdx] + 1
}

// leaderSendEntries handles sending log entries to a single peer
//
// Assumes the lock is held when called
func (rf *Raft) leaderSendEntries(peerIdx int) {
	prevLogIdx := rf.nextIdx[peerIdx] - 1
	prevLogTerm := rf.getTerm(prevLogIdx)

	sliceIndex := rf.nextIdx[peerIdx] - rf.lastIncludedIndex - 1
	entries := make([]*raftpb.LogEntry, len(rf.log[sliceIndex:]))
	copy(entries, rf.log[sliceIndex:])

	args := &raftpb.AppendEntriesRequest{
		Term:              rf.curTerm,
		LeaderId:          rf.me,
		PrevLogIndex:      prevLogIdx,
		PrevLogTerm:       prevLogTerm,
		LeaderCommitIndex: rf.commitIdx,
		Entries:           entries,
	}
	rf.mu.RUnlock()

	reply, err := rf.sendAppendEntriesRPC(peerIdx, args)
	if err != nil {
		// TODO: better handling
		log.Printf("failed to send AppendEntries to server $%d: %v", peerIdx, err)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.curTerm != args.Term {
		return
	}

	rf.handleAppendEntriesReply(peerIdx, args, reply)
}

// handleAppendEntriesReply processes the reply from an AppendEntries RPC
//
// Assumes the lock is held when called
func (rf *Raft) handleAppendEntriesReply(peerIdx int, req *raftpb.AppendEntriesRequest, reply *raftpb.AppendEntriesResponse) {
	if reply.Term > rf.curTerm {
		rf.becomeFollower(reply.Term)
		rf.resetElectionTimer()
		return
	}

	if !rf.isState(leader) || req.Term != rf.curTerm {
		return
	}

	if reply.Success {
		newMatchIdx := req.PrevLogIndex + int64(len(req.Entries))
		if newMatchIdx > rf.matchIdx[peerIdx] {
			rf.matchIdx[peerIdx] = newMatchIdx
		}
		rf.nextIdx[peerIdx] = rf.matchIdx[peerIdx] + 1

		lastCommitIdx := rf.commitIdx
		rf.tryToCommit()
		if rf.commitIdx != lastCommitIdx {
			rf.signalApplier()
		}
		return
	}

	rf.updateNextIndexAfterConflict(peerIdx, reply)
}

// updateNextIndexAfterConflict is a helper function to update a follower's nextIdx
// after a failed AppendEntries RPC
//
// Assumes the lock is held when called.
func (rf *Raft) updateNextIndexAfterConflict(peerIdx int, reply *raftpb.AppendEntriesResponse) {
	if reply.ConflictTerm < 0 {
		rf.nextIdx[peerIdx] = reply.ConflictIndex
		return
	}

	lastLogIdx, _ := rf.lastLogIdxAndTerm()
	for i := lastLogIdx; i > rf.lastIncludedIndex; i-- {
		if rf.getTerm(i) == reply.ConflictTerm {
			rf.nextIdx[peerIdx] = i + 1
			return
		}
	}
	rf.nextIdx[peerIdx] = reply.ConflictIndex
}

func (rf *Raft) tryToCommit() {
	matchIdxCopy := make([]int64, len(rf.matchIdx))
	copy(matchIdxCopy, rf.matchIdx)

	slices.Sort(matchIdxCopy)
	majorityIdx := len(rf.peers) / 2
	newCommitIdx := matchIdxCopy[majorityIdx]

	if newCommitIdx > rf.commitIdx && rf.getTerm(newCommitIdx) == rf.curTerm {
		rf.commitIdx = newCommitIdx
	}
}

// ticker is the main state machine loop for a Raft peer
func (rf *Raft) ticker() {
	defer func() {
		rf.heartbeatTicker.Stop()
		rf.electionTimer.Stop()
		rf.wg.Done()
	}()

	for {
		select {
		case <-rf.raftCtx.Done():
			return
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if !rf.isState(leader) {
				atomic.StoreUint32(&rf.state, candidate)
				go rf.startElection()
			}
			rf.mu.Unlock()
		case <-rf.heartbeatTicker.C:
			if rf.isState(leader) {
				rf.sendAppendEntries()
			}
		}
	}
}

func (rf *Raft) signalApplier() {
	select {
	case rf.signalApplierChan <- struct{}{}:
	default:
	}
}

// applies committed log entries to the state machine in the background
func (rf *Raft) applier() {
	defer func() {
		close(rf.applyChan)
		rf.wg.Done()
	}()

	for {
		select {
		case <-rf.raftCtx.Done():
			return
		case <-rf.signalApplierChan:
			for {
				rf.mu.RLock()
				if rf.lastAppliedIdx >= rf.commitIdx || rf.killed() {
					rf.mu.RUnlock()
					break
				}

				var msg api.ApplyMessage
				if rf.lastAppliedIdx < rf.lastIncludedIndex {
					msg = api.ApplyMessage{
						SnapshotValid: true,
						Snapshot:      rf.persister.ReadSnapshot(),
						SnapshotTerm:  rf.lastIncludedTerm,
						SnapshotIndex: rf.lastIncludedIndex,
					}
				} else {
					applyIdx := rf.lastAppliedIdx + 1
					sliceIdx := applyIdx - rf.lastIncludedIndex - 1
					msg = api.ApplyMessage{
						CommandValid: true,
						Command:      rf.log[sliceIdx].Cmd,
						CommandIndex: applyIdx,
					}
				}
				rf.mu.RUnlock()

				select {
				case <-rf.raftCtx.Done():
					return
				case rf.applyChan <- &msg:
				}

				rf.mu.Lock()
				if msg.SnapshotValid {
					rf.lastAppliedIdx = max(rf.lastAppliedIdx, msg.SnapshotIndex)
				} else {
					rf.lastAppliedIdx = max(rf.lastAppliedIdx, msg.CommandIndex)
				}
				rf.mu.Unlock()
			}
		}
	}
}

// getTerm returns the term of a log entry at a given absolute index.
// It handles cases where the index is part of a snapshot
//
// Assumes the lock is held when called
func (rf *Raft) getTerm(idx int64) int64 {
	if idx == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}

	if idx < rf.lastIncludedIndex {
		return -1
	}

	sliceIndex := idx - rf.lastIncludedIndex - 1
	if sliceIndex >= int64(len(rf.log)) {
		return -1
	}
	return rf.log[sliceIndex].Term
}

// lastLogIdxAndTerm returns the index and term of the last entry in the log
//
// Assumes the lock is held when called
func (rf *Raft) lastLogIdxAndTerm() (lastLogIdx, lastLogTerm int64) {
	if len(rf.log) > 0 {
		lastLogIdx = rf.lastIncludedIndex + int64(len(rf.log))
		lastLogTerm = rf.log[len(rf.log)-1].Term
	} else {
		lastLogIdx = rf.lastIncludedIndex
		lastLogTerm = rf.lastIncludedTerm
	}
	return
}

func (rf *Raft) isState(state State) bool {
	return atomic.LoadUint32(&rf.state) == state
}

// becomeFollower transitions the peer to the follower state and return true if need to persist state
//
// Assumes the lock is held when called
func (rf *Raft) becomeFollower(term int64) (needToPersist bool) {
	atomic.StoreUint32(&rf.state, follower)
	if term > rf.curTerm {
		rf.curTerm = term
		rf.votedFor = votedForNone
		needToPersist = true
	}
	rf.resetElectionTimer()
	return
}

// becomeLeader transitions the peer to the leader state
//
// Assumes the lock is held when called
func (rf *Raft) becomeLeader() {
	atomic.StoreUint32(&rf.state, leader)
	rf.resetHeartbeatTicker()

	lastLogIdx, _ := rf.lastLogIdxAndTerm()
	for i := range rf.peers {
		rf.nextIdx[i] = lastLogIdx + 1
		rf.matchIdx[i] = 0
	}
	rf.matchIdx[rf.me] = lastLogIdx
}

// activateLeaderTimers stops election timer and starts heartbeat ticker
func (rf *Raft) resetHeartbeatTicker() {
	rf.timerMu.Lock()
	defer rf.timerMu.Unlock()
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
	rf.heartbeatTicker.Reset(HeartbeatInterval)
}

// resetElectionTimer stops heartbeat ticker and resets election timer
func (rf *Raft) resetElectionTimer() {
	rf.timerMu.Lock()
	defer rf.timerMu.Unlock()
	rf.heartbeatTicker.Stop()
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
	rf.electionTimer.Reset(randElectionIntervalMs())
}

func randElectionIntervalMs() time.Duration {
	return ElectionTimeoutBase + time.Duration(rand.Int63n(int64(ElectionTimeoutRand)))
}

// Make creates and starts a new Raft peer
func Make(peerAddrs []string, me int64,
	persister *tester.Persister, applyCh chan *api.ApplyMessage) api.Raft {
	rf := &Raft{}
	rf.peers = make([]raftpb.RaftServiceClient, len(peerAddrs))
	rf.persister = persister
	rf.me = me

	rf.raftCtx, rf.raftCancel = context.WithCancel(context.Background())

	rf.signalApplierChan = make(chan struct{}, 1)

	atomic.StoreUint32(&rf.state, follower)
	rf.log = make([]*raftpb.LogEntry, 0)
	rf.applyChan = applyCh

	rf.electionTimer = time.NewTimer(randElectionIntervalMs())
	rf.heartbeatTicker = time.NewTicker(HeartbeatInterval)
	rf.heartbeatTicker.Stop()

	rf.readPersist(persister.ReadRaftState())

	lastLogIdx, _ := rf.lastLogIdxAndTerm()
	rf.nextIdx = make([]int64, len(peerAddrs))
	for i := range rf.nextIdx {
		rf.nextIdx[i] = lastLogIdx + 1
	}
	rf.matchIdx = make([]int64, len(peerAddrs))

	grpcServer := grpc.NewServer()
	raftpb.RegisterRaftServiceServer(grpcServer, rf)

	l, err := net.Listen("tcp", peerAddrs[me])
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	go func() {
		if err := grpcServer.Serve(l); err != nil {
			log.Fatalf("failed to serverr: %v", err)
		}
	}()

	for i, addr := range peerAddrs {
		if i == int(rf.me) {
			continue
		}

		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("failed to connect: %v", err)
		}

		client := raftpb.NewRaftServiceClient(conn)
		rf.peers[i] = client
	}

	rf.wg.Add(2)
	go rf.applier()
	go rf.ticker()

	return rf
}
