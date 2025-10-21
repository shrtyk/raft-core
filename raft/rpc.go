package raft

import (
	"context"
	"fmt"

	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
)

func (rf *Raft) RequestVote(ctx context.Context,
	req *raftpb.RequestVoteRequest) (reply *raftpb.RequestVoteResponse, err error) {
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

func (rf *Raft) AppendEntries(ctx context.Context,
	req *raftpb.AppendEntriesRequest) (reply *raftpb.AppendEntriesResponse, err error) {
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

func (rf *Raft) InstallSnapshot(ctx context.Context,
	req *raftpb.InstallSnapshotRequest) (reply *raftpb.InstallSnapshotResponse, err error) {
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

func (rf *Raft) sendRequestVoteRPC(
	server int,
	req *raftpb.RequestVoteRequest,
) (*raftpb.RequestVoteResponse, error) {
	ctx, cancel := context.WithTimeout(rf.raftCtx, rf.cfg.RPCTimeout)
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
	ctx, cancel := context.WithTimeout(rf.raftCtx, rf.cfg.RPCTimeout)
	defer cancel()

	reply, err := rf.peers[server].AppendEntries(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to call server #%d AppendEntries: %v", server, err)
	}
	return reply, nil
}

func (rf *Raft) sendInstallSnapshotRPC(
	server int,
	args *raftpb.InstallSnapshotRequest,
) (*raftpb.InstallSnapshotResponse, error) {
	ctx, cancel := context.WithTimeout(rf.raftCtx, rf.cfg.RPCTimeout)
	defer cancel()

	reply, err := rf.peers[server].InstallSnapshot(ctx, args)
	if err != nil {
		return nil, fmt.Errorf("failed to call server #%d InstallSnapshot: %v", server, err)
	}
	return reply, nil
}
