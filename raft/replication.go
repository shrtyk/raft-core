package raft

import (
	"log"
	"slices"

	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
)

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
		LeaderId:          int64(rf.me),
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
