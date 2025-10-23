package raft

import (
	"fmt"
	"slices"

	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
)

// leaderSendEntries handles sending log entries to a single peer
//
// Assumes the lock is held when called
func (rf *Raft) leaderSendEntries(peerIdx int) error {
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
		return fmt.Errorf("failed to send AppendEntries to peer #%d: %w", peerIdx, err)
	}

	return rf.processAppendEntriesReply(peerIdx, args, reply)
}

// processAppendEntriesReply processes the reply from an AppendEntries RPC
func (rf *Raft) processAppendEntriesReply(peerIdx int, req *raftpb.AppendEntriesRequest, reply *raftpb.AppendEntriesResponse) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if err := rf.checkOrUpdateTerm("AppendEntries", peerIdx, req.Term, reply.Term); err != nil {
		return err
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
		return nil
	}

	rf.updateNextIndexAfterConflict(peerIdx, reply)
	return nil
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

// tryToCommit updating leader commit index
// if majority of peers got higher commit index
//
// Assumes the lock is held when called.
func (rf *Raft) tryToCommit() {
	matchIdxCopy := make([]int64, len(rf.matchIdx))
	copy(matchIdxCopy, rf.matchIdx)

	slices.Sort(matchIdxCopy)
	majorityIdx := len(rf.peers) / 2
	newCommitIdx := matchIdxCopy[majorityIdx]

	if newCommitIdx > rf.commitIdx && rf.getTerm(newCommitIdx) == rf.curTerm {
		rf.logger.Debug(
			"advancing commit index",
			"old_commit_idx", rf.commitIdx,
			"new_commit_idx", newCommitIdx,
		)
		rf.commitIdx = newCommitIdx
	}
}
