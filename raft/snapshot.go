package raft

import (
	"fmt"

	"github.com/shrtyk/raft-core/api"
	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
)

func (rf *Raft) Snapshot(index int64, snapshot []byte) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		return api.ErrOldSnapshot
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
	return rf.persister.SaveStateAndSnapshot(data, snapshot)
}

// leaderSendSnapshot handles sending a snapshot to a single peer
//
// Assumes the lock is held when called
func (rf *Raft) leaderSendSnapshot(peerIdx int) error {
	snapshot, err := rf.persister.ReadSnapshot()
	if err != nil {
		return fmt.Errorf("failed to read snapshot: %w", err)
	}

	req := &raftpb.InstallSnapshotRequest{
		Term:              rf.curTerm,
		LeaderId:          int64(rf.me),
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              snapshot,
	}
	rf.mu.RUnlock()

	reply, err := rf.transport.SendInstallSnapshot(rf.raftCtx, peerIdx, req)
	if err != nil {
		return fmt.Errorf("failed to send InstallSnapshot to peer #%d: %v", peerIdx, err)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if err := rf.checkOrUpdateTerm("InstallSnapshot", peerIdx, req.Term, reply.Term); err != nil {
		return err
	}

	rf.matchIdx[peerIdx] = max(rf.matchIdx[peerIdx], req.LastIncludedIndex)
	rf.nextIdx[peerIdx] = rf.matchIdx[peerIdx] + 1
	return nil
}
