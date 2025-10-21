package raft

import (
	"log"

	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
)

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

// leaderSendSnapshot handles sending a snapshot to a single peer
//
// Assumes the lock is held when called
func (rf *Raft) leaderSendSnapshot(peerIdx int) {
	req := &raftpb.InstallSnapshotRequest{
		Term:              rf.curTerm,
		LeaderId:          int64(rf.me),
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
