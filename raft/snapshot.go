package raft

import (
	"github.com/shrtyk/raft-core/api"
	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
)

func (rf *Raft) Snapshot(index int64, snapshot []byte) error {
	rf.mu.Lock()

	if index <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return api.ErrOldSnapshot
	}

	term := rf.getTerm(index)
	sliceIndex := index - rf.lastIncludedIndex
	if sliceIndex < int64(len(rf.log)) {
		rf.log = append([]*raftpb.LogEntry(nil), rf.log[sliceIndex:]...)
	} else {
		rf.log = nil
	}

	rf.logSizeInBytes = rf.calculateLogSizeInBytes()

	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = term

	data := rf.getPersistentStateBytes()

	rf.mu.Unlock()

	return rf.persister.SaveStateAndSnapshot(data, snapshot)
}
