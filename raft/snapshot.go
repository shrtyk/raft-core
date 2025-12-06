package raft

import (
	"github.com/shrtyk/raft-core/api"
)

func (rf *Raft) Snapshot(index int64, snapshot []byte) error {
	rf.mu.Lock()

	if index <= rf.log.lastIncludedIndex {
		rf.mu.Unlock()
		return api.ErrOldSnapshot
	}

	term := rf.log.getTerm(index)
	rf.log.compact(index, term)

	data := rf.getPersistentStateBytes()

	rf.mu.Unlock()

	return rf.persister.SaveStateAndSnapshot(data, snapshot)
}
