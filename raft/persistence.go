package raft

import (
	"log"

	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
	"google.golang.org/protobuf/proto"
)

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
	if len(data) < 1 { // bootstrap without any state?
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
