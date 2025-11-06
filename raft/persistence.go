package raft

import (
	"log/slog"

	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
	"github.com/shrtyk/raft-core/pkg/logger"
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
		rf.logger.Error("failed to marshal state", logger.ErrAttr(err))
		return nil
	}

	return b
}

// persistAndUnlock captures the persistent state, locks the persister mutex,
// unlocks the main mutex, and then persists the state
//
// It must be called with rf.mu held, and it will unlock it
func (rf *Raft) persistAndUnlock(snapshot []byte) error {
	state := rf.getPersistentStateBytes()

	rf.pmu.Lock()
	defer rf.pmu.Unlock()
	rf.mu.Unlock()

	if snapshot == nil {
		return rf.persister.SaveRaftState(state)
	}
	return rf.persister.SaveStateAndSnapshot(state, snapshot)
}

// unlockConditionally unlocks the main mutex, and persists the state if needed
//
// It must be called with rf.mu held, and it will unlock it
func (rf *Raft) unlockConditionally(needToPersist bool, snapshot []byte) error {
	if needToPersist {
		if err := rf.persistAndUnlock(snapshot); err != nil {
			return err
		}
	} else {
		rf.mu.Unlock()
	}
	return nil
}

// restoreState restores previously persisted state from data
func (rf *Raft) restoreState(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}

	state := &raftpb.RaftPersistentState{}
	err := proto.Unmarshal(data, state)
	if err != nil {
		rf.logger.Error("failed to unmarshal data into state struct", logger.ErrAttr(err))
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

func (rf *Raft) PersistedStateSize() (int, error) {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) handlePersistenceError(rpcName string, err error) {
	rf.logger.Error(
		"CRITICAL: failed to persist state, shutting down",
		slog.String("rpc", rpcName),
		logger.ErrAttr(err),
	)
	rf.Stop()
}
