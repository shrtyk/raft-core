package raft

import (
	"errors"
	"fmt"
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
		Log:               rf.log.entries,
		LastIncludedIndex: rf.log.lastIncludedIndex,
		LastIncludedTerm:  rf.log.lastIncludedTerm,
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
	rf.mu.Unlock()
	return rf.persister.SaveStateAndSnapshot(state, snapshot)
}

// persistMetadataAndUnlock captures the current term and votedFor, unlocks the main mutex,
// and then persists only the metadata.
//
// It must be called with rf.mu held, and it will unlock it.
func (rf *Raft) persistMetadataAndUnlock() error {
	term := rf.curTerm
	votedFor := rf.votedFor
	rf.mu.Unlock()
	return rf.persister.SetMetadata(term, votedFor)
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
	rf.log.entries = state.GetLog()
	rf.log.lastIncludedIndex = state.GetLastIncludedIndex()
	rf.log.lastIncludedTerm = state.GetLastIncludedTerm()
	rf.log.logSizeInBytes = rf.log.calculateLogSizeInBytes()

	rf.commitIdx = rf.log.lastIncludedIndex
	rf.lastAppliedIdx = rf.log.lastIncludedIndex
}

func (rf *Raft) PersistedStateSize() (int, error) {
	return rf.persister.RaftStateSize()
}

// handlePersistenceError logs a critical error, sends it to the client on the
// error channel, and gracefully shuts down the Raft node.
func (rf *Raft) handlePersistenceError(rpcName string, err error) {
	rf.errOnce.Do(func() {
		fullErr := fmt.Errorf(
			"CRITICAL: failed to persist state in '%s'. The node's state is now corrupted! Shutting down to prevent further inconsistency. Error: %w",
			rpcName,
			err,
		)
		rf.logger.Error(
			fullErr.Error(),
			slog.String("rpc", rpcName),
			logger.ErrAttr(err),
		)

		serr := rf.Stop()
		fullErr = errors.Join(fullErr, fmt.Errorf("error occurred during gracefull shutdown: %w", serr))

		rf.errChan <- fullErr
		close(rf.errChan)
	})
}
