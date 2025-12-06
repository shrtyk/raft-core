package api

import (
	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
)

// RaftMetadata contains the persisted metadata of the Raft algorithm,
// excluding the log entries.
type RaftMetadata struct {
	CurrentTerm       int64
	VotedFor          int64
	LastIncludedIndex int64
	LastIncludedTerm  int64
}

// Persister defines the interface for Raft's persistent storage.
// It combines the methods for managing Raft state, snapshots,
// and granular WAL operations for performance.
type Persister interface {
	// AppendEntries adds a batch of new log entries to the WAL.
	AppendEntries(entries []*raftpb.LogEntry) error

	// SetMetadata updates and persists the term and votedFor information.
	SetMetadata(term int64, votedFor int64) error

	// SaveStateAndSnapshot atomically replaces both the persisted Raft state and snapshot.
	// After a crash, either both new values must be visible or neither.
	SaveStateAndSnapshot(state, snapshot []byte) error

	// ReadRaftState returns the previously persisted Raft state, if any.
	ReadRaftState() ([]byte, error)

	// ReadSnapshot returns the last persisted snapshot, if any.
	ReadSnapshot() ([]byte, error)

	// RaftStateSize returns the size in bytes of the persisted Raft state.
	//
	// This is typically used only in tests.
	RaftStateSize() (int, error)

	// Close releases any underlying resources, like file handles.
	Close() error
}
