package api

import "context"

// FSM represents the application state managed by Raft.
// It defines how committed log entries are applied, and how the state can be
// snapshotted and restored.
// The application using Raft must implement this interface.
type FSM interface {
	Start(ctx context.Context)         // Run a long-lived goroutine consuming ApplyMessages
	Snapshot() ([]byte, error)         // Serialize the current application state
	Restore(snapshot []byte) error     // Restore application state from snapshot
	Read(query []byte) ([]byte, error) // Read data from the state machine
}

type ApplyMessage struct {
	CommandValid bool
	Command      []byte
	CommandIndex int64

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex int64
	SnapshotTerm  int64
}
