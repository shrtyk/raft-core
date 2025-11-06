package api

import "context"

// Coordinator provides a client-facing interface to interact with the Raft cluster.
// It automatically handles leader discovery and retries.
type Coordinator interface {
	// Submit proposes a command to the Raft cluster. It finds the leader,
	// submits the command, and retries if the leader changes.
	Submit(ctx context.Context, cmd []byte) (*SubmitResult, error)
	// Read performs a linearizable read from the Raft cluster.
	Read(ctx context.Context, query []byte) ([]byte, error)
}

// SubmitResult holds the result of a successful command submission.
type SubmitResult struct {
	Term     int64
	LogIndex int64
	IsLeader bool
}
