package api

import "log/slog"

// NodeBuilder is an interface for constructing a Raft node.
type NodeBuilder interface {
	// Build constructs and returns a new Raft instance based on the
	// configurations provided to the builder. It returns the Raft
	// interface and an error if any required components are missing
	// or if there's an issue during the initialization of default components.
	Build() (Raft, error)

	// WithConfig sets the Raft configuration for the node.
	// If not provided, a DefaultConfig will be used.
	WithConfig(*RaftConfig) NodeBuilder

	// WithPersister sets a custom Persister implementation for the node.
	// If not provided, a DefaultStorage (filesystem-based) will be used.
	WithPersister(Persister) NodeBuilder

	// WithLogger sets a custom slog.Logger for the node.
	// If not provided, a default logger based on the RaftConfig's Log.Env
	// will be used.
	WithLogger(*slog.Logger) NodeBuilder
}
