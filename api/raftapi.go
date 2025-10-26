package api

import "errors"

var (
	ErrOutdatedTerm = errors.New("raft: term has been updated.")
	ErrHigherTerm   = errors.New("raft: recieved higher term in reply.")
	ErrOldSnapshot  = errors.New("raft: snapshot index is not newer than the last included index.")
)

// Raft defines the public interface exposed by a single Raft peer.
// It allows higher-level services to propose commands, query leadership state,
// and manage snapshots and lifecycle events.
type Raft interface {
	// Propose submits a new command to the Raft cluster for replication.
	//
	// Returns:
	//   - index: the log index assigned to this command (if accepted)
	//   - term:  the current term at the time of submission
	//   - isLeader: true if this peer believes it is the current leader
	//
	// If isLeader is false, the command was not accepted and should be redirected
	// to leader.
	//
	// This is non blocking call.
	Submit(command []byte) (index int64, term int64, isLeader bool)

	// State returns the current term and whether this peer believes it is the leader.
	State() (int64, bool)

	// Snapshot informs Raft that the service has created a snapshot
	// that replaces all log entries up through the given index.
	Snapshot(index int64, snapshot []byte) error

	// PersistedSize returns the size in bytes of the persisted Raft state.
	// Typically used by tests.
	PersistedStateSize() (int, error)

	// Start starts all background processes of the Raft peer.
	// It should be called after the Raft instance is created.
	Start() error

	// Stop gracefully terminates the Raft instance, closing all background
	// goroutines and network connections.
	Stop() error

	// Killed returns true if peers has been stoped.
	// Typically used by tests.
	Killed() bool
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
