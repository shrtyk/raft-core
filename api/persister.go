package api

type Persister interface {
	// SaveRaftState persists the Raft state (current term, vote, and log entries).
	SaveRaftState(state []byte) error

	// SaveSnapshot persists the snapshot.
	SaveSnapshot(snapshot []byte) error

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
}
