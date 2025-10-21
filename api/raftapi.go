package api

// The Raft interface
type Raft interface {
	// Start agreement on a new log entry, and return the log index
	// for that entry, the term, and whether the peer is the leader.
	Start(command []byte) (int64, int64, bool)

	// Ask a Raft for its current term, and whether it thinks it is
	// leader
	GetState() (int64, bool)

	// For Snaphots
	Snapshot(index int64, snapshot []byte)
	PersistBytes() int

	Shutdown()
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
