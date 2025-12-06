package testsim

import (
	"github.com/shrtyk/raft-core/api"
	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
	"github.com/shrtyk/raft-core/tests/harness"
	"google.golang.org/protobuf/proto"
)

// MemPersister is an in-memory persister that implements the api.Persister interface.
// It uses the harness.Persister for storage.
type MemPersister struct {
	p *harness.Persister
}

var _ api.Persister = (*MemPersister)(nil)

func NewMemPersister() *MemPersister {
	return &MemPersister{
		p: harness.MakePersister(),
	}
}

// readState is a helper to read and unmarshal the current Raft state.
func (ps *MemPersister) readState() (*raftpb.RaftPersistentState, error) {
	stateBytes := ps.p.ReadRaftState()
	if len(stateBytes) == 0 {
		return &raftpb.RaftPersistentState{}, nil
	}
	state := &raftpb.RaftPersistentState{}
	if err := proto.Unmarshal(stateBytes, state); err != nil {
		return nil, err
	}
	return state, nil
}

// save is a helper to marshal and save the Raft state, preserving the snapshot.
func (ps *MemPersister) save(state *raftpb.RaftPersistentState) error {
	stateBytes, err := proto.Marshal(state)
	if err != nil {
		return err
	}
	ps.p.Save(stateBytes, ps.p.ReadSnapshot())
	return nil
}

func (ps *MemPersister) SaveRaftState(state []byte) error {
	ps.p.Save(state, ps.p.ReadSnapshot())
	return nil
}

func (ps *MemPersister) SaveSnapshot(snapshot []byte) error {
	ps.p.Save(ps.p.ReadRaftState(), snapshot)
	return nil
}

func (ps *MemPersister) SaveStateAndSnapshot(state, snapshot []byte) error {
	if snapshot == nil {
		snapshot = ps.p.ReadSnapshot()
	}
	ps.p.Save(state, snapshot)
	return nil
}

func (ps *MemPersister) ReadRaftState() ([]byte, error) {
	return ps.p.ReadRaftState(), nil
}

func (ps *MemPersister) ReadSnapshot() ([]byte, error) {
	return ps.p.ReadSnapshot(), nil
}

func (ps *MemPersister) RaftStateSize() (int, error) {
	return ps.p.RaftStateSize(), nil
}

func (ps *MemPersister) AppendEntries(entries []*raftpb.LogEntry) error {
	state, err := ps.readState()
	if err != nil {
		return err
	}
	state.Log = append(state.Log, entries...)
	return ps.save(state)
}

func (ps *MemPersister) SetMetadata(term int64, votedFor int64) error {
	state, err := ps.readState()
	if err != nil {
		return err
	}
	state.CurrentTerm = term
	state.VotedFor = votedFor
	return ps.save(state)
}

func (ps *MemPersister) Close() error {
	return nil
}
