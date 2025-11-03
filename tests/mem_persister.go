package testsim

import (
	"github.com/shrtyk/raft-core/tests/harness"
)

// MemPersister is an in-memory persister that implements the api.Persister interface.
// It uses the harness.Persister for storage.
type MemPersister struct {
	p *harness.Persister
}

func NewMemPersister() *MemPersister {
	return &MemPersister{
		p: harness.MakePersister(),
	}
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
