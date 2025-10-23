package storage

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/shrtyk/raft-core/api"
)

const (
	stateFileName    = "state.bin"
	snapshotFileName = "snapshot.bin"
)

var _ api.Persister = (*DefaultStorage)(nil)

// DefaultStorage implements the api.Persister interface using the local filesystem.
// It is safe for concurrent use.
type DefaultStorage struct {
	mu           sync.RWMutex
	statePath    string
	snapshotPath string
}

// NewDefaultStorage creates a new DefaultStorage in the given directory.
func NewDefaultStorage(dir string) (*DefaultStorage, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	return &DefaultStorage{
		statePath:    filepath.Join(dir, stateFileName),
		snapshotPath: filepath.Join(dir, snapshotFileName),
	}, nil
}

func (s *DefaultStorage) SaveRaftState(state []byte) error {
	// TODO: Implement
	return nil
}

func (s *DefaultStorage) ReadRaftState() ([]byte, error) {
	// TODO: Implement
	return nil, nil
}

func (s *DefaultStorage) RaftStateSize() (int, error) {
	// TODO: Implement
	return 0, nil
}

func (s *DefaultStorage) SaveSnapshot(snapshot []byte) error {
	// TODO: Implement
	return nil
}

func (s *DefaultStorage) ReadSnapshot() ([]byte, error) {
	// TODO: Implement
	return nil, nil
}

func (s *DefaultStorage) SaveStateAndSnapshot(state, snapshot []byte) error {
	// TODO: Implement
	return nil
}
