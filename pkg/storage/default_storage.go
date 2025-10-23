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

func (p *DefaultStorage) SaveRaftState(state []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return os.WriteFile(p.statePath, state, 0644)
}

func (p *DefaultStorage) ReadRaftState() ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	data, err := os.ReadFile(p.statePath)
	if os.IsNotExist(err) {
		return nil, nil
	}
	return data, err
}

func (p *DefaultStorage) RaftStateSize() (int, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	info, err := os.Stat(p.statePath)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return int(info.Size()), nil
}

func (p *DefaultStorage) SaveSnapshot(snapshot []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return os.WriteFile(p.snapshotPath, snapshot, 0644)
}

func (p *DefaultStorage) ReadSnapshot() ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	data, err := os.ReadFile(p.snapshotPath)
	if os.IsNotExist(err) {
		return nil, nil
	}
	return data, err
}

func (p *DefaultStorage) SaveStateAndSnapshot(state, snapshot []byte) error {
	// TODO: Implement
	return nil
}
