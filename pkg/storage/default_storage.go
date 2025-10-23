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
	p.mu.Lock()
	defer p.mu.Unlock()

	stateTmpPath := p.statePath + ".tmp"
	snapshotTmpPath := p.snapshotPath + ".tmp"

	if err := os.WriteFile(stateTmpPath, state, 0644); err != nil {
		return err
	}

	if err := os.WriteFile(snapshotTmpPath, snapshot, 0644); err != nil {
		os.Remove(stateTmpPath)
		return err
	}

	// TODO: implement more more robust way of handling following part
	// Atomically rename files.
	// There is a small window of inconsistency if a crash occurs between the two renames.

	if err := os.Rename(stateTmpPath, p.statePath); err != nil {
		os.Remove(stateTmpPath)
		os.Remove(snapshotTmpPath)
		return err
	}

	if err := os.Rename(snapshotTmpPath, p.snapshotPath); err != nil {
		// At this point, state is new but snapshot is old. This is the inconsistent state.
		return err
	}

	return nil
}
