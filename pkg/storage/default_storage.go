package storage

import (
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/shrtyk/raft-core/api"
)

const (
	stateFileName      = "state.bin"
	snapshotFileName   = "snapshot.bin"
	versionsDirName    = "versions"
	currentSymlinkName = "current"
)

var _ api.Persister = (*DefaultStorage)(nil)

// DefaultStorage implements the api.Persister interface using the local filesystem.
// It uses a directory-swap mechanism with symlinks to ensure atomic updates.
//
// Safe for concurrent use.
type DefaultStorage struct {
	mu       sync.RWMutex
	dir      string
	current  string
	versions string
}

// NewDefaultStorage creates a new DefaultStorage in the given directory.
func NewDefaultStorage(dir string) (*DefaultStorage, error) {
	versionsPath := filepath.Join(dir, versionsDirName)
	if err := os.MkdirAll(versionsPath, 0755); err != nil {
		return nil, err
	}

	return &DefaultStorage{
		dir:      dir,
		current:  filepath.Join(dir, currentSymlinkName),
		versions: versionsPath,
	}, nil
}

// resolvePaths reads the symlink, find the active version directory
// and returns the full paths to the state and snapshot files.
func (p *DefaultStorage) resolvePaths() (statePath, snapshotPath string, err error) {
	link, err := os.Readlink(p.current)
	if err != nil {
		return "", "", err
	}

	versionDir := filepath.Join(p.dir, link)
	return filepath.Join(versionDir, stateFileName), filepath.Join(versionDir, snapshotFileName), nil
}

func (p *DefaultStorage) ReadRaftState() ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	statePath, _, err := p.resolvePaths()
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	data, err := os.ReadFile(statePath)
	if os.IsNotExist(err) {
		return nil, nil
	}
	return data, err
}

func (p *DefaultStorage) RaftStateSize() (int, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	statePath, _, err := p.resolvePaths()
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	info, err := os.Stat(statePath)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return int(info.Size()), nil
}

func (p *DefaultStorage) ReadSnapshot() ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	_, snapshotPath, err := p.resolvePaths()
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(snapshotPath)
	if os.IsNotExist(err) {
		return nil, nil
	}
	return data, err
}

func (p *DefaultStorage) SaveRaftState(state []byte) error {
	currentSnapshot, err := p.ReadSnapshot()
	if err != nil {
		return err
	}
	return p.SaveStateAndSnapshot(state, currentSnapshot)
}

func (p *DefaultStorage) SaveSnapshot(snapshot []byte) error {
	currentState, err := p.ReadRaftState()
	if err != nil {
		return err
	}
	return p.SaveStateAndSnapshot(currentState, snapshot)
}

func (p *DefaultStorage) SaveStateAndSnapshot(state, snapshot []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	versionName := strconv.FormatInt(time.Now().UnixNano(), 10)
	newVersionPath := filepath.Join(p.versions, versionName)
	if err := os.MkdirAll(newVersionPath, 0755); err != nil {
		return err
	}

	statePath := filepath.Join(newVersionPath, stateFileName)
	if err := os.WriteFile(statePath, state, 0644); err != nil {
		os.RemoveAll(newVersionPath)
		return err
	}

	if snapshot != nil {
		snapshotPath := filepath.Join(newVersionPath, snapshotFileName)
		if err := os.WriteFile(snapshotPath, snapshot, 0644); err != nil {
			os.RemoveAll(newVersionPath)
			return err
		}
	}

	tmpSymlinkPath := p.current + ".tmp"
	symlinkTarget := filepath.Join(versionsDirName, versionName)

	if err := os.Symlink(symlinkTarget, tmpSymlinkPath); err != nil {
		os.RemoveAll(newVersionPath)
		return err
	}

	if err := os.Rename(tmpSymlinkPath, p.current); err != nil {
		os.Remove(tmpSymlinkPath)
		os.RemoveAll(newVersionPath)
		return err
	}

	return nil
}
