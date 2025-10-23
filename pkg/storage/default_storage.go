package storage

import (
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/shrtyk/raft-core/api"
	"github.com/shrtyk/raft-core/pkg/logger"
)

const (
	stateFileName      = "state.bin"
	snapshotFileName   = "snapshot.bin"
	versionsDirName    = "versions"
	currentSymlinkName = "current"
	versionsToKeep     = 2
)

var _ api.Persister = (*DefaultStorage)(nil)

// DefaultStorage implements the api.Persister interface using the local filesystem.
// It uses a directory-swap mechanism with symlinks to ensure atomic updates.
//
// Safe for concurrent use.
type DefaultStorage struct {
	mu           sync.RWMutex
	logger       *slog.Logger
	dir          string
	current      string
	versions     string
	versionNames []string
}

// NewDefaultStorage creates a new DefaultStorage in the given directory.
func NewDefaultStorage(dir string, logger *slog.Logger) (*DefaultStorage, error) {
	versionsPath := filepath.Join(dir, versionsDirName)
	if err := os.MkdirAll(versionsPath, 0755); err != nil {
		return nil, err
	}

	// Restores initial versions list from the filesystem.
	versionNames, err := restoreVersionNames(versionsPath)
	if err != nil {
		return nil, err
	}

	return &DefaultStorage{
		logger:       logger,
		dir:          dir,
		current:      filepath.Join(dir, currentSymlinkName),
		versions:     versionsPath,
		versionNames: versionNames,
	}, nil
}

func restoreVersionNames(versionsPath string) ([]string, error) {
	entries, err := os.ReadDir(versionsPath)
	if err != nil {
		return nil, err
	}

	var versionNames []string
	for _, entry := range entries {
		if entry.IsDir() {
			versionNames = append(versionNames, entry.Name())
		}
	}
	sort.Strings(versionNames)
	return versionNames, nil
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
	p.mu.Lock()
	defer p.mu.Unlock()

	var currentSnapshot []byte
	_, snapshotPath, err := p.resolvePaths()
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	currentSnapshot, _ = os.ReadFile(snapshotPath)
	return p.saveStateAndSnapshotUnlocked(state, currentSnapshot)
}

func (p *DefaultStorage) SaveSnapshot(snapshot []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var currentState []byte
	statePath, _, err := p.resolvePaths()
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err == nil {
		currentState, err = os.ReadFile(statePath)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	if currentState == nil {
		return errors.New("cannot save snapshot without existing raft state")
	}

	return p.saveStateAndSnapshotUnlocked(currentState, snapshot)
}

func (p *DefaultStorage) SaveStateAndSnapshot(state, snapshot []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.saveStateAndSnapshotUnlocked(state, snapshot)
}

// saveStateAndSnapshotUnlocked contains the core swap logic
//
// Assumes the lock is held when called
func (p *DefaultStorage) saveStateAndSnapshotUnlocked(state, snapshot []byte) error {
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

	p.versionNames = append(p.versionNames, versionName)
	go p.cleanupVersions()

	return nil
}

func (p *DefaultStorage) cleanupVersions() {
	p.mu.Lock()
	if len(p.versionNames) <= versionsToKeep {
		p.mu.Unlock()
		return
	}

	versionsToDelete := p.versionNames[:len(p.versionNames)-versionsToKeep]
	p.versionNames = p.versionNames[len(p.versionNames)-versionsToKeep:]
	p.mu.Unlock()

	for _, versionName := range versionsToDelete {
		pathToDelete := filepath.Join(p.versions, versionName)
		if err := os.RemoveAll(pathToDelete); err != nil {
			p.logger.Warn(
				"failed to delete outdated version",
				"version", versionName,
				logger.ErrAttr(err),
			)
		}
	}
}
