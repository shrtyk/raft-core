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
	versionsToKeep     = 2 // TODO configurable
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

// NewDefaultStorage creates a new DefaultStorage
// in the given directory, returning an error if initialization fails.
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
	if err := writeAndSyncFile(statePath, state, 0644); err != nil {
		return errors.Join(err, os.RemoveAll(newVersionPath))
	}

	if snapshot != nil {
		snapshotPath := filepath.Join(newVersionPath, snapshotFileName)
		if err := writeAndSyncFile(snapshotPath, snapshot, 0644); err != nil {
			return errors.Join(err, os.RemoveAll(newVersionPath))
		}
	}

	// Sync the new version directory to make sure file entries are durable.
	if err := syncDir(newVersionPath); err != nil {
		return errors.Join(err, os.RemoveAll(newVersionPath))
	}

	tmpSymlinkPath := p.current + ".tmp"
	symlinkTarget := filepath.Join(versionsDirName, versionName)

	// Try to remove any temporary symlink from a previous failed operation.
	if err := os.Remove(tmpSymlinkPath); err != nil && !os.IsNotExist(err) {
		return errors.Join(err, os.RemoveAll(newVersionPath))

	}

	if err := os.Symlink(symlinkTarget, tmpSymlinkPath); err != nil {
		return errors.Join(err, os.RemoveAll(newVersionPath))
	}

	// Sync the parent directory to make the temporary symlink durable.
	if err := syncDir(p.dir); err != nil {
		return errors.Join(err, os.RemoveAll(newVersionPath), os.Remove(tmpSymlinkPath))
	}

	if err := os.Rename(tmpSymlinkPath, p.current); err != nil {
		return errors.Join(err, os.RemoveAll(newVersionPath), os.Remove(tmpSymlinkPath))
	}

	// Sync the parent directory again to make the rename durable.
	if err := syncDir(p.dir); err != nil {
		// The state is updated, but not guaranteed to be durable.
		p.logger.Warn("failed to sync directory after rename", logger.ErrAttr(err))
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

// writeAndSyncFile opens or creates a file, writes data to it
// and calls Sync to ensure the data is flushed to stable storage.
func writeAndSyncFile(filename string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	return err
}

// syncDir opens a directory and calls Sync to ensure its metadata is flushed to stable storage.
func syncDir(dir string) (err error) {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}

	defer func() {
		if cerr := f.Close(); cerr != nil {
			if err != nil {
				err = errors.Join(err, cerr)
			} else {
				err = cerr
			}
		}
	}()

	return f.Sync()
}
