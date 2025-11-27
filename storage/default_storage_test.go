package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/shrtyk/raft-core/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDefaultStorage(t *testing.T) {
	dir := t.TempDir()
	_, log := logger.NewTestLogger()
	p, err := NewDefaultStorage(dir, log)
	require.NoError(t, err)
	require.NotNil(t, p)
	assert.DirExists(t, filepath.Join(dir, versionsDirName))
}

func TestSaveAndRead(t *testing.T) {
	dir := t.TempDir()
	_, log := logger.NewTestLogger()
	p, err := NewDefaultStorage(dir, log)
	require.NoError(t, err)

	// 1. Initial read should be empty
	state, err := p.ReadRaftState()
	require.NoError(t, err)
	assert.Nil(t, state)

	snapshot, err := p.ReadSnapshot()
	require.NoError(t, err)
	assert.Nil(t, snapshot)

	size, err := p.RaftStateSize()
	require.NoError(t, err)
	assert.Equal(t, 0, size)

	// 2. Save state and snapshot
	testState := []byte("test-state")
	testSnapshot := []byte("test-snapshot")
	err = p.SaveStateAndSnapshot(testState, testSnapshot)
	require.NoError(t, err)

	// 3. Read them back
	state, err = p.ReadRaftState()
	require.NoError(t, err)
	assert.True(t, bytes.Equal(testState, state))

	size, err = p.RaftStateSize()
	require.NoError(t, err)
	assert.Equal(t, len(testState), size)

	snapshot, err = p.ReadSnapshot()
	require.NoError(t, err)
	assert.True(t, bytes.Equal(testSnapshot, snapshot))
}

func TestSaveRaftState(t *testing.T) {
	dir := t.TempDir()
	_, log := logger.NewTestLogger()
	p, err := NewDefaultStorage(dir, log)
	require.NoError(t, err)

	// 1. Save initial state and snapshot
	initialState := []byte("initial-state")
	initialSnapshot := []byte("initial-snapshot")
	err = p.SaveStateAndSnapshot(initialState, initialSnapshot)
	require.NoError(t, err)

	// 2. Save new state
	newState := []byte("new-state")
	err = p.SaveRaftState(newState)
	require.NoError(t, err)

	// 3. Verify new state and old snapshot
	state, err := p.ReadRaftState()
	require.NoError(t, err)
	assert.True(t, bytes.Equal(newState, state))

	snapshot, err := p.ReadSnapshot()
	require.NoError(t, err)
	assert.True(t, bytes.Equal(initialSnapshot, snapshot))
}

func TestSaveSnapshot(t *testing.T) {
	dir := t.TempDir()
	_, log := logger.NewTestLogger()
	p, err := NewDefaultStorage(dir, log)
	require.NoError(t, err)

	// 1. Saving snapshot before state should fail
	err = p.SaveSnapshot([]byte("should-fail"))
	assert.Error(t, err)

	// 2. Save initial state and snapshot
	initialState := []byte("initial-state")
	initialSnapshot := []byte("initial-snapshot")
	err = p.SaveStateAndSnapshot(initialState, initialSnapshot)
	require.NoError(t, err)

	// 3. Save new snapshot
	newSnapshot := []byte("new-snapshot")
	err = p.SaveSnapshot(newSnapshot)
	require.NoError(t, err)

	// 4. Verify old state and new snapshot
	state, err := p.ReadRaftState()
	require.NoError(t, err)
	assert.True(t, bytes.Equal(initialState, state))

	snapshot, err := p.ReadSnapshot()
	require.NoError(t, err)
	assert.True(t, bytes.Equal(newSnapshot, snapshot))
}

func TestCleanup(t *testing.T) {
	dir := t.TempDir()
	_, log := logger.NewTestLogger()
	p, err := NewDefaultStorage(dir, log)
	require.NoError(t, err)

	// Create more versions than versionsToKeep
	for range versionsToKeep + 2 {
		err := p.SaveStateAndSnapshot([]byte("state"), nil)
		require.NoError(t, err)
	}
	p.cleanupVersions()

	versions, err := os.ReadDir(filepath.Join(dir, versionsDirName))
	require.NoError(t, err)
	assert.Len(t, versions, versionsToKeep)
}
