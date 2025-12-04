package storage

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/shrtyk/raft-core/api"
	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
	"github.com/shrtyk/raft-core/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// newTestWAL creates a new WALStorage in a temporary directory for testing.
// It returns the storage instance, the directory path, and a cleanup function.
func newTestWAL(t *testing.T) (*WALStorage, string, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "wal_test")
	require.NoError(t, err)

	// Discard logger output for cleaner test logs
	_, logger := logger.NewTestLogger()
	ws, err := NewWALStorage(dir, logger)
	require.NoError(t, err)

	cleanup := func() {
		os.RemoveAll(dir)
	}

	return ws, dir, cleanup
}

func TestNewWALStorage(t *testing.T) {
	t.Run("creates dir if not exists", func(t *testing.T) {
		dir := filepath.Join(os.TempDir(), "wal_test_new_dir")
		os.RemoveAll(dir)

		logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
		_, err := NewWALStorage(dir, logger)
		require.NoError(t, err)
		defer os.RemoveAll(dir)

		_, err = os.Stat(dir)
		require.NoError(t, err, "directory should be created")
	})

	t.Run("loads existing data on startup", func(t *testing.T) {
		ws, dir, cleanup := newTestWAL(t)
		defer cleanup()

		// 1. Add data to the first WAL instance
		entries := []*raftpb.LogEntry{{Term: 1, Cmd: []byte("command")}}
		require.NoError(t, ws.AppendEntries(entries))
		require.NoError(t, ws.SetMetadata(1, 1))

		// 2. Create a new instance pointing to the same directory
		logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
		ws2, err := NewWALStorage(dir, logger)
		require.NoError(t, err)

		// 3. Verify the data was loaded
		assert.Equal(t, int64(1), ws2.metadata.CurrentTerm)
		assert.Equal(t, int64(1), ws2.metadata.VotedFor)
		require.Len(t, ws2.log, 1)
		assert.True(t, proto.Equal(entries[0], ws2.log[0]))
	})
}

func TestWALStorage_StateOperations(t *testing.T) {
	ws, _, cleanup := newTestWAL(t)
	defer cleanup()

	stateBytes, err := ws.ReadRaftState()
	require.NoError(t, err)
	assert.Nil(t, stateBytes)
	size, err := ws.RaftStateSize()
	require.NoError(t, err)
	assert.Equal(t, 0, size)

	entries := []*raftpb.LogEntry{
		{Term: 1, Cmd: []byte("entry1")},
		{Term: 1, Cmd: []byte("entry2")},
		{Term: 2, Cmd: nil}, // no-op entry
	}
	err = ws.AppendEntries(entries)
	require.NoError(t, err)

	// Set metadata
	err = ws.SetMetadata(2, 5)
	require.NoError(t, err)

	// Check in-memory state
	assert.Len(t, ws.log, 3)
	assert.Equal(t, int64(2), ws.metadata.CurrentTerm)
	assert.Equal(t, int64(5), ws.metadata.VotedFor)

	// Check ReadRaftState
	stateBytes, err = ws.ReadRaftState()
	require.NoError(t, err)
	assert.NotNil(t, stateBytes)

	var state raftpb.RaftPersistentState
	err = proto.Unmarshal(stateBytes, &state)
	require.NoError(t, err)

	assert.Equal(t, int64(2), state.CurrentTerm)
	assert.Equal(t, int64(5), state.VotedFor)
	require.Len(t, state.Log, 3)
	assert.True(t, proto.Equal(entries[0], state.Log[0]))
	assert.True(t, proto.Equal(entries[1], state.Log[1]))
	assert.True(t, proto.Equal(entries[2], state.Log[2]))

	// Check RaftStateSize
	size, err = ws.RaftStateSize()
	require.NoError(t, err)
	assert.Equal(t, len(stateBytes), size)
}

func TestWALStorage_Overwrite_And_SaveRaftState(t *testing.T) {
	ws, dir, cleanup := newTestWAL(t)
	defer cleanup()

	require.NoError(t, ws.AppendEntries([]*raftpb.LogEntry{{Term: 1, Cmd: []byte("old")}}))
	require.NoError(t, ws.SetMetadata(1, 1))

	// Define the new state to be written
	newLog := []*raftpb.LogEntry{
		{Term: 2, Cmd: []byte("new")},
	}
	newMeta := api.RaftMetadata{
		CurrentTerm:       2,
		VotedFor:          2,
		LastIncludedIndex: 3,
		LastIncludedTerm:  2,
	}

	t.Run("Overwrite", func(t *testing.T) {
		err := ws.Overwrite(newLog, newMeta)
		require.NoError(t, err)

		// Verify in-memory state
		assert.Equal(t, newMeta.CurrentTerm, ws.metadata.CurrentTerm)
		assert.Equal(t, newMeta.VotedFor, ws.metadata.VotedFor)
		assert.Equal(t, newMeta.LastIncludedIndex, ws.metadata.LastIncludedIndex)
		require.Len(t, ws.log, 1)
		assert.True(t, proto.Equal(newLog[0], ws.log[0]))

		// Verify persisted state by reloading
		logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
		reloadedWS, err := NewWALStorage(dir, logger)
		require.NoError(t, err)
		assert.Equal(t, newMeta.CurrentTerm, reloadedWS.metadata.CurrentTerm)
		require.Len(t, reloadedWS.log, 1)
		assert.True(t, proto.Equal(newLog[0], reloadedWS.log[0]))
	})

	t.Run("SaveRaftState", func(t *testing.T) {
		// This should overwrite the previous state
		finalLog := []*raftpb.LogEntry{{Term: 3, Cmd: []byte("final")}}
		finalState := &raftpb.RaftPersistentState{
			CurrentTerm: 3,
			VotedFor:    3,
			Log:         finalLog,
		}
		stateBytes, err := proto.Marshal(finalState)
		require.NoError(t, err)

		err = ws.SaveRaftState(stateBytes)
		require.NoError(t, err)

		// Verify in-memory state
		assert.Equal(t, finalState.CurrentTerm, ws.metadata.CurrentTerm)
		require.Len(t, ws.log, 1)
		assert.True(t, proto.Equal(finalLog[0], ws.log[0]))

		// Verify persisted state
		logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
		reloadedWS, err := NewWALStorage(dir, logger)
		require.NoError(t, err)
		assert.Equal(t, finalState.CurrentTerm, reloadedWS.metadata.CurrentTerm)
		require.Len(t, reloadedWS.log, 1)
	})
}

func TestWALStorage_Snapshots(t *testing.T) {
	ws, dir, cleanup := newTestWAL(t)
	defer cleanup()
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))

	require.NoError(t, ws.AppendEntries([]*raftpb.LogEntry{{Term: 1, Cmd: []byte("cmd1")}}))
	require.NoError(t, ws.SetMetadata(1, -1))

	snapshotData := []byte("snapshot-data-1")
	// The new raft state post-snapshot
	state := &raftpb.RaftPersistentState{
		CurrentTerm:       2,
		VotedFor:          -1,
		Log:               []*raftpb.LogEntry{{Term: 2}}, // Log is truncated
		LastIncludedIndex: 5,
		LastIncludedTerm:  1,
	}
	stateBytes, err := proto.Marshal(state)
	require.NoError(t, err)

	t.Run("SaveStateAndSnapshot", func(t *testing.T) {
		err := ws.SaveStateAndSnapshot(stateBytes, snapshotData)
		require.NoError(t, err)

		// Verify in-memory state
		snap, err := ws.ReadSnapshot()
		require.NoError(t, err)
		assert.Equal(t, snapshotData, snap)
		assert.Equal(t, state.CurrentTerm, ws.metadata.CurrentTerm)
		assert.Equal(t, state.LastIncludedIndex, ws.metadata.LastIncludedIndex)
		require.Len(t, ws.log, 1)

		// Verify persisted state
		reloadedWS, err := NewWALStorage(dir, logger)
		require.NoError(t, err)

		reloadedSnap, err := reloadedWS.ReadSnapshot()
		require.NoError(t, err)
		assert.Equal(t, snapshotData, reloadedSnap)
		assert.Equal(t, state.CurrentTerm, reloadedWS.metadata.CurrentTerm)
		require.Len(t, reloadedWS.log, 1)
	})

	t.Run("SaveSnapshot compatibility", func(t *testing.T) {
		// Setup new state
		newLog := []*raftpb.LogEntry{{Term: 3, Cmd: []byte("another-cmd")}}
		newMeta := api.RaftMetadata{CurrentTerm: 3, VotedFor: 3}
		require.NoError(t, ws.Overwrite(newLog, newMeta))

		snapshotData2 := []byte("snapshot-data-2")

		err := ws.SaveSnapshot(snapshotData2)
		require.NoError(t, err)

		// Verify persisted state
		reloadedWS, err := NewWALStorage(dir, logger)
		require.NoError(t, err)
		reloadedSnap, err := reloadedWS.ReadSnapshot()
		require.NoError(t, err)

		assert.Equal(t, snapshotData2, reloadedSnap)
		assert.Equal(t, newMeta.CurrentTerm, reloadedWS.metadata.CurrentTerm)
		require.Len(t, reloadedWS.log, 1)
		assert.True(t, proto.Equal(newLog[0], reloadedWS.log[0]))
	})
}

func TestWALStorage_Corruption(t *testing.T) {
	t.Run("CRC mismatch", func(t *testing.T) {
		_, dir, cleanup := newTestWAL(t)
		defer cleanup()

		entry := &raftpb.LogEntry{Term: 1, Cmd: []byte("command")}
		encoded, err := encodeEntry(entry)
		require.NoError(t, err)

		// Corrupt the CRC
		encoded[5]++

		// Write the corrupted entry directly
		walPath := filepath.Join(dir, walFileName)
		require.NoError(t, os.WriteFile(walPath, encoded, 0644))

		// Try to load it
		logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
		_, err = NewWALStorage(dir, logger)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "crc mismatch")
	})

	t.Run("partial write", func(t *testing.T) {
		_, dir, cleanup := newTestWAL(t)
		defer cleanup()

		entry := &raftpb.LogEntry{Term: 1, Cmd: []byte("a long entry")}
		encoded, err := encodeEntry(entry)
		require.NoError(t, err)

		// Write only part of the entry
		walPath := filepath.Join(dir, walFileName)
		require.NoError(t, os.WriteFile(walPath, encoded[:len(encoded)-5], 0644))

		// Should not return an error on load, but log should be empty
		logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
		ws2, err := NewWALStorage(dir, logger)
		require.NoError(t, err)
		assert.Empty(t, ws2.log, "log should be empty after partial write")
	})
}

func TestEncodeDecodeEntry(t *testing.T) {
	entry := &raftpb.LogEntry{
		Term: 1,
		Cmd:  []byte("some data"),
	}

	encoded, err := encodeEntry(entry)
	require.NoError(t, err)

	decoded, err := decodeEntry(bytes.NewReader(encoded))
	require.NoError(t, err)
	assert.True(t, proto.Equal(entry, decoded))
}
