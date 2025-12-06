package storage

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/shrtyk/raft-core/api"
	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
	"github.com/shrtyk/raft-core/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func testFsyncConfig() api.FsyncCfg {
	return api.FsyncCfg{
		BatchSize: 10,
		Timeout:   10 * time.Millisecond,
	}
}

// newTestDir creates a new temporary directory for testing.
func newTestDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "wal_test")
	require.NoError(t, err)
	return dir
}

func TestNewWALStorage(t *testing.T) {
	t.Run("creates dir if not exists", func(t *testing.T) {
		dir := filepath.Join(os.TempDir(), "wal_test_new_dir")
		os.RemoveAll(dir)
		defer os.RemoveAll(dir)

		log := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
		ws, err := NewWALStorage(dir, log, testFsyncConfig())
		require.NoError(t, err)
		require.NoError(t, ws.Close())

		_, err = os.Stat(dir)
		require.NoError(t, err, "directory should be created")
	})

	t.Run("loads existing data on startup", func(t *testing.T) {
		dir := newTestDir(t)
		defer os.RemoveAll(dir)

		cfg := testFsyncConfig()
		_, log := logger.NewTestLogger()

		// 1. Create and populate the first WAL instance
		ws1, err := NewWALStorage(dir, log, cfg)
		require.NoError(t, err)

		entries := []*raftpb.LogEntry{{Term: 1, Cmd: []byte("command")}}
		require.NoError(t, ws1.AppendEntries(entries))
		require.NoError(t, ws1.SetMetadata(1, 1))

		// 2. Close the first instance to ensure data is flushed
		require.NoError(t, ws1.Close())

		// 3. Create a new instance pointing to the same directory
		ws2, err := NewWALStorage(dir, log, cfg)
		require.NoError(t, err)
		defer ws2.Close()

		// 4. Verify the data was loaded
		assert.Equal(t, int64(1), ws2.metadata.CurrentTerm)
		assert.Equal(t, int64(1), ws2.metadata.VotedFor)

		stateBytes, err := ws2.ReadRaftState()
		require.NoError(t, err)
		var state raftpb.RaftPersistentState
		require.NoError(t, proto.Unmarshal(stateBytes, &state))
		require.Len(t, state.Log, 1)
		assert.True(t, proto.Equal(entries[0], state.Log[0]))
	})
}

func TestWALStorage_StateOperations(t *testing.T) {
	dir := newTestDir(t)
	defer os.RemoveAll(dir)
	cfg := testFsyncConfig()
	_, log := logger.NewTestLogger()

	ws1, err := NewWALStorage(dir, log, cfg)
	require.NoError(t, err)

	stateBytes, err := ws1.ReadRaftState()
	require.NoError(t, err)
	assert.Nil(t, stateBytes)

	entries := []*raftpb.LogEntry{{Term: 1, Cmd: []byte("entry1")}}
	require.NoError(t, ws1.AppendEntries(entries))
	require.NoError(t, ws1.SetMetadata(2, 5))

	require.NoError(t, ws1.Close()) // Close to flush and restart

	ws2, err := NewWALStorage(dir, log, cfg)
	require.NoError(t, err)
	defer ws2.Close()

	assert.Equal(t, int64(2), ws2.metadata.CurrentTerm)
	assert.Equal(t, int64(5), ws2.metadata.VotedFor)

	stateBytes, err = ws2.ReadRaftState()
	require.NoError(t, err)
	var state raftpb.RaftPersistentState
	require.NoError(t, proto.Unmarshal(stateBytes, &state))
	assert.Equal(t, int64(2), state.CurrentTerm)
	require.Len(t, state.Log, 1)
}

func TestWALStorage_Overwrite(t *testing.T) {
	dir := newTestDir(t)
	defer os.RemoveAll(dir)
	cfg := testFsyncConfig()
	_, log := logger.NewTestLogger()

	ws1, err := NewWALStorage(dir, log, cfg)
	require.NoError(t, err)
	require.NoError(t, ws1.AppendEntries([]*raftpb.LogEntry{{Term: 1, Cmd: []byte("old")}}))
	require.NoError(t, ws1.SetMetadata(1, 1))

	newLog := []*raftpb.LogEntry{{Term: 2, Cmd: []byte("new")}}
	newMeta := api.RaftMetadata{CurrentTerm: 2, VotedFor: 2, LastIncludedIndex: 3, LastIncludedTerm: 2}

	err = ws1.Overwrite(newLog, newMeta)
	require.NoError(t, err)
	require.NoError(t, ws1.Close())

	ws2, err := NewWALStorage(dir, log, cfg)
	require.NoError(t, err)
	defer ws2.Close()

	assert.Equal(t, newMeta.CurrentTerm, ws2.metadata.CurrentTerm)
	assert.Equal(t, newMeta.VotedFor, ws2.metadata.VotedFor)
	assert.Equal(t, newMeta.LastIncludedIndex, ws2.metadata.LastIncludedIndex)
}

func TestWALStorage_Snapshots(t *testing.T) {
	dir := newTestDir(t)
	defer os.RemoveAll(dir)
	cfg := testFsyncConfig()
	_, log := logger.NewTestLogger()

	ws1, err := NewWALStorage(dir, log, cfg)
	require.NoError(t, err)
	require.NoError(t, ws1.AppendEntries([]*raftpb.LogEntry{{Term: 1, Cmd: []byte("cmd1")}}))
	require.NoError(t, ws1.SetMetadata(1, -1))

	snapshotData := []byte("snapshot-data-1")
	state := &raftpb.RaftPersistentState{CurrentTerm: 2, VotedFor: -1, Log: []*raftpb.LogEntry{{Term: 2}}, LastIncludedIndex: 5, LastIncludedTerm: 1}
	stateBytes, err := proto.Marshal(state)
	require.NoError(t, err)

	err = ws1.SaveStateAndSnapshot(stateBytes, snapshotData)
	require.NoError(t, err)
	require.NoError(t, ws1.Close())

	ws2, err := NewWALStorage(dir, log, cfg)
	require.NoError(t, err)
	defer ws2.Close()

	snap, err := ws2.ReadSnapshot()
	require.NoError(t, err)
	assert.Equal(t, snapshotData, snap)
	assert.Equal(t, state.CurrentTerm, ws2.metadata.CurrentTerm)
	assert.Equal(t, state.LastIncludedIndex, ws2.metadata.LastIncludedIndex)
}

func TestWALStorage_Corruption(t *testing.T) {
	cfg := testFsyncConfig()
	_, log := logger.NewTestLogger()

	t.Run("CRC mismatch", func(t *testing.T) {
		dir := newTestDir(t)
		defer os.RemoveAll(dir)

		entry := &raftpb.LogEntry{Term: 1, Cmd: []byte("command")}
		encoded, err := encodeEntry(entry)
		require.NoError(t, err)
		encoded[5]++ // Corrupt the CRC

		walPath := filepath.Join(dir, walFileName)
		require.NoError(t, os.WriteFile(walPath, encoded, 0644))

		_, err = NewWALStorage(dir, log, cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "crc mismatch")
	})

	t.Run("partial write", func(t *testing.T) {
		dir := newTestDir(t)
		defer os.RemoveAll(dir)

		entry := &raftpb.LogEntry{Term: 1, Cmd: []byte("a long entry")}
		encoded, err := encodeEntry(entry)
		require.NoError(t, err)

		walPath := filepath.Join(dir, walFileName)
		require.NoError(t, os.WriteFile(walPath, encoded[:len(encoded)-5], 0644))

		ws, err := NewWALStorage(dir, log, cfg)
		require.NoError(t, err)
		defer ws.Close()

		stateBytes, err := ws.ReadRaftState()
		require.NoError(t, err)
		assert.Nil(t, stateBytes)
	})
}

func TestWALStorage_ConcurrentAppends(t *testing.T) {
	dir := newTestDir(t)
	defer os.RemoveAll(dir)
	cfg := testFsyncConfig()
	_, log := logger.NewTestLogger()

	ws, err := NewWALStorage(dir, log, cfg)
	require.NoError(t, err)

	numAppenders := 50
	appendsPerGoRoutine := 10
	var wg sync.WaitGroup
	wg.Add(numAppenders)

	for i := range numAppenders {
		go func(i int) {
			defer wg.Done()
			for j := range appendsPerGoRoutine {
				entries := []*raftpb.LogEntry{{
					Term: int64(i),
					Cmd:  []byte{byte(j)},
				}}
				err := ws.AppendEntries(entries)
				require.NoError(t, err)
			}
		}(i)
	}

	wg.Wait()
	require.NoError(t, ws.Close())

	reopenedWs, err := NewWALStorage(ws.dir, log, cfg)
	require.NoError(t, err)
	defer reopenedWs.Close()

	stateBytes, err := reopenedWs.ReadRaftState()
	require.NoError(t, err)

	var state raftpb.RaftPersistentState
	require.NoError(t, proto.Unmarshal(stateBytes, &state))
	assert.Len(t, state.Log, numAppenders*appendsPerGoRoutine)
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
