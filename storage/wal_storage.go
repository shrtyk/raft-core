package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/shrtyk/raft-core/api"
	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
	"github.com/shrtyk/raft-core/pkg/logger"
	"google.golang.org/protobuf/proto"
)

const (
	metadataFileName = "metadata.json"
	walFileName      = "log.wal"
	snapFileName     = "snapshot.bin"
	tmpSuffix        = ".tmp"
)

// |_________________|____________|_______...
// | EntryLen 4bytes | CRC 4bytes | Entry
// |_________________|____________|_______...

const entryHeaderSize = 8 // 4 bytes for length, 4 for CRC

var (
	crc32cTable = crc32.MakeTable(crc32.Castagnoli)
)

// walMetadata represents the data stored in metadata.json
type walMetadata struct {
	CurrentTerm       int64 `json:"current_term"`
	VotedFor          int64 `json:"voted_for"`
	LastIncludedIndex int64 `json:"last_included_index"`
	LastIncludedTerm  int64 `json:"last_included_term"`
}

// WALStorage implements the api.WALPersister interface using a WAL file.
// It is safe for concurrent use.
type WALStorage struct {
	mu     sync.RWMutex
	logger *slog.Logger
	dir    string

	// File paths
	metadataPath string
	walPath      string
	snapshotPath string

	// In-memory state cache
	metadata walMetadata
	log      []*raftpb.LogEntry
	snapshot []byte
}

var _ api.Persister = (*WALStorage)(nil)

// NewWALStorage creates a new WALStorage.
func NewWALStorage(dir string, logger *slog.Logger) (*WALStorage, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory %s: %w", dir, err)
	}

	ws := &WALStorage{
		logger:       logger,
		dir:          dir,
		metadataPath: filepath.Join(dir, metadataFileName),
		walPath:      filepath.Join(dir, walFileName),
		snapshotPath: filepath.Join(dir, snapFileName),
		log:          make([]*raftpb.LogEntry, 0),
	}

	if err := ws.load(); err != nil {
		return nil, fmt.Errorf("failed to load WAL data: %w", err)
	}

	return ws, nil
}

// load reads all persisted data from disk into memory.
func (ws *WALStorage) load() error {
	// Load metadata
	metaData, err := os.ReadFile(ws.metadataPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to read metadata file: %w", err)
	}
	if len(metaData) > 0 {
		if err := json.Unmarshal(metaData, &ws.metadata); err != nil {
			return fmt.Errorf("failed to unmarshal metadata: %w", err)
		}
	}

	// Load WAL entries
	f, err := os.Open(ws.walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No WAL file is not an error, just an empty log
		}
		return fmt.Errorf("failed to open WAL file: %w", err)
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	for {
		entry, err := decodeEntry(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				ws.logger.Warn("unexpected EOF in WAL file, possibly due to a partial write", logger.ErrAttr(err))
				break
			}
			return fmt.Errorf("failed to decode WAL entry: %w", err)
		}
		ws.log = append(ws.log, entry)
	}

	// Load snapshot
	snapData, err := os.ReadFile(ws.snapshotPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to read snapshot file: %w", err)
	}
	if len(snapData) > 0 {
		ws.snapshot = snapData
	}

	return nil
}

func (ws *WALStorage) AppendEntries(entries []*raftpb.LogEntry) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	f, err := os.OpenFile(ws.walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open WAL file for append: %w", err)
	}
	defer f.Close()

	for _, entry := range entries {
		encoded, err := encodeEntry(entry)
		if err != nil {
			return fmt.Errorf("failed to encode entry: %w", err)
		}

		if _, err := f.Write(encoded); err != nil {
			return fmt.Errorf("failed to write to WAL file: %w", err)
		}
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL file: %w", err)
	}

	ws.log = append(ws.log, entries...)
	return nil
}

func (ws *WALStorage) SetMetadata(term int64, votedFor int64) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	newMeta := ws.metadata
	newMeta.CurrentTerm = term
	newMeta.VotedFor = votedFor
	metaBytes, err := json.Marshal(newMeta)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}
	if err := syncFile(ws.metadataPath, metaBytes, 0644); err != nil {
		ws.logger.Warn("failed to sync metadata", logger.ErrAttr(err))
		return fmt.Errorf("failed to sync metadata file: %w", err)
	}

	ws.metadata = newMeta
	return nil
}

func (ws *WALStorage) Overwrite(log []*raftpb.LogEntry, metadata api.RaftMetadata) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	// 1. Prepare new log content
	walBuf := new(bytes.Buffer)
	for _, entry := range log {
		encoded, err := encodeEntry(entry)
		if err != nil {
			return fmt.Errorf("failed to encode entry for overwrite: %w", err)
		}
		if _, err := walBuf.Write(encoded); err != nil {
			return fmt.Errorf("failed to write to buffer for overwrite: %w", err)
		}
	}

	// 2. Prepare new metadata content
	newMeta := walMetadata{
		CurrentTerm:       metadata.CurrentTerm,
		VotedFor:          metadata.VotedFor,
		LastIncludedIndex: metadata.LastIncludedIndex,
		LastIncludedTerm:  metadata.LastIncludedTerm,
	}
	metaBytes, err := json.Marshal(newMeta)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata for overwrite: %w", err)
	}

	// 3. Persist atomically, data first, metadata last (as commit point)
	if err := syncFile(ws.walPath, walBuf.Bytes(), 0644); err != nil {
		return fmt.Errorf("failed to sync WAL file for overwrite: %w", err)
	}
	if err := syncFile(ws.metadataPath, metaBytes, 0644); err != nil {
		return fmt.Errorf("failed to sync metadata file for overwrite: %w", err)
	}

	// 4. Update in-memory state
	ws.log = make([]*raftpb.LogEntry, len(log))
	copy(ws.log, log)
	ws.metadata = newMeta

	return nil
}

func (ws *WALStorage) ReadRaftState() ([]byte, error) {
	ws.mu.RLock()
	defer ws.mu.RUnlock()

	if ws.metadata.CurrentTerm == 0 && ws.metadata.VotedFor == 0 && len(ws.log) == 0 && ws.metadata.LastIncludedIndex == 0 {
		return nil, nil
	}

	state := &raftpb.RaftPersistentState{
		CurrentTerm:       ws.metadata.CurrentTerm,
		VotedFor:          ws.metadata.VotedFor,
		Log:               ws.log,
		LastIncludedIndex: ws.metadata.LastIncludedIndex,
		LastIncludedTerm:  ws.metadata.LastIncludedTerm,
	}

	return proto.Marshal(state)
}

func (ws *WALStorage) RaftStateSize() (int, error) {
	stateBytes, err := ws.ReadRaftState()
	if err != nil {
		return 0, err
	}
	return len(stateBytes), nil
}

func (ws *WALStorage) ReadSnapshot() ([]byte, error) {
	ws.mu.RLock()
	defer ws.mu.RUnlock()
	if len(ws.snapshot) == 0 {
		return nil, nil
	}
	snap := make([]byte, len(ws.snapshot))
	copy(snap, ws.snapshot)
	return snap, nil
}

// SaveRaftState is the compatibility method. It performs a full overwrite.
func (ws *WALStorage) SaveRaftState(state []byte) error {
	if state == nil {
		return nil // Nothing to save
	}

	ps := &raftpb.RaftPersistentState{}
	if err := proto.Unmarshal(state, ps); err != nil {
		return fmt.Errorf("failed to unmarshal state for overwrite: %w", err)
	}

	metadata := api.RaftMetadata{
		CurrentTerm:       ps.CurrentTerm,
		VotedFor:          ps.VotedFor,
		LastIncludedIndex: ps.LastIncludedIndex,
		LastIncludedTerm:  ps.LastIncludedTerm,
	}

	return ws.Overwrite(ps.Log, metadata)
}

// SaveSnapshot requires the full raft state to be consistent.
func (ws *WALStorage) SaveSnapshot(snapshot []byte) error {
	ws.mu.RLock()
	state := &raftpb.RaftPersistentState{
		CurrentTerm:       ws.metadata.CurrentTerm,
		VotedFor:          ws.metadata.VotedFor,
		Log:               ws.log,
		LastIncludedIndex: ws.metadata.LastIncludedIndex,
		LastIncludedTerm:  ws.metadata.LastIncludedTerm,
	}
	ws.mu.RUnlock()

	stateBytes, err := proto.Marshal(state)
	if err != nil {
		return err
	}

	return ws.SaveStateAndSnapshot(stateBytes, snapshot)
}

func (ws *WALStorage) SaveStateAndSnapshot(state, snapshot []byte) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	if state == nil {
		return errors.New("cannot save snapshot with nil raft state")
	}

	ps := &raftpb.RaftPersistentState{}
	if err := proto.Unmarshal(state, ps); err != nil {
		return fmt.Errorf("failed to unmarshal state for snapshot: %w", err)
	}

	// 1. Prepare new log content (it's the post-snapshot log)
	walBuf := new(bytes.Buffer)
	for _, entry := range ps.Log {
		encoded, err := encodeEntry(entry)
		if err != nil {
			return fmt.Errorf("failed to encode entry for snapshot overwrite: %w", err)
		}
		walBuf.Write(encoded)
	}

	// 2. Prepare new metadata content
	newMeta := walMetadata{
		CurrentTerm:       ps.CurrentTerm,
		VotedFor:          ps.VotedFor,
		LastIncludedIndex: ps.LastIncludedIndex,
		LastIncludedTerm:  ps.LastIncludedTerm,
	}
	metaBytes, err := json.Marshal(newMeta)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata for snapshot: %w", err)
	}

	// --- Persist new state atomically ---
	// Write snapshot first, as it's the largest.
	if err := syncFile(ws.snapshotPath, snapshot, 0644); err != nil {
		return fmt.Errorf("failed to sync snapshot file: %w", err)
	}
	// Then WAL.
	if err := syncFile(ws.walPath, walBuf.Bytes(), 0644); err != nil {
		return fmt.Errorf("failed to sync WAL file for snapshot: %w", err)
	}
	// Finally, the metadata, which acts as the commit point for the new state.
	if err := syncFile(ws.metadataPath, metaBytes, 0644); err != nil {
		return fmt.Errorf("failed to sync metadata file for snapshot: %w", err)
	}

	// Update in-memory state
	ws.snapshot = snapshot
	ws.metadata = newMeta
	ws.log = make([]*raftpb.LogEntry, len(ps.Log))
	copy(ws.log, ps.Log)

	return nil
}

func encodeEntry(entry *raftpb.LogEntry) ([]byte, error) {
	payload, err := proto.Marshal(entry)
	if err != nil {
		return nil, err
	}

	header := make([]byte, entryHeaderSize)
	binary.BigEndian.PutUint32(header[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[4:8], crc32.Checksum(payload, crc32cTable))

	return append(header, payload...), nil
}

func decodeEntry(r io.Reader) (*raftpb.LogEntry, error) {
	header := make([]byte, entryHeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(header[0:4])
	crc := binary.BigEndian.Uint32(header[4:8])

	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}

	if actualCRC := crc32.Checksum(payload, crc32cTable); actualCRC != crc {
		return nil, fmt.Errorf("crc mismatch: expected %d, got %d", crc, actualCRC)
	}

	entry := &raftpb.LogEntry{}
	if err := proto.Unmarshal(payload, entry); err != nil {
		return nil, fmt.Errorf("failed to unmarshal log entry: %w", err)
	}

	return entry, nil
}

// syncFile writes data to a temporary file, syncs it, and then atomically renames it.
func syncFile(path string, data []byte, perm os.FileMode) error {
	tempPath := path + tmpSuffix
	f, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}

	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tempPath)
		return err
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tempPath)
		return err
	}
	f.Close()

	return os.Rename(tempPath, path)
}
