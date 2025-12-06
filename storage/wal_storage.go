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
	"time"

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

const entryHeaderSize = 8 // 4 bytes for length, 4 for CRC

//  ______________________________________________________________ ...
// | Key+Value length (4 byte) | CRC Hash (4 byte) |   Key+Value   ...
// |___________________________|___________________|______________ ...

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

type opType int

const (
	opAppendEntries opType = iota
	opSetMetadata
	opSaveStateAndSnapshot
)

// persistRequest is a request sent to the persister worker.
type persistRequest struct {
	op      opType
	data    any
	errChan chan error
}

// WALStorage implements the api.Persister interface using a WAL file with a background worker for batching.
// It is safe for concurrent use.
type WALStorage struct {
	mu       sync.RWMutex
	logger   *slog.Logger
	dir      string
	fsyncCfg api.FsyncCfg

	metadataPath string
	walPath      string
	snapshotPath string

	walFile      *os.File
	metadata     walMetadata
	opChan       chan *persistRequest
	shutdownChan chan struct{}
	wg           sync.WaitGroup
}

var _ api.Persister = (*WALStorage)(nil)

// NewWALStorage creates a new WALStorage and starts its background persister worker.
func NewWALStorage(dir string, log *slog.Logger, cfg api.FsyncCfg) (*WALStorage, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory %s: %w", dir, err)
	}

	ws := &WALStorage{
		logger:       log,
		dir:          dir,
		fsyncCfg:     cfg,
		metadataPath: filepath.Join(dir, metadataFileName),
		walPath:      filepath.Join(dir, walFileName),
		snapshotPath: filepath.Join(dir, snapFileName),
		opChan:       make(chan *persistRequest, cfg.BatchSize*2),
		shutdownChan: make(chan struct{}),
	}

	if err := ws.load(); err != nil {
		return nil, fmt.Errorf("failed to load WAL data: %w", err)
	}

	walFile, err := os.OpenFile(ws.walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file %s: %w", ws.walPath, err)
	}
	ws.walFile = walFile

	ws.wg.Add(1)
	go ws.persister()

	return ws, nil
}

// Close gracefully shuts down the persister worker and closes the WAL file.
func (ws *WALStorage) Close() error {
	close(ws.shutdownChan)
	ws.wg.Wait()
	return ws.walFile.Close()
}

// submitRequest sends a request to the persister worker and waits for a response.
func (ws *WALStorage) submitRequest(op opType, data any) error {
	req := &persistRequest{
		op:      op,
		data:    data,
		errChan: make(chan error, 1),
	}
	ws.opChan <- req
	return <-req.errChan
}

// stopTimer safely stops a timer and drains its channel if the stop fails.
// This is the required pattern for reusing a timer.
func stopTimer(t *time.Timer) {
	if !t.Stop() {
		// The timer fired before we could stop it.
		// We must drain the channel to prevent a spurious wakeup.
		select {
		case <-t.C:
		default:
			// The channel was already drained, do nothing.
		}
	}
}

// persister is the background worker that batches and writes to disk.
func (ws *WALStorage) persister() {
	defer ws.wg.Done()
	batch := make([]*persistRequest, 0, ws.fsyncCfg.BatchSize)
	timer := time.NewTimer(ws.fsyncCfg.Timeout)
	stopTimer(timer)

	for {
		select {
		case req := <-ws.opChan:
			if req.op == opAppendEntries {
				batch = append(batch, req)
				if len(batch) == 1 {
					timer.Reset(ws.fsyncCfg.Timeout)
				}
				if len(batch) >= ws.fsyncCfg.BatchSize {
					ws.flush(batch)
					batch = batch[:0]
					stopTimer(timer)
				}
			} else {
				// For non-append ops, flush any pending batch first.
				if len(batch) > 0 {
					ws.flush(batch)
					batch = batch[:0]
					stopTimer(timer)
				}
				ws.handleSyncOp(req)
			}
		case <-timer.C:
			if len(batch) > 0 {
				ws.flush(batch)
				batch = batch[:0]
			}
		case <-ws.shutdownChan:
			if len(batch) > 0 {
				ws.flush(batch)
			}
			return
		}
	}
}

// handleSyncOp handles non-batchable operations.
func (ws *WALStorage) handleSyncOp(req *persistRequest) {
	var err error
	switch req.op {
	case opSetMetadata:
		data := req.data.([2]int64)
		err = ws.setMetadata(data[0], data[1])
	case opSaveStateAndSnapshot:
		data := req.data.([2][]byte)
		err = ws.saveStateAndSnapshot(data[0], data[1])
	default:
		err = fmt.Errorf("unknown op type: %v", req.op)
	}
	req.errChan <- err
}

// flush writes a batch of append requests to disk and fsyncs.
func (ws *WALStorage) flush(batch []*persistRequest) {
	var totalErr error
	for _, req := range batch {
		entries := req.data.([]*raftpb.LogEntry)
		for _, entry := range entries {
			encoded, err := encodeEntry(entry)
			if err != nil {
				totalErr = errors.Join(totalErr, fmt.Errorf("failed to encode entry: %w", err))
				continue
			}
			if _, err := ws.walFile.Write(encoded); err != nil {
				totalErr = errors.Join(totalErr, fmt.Errorf("failed to write to WAL file: %w", err))
			}
		}
	}

	if totalErr == nil {
		if err := ws.walFile.Sync(); err != nil {
			totalErr = fmt.Errorf("failed to sync WAL file: %w", err)
		}
	}

	for _, req := range batch {
		req.errChan <- totalErr
	}
}

// load reads metadata from disk into memory and validates the WAL.
func (ws *WALStorage) load() error {
	metaData, err := os.ReadFile(ws.metadataPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to read metadata file: %w", err)
	}
	if len(metaData) > 0 {
		if err := json.Unmarshal(metaData, &ws.metadata); err != nil {
			return fmt.Errorf("failed to unmarshal metadata: %w", err)
		}
	}

	f, err := os.Open(ws.walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to open WAL file for validation: %w", err)
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	for {
		_, err := decodeEntry(reader)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return fmt.Errorf("failed to decode/validate WAL entry: %w", err)
		}
	}
	return nil
}

func (ws *WALStorage) readLog() ([]*raftpb.LogEntry, error) {
	f, err := os.Open(ws.walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return []*raftpb.LogEntry{}, nil
		}
		return nil, fmt.Errorf("failed to open WAL file for reading: %w", err)
	}
	defer f.Close()

	var log []*raftpb.LogEntry
	reader := bufio.NewReader(f)
	for {
		entry, err := decodeEntry(reader)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, fmt.Errorf("failed to decode WAL entry: %w", err)
		}
		log = append(log, entry)
	}
	return log, nil
}

func (ws *WALStorage) AppendEntries(entries []*raftpb.LogEntry) error {
	return ws.submitRequest(opAppendEntries, entries)
}

func (ws *WALStorage) SetMetadata(term int64, votedFor int64) error {
	return ws.submitRequest(opSetMetadata, [2]int64{term, votedFor})
}

func (ws *WALStorage) setMetadata(term, votedFor int64) error {
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
		return fmt.Errorf("failed to sync metadata file: %w", err)
	}
	ws.metadata = newMeta
	return nil
}

func (ws *WALStorage) ReadRaftState() ([]byte, error) {
	ws.mu.RLock()
	defer ws.mu.RUnlock()

	log, err := ws.readLog()
	if err != nil {
		return nil, fmt.Errorf("failed to read log for ReadRaftState: %w", err)
	}

	if ws.metadata.CurrentTerm == 0 && ws.metadata.VotedFor == 0 && len(log) == 0 && ws.metadata.LastIncludedIndex == 0 {
		return nil, nil
	}

	state := &raftpb.RaftPersistentState{
		CurrentTerm:       ws.metadata.CurrentTerm,
		VotedFor:          ws.metadata.VotedFor,
		Log:               log,
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

	snapData, err := os.ReadFile(ws.snapshotPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read snapshot file: %w", err)
	}
	return snapData, nil
}

func (ws *WALStorage) SaveStateAndSnapshot(state, snapshot []byte) error {
	return ws.submitRequest(opSaveStateAndSnapshot, [2][]byte{state, snapshot})
}

func (ws *WALStorage) saveStateAndSnapshot(state, snapshot []byte) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	if state == nil {
		return errors.New("cannot save snapshot with nil raft state")
	}

	ps := &raftpb.RaftPersistentState{}
	if err := proto.Unmarshal(state, ps); err != nil {
		return fmt.Errorf("failed to unmarshal state for snapshot: %w", err)
	}

	walBuf := new(bytes.Buffer)
	for _, entry := range ps.Log {
		encoded, err := encodeEntry(entry)
		if err != nil {
			return fmt.Errorf("failed to encode entry for snapshot overwrite: %w", err)
		}
		walBuf.Write(encoded)
	}

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

	if ws.walFile != nil {
		if err := ws.walFile.Close(); err != nil {
			ws.logger.Warn("failed to close WAL file before snapshot", logger.ErrAttr(err))
		}
	}

	if snapshot != nil {
		if err := syncFile(ws.snapshotPath, snapshot, 0644); err != nil {
			return fmt.Errorf("failed to sync snapshot file: %w", err)
		}
	}

	if err := syncFile(ws.walPath, walBuf.Bytes(), 0644); err != nil {
		return fmt.Errorf("failed to sync WAL file for snapshot: %w", err)
	}
	if err := syncFile(ws.metadataPath, metaBytes, 0644); err != nil {
		return fmt.Errorf("failed to sync metadata file for snapshot: %w", err)
	}

	newWalFile, err := os.OpenFile(ws.walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen WAL file after snapshot: %w", err)
	}
	ws.walFile = newWalFile
	ws.metadata = newMeta

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
		return nil, io.ErrUnexpectedEOF
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
