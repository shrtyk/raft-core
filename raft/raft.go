package raft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shrtyk/raft-core/api"
	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
	"github.com/shrtyk/raft-core/pkg/logger"
	"github.com/shrtyk/raft-core/pkg/storage"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	wg sync.WaitGroup
	mu sync.RWMutex // Lock to protect shared access to this peer's state
	// Lock for persistence writes to provide atomicity of operations without holding global lock.
	//
	// Lock mu -> update persistence state -> make a copy state -> lock pmu -> unlock mu -> persist copy of state -> unlock pmu
	pmu        sync.RWMutex
	peersCount int           // Amount of peers in cluster
	transport  api.Transport // RPC clients layer abstraction
	persister  api.Persister // Persistence layer abstraction (should be concurrent safe)
	me         int           // this peer's index
	dead       int32         // set by Stop()

	state State           // State of the peer
	cfg   *api.RaftConfig // Config of the peer

	// Lock to provide atomic timers updates.
	//
	// lock timerMu -> stop one of the timers -> reser another one -> unlock timerMu
	timerMu         sync.Mutex
	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker

	applyChan           chan *api.ApplyMessage
	signalAppliererChan chan struct{}
	electionDone        chan struct{}

	// Persistent state:

	curTerm  int64              // latest term server has seen
	votedFor int64              // index of peer in peers
	log      []*raftpb.LogEntry // log entries

	// Volatile state on all servers:

	// index of highest log entry known to be committed
	commitIdx int64
	// index of the highest log entry applied to state machine
	lastAppliedIdx int64

	// Volatile state leaders only (reinitialized after election):

	// for each server, index of the next log entry
	// to send to that server (initialized to leader last log index + 1)
	nextIdx []int64
	// for each server, index of highest log entry known
	// to be replicated on server (initialized to 0, increases monotonically)
	matchIdx []int64

	// the index of the last entry in the log that the snapshot replaces
	lastIncludedIndex int64
	// the term of the last entry in the log that the snapshot replaces
	lastIncludedTerm int64

	raftCtx    context.Context
	raftCancel func()
	logger     *slog.Logger

	monitoringServer MonitoringServer
	grpcServer       GRPCServer
	fsm              api.FSM
	raftpb.UnimplementedRaftServiceServer
}

// NewRaft creates a new Raft peer
func NewRaft(
	cfg *api.RaftConfig,
	me int,
	persister api.Persister,
	applyCh chan *api.ApplyMessage,
	transport api.Transport,
	fsm api.FSM,
) (api.Raft, error) {
	ctx, cancel := context.WithCancel(context.Background())

	rf := &Raft{
		peersCount:          transport.PeersCount(),
		transport:           transport,
		me:                  me,
		applyChan:           applyCh,
		signalAppliererChan: make(chan struct{}, 1),
		log:                 make([]*raftpb.LogEntry, 0),
		nextIdx:             make([]int64, transport.PeersCount()),
		matchIdx:            make([]int64, transport.PeersCount()),
		fsm:                 fsm,
		raftCtx:             ctx,
		raftCancel:          cancel,
	}

	if cfg == nil {
		cfg = DefaultConfig()
	}

	rf.cfg = cfg
	if cfg.Log.Env == logger.Dev {
		_, rf.logger = logger.NewTestLogger()
	} else {
		rf.logger = logger.NewLogger(rf.cfg.Log.Env, false).With(slog.Int("me", me))
	}

	if persister == nil {
		s, err := storage.NewDefaultStorage("data", rf.logger)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize default storage: %w", err)
		}
		persister = s
	}
	rf.persister = persister
	return rf, nil
}

func (rf *Raft) Start() error {
	state, err := rf.persister.ReadRaftState()
	if err != nil {
		return fmt.Errorf("failed to read peer #%d state: %w", rf.me, err)
	}
	rf.restoreState(state)
	rf.initializeNextIndexes()
	rf.electionTimer = time.NewTimer(rf.randElectionInterval())
	rf.heartbeatTicker = time.NewTicker(rf.cfg.Timings.HeartbeatTimeout)
	rf.heartbeatTicker.Stop()
	rf.becomeFollower(rf.curTerm)

	if rf.grpcServer != nil {
		if err := rf.grpcServer.Start(); err != nil {
			return fmt.Errorf("failed to start gRPC server: %w", err)
		}
	}

	if rf.monitoringServer != nil {
		if err := rf.monitoringServer.Start(); err != nil {
			return fmt.Errorf("failed to start monitoring HTTP server: %w", err)
		}
	}

	rf.wg.Add(2)
	go rf.applier()
	go rf.ticker()

	return nil
}

// Stop sets the peer to a dead state and stops completely
func (rf *Raft) Stop() error {
	tctx, tcancel := context.WithTimeout(rf.raftCtx, rf.cfg.Timings.ShutdownTimeout)
	defer tcancel()

	var err error
	atomic.StoreInt32(&rf.dead, 1)
	if rf.grpcServer != nil {
		if serr := rf.grpcServer.Stop(); serr != nil {
			err = errors.Join(err, fmt.Errorf("failed to shutdown gRPC server: %w", serr))
		}
	}

	if rf.monitoringServer != nil {
		if serr := rf.monitoringServer.Stop(tctx); serr != nil {
			err = errors.Join(err, fmt.Errorf("failed to shutdown monitoring server: %w", serr))
		}
	}

	rf.raftCancel()
	rf.wg.Wait()
	return err
}

// Submit proposes a new command to be replicated
func (rf *Raft) Submit(command []byte) (int64, int64, bool) {
	rf.mu.Lock()

	if !rf.isState(leader) {
		term := rf.curTerm
		rf.mu.Unlock()
		return -1, term, false
	}

	term := rf.curTerm
	rf.log = append(rf.log, &raftpb.LogEntry{
		Term: rf.curTerm,
		Cmd:  command,
	})
	lastLogIdx, _ := rf.lastLogIdxAndTerm()
	rf.matchIdx[rf.me] = lastLogIdx
	rf.nextIdx[rf.me] = lastLogIdx + 1

	// If persistence fails, the node must not continue as leader,
	// because it’s no longer a reliable state machine replica.
	if err := rf.persistAndUnlock(nil); err != nil {
		rf.logger.Warn("failed to persist log, stepping down as leader", logger.ErrAttr(err))

		rf.mu.Lock()
		if rf.curTerm == term && rf.isState(leader) {
			rf.becomeFollower(term)
		}
		rf.mu.Unlock()
		return -1, term, false
	}

	go rf.sendSnapshotOrEntries()

	return lastLogIdx, term, true
}
