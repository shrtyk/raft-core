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
	leaderId   int           // ID of the current leader
	dead       int32         // set by Stop()

	state State           // State of the peer
	cfg   *api.RaftConfig // Config of the peer

	// Lock to provide atomic timers updates.
	//
	// lock timerMu -> stop one of the timers -> reser another one -> unlock timerMu
	timerMu                   sync.Mutex
	electionTimer             *time.Timer
	heartbeatTicker           *time.Ticker
	lastHeartbeatMajorityTime time.Time

	applyChan         chan *api.ApplyMessage
	signalApplierChan chan struct{}
	electionDone      chan struct{}

	// Persistent state:

	curTerm        int64              // latest term server has seen
	votedFor       int64              // index of peer in peers
	log            []*raftpb.LogEntry // log entries
	logSizeInBytes int

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

func (rf *Raft) Start() error {
	state, err := rf.persister.ReadRaftState()
	if err != nil {
		return fmt.Errorf("failed to read peer #%d state: %w", rf.me, err)
	}
	rf.restoreState(state)
	rf.logSizeInBytes = rf.calculateLogSizeInBytes()
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

	// Start background snapshotter when automatic snapshots are enabled
	if rf.cfg.Snapshots.ThresholdBytes > 0 {
		rf.wg.Add(1)
		go rf.snapshotter()
	}

	return nil
}

// Stop sets the peer to a dead state and stops completely
func (rf *Raft) Stop() error {
	tctx, tcancel := context.WithTimeout(context.Background(), rf.cfg.Timings.ShutdownTimeout)
	defer tcancel()

	var err error
	atomic.StoreInt32(&rf.dead, 1)
	rf.raftCancel()

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

	rf.wg.Wait()
	return err
}

// Submit proposes a new command to be replicated
func (rf *Raft) Submit(command []byte) *api.SubmitResult {
	res := &api.SubmitResult{
		LogIndex: -1,
	}
	rf.mu.Lock()

	if !rf.isState(leader) {
		res.Term = rf.curTerm
		res.LeaderID = rf.leaderId
		rf.mu.Unlock()
		return res
	}

	res.Term = rf.curTerm
	res.LeaderID = rf.me
	rf.log = append(rf.log, &raftpb.LogEntry{
		Term: rf.curTerm,
		Cmd:  command,
	})
	rf.logSizeInBytes += len(command)
	lastLogIdx, _ := rf.lastLogIdxAndTerm()
	rf.matchIdx[rf.me] = lastLogIdx
	rf.nextIdx[rf.me] = lastLogIdx + 1

	// If persistence fails, the node must not continue as leader,
	// because it’s no longer a reliable state machine replica.
	if err := rf.persistAndUnlock(nil); err != nil {
		rf.logger.Warn("failed to persist log, stepping down as leader", logger.ErrAttr(err))

		rf.mu.Lock()
		if rf.curTerm == res.Term && rf.isState(leader) {
			rf.becomeFollower(res.Term)
		}
		rf.mu.Unlock()
		return res
	}
	res.LogIndex = lastLogIdx
	res.IsLeader = true
	go rf.sendSnapshotOrEntries()

	return res
}

func (rf *Raft) ReadOnly(ctx context.Context, key []byte) (*api.ReadOnlyResult, error) {
	rf.mu.RLock()
	isLeader := rf.isState(leader)
	commitIndex := rf.commitIdx
	lastHeartbeat := rf.lastHeartbeatMajorityTime
	rf.mu.RUnlock()

	if !isLeader {
		return &api.ReadOnlyResult{IsLeader: false}, nil
	}

	// For linearizable reads, the lease duration must be shorter than the election timeout.
	leaseDuration := rf.cfg.Timings.ElectionTimeoutBase
	if time.Since(lastHeartbeat) > leaseDuration {
		rf.logger.Debug("leader lease expired, confirming leadership with heartbeats")
		// Confirm if it is still the leader before serving a read.
		tctx, tcancel := context.WithTimeout(ctx, rf.cfg.Timings.RPCTimeout)
		defer tcancel()
		if !rf.ConfirmLeadership(tctx) {
			rf.logger.Warn("failed to confirm leadership, aborting read-only request")
			return &api.ReadOnlyResult{IsLeader: false}, nil
		}
		rf.logger.Debug("leader lease renewed")
	}

	// Wait for the state machine to catch up to the commit index known at the time the read was requested.
	tctx, tcancel := context.WithTimeout(ctx, rf.cfg.Timings.RPCTimeout)
	defer tcancel()

	for {
		rf.mu.RLock()
		lastApplied := rf.lastAppliedIdx
		rf.mu.RUnlock()

		if lastApplied >= commitIndex {
			break
		}

		select {
		case <-tctx.Done():
			return nil, fmt.Errorf("timeout waiting for state machine to catch up for read at commit index %d: %w", commitIndex, tctx.Err())
		// TODO: This could be improved with a channel notification from the applier.
		case <-time.After(5 * time.Millisecond):
			continue
		}
	}

	// State machine is up-to-date, we can now safely read from it.
	data, err := rf.fsm.Read(key)
	if err != nil {
		return nil, fmt.Errorf("failed to read from FSM: %w", err)
	}

	return &api.ReadOnlyResult{
		Data:     data,
		IsLeader: true,
	}, nil
}
