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

// An implementation of a single Raft peer.
type Raft struct {
	wg      sync.WaitGroup // Global lock to protect node state.
	mu      sync.RWMutex   // Lock to protect shared access to this peer's state.
	errOnce sync.Once      // Used in case of persistence errors and guarantees at most one fatal error to be sent to client

	state      State // State of the peer.
	peersCount int   // Amount of peers in cluster.
	me         int   // This peer's index.
	leaderId   int   // ID of the current leader.
	dead       int32 // Set by Stop().

	cfg       *api.RaftConfig // Config of the peer.
	persister api.Persister   // Persistence layer abstraction (should be concurrent safe).
	fsm       api.FSM         // Application finite state machine abstraction to be implemented by clients.
	transport api.Transport   // RPC clients layer abstraction.
	logger    *slog.Logger

	electionTimer             *time.Timer
	heartbeatTicker           *time.Ticker
	lastHeartbeatMajorityTime time.Time

	applyChan         chan *api.ApplyMessage
	signalApplierChan chan struct{}
	electionDone      chan struct{}

	resetElectionTimerCh   chan struct{}
	resetHeartbeatTickerCh chan struct{}
	errChan                chan error

	log *raftLog

	// Persistent state:
	curTerm  int64 // Latest term server has seen.
	votedFor int64 // Index of peer in peers.

	// Volatile state on all servers:
	commitIdx      int64 // Index of highest log entry known to be committed.
	lastAppliedIdx int64 // Index of the highest log entry applied to state machine.

	// Volatile state leaders only (reinitialized after election):

	// For each server, index of the next log entry
	// to send to that server (initialized to leader last log index + 1).
	nextIdx []int64
	// For each server, index of highest log entry known
	// to be replicated on server (initialized to 0, increases monotonically).
	matchIdx []int64

	raftCtx    context.Context
	raftCancel func()

	monitoringServer MonitoringServer
	grpcServer       GRPCServer

	raftpb.UnimplementedRaftServiceServer
}

func (rf *Raft) Errors() <-chan error {
	return rf.errChan
}

// initializeNextIndexes initializes indexes based on the current log state.
//
// If for some reason it used outside of Start function the mutex should be locked.
func (rf *Raft) initializeNextIndexes() {
	lastLogIdx, _ := rf.log.lastLogIdxAndTerm()
	for i := range rf.nextIdx {
		rf.nextIdx[i] = lastLogIdx + 1
	}
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

	if rf.persister != nil {
		if perr := rf.persister.Close(); perr != nil {
			err = errors.Join(err, fmt.Errorf("failed to close persister: %w", perr))
		}
	}

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

	newEntry := &raftpb.LogEntry{
		Term: rf.curTerm,
		Cmd:  command,
	}
	rf.log.append(newEntry)
	lastLogIdx, _ := rf.log.lastLogIdxAndTerm()
	rf.matchIdx[rf.me] = lastLogIdx
	rf.nextIdx[rf.me] = lastLogIdx + 1

	rf.mu.Unlock()

	pErr := rf.persister.AppendEntries([]*raftpb.LogEntry{newEntry})

	// If persistence fails, the node must not continue as leader,
	// because it’s no longer a reliable state machine replica.
	if pErr != nil {
		rf.logger.Warn("failed to persist log, stepping down as leader", logger.ErrAttr(pErr))

		rf.mu.Lock()
		if rf.curTerm == res.Term && rf.isState(leader) {
			rf.becomeFollower(res.Term)
		}
		rf.mu.Unlock()
		return res
	}

	res.LogIndex = lastLogIdx
	res.IsLeader = true

	return res
}

func (rf *Raft) ReadOnly(ctx context.Context, key []byte) (*api.ReadOnlyResult, error) {
	rf.mu.RLock()
	isLeader := rf.isState(leader)
	commitIndex := rf.commitIdx
	lastHeartbeat := rf.lastHeartbeatMajorityTime
	leaderId := rf.leaderId
	rf.mu.RUnlock()

	if !isLeader {
		return &api.ReadOnlyResult{IsLeader: false, LeaderId: leaderId}, nil
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
			rf.mu.RLock()
			leaderId = rf.leaderId
			rf.mu.RUnlock()
			return &api.ReadOnlyResult{IsLeader: false, LeaderId: leaderId}, nil
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

	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return &api.ReadOnlyResult{
		Data:     data,
		IsLeader: true,
		LeaderId: rf.me,
	}, nil
}
