package raft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shrtyk/raft-core/api"
	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
	"github.com/shrtyk/raft-core/pkg/logger"
	"github.com/shrtyk/raft-core/pkg/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	wg         sync.WaitGroup
	mu         sync.RWMutex               // Lock to protect shared access to this peer's state
	peersConns []*grpc.ClientConn         // underlying gRPC connections to be closed after shutdown
	peers      []raftpb.RaftServiceClient // gRPC end points of all peers
	persister  api.Persister              // Object to hold this peer's persisted state (should be concurrent safe)
	me         int                        // this peer's index into peers[]
	dead       int32                      // set by Shutdown()

	state State
	cfg   *api.RaftConfig

	timerMu         sync.Mutex
	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker

	applyChan         chan *api.ApplyMessage
	signalApplierChan chan struct{}

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

	raftCtx          context.Context
	raftCancel       func()
	logger           *slog.Logger
	grpcServer       *grpc.Server
	monitoringServer *http.Server

	raftpb.UnimplementedRaftServiceServer
}

// State returns current term and whether this server believes it is the leader
func (rf *Raft) State() (int64, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.curTerm, rf.isState(leader)
}

func (rf *Raft) PersistedStateSize() (int, error) {
	return rf.persister.RaftStateSize()
}

// Submit proposes a new command to be replicated
func (rf *Raft) Submit(command []byte) (int64, int64, bool) {
	rf.mu.Lock()

	isLeader := rf.isState(leader)
	term := rf.curTerm
	if !isLeader {
		rf.mu.Unlock()
		return -1, term, false
	}

	rf.log = append(rf.log, &raftpb.LogEntry{
		Term: rf.curTerm,
		Cmd:  command,
	})
	lastLogIdx, _ := rf.lastLogIdxAndTerm()
	rf.matchIdx[rf.me] = lastLogIdx
	rf.nextIdx[rf.me] = lastLogIdx + 1

	rf.persistAndUnlock(nil)

	go rf.sendSnapshotOrEntries()

	return lastLogIdx, term, isLeader
}

// Stop sets the peer to a dead state and stops completely
func (rf *Raft) Stop() error {
	tctx, tcancel := context.WithTimeout(rf.raftCtx, rf.cfg.ShutdownTimeout)
	defer tcancel()

	var err error
	atomic.StoreInt32(&rf.dead, 1)
	rf.grpcServer.GracefulStop()

	if rf.monitoringServer != nil {
		if shutdownErr := rf.monitoringServer.Shutdown(tctx); shutdownErr != nil {
			err = errors.Join(err, fmt.Errorf("failed to shutdown monitoring server: %w", shutdownErr))
		}
	}

	for i, c := range rf.peersConns {
		if i == rf.me {
			continue
		}
		if closeErr := c.Close(); closeErr != nil {
			cerr := fmt.Errorf("failed to close connection for server #%d: %v", i, closeErr)
			err = errors.Join(err, cerr)
		}
	}

	rf.raftCancel()
	rf.wg.Wait()
	return err
}

// Make creates and starts a new Raft peer
func Make(
	cfg *api.RaftConfig, peerAddrs []string, me int,
	persister api.Persister, applyCh chan *api.ApplyMessage,
) (api.Raft, error) {
	rf := &Raft{}
	rf.peersConns = make([]*grpc.ClientConn, len(peerAddrs))
	rf.peers = make([]raftpb.RaftServiceClient, len(peerAddrs))
	rf.me = me
	rf.applyChan = applyCh

	if cfg == nil {
		cfg = DefaultConfig()
	}

	rf.cfg = cfg
	rf.logger = logger.NewLogger(rf.cfg.Env)

	if persister == nil {
		s, err := storage.NewDefaultStorage("data", rf.logger)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize default storage: %w", err)
		}
		persister = s
	}
	rf.persister = persister

	rf.raftCtx, rf.raftCancel = context.WithCancel(context.Background())
	rf.signalApplierChan = make(chan struct{}, 1)
	rf.log = make([]*raftpb.LogEntry, 0)

	rf.electionTimer = time.NewTimer(rf.randElectionInterval())
	rf.heartbeatTicker = time.NewTicker(rf.cfg.HeartbeatTimeout)
	rf.heartbeatTicker.Stop()
	rf.becomeFollower(-1)

	state, err := persister.ReadRaftState()
	if err != nil {
		return nil, fmt.Errorf("failed to read peer #%d state: %w", me, err)
	}
	rf.restoreState(state)

	lastLogIdx, _ := rf.lastLogIdxAndTerm()
	rf.nextIdx = make([]int64, len(peerAddrs))
	for i := range rf.nextIdx {
		rf.nextIdx[i] = lastLogIdx + 1
	}
	rf.matchIdx = make([]int64, len(peerAddrs))

	rf.grpcServer = grpc.NewServer()
	raftpb.RegisterRaftServiceServer(rf.grpcServer, rf)

	l, err := net.Listen("tcp", peerAddrs[me])
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %v", err)
	}

	go func() {
		if err := rf.grpcServer.Serve(l); err != nil {
			rf.logger.Error("failed to serve", logger.ErrAttr(err))
			os.Exit(1)
		}
	}()

	for i, addr := range peerAddrs {
		if i == rf.me {
			continue
		}

		conn, err := grpc.NewClient(
			addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("failed to establish connection: %w", err)
		}

		client := raftpb.NewRaftServiceClient(conn)
		rf.peersConns[i] = conn
		rf.peers[i] = client
	}

	rf.wg.Add(2)
	go rf.applier()
	go rf.ticker()
	rf.startMonitoringServer()

	return rf, nil
}
