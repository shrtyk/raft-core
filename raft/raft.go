package raft

import (
	"context"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shrtyk/raft-core/api"
	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
	tester "github.com/shrtyk/raft/tester1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type State = uint32

const (
	_ State = iota
	follower
	candidate
	leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	wg          sync.WaitGroup
	mu          sync.RWMutex               // Lock to protect shared access to this peer's state
	peersConns  []*grpc.ClientConn         // underlying gRPC connections to be closed after shutdown
	peers       []raftpb.RaftServiceClient // gRPC end points of all peers
	persisterMu sync.Mutex
	persister   *tester.Persister // Object to hold this peer's persisted state
	me          int64             // this peer's index into peers[]
	dead        int32             // set by Shutdown()

	state State
	cfg   *Config

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

	commitIdx      int64 // index of highest log entry known to be committed
	lastAppliedIdx int64 // index of the highest log entry applied to state machine

	// Volatile state leaders only (reinitialized after election):

	// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	nextIdx []int64
	// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	matchIdx []int64

	lastIncludedIndex int64 // the index of the last entry in the log that the snapshot replaces
	lastIncludedTerm  int64 // the term of the last entry in the log that the snapshot replaces

	raftCtx    context.Context
	raftCancel func()

	raftpb.UnimplementedRaftServiceServer
}

// GetState returns current term and whether this server believes it is the leader
func (rf *Raft) GetState() (int64, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.curTerm, rf.isState(leader)
}

func (rf *Raft) PersistBytes() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.persister.RaftStateSize()
}

// Start proposes a new command to be replicated
func (rf *Raft) Start(command []byte) (int64, int64, bool) {
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

	go rf.sendAppendEntries()

	return lastLogIdx, term, isLeader
}

// Shutdown sets the peer to a dead state and stops completely
func (rf *Raft) Shutdown() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.raftCancel()

	for i, c := range rf.peersConns {
		if i == int(rf.me) {
			continue
		}
		if err := c.Close(); err != nil {
			log.Printf("failed to close connection for server #%d: %v", i, err)
		}
	}

	rf.wg.Wait()
}

// Make creates and starts a new Raft peer
func Make(
	peerAddrs []string, me int64,
	persister *tester.Persister, applyCh chan *api.ApplyMessage,
	cfg *Config,
) api.Raft {
	rf := &Raft{}
	rf.peersConns = make([]*grpc.ClientConn, len(peerAddrs))
	rf.peers = make([]raftpb.RaftServiceClient, len(peerAddrs))
	rf.persister = persister
	rf.me = me
	if cfg == nil {
		cfg = DefaultConfig()
	}
	rf.cfg = cfg

	rf.raftCtx, rf.raftCancel = context.WithCancel(context.Background())

	rf.signalApplierChan = make(chan struct{}, 1)

	atomic.StoreUint32(&rf.state, follower)
	rf.log = make([]*raftpb.LogEntry, 0)
	rf.applyChan = applyCh

	rf.electionTimer = time.NewTimer(rf.randElectionInterval())
	rf.heartbeatTicker = time.NewTicker(rf.cfg.HeartbeatTimeout)
	rf.heartbeatTicker.Stop()

	rf.readPersist(persister.ReadRaftState())

	lastLogIdx, _ := rf.lastLogIdxAndTerm()
	rf.nextIdx = make([]int64, len(peerAddrs))
	for i := range rf.nextIdx {
		rf.nextIdx[i] = lastLogIdx + 1
	}
	rf.matchIdx = make([]int64, len(peerAddrs))

	grpcServer := grpc.NewServer()
	raftpb.RegisterRaftServiceServer(grpcServer, rf)

	l, err := net.Listen("tcp", peerAddrs[me])
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	go func() {
		if err := grpcServer.Serve(l); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	for i, addr := range peerAddrs {
		if i == int(rf.me) {
			continue
		}

		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("failed to connect: %v", err)
		}

		client := raftpb.NewRaftServiceClient(conn)
		rf.peersConns[i] = conn
		rf.peers[i] = client
	}

	rf.wg.Add(2)
	go rf.applier()
	go rf.ticker()

	return rf
}
