package raft

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/shrtyk/raft-core/api"
	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
	"github.com/shrtyk/raft-core/pkg/logger"
	"github.com/shrtyk/raft-core/pkg/storage"
)

type nodeBuilder struct {
	// required
	ctx       context.Context
	me        int
	applyCh   chan *api.ApplyMessage
	fsm       api.FSM
	transport api.Transport

	// optional with defaults
	cfg       *api.RaftConfig
	persister api.Persister
	logger    *slog.Logger
}

func NewNodeBuilder(
	ctx context.Context,
	nodeIdx int,
	applyCh chan *api.ApplyMessage,
	fsm api.FSM,
	transport api.Transport,
) api.NodeBuilder {
	return &nodeBuilder{
		ctx:       ctx,
		me:        nodeIdx,
		applyCh:   applyCh,
		fsm:       fsm,
		transport: transport,
		cfg:       DefaultConfig(),
	}
}

func (nb *nodeBuilder) Build() (api.Raft, error) {
	if err := validateTimings(nb.cfg); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(nb.ctx)

	log := nb.logger
	if log == nil {
		log = logger.NewLogger(nb.cfg.Log.Env, false).With(slog.Int("me", nb.me))
	}

	persister := nb.persister
	if persister == nil {
		var err error
		persister, err = storage.NewDefaultStorage(fmt.Sprintf("data-%d", nb.me), log)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("builder: failed to create default storage: %w", err)
		}
	}

	rf := &Raft{
		raftCtx:           ctx,
		raftCancel:        cancel,
		me:                nb.me,
		peersCount:        nb.transport.PeersCount(),
		applyChan:         nb.applyCh,
		fsm:               nb.fsm,
		transport:         nb.transport,
		persister:         persister,
		cfg:               nb.cfg,
		logger:            log,
		signalApplierChan: make(chan struct{}, 1),
		log:               make([]*raftpb.LogEntry, 0),
		nextIdx:           make([]int64, nb.transport.PeersCount()),
		matchIdx:          make([]int64, nb.transport.PeersCount()),
	}

	if nb.cfg.GRPCAddr != "" {
		rf.grpcServer = NewGRPCServer(rf, nb.cfg.GRPCAddr)
	}

	if nb.cfg.HttpMonitoringAddr != "" {
		rf.monitoringServer = NewMonitoringServer(rf)
	}

	return rf, nil
}

func (nb *nodeBuilder) WithConfig(cfg *api.RaftConfig) api.NodeBuilder {
	nb.cfg = cfg
	return nb
}

func (nb *nodeBuilder) WithLogger(l *slog.Logger) api.NodeBuilder {
	nb.logger = l
	return nb
}

func (nb *nodeBuilder) WithPersister(p api.Persister) api.NodeBuilder {
	nb.persister = p
	return nb
}

func validateTimings(cfg *api.RaftConfig) error {
	if cfg.Timings.HeartbeatTimeout >= cfg.Timings.ElectionTimeoutBase {
		return fmt.Errorf(
			"builder: HeartbeatTimeout (%s) must be less than ElectionTimeoutBase (%s)",
			cfg.Timings.HeartbeatTimeout,
			cfg.Timings.ElectionTimeoutBase,
		)
	}
	return nil
}
