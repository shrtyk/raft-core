package raft

import (
	"time"

	"github.com/shrtyk/raft-core/api"
	"github.com/shrtyk/raft-core/pkg/logger"
)

const votedForNone = -1

func DefaultConfig() *api.RaftConfig {
	return &api.RaftConfig{
		Env:                        logger.Dev,
		ElectionTimeoutBase:        300 * time.Millisecond,
		ElectionTimeoutRandomDelta: 300 * time.Millisecond,
		HeartbeatTimeout:           70 * time.Millisecond,
		ShutdownTimeout:            3 * time.Second,
		RPCTimeout:                 100 * time.Millisecond,
	}
}
