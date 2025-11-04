package raft

import (
	"time"

	"github.com/shrtyk/raft-core/api"
	"github.com/shrtyk/raft-core/pkg/logger"
)

const votedForNone = -1

const defaultHttpMonitoringAddr = "localhost:12321"

func DefaultConfig() *api.RaftConfig {
	return &api.RaftConfig{
		Log: api.LoggerCfg{
			Env: logger.Dev,
		},
		Timings: api.RaftTimings{
			ElectionTimeoutBase:        150 * time.Millisecond,
			ElectionTimeoutRandomDelta: 150 * time.Millisecond,
			HeartbeatTimeout:           60 * time.Millisecond,
			ShutdownTimeout:            3 * time.Second,
			RPCTimeout:                 100 * time.Millisecond,
		},
		HttpMonitoringAddr: defaultHttpMonitoringAddr,
		GRPCAddr:           "",
	}
}
