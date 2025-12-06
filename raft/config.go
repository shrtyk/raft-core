package raft

import (
	"time"

	"github.com/shrtyk/raft-core/api"
	"github.com/shrtyk/raft-core/pkg/logger"
)

const votedForNone = -1

const (
	defaultHttpMonitoringAddr = ""
	defaultGRPCAddr           = ""
)

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
		CBreaker: api.CircuitBreakerCfg{
			FailureThreshold: 6,
			SuccessThreshold: 4,
			ResetTimeout:     5 * time.Second,
		},
		Fsync: api.FsyncCfg{
			BatchSize: 128,
			Timeout:   15 * time.Millisecond,
		},
		Snapshots: api.SnapshotsCfg{
			CheckLogSizeInterval: 30 * time.Second,
			ThresholdBytes:       0,
		},
		HttpMonitoringAddr: defaultHttpMonitoringAddr,
		GRPCAddr:           defaultGRPCAddr,
		CommitNoOpOn:       true,
	}
}

func TestsConfig() *api.RaftConfig {
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
		CBreaker: api.CircuitBreakerCfg{
			FailureThreshold: 6,
			SuccessThreshold: 4,
			ResetTimeout:     5 * time.Second,
		},
		Fsync: api.FsyncCfg{
			BatchSize: 10,
			Timeout:   10 * time.Millisecond,
		},
		Snapshots: api.SnapshotsCfg{
			CheckLogSizeInterval: 30 * time.Second,
			ThresholdBytes:       0,
		},
		CommitNoOpOn: false,
	}
}
