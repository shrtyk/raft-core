package api

import (
	"time"

	"github.com/shrtyk/raft-core/pkg/logger"
)

// RaftConfig holds the configuration for a Raft node.
type RaftConfig struct {
	Log                LoggerCfg
	Timings            RaftTimings
	HttpMonitoringAddr string
	GRPCAddr           string
	Snapshots          SnapshotsCfg
	CommitNoOpOn       bool
}

// LoggerCfg holds the configuration for the logger.
type LoggerCfg struct {
	Env logger.Enviroment
}

// RaftTimings holds various timings and timeouts settings for the Raft algorithm.
type RaftTimings struct {
	ElectionTimeoutBase        time.Duration
	ElectionTimeoutRandomDelta time.Duration
	HeartbeatTimeout           time.Duration
	RPCTimeout                 time.Duration
	ShutdownTimeout            time.Duration
}

type SnapshotsCfg struct {
	CheckLogSizeInterval time.Duration
	ThresholdBytes       int
}
