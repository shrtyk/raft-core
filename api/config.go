package api

import (
	"time"

	"github.com/shrtyk/raft-core/pkg/logger"
)

type RaftConfig struct {
	Log                LoggerCfg
	Timings            RaftTimings
	HttpMonitoringAddr string
	MessagesQueueSize  int
}

type LoggerCfg struct {
	Env logger.Enviroment
}

type RaftTimings struct {
	ElectionTimeoutBase        time.Duration
	ElectionTimeoutRandomDelta time.Duration
	HeartbeatTimeout           time.Duration
	RPCTimeout                 time.Duration
	ShutdownTimeout            time.Duration
}
