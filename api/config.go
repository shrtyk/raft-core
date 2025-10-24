package api

import (
	"time"

	"github.com/shrtyk/raft-core/pkg/logger"
)

type RaftConfig struct {
	Env                        logger.Enviroment
	ElectionTimeoutBase        time.Duration
	ElectionTimeoutRandomDelta time.Duration
	HeartbeatTimeout           time.Duration
	RPCTimeout                 time.Duration
	ShutdownTimeout            time.Duration
	MonitoringAddr             string
}
