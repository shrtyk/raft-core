package raft

import (
	"time"

	"github.com/shrtyk/raft-core/pkg/logger"
)

type State = uint32

const (
	_ State = iota
	follower
	candidate
	leader
)

const votedForNone = -1

type RaftConfig struct {
	Env                        logger.Enviroment
	ElectionTimeoutBase        time.Duration
	ElectionTimeoutRandomDelta time.Duration
	HeartbeatTimeout           time.Duration
	RPCTimeout                 time.Duration
}

func DefaultConfig() *RaftConfig {
	return &RaftConfig{
		Env:                        logger.Dev,
		ElectionTimeoutBase:        300 * time.Millisecond,
		ElectionTimeoutRandomDelta: 300 * time.Millisecond,
		HeartbeatTimeout:           70 * time.Millisecond,
		RPCTimeout:                 100 * time.Millisecond,
	}
}
