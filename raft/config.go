package raft

import "time"

type State = uint32

const (
	_ State = iota
	follower
	candidate
	leader
)

const votedForNone = -1

type Config struct {
	ElectionTimeoutBase        time.Duration
	ElectionTimeoutRandomDelta time.Duration
	HeartbeatTimeout           time.Duration
	RPCTimeout                 time.Duration
}

func DefaultConfig() *Config {
	return &Config{
		ElectionTimeoutBase:        300 * time.Millisecond,
		ElectionTimeoutRandomDelta: 300 * time.Millisecond,
		HeartbeatTimeout:           70 * time.Millisecond,
		RPCTimeout:                 100 * time.Millisecond,
	}
}
