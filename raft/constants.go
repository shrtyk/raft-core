package raft

import "time"

const (
	votedForNone = -1
)

const (
	defaultElectionTimeoutRand = 300 * time.Millisecond
	defaultElectionTimeoutBase = 300 * time.Millisecond
	defaultHeartbeatInterval   = 70 * time.Millisecond

	defaultRpcTimeout = 100 * time.Millisecond
)
