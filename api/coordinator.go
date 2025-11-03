package api

import "context"

type Coordinator interface {
	Submit(ctx context.Context, cmd []byte) (*SubmitResult, error)
}

type SubmitResult struct {
	Term     int64
	LogIndex int64
	IsLeader bool
}
