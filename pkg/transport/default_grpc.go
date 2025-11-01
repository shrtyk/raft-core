package transport

import (
	"context"
	"time"

	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
)

type GRPCTransport struct {
	requestTimeout time.Duration
	clients        []raftpb.RaftServiceClient
}

func NewGRPCTransport(reqTimeout time.Duration, peers []raftpb.RaftServiceClient) *GRPCTransport {
	tr := &GRPCTransport{
		requestTimeout: reqTimeout,
		clients:        peers,
	}

	return tr
}

func (t *GRPCTransport) SendRequestVote(
	ctx context.Context,
	to int,
	req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	tctx, tcancel := context.WithTimeout(ctx, t.requestTimeout)
	defer tcancel()
	return t.clients[to].RequestVote(tctx, req)
}

func (t *GRPCTransport) SendAppendEntries(
	ctx context.Context,
	to int,
	req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	tctx, tcancel := context.WithTimeout(ctx, t.requestTimeout)
	defer tcancel()
	return t.clients[to].AppendEntries(tctx, req)
}

func (t *GRPCTransport) SendInstallSnapshot(
	ctx context.Context,
	to int,
	req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	tctx, tcancel := context.WithTimeout(ctx, t.requestTimeout)
	defer tcancel()
	return t.clients[to].InstallSnapshot(tctx, req)
}

func (t *GRPCTransport) PeersCount() int {
	return len(t.clients)
}
