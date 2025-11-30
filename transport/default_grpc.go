package transport

import (
	"context"
	"time"

	"github.com/shrtyk/raft-core/api"
	"github.com/shrtyk/raft-core/internal/cbreaker"
	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
	"google.golang.org/grpc"
)

var _ api.Transport = (*GRPCTransport)(nil)

type client struct {
	raftClient raftpb.RaftServiceClient
	cBreaker   *cbreaker.CircuitBreaker
}

type GRPCTransport struct {
	requestTimeout time.Duration
	clients        []*client
}

func NewGRPCTransport(cfg *api.RaftConfig, conns []*grpc.ClientConn) (*GRPCTransport, error) {
	tr := &GRPCTransport{
		requestTimeout: cfg.Timings.RPCTimeout,
		clients:        make([]*client, len(conns)),
	}

	for i, conn := range conns {
		tr.clients[i] = &client{
			raftClient: raftpb.NewRaftServiceClient(conn),
			cBreaker: cbreaker.NewCircuitBreaker(
				cfg.CBreaker.FailureThreshold,
				cfg.CBreaker.SuccessThreshold,
				cfg.CBreaker.ResetTimeout,
			),
		}
	}

	return tr, nil
}

func (t *GRPCTransport) SendRequestVote(
	ctx context.Context,
	to int,
	req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	tctx, tcancel := context.WithTimeout(ctx, t.requestTimeout)
	defer tcancel()

	return cbreaker.Do(tctx, t.clients[to].cBreaker, func(ctx context.Context) (*raftpb.RequestVoteResponse, error) {
		return t.clients[to].raftClient.RequestVote(tctx, req)
	})
}

func (t *GRPCTransport) SendAppendEntries(
	ctx context.Context,
	to int,
	req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	tctx, tcancel := context.WithTimeout(ctx, t.requestTimeout)
	defer tcancel()
	return cbreaker.Do(tctx, t.clients[to].cBreaker, func(ctx context.Context) (*raftpb.AppendEntriesResponse, error) {
		return t.clients[to].raftClient.AppendEntries(tctx, req)
	})
}

func (t *GRPCTransport) SendInstallSnapshot(
	ctx context.Context,
	to int,
	req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	tctx, tcancel := context.WithTimeout(ctx, t.requestTimeout)
	defer tcancel()
	return cbreaker.Do(tctx, t.clients[to].cBreaker, func(ctx context.Context) (*raftpb.InstallSnapshotResponse, error) {
		return t.clients[to].raftClient.InstallSnapshot(tctx, req)
	})
}

func (t *GRPCTransport) PeersCount() int {
	return len(t.clients)
}

func (t *GRPCTransport) IsPeerAvailable(peerID int) bool {
	return t.clients[peerID].cBreaker.IsClosed()
}
