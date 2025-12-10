package transport

import (
	"context"
	"time"

	"github.com/shrtyk/raft-core/api"
	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
	"google.golang.org/grpc"
)

var _ api.Transport = (*GRPCTransport)(nil)

type GRPCTransport struct {
	requestTimeout time.Duration
	clients        []raftpb.RaftServiceClient
}

func NewGRPCTransport(cfg *api.RaftConfig, conns []*grpc.ClientConn) (*GRPCTransport, error) {
	tr := &GRPCTransport{
		requestTimeout: cfg.Timings.RPCTimeout,
		clients:        make([]raftpb.RaftServiceClient, len(conns)),
	}

	for i, conn := range conns {
		tr.clients[i] = raftpb.NewRaftServiceClient(conn)
	}

	return tr, nil
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
