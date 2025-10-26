package transport

import (
	"context"
	"errors"
	"fmt"
	"time"

	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GRPCTransport struct {
	requestTimeout time.Duration
	conns          []*grpc.ClientConn
	clients        []raftpb.RaftServiceClient
}

func NewGRPCTransport(reqTimeout time.Duration, peerAddrs []string) (*GRPCTransport, error) {
	tr := &GRPCTransport{
		requestTimeout: reqTimeout,
		clients:        make([]raftpb.RaftServiceClient, len(peerAddrs)),
		conns:          make([]*grpc.ClientConn, len(peerAddrs)),
	}

	for i, addr := range peerAddrs {
		conn, err := grpc.NewClient(
			addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("dial peer %d: %w", i, err)
		}
		tr.conns[i] = conn
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

func (t *GRPCTransport) Close() error {
	var err error
	for i, conn := range t.conns {
		if cerr := conn.Close(); cerr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("failed to close peer %d connection: %w", i, cerr))
		}
	}
	return err
}
