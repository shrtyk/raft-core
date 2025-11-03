package testsim

import (
	"context"
	"errors"

	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
	"github.com/shrtyk/raft-core/tests/simrpc"
)

// SimTransport is a mock transport layer that implements the api.Transport interface.
// It uses the simrpc.Network to simulate RPCs.
type SimTransport struct {
	ends []*simrpc.ClientEnd
}

func NewSimTransport(ends []*simrpc.ClientEnd) *SimTransport {
	return &SimTransport{
		ends: ends,
	}
}

func (st *SimTransport) SendRequestVote(ctx context.Context, to int, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	reply := &raftpb.RequestVoteResponse{}
	ok := st.ends[to].Call("RaftService.RequestVote", req, reply)
	if !ok {
		return nil, errors.New("RPC failed")
	}
	return reply, nil
}

func (st *SimTransport) SendAppendEntries(ctx context.Context, to int, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	reply := &raftpb.AppendEntriesResponse{}
	ok := st.ends[to].Call("RaftService.AppendEntries", req, reply)
	if !ok {
		return nil, errors.New("RPC failed")
	}
	return reply, nil
}

func (st *SimTransport) SendInstallSnapshot(ctx context.Context, to int, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	reply := &raftpb.InstallSnapshotResponse{}
	ok := st.ends[to].Call("RaftService.InstallSnapshot", req, reply)
	if !ok {
		return nil, errors.New("RPC failed")
	}
	return reply, nil
}

func (st *SimTransport) PeersCount() int {
	return len(st.ends)
}
