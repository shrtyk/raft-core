package api

import (
	"context"

	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
)

// Defines how Raft peers send RPCs to each other
type Transport interface {
	SendRequestVote(
		ctx context.Context, to int, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error)
	SendAppendEntries(
		ctx context.Context, to int, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error)
	SendInstallSnapshot(
		ctx context.Context, to int, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error)

	PeersCount() int
}
