package api

import (
	"context"

	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
)

// Transport defines the interface for Raft peers to communicate via RPCs.
type Transport interface {
	// SendRequestVote sends a RequestVote RPC to a specific peer.
	SendRequestVote(
		ctx context.Context, to int, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error)
	// SendAppendEntries sends an AppendEntries RPC to a specific peer.
	SendAppendEntries(
		ctx context.Context, to int, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error)
	// SendInstallSnapshot sends an InstallSnapshot RPC to a specific peer.
	SendInstallSnapshot(
		ctx context.Context, to int, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error)

	// PeersCount returns the total number of peers in the Raft cluster.
	PeersCount() int

	// IsPeerAvailable return true if peer currently available to be called and false otherwise.
	IsPeerAvailable(peerID int) bool
}
