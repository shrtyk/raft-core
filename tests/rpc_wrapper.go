package testsim

import (
	"context"

	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
	"github.com/shrtyk/raft-core/raft"
)

type RaftService struct {
	rf *raft.Raft
}

func (rs *RaftService) RequestVote(args *raftpb.RequestVoteRequest, reply *raftpb.RequestVoteResponse) {
	r, err := rs.rf.RequestVote(context.Background(), args)
	if err == nil {
		*reply = *r
	}
}

func (rs *RaftService) AppendEntries(args *raftpb.AppendEntriesRequest, reply *raftpb.AppendEntriesResponse) {
	r, err := rs.rf.AppendEntries(context.Background(), args)
	if err == nil {
		*reply = *r
	}
}

func (rs *RaftService) InstallSnapshot(args *raftpb.InstallSnapshotRequest, reply *raftpb.InstallSnapshotResponse) {
	r, err := rs.rf.InstallSnapshot(context.Background(), args)
	if err == nil {
		*reply = *r
	}
}

func (rs *RaftService) Kill() {
	rs.rf.Stop()
}
