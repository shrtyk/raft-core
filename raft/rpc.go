package raft

import (
	"context"

	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
)

func (rf *Raft) RequestVote(ctx context.Context,
	req *raftpb.RequestVoteRequest) (reply *raftpb.RequestVoteResponse, err error) {
	reply = &raftpb.RequestVoteResponse{}
	var needToPersist bool

	rf.mu.Lock()
	defer func() {
		rf.unlockConditionally(needToPersist, nil)
	}()

	reply.VoteGranted = false
	reply.VoterId = int64(rf.me)

	if req.Term < rf.curTerm {
		rf.logger.Warn(
			"denying vote, candidate term too low",
			"candidate_id", req.CandidateId,
			"candidate_term", req.Term,
			"current_term", rf.curTerm,
		)
		reply.Term = rf.curTerm
		return
	}

	if req.Term > rf.curTerm {
		needToPersist = rf.becomeFollower(req.Term)
	}

	reply.Term = rf.curTerm
	if !rf.isCandidateLogUpToDate(req.LastLogIndex, req.LastLogTerm) {
		myLastLogIdx, myLastLogTerm := rf.lastLogIdxAndTerm()
		rf.logger.Warn(
			"denying vote, candidate log not up-to-date",
			"candidate_id", req.CandidateId,
			"candidate_last_log_idx", req.LastLogIndex,
			"candidate_last_log_term", req.LastLogTerm,
			"my_last_log_idx", myLastLogIdx,
			"my_last_log_term", myLastLogTerm,
		)
		return
	}

	if rf.votedFor != votedForNone && rf.votedFor != req.CandidateId {
		rf.logger.Warn(
			"denying vote, already voted for another candidate",
			"candidate_id", req.CandidateId,
			"voted_for", rf.votedFor,
		)
		return
	}

	reply.VoteGranted = true
	rf.votedFor = req.CandidateId
	needToPersist = true
	rf.resetElectionTimer()
	rf.logger.Info(
		"voting for candidate",
		"candidate_id", req.CandidateId,
		"term", rf.curTerm,
	)

	return
}

func (rf *Raft) AppendEntries(ctx context.Context,
	req *raftpb.AppendEntriesRequest) (reply *raftpb.AppendEntriesResponse, err error) {
	reply = &raftpb.AppendEntriesResponse{}
	var needToPersist bool
	var shouldSignalApplier bool

	rf.mu.Lock()
	defer func() {
		rf.unlockConditionally(needToPersist, nil)
		if shouldSignalApplier {
			rf.signalApplier()
		}
	}()

	if len(req.Entries) == 0 {
		rf.logger.Debug("heartbeat received", "leader_id", req.LeaderId, "term", req.Term)
	} else {
		rf.logger.Debug("append entries received", "leader_id", req.LeaderId, "term", req.Term, "num_entries", len(req.Entries))
	}

	reply.Success = false
	reply.Term = rf.curTerm

	if req.Term < rf.curTerm {
		return
	}

	if req.Term > rf.curTerm || rf.isState(candidate) {
		needToPersist = rf.becomeFollower(req.Term)
	}

	rf.resetElectionTimer()
	reply.Term = rf.curTerm
	if req.PrevLogIndex < rf.lastIncludedIndex {
		reply.Success = false
		return
	}

	if !rf.isLogConsistent(req.PrevLogIndex, req.PrevLogTerm) {
		myPrevLogTerm := rf.getTerm(req.PrevLogIndex)
		rf.logger.Warn(
			"log inconsistent, rejecting append entries",
			"prev_log_idx", req.PrevLogIndex,
			"prev_log_term", req.PrevLogTerm,
			"my_prev_log_term", myPrevLogTerm,
		)
		rf.fillConflictReply(req, reply)
		return
	}

	if rf.processEntries(req) {
		needToPersist = true
	}

	if req.LeaderCommitIndex > rf.commitIdx {
		lastLogIndex, _ := rf.lastLogIdxAndTerm()
		rf.commitIdx = min(req.LeaderCommitIndex, lastLogIndex)
		shouldSignalApplier = true
	}

	reply.Success = true

	return
}

func (rf *Raft) InstallSnapshot(ctx context.Context,
	req *raftpb.InstallSnapshotRequest) (reply *raftpb.InstallSnapshotResponse, err error) {
	reply = &raftpb.InstallSnapshotResponse{}
	var needToPersist, shouldSignalApplier bool
	var snapshotData []byte

	rf.mu.Lock()
	defer func() {
		rf.unlockConditionally(needToPersist, snapshotData)
		if shouldSignalApplier {
			rf.signalApplier()
		}
	}()

	reply.Term = rf.curTerm
	if req.Term < rf.curTerm {
		return
	}

	if req.Term > rf.curTerm {
		needToPersist = rf.becomeFollower(req.Term)
	}

	rf.resetElectionTimer()

	rf.logger.Info(
		"installing snapshot",
		"leader_id", req.LeaderId,
		"last_included_index", req.LastIncludedIndex,
	)

	if req.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	needToPersist = true
	snapshotData = req.Data
	shouldSignalApplier = true

	sliceIndex := req.LastIncludedIndex - rf.lastIncludedIndex
	if sliceIndex < int64(len(rf.log)) && rf.getTerm(req.LastIncludedIndex) == req.LastIncludedTerm {
		rf.log = append([]*raftpb.LogEntry(nil), rf.log[sliceIndex:]...)
	} else {
		rf.log = nil
	}

	rf.lastIncludedIndex = req.LastIncludedIndex
	rf.lastIncludedTerm = req.LastIncludedTerm

	if rf.commitIdx < req.LastIncludedIndex {
		rf.commitIdx = req.LastIncludedIndex
	}

	return
}

func (rf *Raft) SubmitCommand(ctx context.Context, req *raftpb.SubmitRequest) (*raftpb.SubmitResponse, error) {
	index, term, isLeader := rf.Submit(req.Command)
	return &raftpb.SubmitResponse{
		Index:    index,
		Term:     term,
		IsLeader: isLeader,
	}, nil
}
