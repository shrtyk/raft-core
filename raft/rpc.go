package raft

import (
	"context"

	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
	"google.golang.org/protobuf/proto"
)

func (rf *Raft) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (reply *raftpb.RequestVoteResponse, err error) {
	reply = &raftpb.RequestVoteResponse{}
	var persistMetadata bool

	rf.mu.Lock()
	defer func() {
		if persistMetadata {
			if pErr := rf.persistMetadataAndUnlock(); pErr != nil {
				rf.handlePersistenceError("RequestVote", pErr)
			}
		} else {
			rf.mu.Unlock()
		}
	}()

	reply.VoteGranted = false
	reply.VoterId = int64(rf.me)

	if req.Term < rf.curTerm {
		reply.Term = rf.curTerm
		return
	}

	if req.Term > rf.curTerm {
		rf.becomeFollower(req.Term)
		persistMetadata = true
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
	persistMetadata = true
	rf.resetElectionTimer()
	rf.logger.Info(
		"voting for candidate",
		"candidate_id", req.CandidateId,
		"term", rf.curTerm,
	)

	return
}

func (rf *Raft) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (reply *raftpb.AppendEntriesResponse, err error) {
	reply = &raftpb.AppendEntriesResponse{}
	rf.mu.Lock()

	if req.Term < rf.curTerm {
		reply.Term = rf.curTerm
		rf.mu.Unlock()
		return
	}

	if len(req.Entries) > 0 {
		rf.logger.Debug("append entries received", "leader_id", req.LeaderId, "term", req.Term, "num_entries", len(req.Entries))
	}

	rf.resetElectionTimer()

	termChanged := false
	if req.Term > rf.curTerm || rf.isState(candidate) {
		rf.becomeFollower(req.Term)
		termChanged = true
	}
	rf.leaderId = int(req.LeaderId)
	reply.Term = rf.curTerm

	if !rf.isLogConsistent(req.PrevLogIndex, req.PrevLogTerm) {
		rf.fillConflictReply(req, reply)
		if termChanged {
			stateCopy := rf.getPersistentStateBytes()
			rf.mu.Unlock()
			if err := rf.persister.SaveStateAndSnapshot(stateCopy, nil); err != nil {
				rf.handlePersistenceError("AppendEntries-inconsistent", err)
			}
		} else {
			rf.mu.Unlock()
		}
		return
	}

	didTruncate, appendedEntries := rf.processEntries(req)

	var shouldSignalApplier bool
	if req.LeaderCommitIndex > rf.commitIdx {
		lastLogIndex, _ := rf.lastLogIdxAndTerm()
		rf.commitIdx = min(req.LeaderCommitIndex, lastLogIndex)
		shouldSignalApplier = true
	}

	var persistOp func() error
	switch {
	case didTruncate || termChanged:
		stateCopy := rf.getPersistentStateBytes()
		persistOp = func() error { return rf.persister.SaveStateAndSnapshot(stateCopy, nil) }
	case len(appendedEntries) > 0:
		entriesCopy := make([]*raftpb.LogEntry, len(appendedEntries))
		for i, e := range appendedEntries {
			entriesCopy[i] = proto.Clone(e).(*raftpb.LogEntry)
		}
		persistOp = func() error { return rf.persister.AppendEntries(entriesCopy) }
	}

	rf.mu.Unlock()

	if persistOp != nil {
		if perr := persistOp(); perr != nil {
			rf.handlePersistenceError("AppendEntries", perr)
			return
		}
	}

	if shouldSignalApplier {
		rf.signalApplier()
	}

	reply.Success = true
	return
}

func (rf *Raft) InstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotRequest) (reply *raftpb.InstallSnapshotResponse, err error) {
	reply = &raftpb.InstallSnapshotResponse{}
	var needToPersist, shouldSignalApplier bool
	var snapshotData []byte

	rf.mu.Lock()
	defer func() {
		if pErr := rf.unlockConditionally(needToPersist, snapshotData); pErr != nil {
			rf.handlePersistenceError("InstallSnapshot", pErr)
			return
		}
		if shouldSignalApplier {
			rf.signalApplier()
		}
	}()

	reply.Term = rf.curTerm
	if req.Term < rf.curTerm {
		return
	}

	rf.leaderId = int(req.LeaderId)
	if req.Term > rf.curTerm {
		rf.becomeFollower(req.Term)
		needToPersist = true
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
