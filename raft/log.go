package raft

import raftpb "github.com/shrtyk/raft-core/internal/proto/gen"

// getTerm returns the term of a log entry at a given absolute index.
// It handles cases where the index is part of a snapshot
//
// Assumes the lock is held when called
func (rf *Raft) getTerm(idx int64) int64 {
	if idx == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}

	if idx < rf.lastIncludedIndex {
		return -1
	}

	sliceIndex := idx - rf.lastIncludedIndex - 1
	if sliceIndex >= int64(len(rf.log)) {
		return -1
	}
	return rf.log[sliceIndex].Term
}

// lastLogIdxAndTerm returns the index and term of the last entry in the log
//
// Assumes the lock is held when called
func (rf *Raft) lastLogIdxAndTerm() (lastLogIdx, lastLogTerm int64) {
	if len(rf.log) > 0 {
		lastLogIdx = rf.lastIncludedIndex + int64(len(rf.log))
		lastLogTerm = rf.log[len(rf.log)-1].Term
	} else {
		lastLogIdx = rf.lastIncludedIndex
		lastLogTerm = rf.lastIncludedTerm
	}
	return
}

// isLogConsistent is a helper function that checks if the log is consistent
// with the leader's AppendEntries request at a given index and term.
//
// Assumes the lock is held when called.
func (rf *Raft) isLogConsistent(prevLogIdx int64, prevLogTerm int64) bool {
	lastLogIdx, _ := rf.lastLogIdxAndTerm()
	if prevLogIdx > lastLogIdx {
		return false
	}
	return rf.getTerm(prevLogIdx) == prevLogTerm
}

// processEntries handles appending/truncating entries to the follower's log
// and returns true if there is need to persist
//
// Assumes the lock is held when called
func (rf *Raft) processEntries(req *raftpb.AppendEntriesRequest) (needToPersist bool) {
	for i, entry := range req.Entries {
		absIdx := req.PrevLogIndex + 1 + int64(i)
		lastAbsIdx, _ := rf.lastLogIdxAndTerm()
		if absIdx > lastAbsIdx {
			rf.log = append(rf.log, req.Entries[i:]...)
			needToPersist = true
			break
		}

		if rf.getTerm(absIdx) != entry.Term {
			sliceIdx := absIdx - rf.lastIncludedIndex - 1
			rf.log = rf.log[:sliceIdx]
			rf.log = append(rf.log, req.Entries[i:]...)
			needToPersist = true
			break
		}
	}
	return
}

// fillConflictReply sets the conflict fields in an AppendEntries reply
//
// Assumes the lock is held when called
func (rf *Raft) fillConflictReply(req *raftpb.AppendEntriesRequest, reply *raftpb.AppendEntriesResponse) {
	lastLogIdx, _ := rf.lastLogIdxAndTerm()
	if req.PrevLogIndex > lastLogIdx {
		reply.ConflictIndex = lastLogIdx + 1
		reply.ConflictTerm = -1
	} else {
		reply.ConflictTerm = rf.getTerm(req.PrevLogIndex)
		firstIndexOfTerm := req.PrevLogIndex
		for firstIndexOfTerm > rf.lastIncludedIndex+1 && rf.getTerm(firstIndexOfTerm-1) == reply.ConflictTerm {
			firstIndexOfTerm--
		}
		reply.ConflictIndex = firstIndexOfTerm
	}
}

// isCandidateLogUpToDate determines if the candidate's log is at least as up-to-date as receiver's log
//
// Assumes the lock is held when called
func (rf *Raft) isCandidateLogUpToDate(candidateLastLogIdx int64, candidateLastLogTerm int64) bool {
	myLastLogIdx, myLastLogTerm := rf.lastLogIdxAndTerm()
	if candidateLastLogTerm != myLastLogTerm {
		return candidateLastLogTerm > myLastLogTerm
	}
	return candidateLastLogIdx >= myLastLogIdx
}

// initializeNextIndexes initializes indexes based on the current log state.
//
// If for some reason it used outside of Start function the mutex should be locked.
func (rf *Raft) initializeNextIndexes() {
	lastLogIdx, _ := rf.lastLogIdxAndTerm()
	for i := range rf.nextIdx {
		rf.nextIdx[i] = lastLogIdx + 1
	}
}

// Assumes the lock is held when called, unless it is the raft.Start() function
func (rf *Raft) calculateLogSizeInBytes() int {
	size := 0
	for _, entry := range rf.log {
		size += len(entry.Cmd)
	}
	return size
}
