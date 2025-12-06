package raft

import (
	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
)

// raftLog is a component of a Raft node that manages the Raft log.
// It is not safe for concurrent access; the parent Raft node is responsible for synchronization.
type raftLog struct {
	entries           []*raftpb.LogEntry
	lastIncludedIndex int64
	lastIncludedTerm  int64
	logSizeInBytes    int
}

func newLog(lastIncludedIndex, lastIncludedTerm int64, entries []*raftpb.LogEntry) *raftLog {
	rl := &raftLog{
		entries:           entries,
		lastIncludedIndex: lastIncludedIndex,
		lastIncludedTerm:  lastIncludedTerm,
	}
	rl.logSizeInBytes = rl.calculateLogSizeInBytes()
	return rl
}

// getTerm returns the term of a log entry at a given absolute index.
// It handles cases where the index is part of a snapshot.
func (rl *raftLog) getTerm(idx int64) int64 {
	if idx == rl.lastIncludedIndex {
		return rl.lastIncludedTerm
	}

	if idx < rl.lastIncludedIndex {
		return -1
	}

	sliceIndex := idx - rl.lastIncludedIndex - 1
	if sliceIndex < 0 || sliceIndex >= int64(len(rl.entries)) {
		return -1
	}
	return rl.entries[sliceIndex].Term
}

// lastLogIdxAndTerm returns the index and term of the last entry in the log.
func (rl *raftLog) lastLogIdxAndTerm() (lastLogIdx, lastLogTerm int64) {
	if len(rl.entries) > 0 {
		lastLogIdx = rl.lastIncludedIndex + int64(len(rl.entries))
		lastLogTerm = rl.entries[len(rl.entries)-1].Term
	} else {
		lastLogIdx = rl.lastIncludedIndex
		lastLogTerm = rl.lastIncludedTerm
	}
	return
}

// isLogConsistent is a helper function that checks if the log is consistent
// with the leader's AppendEntries request at a given index and term.
func (rl *raftLog) isLogConsistent(prevLogIdx int64, prevLogTerm int64) bool {
	lastLogIdx, _ := rl.lastLogIdxAndTerm()
	if prevLogIdx > lastLogIdx {
		return false
	}
	return rl.getTerm(prevLogIdx) == prevLogTerm
}

// processEntries handles appending/truncating entries to the follower's log.
// It returns a boolean indicating if truncation occurred, and a slice of the entries that were appended.
func (rl *raftLog) processEntries(req *raftpb.AppendEntriesRequest) (didTruncate bool, appendedEntries []*raftpb.LogEntry) {
	for i, entry := range req.Entries {
		absIdx := req.PrevLogIndex + 1 + int64(i)
		lastAbsIdx, _ := rl.lastLogIdxAndTerm()

		if absIdx > lastAbsIdx {
			appendedEntries = req.Entries[i:]
			rl.entries = append(rl.entries, appendedEntries...)
			for _, e := range appendedEntries {
				rl.logSizeInBytes += len(e.Cmd)
			}
			return false, appendedEntries
		}

		if rl.getTerm(absIdx) != entry.Term {
			sliceIdx := absIdx - rl.lastIncludedIndex - 1
			rl.entries = rl.entries[:sliceIdx]
			appendedEntries = req.Entries[i:]
			rl.entries = append(rl.entries, appendedEntries...)
			rl.logSizeInBytes = rl.calculateLogSizeInBytes()
			return true, appendedEntries
		}
	}
	return false, nil
}

// fillConflictReply sets the conflict fields in an AppendEntries reply.
func (rl *raftLog) fillConflictReply(req *raftpb.AppendEntriesRequest, reply *raftpb.AppendEntriesResponse) {
	lastLogIdx, _ := rl.lastLogIdxAndTerm()
	if req.PrevLogIndex > lastLogIdx {
		reply.ConflictIndex = lastLogIdx + 1
		reply.ConflictTerm = -1
	} else {
		reply.ConflictTerm = rl.getTerm(req.PrevLogIndex)
		firstIndexOfTerm := req.PrevLogIndex
		for firstIndexOfTerm > rl.lastIncludedIndex+1 && rl.getTerm(firstIndexOfTerm-1) == reply.ConflictTerm {
			firstIndexOfTerm--
		}
		reply.ConflictIndex = firstIndexOfTerm
	}
}

// isCandidateLogUpToDate determines if the candidate's log is at least as up-to-date as receiver's log.
func (rl *raftLog) isCandidateLogUpToDate(candidateLastLogIdx int64, candidateLastLogTerm int64) bool {
	myLastLogIdx, myLastLogTerm := rl.lastLogIdxAndTerm()
	if candidateLastLogTerm != myLastLogTerm {
		return candidateLastLogTerm > myLastLogTerm
	}
	return candidateLastLogIdx >= myLastLogIdx
}

func (rl *raftLog) append(entry *raftpb.LogEntry) {
	rl.entries = append(rl.entries, entry)
	rl.logSizeInBytes += len(entry.Cmd)
}

// compact truncates the log up to a given index, which is now covered by a snapshot.
func (rl *raftLog) compact(index int64, term int64) {
	if rl.getTerm(index) == term {
		sliceIndex := index - rl.lastIncludedIndex
		// The snapshot index must be within the bounds of the current log.
		if sliceIndex >= 0 && sliceIndex <= int64(len(rl.entries)) {
			rl.entries = append([]*raftpb.LogEntry(nil), rl.entries[sliceIndex:]...)
		} else {
			// If the index is outside the log.
			rl.entries = nil
		}
	} else {
		rl.entries = nil
	}

	rl.lastIncludedIndex = index
	rl.lastIncludedTerm = term
	rl.logSizeInBytes = rl.calculateLogSizeInBytes()
}

func (rl *raftLog) calculateLogSizeInBytes() int {
	size := 0
	for _, entry := range rl.entries {
		size += len(entry.Cmd)
	}
	return size
}
