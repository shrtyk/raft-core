package raft

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
	"github.com/shrtyk/raft-core/pkg/logger"
)

// sendHeartbeats sends heartbeats to all peers to confirm leadership and returns
// true if a majority acknowledges the leader in the current term.
func (rf *Raft) ConfirmLeadership(ctx context.Context) bool {
	rf.mu.RLock()
	curTerm := rf.curTerm
	if !rf.isState(leader) {
		rf.mu.RUnlock()
		return false
	}
	rf.mu.RUnlock()

	acks := make(chan bool, rf.peersCount-1)
	for i := range rf.peersCount {
		if i == rf.me {
			continue
		}
		go func(peerIdx int) {
			rf.mu.RLock()
			// A heartbeat is an AppendEntries RPC with no log entries.
			req := &raftpb.AppendEntriesRequest{
				Term:              curTerm,
				LeaderId:          int64(rf.me),
				PrevLogIndex:      rf.nextIdx[peerIdx] - 1,
				PrevLogTerm:       rf.log.getTerm(rf.nextIdx[peerIdx] - 1),
				LeaderCommitIndex: rf.commitIdx,
				Entries:           nil,
			}
			rf.mu.RUnlock()

			tctx, tcancel := context.WithTimeout(ctx, rf.cfg.Timings.RPCTimeout)
			defer tcancel()

			reply, err := rf.transport.SendAppendEntries(tctx, peerIdx, req)
			if err != nil {
				rf.logger.Warn("failed to get heartbeat response from peer", "peer_id", peerIdx, logger.ErrAttr(err))
				acks <- false
				return
			}

			rf.mu.Lock()
			// If a peer has a higher term, we are no longer the leader.
			if reply.Term > rf.curTerm {
				rf.becomeFollower(reply.Term)
				if pErr := rf.persistAndUnlock(nil); pErr != nil {
					rf.handlePersistenceError("ConfirmLeadership", pErr)
				}
				acks <- false
				return
			}
			rf.mu.Unlock()
			acks <- true
		}(i)
	}

	confirmedAcks := 1 // Start with 1 for the leader itself.
	majority := rf.peersCount/2 + 1
	for range rf.peersCount - 1 {
		select {
		case <-ctx.Done():
			return false // Timeout.
		case ack := <-acks:
			if ack {
				confirmedAcks++
			}
			if confirmedAcks >= majority {
				rf.mu.Lock()
				rf.lastHeartbeatMajorityTime = time.Now()
				rf.mu.Unlock()
				return true
			}
		}
	}
	return confirmedAcks >= majority
}

// sendSnapshotOrEntries is invoked by the leader to replicate its state to all peers
func (rf *Raft) sendSnapshotOrEntries() {
	rf.mu.Lock() // Берем Write Lock сразу, чтобы проверить/обновить время
	if time.Since(rf.lastReplicationTime) < 10*time.Millisecond {
		rf.mu.Unlock()
		return
	}
	rf.lastReplicationTime = time.Now()
	curTerm := rf.curTerm
	isLeader := rf.isState(leader)
	rf.mu.Unlock()

	if !isLeader {
		return
	}

	for i := range rf.peersCount {
		if i == rf.me {
			continue
		}
		go func(peerIdx int) {
			rf.mu.RLock()
			if rf.curTerm != curTerm || !rf.isState(leader) {
				rf.mu.RUnlock()
				return
			}

			if !rf.transport.IsPeerAvailable(peerIdx) {
				rf.mu.RUnlock()
				return
			}

			needSnapshot := rf.nextIdx[peerIdx] <= rf.log.lastIncludedIndex
			var err error
			if needSnapshot {
				err = rf.leaderSendSnapshot(peerIdx)
			} else {
				err = rf.leaderSendEntries(peerIdx)
			}

			if err != nil {
				rf.logger.Debug("failed to replication", slog.Int("peer_id", peerIdx), logger.ErrAttr(err))
			}
		}(i)
	}
}

// leaderSendEntries handles sending log entries to a single peer
//
// Assumes the lock is held when called
func (rf *Raft) leaderSendEntries(peerIdx int) error {
	prevLogIdx := rf.nextIdx[peerIdx] - 1
	prevLogTerm := rf.log.getTerm(prevLogIdx)

	sliceIndex := rf.nextIdx[peerIdx] - rf.log.lastIncludedIndex - 1
	entries := make([]*raftpb.LogEntry, len(rf.log.entries[sliceIndex:]))
	copy(entries, rf.log.entries[sliceIndex:])

	args := &raftpb.AppendEntriesRequest{
		Term:              rf.curTerm,
		LeaderId:          int64(rf.me),
		PrevLogIndex:      prevLogIdx,
		PrevLogTerm:       prevLogTerm,
		LeaderCommitIndex: rf.commitIdx,
		Entries:           entries,
	}
	rf.mu.RUnlock()

	tctx, tcancel := context.WithTimeout(rf.raftCtx, rf.cfg.Timings.RPCTimeout)
	defer tcancel()

	reply, err := rf.transport.SendAppendEntries(tctx, peerIdx, args)
	if err != nil {
		return fmt.Errorf("failed to send AppendEntries to peer #%d: %w", peerIdx, err)
	}

	return rf.processAppendEntriesReply(peerIdx, args, reply)
}

// processAppendEntriesReply processes the reply from an AppendEntries RPC
func (rf *Raft) processAppendEntriesReply(peerIdx int, req *raftpb.AppendEntriesRequest, reply *raftpb.AppendEntriesResponse) error {
	rf.mu.Lock()

	if err := rf.checkOrUpdateTerm("AppendEntries", peerIdx, req.Term, reply.Term); err != nil {
		if pErr := rf.persistAndUnlock(nil); pErr != nil {
			rf.handlePersistenceError("AppendEntriesReply", pErr)
		}
		return err
	}

	if reply.Success {
		newMatchIdx := req.PrevLogIndex + int64(len(req.Entries))
		if newMatchIdx > rf.matchIdx[peerIdx] {
			rf.matchIdx[peerIdx] = newMatchIdx
		}
		rf.nextIdx[peerIdx] = rf.matchIdx[peerIdx] + 1

		lastCommitIdx := rf.commitIdx
		rf.tryToCommit()
		if rf.commitIdx != lastCommitIdx {
			rf.mu.Unlock()
			rf.signalApplier()
		} else {
			rf.mu.Unlock()
		}
		return nil
	}

	rf.updateNextIndexAfterConflict(peerIdx, reply)
	rf.mu.Unlock()
	return nil
}

// updateNextIndexAfterConflict is a helper function to update a follower's nextIdx
// after a failed AppendEntries RPC
//
// Assumes the lock is held when called.
func (rf *Raft) updateNextIndexAfterConflict(peerIdx int, reply *raftpb.AppendEntriesResponse) {
	if reply.ConflictTerm < 0 {
		rf.nextIdx[peerIdx] = reply.ConflictIndex
		return
	}

	lastLogIdx, _ := rf.log.lastLogIdxAndTerm()
	for i := lastLogIdx; i > rf.log.lastIncludedIndex; i-- {
		if rf.log.getTerm(i) == reply.ConflictTerm {
			rf.nextIdx[peerIdx] = i + 1
			return
		}
	}
	rf.nextIdx[peerIdx] = reply.ConflictIndex
}

// tryToCommit updating leader commit index
// if majority of peers got higher commit index
//
// Assumes the lock is held when called.
func (rf *Raft) tryToCommit() {
	matchIdxCopy := make([]int64, len(rf.matchIdx))
	copy(matchIdxCopy, rf.matchIdx)

	slices.Sort(matchIdxCopy)
	majorityIdx := rf.peersCount / 2
	newCommitIdx := matchIdxCopy[majorityIdx]

	if newCommitIdx > rf.commitIdx && rf.log.getTerm(newCommitIdx) == rf.curTerm {
		rf.logger.Debug(
			"advancing commit index",
			"old_commit_idx", rf.commitIdx,
			"new_commit_idx", newCommitIdx,
		)
		rf.commitIdx = newCommitIdx
		rf.lastHeartbeatMajorityTime = time.Now()
	}
}

// leaderSendSnapshot handles sending a snapshot to a single peer
//
// Assumes the lock is held when called
func (rf *Raft) leaderSendSnapshot(peerIdx int) error {
	snapshot, err := rf.persister.ReadSnapshot()
	if err != nil {
		return fmt.Errorf("failed to read snapshot: %w", err)
	}

	req := &raftpb.InstallSnapshotRequest{
		Term:              rf.curTerm,
		LeaderId:          int64(rf.me),
		LastIncludedIndex: rf.log.lastIncludedIndex,
		LastIncludedTerm:  rf.log.lastIncludedTerm,
		Data:              snapshot,
	}
	rf.mu.RUnlock()

	tctx, tcancel := context.WithTimeout(rf.raftCtx, rf.cfg.Timings.RPCTimeout)
	defer tcancel()

	reply, err := rf.transport.SendInstallSnapshot(tctx, peerIdx, req)
	if err != nil {
		return fmt.Errorf("failed to send InstallSnapshot to peer #%d: %v", peerIdx, err)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if err := rf.checkOrUpdateTerm("InstallSnapshot", peerIdx, req.Term, reply.Term); err != nil {
		return err
	}

	rf.matchIdx[peerIdx] = max(rf.matchIdx[peerIdx], req.LastIncludedIndex)
	rf.nextIdx[peerIdx] = rf.matchIdx[peerIdx] + 1
	return nil
}
