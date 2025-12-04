package raft

import (
	"context"
	"sync/atomic"

	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
	"github.com/shrtyk/raft-core/pkg/logger"
)

// startElection begins the leader election process for a new term
func (rf *Raft) startElection(electionDone chan struct{}) {
	rf.mu.Lock()
	atomic.StoreUint32(&rf.state, candidate)
	rf.curTerm++
	rf.logger.Info("starting election", "term", rf.curTerm)
	rf.votedFor = int64(rf.me)
	lastLogIdx, lastLogTerm := rf.lastLogIdxAndTerm()
	electionTerm := rf.curTerm

	rf.persistAndUnlock(nil)

	// Buffered channel to collect replies without blocking
	repliesChan := make(chan *raftpb.RequestVoteResponse, rf.peersCount-1)
	args := &raftpb.RequestVoteRequest{
		Term:         electionTerm,
		CandidateId:  int64(rf.me),
		LastLogIndex: lastLogIdx,
		LastLogTerm:  lastLogTerm,
	}

	// Send RequestVote RPCs in parallel to all peers
	for i := range rf.peersCount {
		if i == int(rf.me) {
			continue
		}
		go func(idx int) {
			tctx, tcancel := context.WithTimeout(rf.raftCtx, rf.cfg.Timings.RPCTimeout)
			defer tcancel()

			reply, err := rf.transport.SendRequestVote(tctx, idx, args)
			if err != nil {
				rf.logger.Warn("failed to get vote response from peer", "peer_id", idx, logger.ErrAttr(err))
				return
			}
			repliesChan <- reply
		}(i)
	}

	rf.countVotes(repliesChan, electionTerm, electionDone)
}

// countVotes collects RequestVote responses until timeout or majority is reached.
// It steps down on higher-term replies.
func (rf *Raft) countVotes(repliesChan <-chan *raftpb.RequestVoteResponse, electionTerm int64, electionDone chan struct{}) {
	votes := make([]bool, rf.peersCount)
	votes[rf.me] = true

	for {
		select {
		case <-electionDone:
			rf.logger.Debug("election cancelled")
			return
		case reply := <-repliesChan:
			rf.mu.Lock()
			rf.logger.Debug("received vote reply", "voter", reply.VoterId, "granted", reply.VoteGranted, "term", reply.Term)

			// Step down if reply term is newer
			if reply.Term > rf.curTerm {
				rf.becomeFollower(reply.Term)
				if pErr := rf.persistAndUnlock(nil); pErr != nil {
					rf.handlePersistenceError("countVotes", pErr)
				}
				return
			}

			// Ignore outdated election responses
			if rf.curTerm != electionTerm {
				rf.mu.Unlock()
				return
			}

			// Count granted votes only if still candidate
			if reply.VoteGranted && rf.isState(candidate) {
				rf.logger.Debug("vote granted", "voter_id", reply.VoterId)
				votes[reply.VoterId] = true
				if rf.isEnoughVotes(votes) {
					rf.becomeLeader()
					rf.mu.Unlock()
					rf.sendSnapshotOrEntries()
					return
				}
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) isEnoughVotes(votes []bool) bool {
	var vc int
	for _, voted := range votes {
		if voted {
			vc++
		}
	}
	return vc > rf.peersCount/2
}
