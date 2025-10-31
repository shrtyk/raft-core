package raft

import (
	"fmt"
	"sync/atomic"

	"github.com/shrtyk/raft-core/api"
)

type State = uint32

const (
	_ State = iota
	follower
	candidate
	leader
)

// stateToString converts a State to its string representation.
func stateToString(s State) string {
	switch s {
	case follower:
		return "follower"
	case candidate:
		return "candidate"
	case leader:
		return "leader"
	default:
		return "unknown"
	}
}

func (rf *Raft) isState(state State) bool {
	return atomic.LoadUint32(&rf.state) == state
}

// becomeFollower transitions the peer to the follower state
// and return true if need to persist state.
//
// Assumes the lock is held when called
func (rf *Raft) becomeFollower(term int64) (needToPersist bool) {
	rf.logger.Info("transitioning to follower", "term", term)
	atomic.StoreUint32(&rf.state, follower)
	if term > rf.curTerm {
		rf.curTerm = term
		rf.votedFor = votedForNone
		needToPersist = true
	}
	rf.resetElectionTimer()
	return
}

// becomeLeader transitions the peer to the leader state
//
// Assumes the lock is held when called
func (rf *Raft) becomeLeader() {
	rf.logger.Info("transitioning to leader", "from_state", stateToString(rf.state), "term", rf.curTerm)
	atomic.StoreUint32(&rf.state, leader)
	rf.resetHeartbeatTicker()

	lastLogIdx, _ := rf.lastLogIdxAndTerm()
	for i := range rf.peersCount {
		rf.nextIdx[i] = lastLogIdx + 1
		rf.matchIdx[i] = 0
	}
	rf.matchIdx[rf.me] = lastLogIdx
}

// checkOrUpdateTerm validates the term from an RPC reply.
// It returns an error if the request's term is outdated. If the reply
// indicates a higher term, it transitions the node to a follower state.
//
// Assumes the lock is held when called.
func (rf *Raft) checkOrUpdateTerm(rpcCallName string, peerIdx int, reqTerm, replyTerm int64) error {
	if replyTerm > rf.curTerm {
		rf.becomeFollower(replyTerm)
		return fmt.Errorf("%w %s reply recieved from peer #%d.", api.ErrHigherTerm, rpcCallName, peerIdx)
	}

	if !rf.isState(leader) || rf.curTerm != reqTerm {
		return fmt.Errorf("%w Ignoring %s reply from peer #%d.", api.ErrOutdatedTerm, rpcCallName, peerIdx)
	}

	return nil
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}
