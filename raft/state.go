package raft

import (
	"math/rand"
	"sync/atomic"
	"time"
)

// ticker is the main state machine loop for a Raft peer
func (rf *Raft) ticker() {
	defer func() {
		rf.heartbeatTicker.Stop()
		rf.electionTimer.Stop()
		rf.wg.Done()
	}()

	for {
		select {
		case <-rf.raftCtx.Done():
			return
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if !rf.isState(leader) {
				atomic.StoreUint32(&rf.state, candidate)
				go rf.startElection()
			}
			rf.mu.Unlock()
		case <-rf.heartbeatTicker.C:
			if rf.isState(leader) {
				rf.sendAppendEntries()
			}
		}
	}
}

func (rf *Raft) isState(state State) bool {
	return atomic.LoadUint32(&rf.state) == state
}

// becomeFollower transitions the peer to the follower state and return true if need to persist state
//
// Assumes the lock is held when called
func (rf *Raft) becomeFollower(term int64) (needToPersist bool) {
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
	atomic.StoreUint32(&rf.state, leader)
	rf.resetHeartbeatTicker()

	lastLogIdx, _ := rf.lastLogIdxAndTerm()
	for i := range rf.peers {
		rf.nextIdx[i] = lastLogIdx + 1
		rf.matchIdx[i] = 0
	}
	rf.matchIdx[rf.me] = lastLogIdx
}

// activateLeaderTimers stops election timer and starts heartbeat ticker
func (rf *Raft) resetHeartbeatTicker() {
	rf.timerMu.Lock()
	defer rf.timerMu.Unlock()
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
	rf.heartbeatTicker.Reset(defaultHeartbeatInterval)
}

// resetElectionTimer stops heartbeat ticker and resets election timer
func (rf *Raft) resetElectionTimer() {
	rf.timerMu.Lock()
	defer rf.timerMu.Unlock()
	rf.heartbeatTicker.Stop()
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
	rf.electionTimer.Reset(randElectionIntervalMs())
}

func randElectionIntervalMs() time.Duration {
	return defaultElectionTimeoutBase + time.Duration(rand.Int63n(int64(defaultElectionTimeoutRand)))
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}
