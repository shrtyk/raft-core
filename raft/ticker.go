package raft

import (
	"math/rand"
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
			rf.logger.Debug("election timer fired, attempting to start election")
			rf.mu.Lock()
			if rf.isState(leader) {
				rf.mu.Unlock()
				continue
			}

			if rf.electionDone != nil {
				close(rf.electionDone)
			}
			rf.electionDone = make(chan struct{})

			rf.resetElectionTimer()
			go rf.startElection(rf.electionDone)
			rf.mu.Unlock()
		case <-rf.heartbeatTicker.C:
			if rf.isState(leader) {
				rf.sendSnapshotOrEntries()
			}
		}
	}
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
	rf.heartbeatTicker.Reset(rf.cfg.Timings.HeartbeatTimeout)
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
	rf.electionTimer.Reset(rf.randElectionInterval())
}

func (rf *Raft) randElectionInterval() time.Duration {
	return rf.cfg.Timings.ElectionTimeoutBase + time.Duration(rand.Int63n(int64(rf.cfg.Timings.ElectionTimeoutRandomDelta)))
}
