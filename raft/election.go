package raft

import (
	"log"
	"time"

	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
)

// startElection begins a new election
func (rf *Raft) startElection() {
	timeout := rf.randElectionInterval()

	rf.mu.Lock()
	rf.curTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimer()
	lastLogIdx, lastLogTerm := rf.lastLogIdxAndTerm()
	currentTerm := rf.curTerm

	rf.persistAndUnlock(nil)

	repliesChan := make(chan *raftpb.RequestVoteResponse, len(rf.peers)-1)
	args := &raftpb.RequestVoteRequest{
		Term:         currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIdx,
		LastLogTerm:  lastLogTerm,
	}
	for i := range rf.peers {
		if i == int(rf.me) {
			continue
		}
		go func(idx int) {
			reply, err := rf.sendRequestVoteRPC(idx, args)
			if err != nil {
				// TODO: better handling
				log.Printf("failed to get response from server #%d", idx)
				return
			}
			repliesChan <- reply
		}(i)
	}

	rf.countVotes(timeout, repliesChan)
}

func (rf *Raft) countVotes(timeout time.Duration, repliesChan <-chan *raftpb.RequestVoteResponse) {
	votes := make([]bool, len(rf.peers))
	votes[rf.me] = true

	for {
		select {
		case <-time.After(timeout):
			return
		case reply := <-repliesChan:
			rf.mu.Lock()
			if reply.Term > rf.curTerm {
				rf.becomeFollower(reply.Term)
				rf.resetElectionTimer()
				rf.mu.Unlock()
				return
			} else if reply.VoteGranted && rf.isState(candidate) {
				votes[reply.VoterId] = true
				if rf.isEnoughVotes(votes) {
					rf.becomeLeader()
					rf.mu.Unlock()
					rf.sendAppendEntries()
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
	return vc > len(rf.peers)/2
}
