package raft

// sendSnapshotOrEntries is invoked by the leader to replicate its state to all peers
func (rf *Raft) sendSnapshotOrEntries() {
	rf.mu.RLock()
	curTerm := rf.curTerm
	rf.mu.RUnlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peerIdx int) {
			rf.mu.RLock()
			if rf.curTerm != curTerm || !rf.isState(leader) {
				rf.mu.RUnlock()
				return
			}

			if rf.nextIdx[peerIdx] <= rf.lastIncludedIndex {
				rf.leaderSendSnapshot(peerIdx)
			} else {
				rf.leaderSendEntries(peerIdx)
			}
		}(i)
	}
}
