package raft

import (
	"log/slog"

	"github.com/shrtyk/raft-core/pkg/logger"
)

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

			var err error
			if rf.nextIdx[peerIdx] <= rf.lastIncludedIndex {
				err = rf.leaderSendSnapshot(peerIdx)
			} else {
				err = rf.leaderSendEntries(peerIdx)
			}

			if err != nil {
				rf.logger.Info("failed to send gRPC call", slog.Int("peer_id", peerIdx), logger.ErrAttr(err))
			}
		}(i)
	}
}
