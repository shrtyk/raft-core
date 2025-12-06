package raft

import (
	"log/slog"
	"time"

	"github.com/shrtyk/raft-core/pkg/logger"
)

// snapshotter is a background goroutine
// that periodically checks if a snapshot needs to be taken.
func (rf *Raft) snapshotter() {
	defer func() {
		rf.wg.Done()
		rf.logger.Info("snapshotter exiting")
	}()

	rf.checkAndTakeSnapshot()

	ticker := time.NewTicker(rf.cfg.Snapshots.CheckLogSizeInterval)
	defer ticker.Stop()

	rf.logger.Info("snapshotter starting")
	for {
		select {
		case <-rf.raftCtx.Done():
			return
		case <-ticker.C:
			rf.checkAndTakeSnapshot()
		}
	}
}

func (rf *Raft) checkAndTakeSnapshot() {
	rf.mu.RLock()
	size := rf.log.logSizeInBytes
	rf.mu.RUnlock()

	if size < rf.cfg.Snapshots.ThresholdBytes {
		return
	}

	rf.logger.Info(
		"log size exceeds threshold, requesting snapshot from FSM",
		slog.Int("size", size),
		slog.Int("threshold", rf.cfg.Snapshots.ThresholdBytes))

	snapshotData, lastApplied, err := rf.fsm.Snapshot()
	if err != nil {
		rf.logger.Warn("failed to get snapshot from FSM", logger.ErrAttr(err))
		return
	}

	if err := rf.Snapshot(lastApplied, snapshotData); err != nil {
		rf.logger.Warn("failed to apply snapshot", logger.ErrAttr(err))
	}
}
