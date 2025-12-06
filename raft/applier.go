package raft

import (
	"github.com/shrtyk/raft-core/api"
	"github.com/shrtyk/raft-core/pkg/logger"
)

// applies committed log entries to the state machine in the background
func (rf *Raft) applier() {
	defer func() {
		rf.wg.Done()
		rf.logger.Info("applier exiting")
	}()

	rf.logger.Info("applier starting")
	for {
		select {
		case <-rf.raftCtx.Done():
			return
		case <-rf.signalApplierChan:
			for {
				rf.mu.RLock()
				if rf.lastAppliedIdx >= rf.commitIdx || rf.Killed() {
					rf.mu.RUnlock()
					break
				}

				var msg api.ApplyMessage
				shouldSendToApplyChan := false

				if rf.lastAppliedIdx < rf.log.lastIncludedIndex {
					rf.logger.Debug("applying snapshot to state machine", "index", rf.log.lastIncludedIndex)

					snapshot, err := rf.persister.ReadSnapshot()
					if err != nil {
						rf.logger.Warn("failed to read snapshot", logger.ErrAttr(err))
						rf.mu.RUnlock()
						continue
					}

					msg = api.ApplyMessage{
						SnapshotValid: true,
						Snapshot:      snapshot,
						SnapshotTerm:  rf.log.lastIncludedTerm,
						SnapshotIndex: rf.log.lastIncludedIndex,
					}
					shouldSendToApplyChan = true
				} else {
					applyIdx := rf.lastAppliedIdx + 1
					sliceIdx := applyIdx - rf.log.lastIncludedIndex - 1

					if rf.log.entries[sliceIdx].Cmd == nil {
						rf.logger.Debug("skipping no-op entry", "index", applyIdx)
						rf.lastAppliedIdx = applyIdx
						rf.mu.RUnlock()
						continue
					}

					rf.logger.Debug("applying command to state machine", "index", applyIdx)
					msg = api.ApplyMessage{
						CommandValid: true,
						Command:      rf.log.entries[sliceIdx].Cmd,
						CommandIndex: applyIdx,
					}
					shouldSendToApplyChan = true
				}
				rf.mu.RUnlock()

				if shouldSendToApplyChan {
					select {
					case <-rf.raftCtx.Done():
						return
					case rf.applyChan <- &msg:
					}
				}

				rf.mu.Lock()
				if msg.SnapshotValid {
					rf.lastAppliedIdx = max(rf.lastAppliedIdx, msg.SnapshotIndex)
				} else {
					rf.lastAppliedIdx = max(rf.lastAppliedIdx, msg.CommandIndex)
				}
				rf.mu.Unlock()
			}
		}
	}
}

func (rf *Raft) signalApplier() {
	select {
	case rf.signalApplierChan <- struct{}{}:
	default:
	}
}
