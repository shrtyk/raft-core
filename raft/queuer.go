package raft

import (
	"github.com/shrtyk/raft-core/api"
	"github.com/shrtyk/raft-core/pkg/logger"
)

// Background worker which prepare messages to be sent to FSM
func (rf *Raft) queuer() {
	defer func() {
		rf.logger.Info("queuer exiting")
		rf.wg.Done()
	}()
	rf.logger.Info("queuer starting")

	for {
		select {
		case <-rf.raftCtx.Done():
			return
		case <-rf.signalApplierChan:
			for {
				rf.mu.RLock()
				if rf.lastAppliedIdx >= rf.commitIdx || rf.killed() {
					rf.mu.RUnlock()
					break
				}

				var msg api.ApplyMessage
				if rf.lastAppliedIdx < rf.lastIncludedIndex {
					rf.logger.Info("applying snapshot to state machine", "index", rf.lastIncludedIndex)

					snapshot, err := rf.persister.ReadSnapshot()
					if err != nil {
						rf.logger.Warn("failed to read snapshot", logger.ErrAttr(err))
						continue
					}

					msg = api.ApplyMessage{
						SnapshotValid: true,
						Snapshot:      snapshot,
						SnapshotTerm:  rf.lastIncludedTerm,
						SnapshotIndex: rf.lastIncludedIndex,
					}
				} else {
					applyIdx := rf.lastAppliedIdx + 1
					rf.logger.Debug("applying command to state machine", "index", applyIdx)
					sliceIdx := applyIdx - rf.lastIncludedIndex - 1
					msg = api.ApplyMessage{
						CommandValid: true,
						Command:      rf.log[sliceIdx].Cmd,
						CommandIndex: applyIdx,
					}
				}
				rf.mu.RUnlock()

				select {
				case <-rf.raftCtx.Done():
					return
				case rf.messagesChan <- &msg:
				default:
				}

				if msg.SnapshotValid {
					rf.lastAppliedIdx = max(rf.lastAppliedIdx, msg.SnapshotIndex)
				} else {
					rf.lastAppliedIdx = max(rf.lastAppliedIdx, msg.CommandIndex)
				}
			}
		}
	}
}
