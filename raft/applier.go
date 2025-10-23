package raft

import (
	"github.com/shrtyk/raft-core/api"
	"github.com/shrtyk/raft-core/pkg/logger"
)

// applies committed log entries to the state machine in the background
func (rf *Raft) applier() {
	defer func() {
		close(rf.applyChan)
		rf.wg.Done()
	}()

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

					rf.persisterMu.RLock()
					snapshot, err := rf.persister.ReadSnapshot()
					if err != nil {
						// TODO: better handling
						rf.logger.Warn("failed to read snapshot", logger.ErrAttr(err))
						rf.persisterMu.Unlock()
						continue
					}
					rf.persisterMu.Unlock()

					msg = api.ApplyMessage{
						SnapshotValid: true,
						Snapshot:      snapshot,
						SnapshotTerm:  rf.lastIncludedTerm,
						SnapshotIndex: rf.lastIncludedIndex,
					}
				} else {
					applyIdx := rf.lastAppliedIdx + 1
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
				case rf.applyChan <- &msg:
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
