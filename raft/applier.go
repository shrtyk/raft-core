package raft

import (
	"log/slog"
	"time"
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
		case msg := <-rf.messagesChan:
			select {
			case rf.applyChan <- msg:
			case <-time.After(10 * time.Millisecond):
				rf.logger.Warn(
					"applyChan blocked, dropping apply",
					slog.Int("me", rf.me),
					slog.Int64("index", msg.CommandIndex),
				)
			}
		}
	}
}

func (rf *Raft) signalApplier() {
	select {
	case rf.signalMessagerChan <- struct{}{}:
	default:
	}
}
