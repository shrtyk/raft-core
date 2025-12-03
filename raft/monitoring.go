package raft

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/shrtyk/raft-core/pkg/logger"
)

// MonitoringServer defines the interface for the HTTP monitoring server.
type MonitoringServer interface {
	Start() error
	Stop() error
}

// httpMonitoringServer implements the MonitoringServer interface.
type httpMonitoringServer struct {
	rf     *Raft
	server *http.Server
}

// NewMonitoringServer creates a new HTTP monitoring server.
func NewMonitoringServer(rf *Raft) MonitoringServer {
	mux := http.NewServeMux()
	mux.Handle("/status", &statusHandler{rf: rf})

	return &httpMonitoringServer{
		rf: rf,
		server: &http.Server{
			Addr:    rf.cfg.HttpMonitoringAddr,
			Handler: mux,
		},
	}
}

// Start starts the HTTP server.
func (s *httpMonitoringServer) Start() error {
	s.rf.logger.Info("starting monitoring server", slog.String("addr", s.rf.cfg.HttpMonitoringAddr))

	s.rf.wg.Go(func() {
		if err := s.server.ListenAndServe(); err != http.ErrServerClosed {
			s.rf.logger.Error("monitoring server failed", logger.ErrAttr(err))
		}
	})

	return nil
}

// Stop gracefully stops the HTTP server.
func (s *httpMonitoringServer) Stop() error {
	if s.server != nil {
		return s.server.Close()
	}
	return nil
}

type status struct {
	NodeID      int    `json:"nodeId"`
	State       string `json:"state"`
	CurrentTerm int64  `json:"currentTerm"`
	VotedFor    int64  `json:"votedFor"`
	CommitIndex int64  `json:"commitIndex"`
	LastApplied int64  `json:"lastApplied"`

	LogInfo struct {
		LastIndex int64 `json:"lastIndex"`
		LastTerm  int64 `json:"lastTerm"`
		Count     int   `json:"count"`
	} `json:"logInfo"`

	SnapshotInfo struct {
		LastIncludedIndex int64 `json:"lastIncludedIndex"`
		LastIncludedTerm  int64 `json:"lastIncludedTerm"`
	} `json:"snapshotInfo"`

	LeaderSpecific *leaderSpecificStatus `json:"leaderSpecific,omitempty"`
}

type leaderSpecificStatus struct {
	PeerReplicationInfo map[string]peerReplicationInfo `json:"peerReplicationInfo"`
}

type peerReplicationInfo struct {
	MatchIndex int64 `json:"matchIndex"`
	NextIndex  int64 `json:"nextIndex"`
}

type statusHandler struct {
	rf *Raft
}

func (h *statusHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s := h.getStatus()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(s); err != nil {
		h.rf.logger.Warn("failed to encode status for monitoring", logger.ErrAttr(err))
		http.Error(w, "failed to encode status", http.StatusInternalServerError)
	}
}

// getStatus collects the current status from the Raft instance.
func (h *statusHandler) getStatus() status {
	h.rf.mu.RLock()
	defer h.rf.mu.RUnlock()

	lastLogIdx, lastLogTerm := h.rf.lastLogIdxAndTerm()
	s := status{
		NodeID:      h.rf.me,
		State:       stateToString(h.rf.state),
		CurrentTerm: h.rf.curTerm,
		VotedFor:    h.rf.votedFor,
		CommitIndex: h.rf.commitIdx,
		LastApplied: h.rf.lastAppliedIdx,
	}
	s.LogInfo.LastIndex = lastLogIdx
	s.LogInfo.LastTerm = lastLogTerm
	s.LogInfo.Count = len(h.rf.log)
	s.SnapshotInfo.LastIncludedIndex = h.rf.lastIncludedIndex
	s.SnapshotInfo.LastIncludedTerm = h.rf.lastIncludedTerm

	if h.rf.isState(leader) {
		s.LeaderSpecific = &leaderSpecificStatus{
			PeerReplicationInfo: make(map[string]peerReplicationInfo),
		}
		for i := range h.rf.peersCount {
			s.LeaderSpecific.PeerReplicationInfo[strconv.Itoa(i)] = peerReplicationInfo{
				MatchIndex: h.rf.matchIdx[i],
				NextIndex:  h.rf.nextIdx[i],
			}
		}
	}

	return s
}
