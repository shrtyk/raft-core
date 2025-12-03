package raft

import (
	"fmt"
	"net"

	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
	"github.com/shrtyk/raft-core/pkg/logger"
	"google.golang.org/grpc"
)

type GRPCServer interface {
	Start() error
	Stop() error
}

type grpcServer struct {
	rf     *Raft
	addr   string
	server *grpc.Server
}

// NewGRPCServer creates a new gRPC server for the Raft node.
func NewGRPCServer(rf *Raft, addr string) GRPCServer {
	s := grpc.NewServer()
	raftpb.RegisterRaftServiceServer(s, rf)
	return &grpcServer{
		rf:     rf,
		addr:   addr,
		server: s,
	}
}

// Start starts the gRPC server
func (s *grpcServer) Start() error {
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	s.rf.wg.Go(func() {
		if err := s.server.Serve(l); err != nil && err != grpc.ErrServerStopped {
			s.rf.logger.Error("gRPC server failed", logger.ErrAttr(err))
		}
	})

	return nil
}

// Stop gracefully stops the gRPC server.
func (s *grpcServer) Stop() error {
	if s.server != nil {
		s.server.Stop()
	}
	return nil
}
