package transport

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/shrtyk/raft-core/api"
	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
)

// mockRaftServer is a mock implementation of the RaftServiceServer
type mockRaftServer struct {
	raftpb.UnimplementedRaftServiceServer
	voteReq    *raftpb.RequestVoteRequest
	voteResp   *raftpb.RequestVoteResponse
	voteErr    error
	appendReq  *raftpb.AppendEntriesRequest
	appendResp *raftpb.AppendEntriesResponse
	appendErr  error
	snapReq    *raftpb.InstallSnapshotRequest
	snapResp   *raftpb.InstallSnapshotResponse
	snapErr    error

	voteFunc func(context.Context, *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error)
}

func (s *mockRaftServer) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	if s.voteFunc != nil {
		return s.voteFunc(ctx, req)
	}
	s.voteReq = req
	return s.voteResp, s.voteErr
}

func (s *mockRaftServer) AppendEntries(_ context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	s.appendReq = req
	return s.appendResp, s.appendErr
}

func (s *mockRaftServer) InstallSnapshot(_ context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	s.snapReq = req
	return s.snapResp, s.snapErr
}

// startMockServer starts a mock gRPC server and returns its address and a function to stop it
func startMockServer(t *testing.T, mockSrv *mockRaftServer) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	s := grpc.NewServer()
	raftpb.RegisterRaftServiceServer(s, mockSrv)
	go func() {
		_ = s.Serve(lis)
	}()
	return lis.Addr().String(), func() { s.GracefulStop() }
}

func TestSetupConnections(t *testing.T) {
	mockSrv1 := &mockRaftServer{}
	addr1, stop1 := startMockServer(t, mockSrv1)
	defer stop1()

	mockSrv2 := &mockRaftServer{}
	addr2, stop2 := startMockServer(t, mockSrv2)
	defer stop2()

	t.Run("success", func(t *testing.T) {
		peerAddrs := []string{addr1, addr2}
		conns, closeFunc, err := SetupConnections(peerAddrs)
		require.NoError(t, err)
		require.NotNil(t, closeFunc)
		assert.Len(t, conns, 2)
		for _, conn := range conns {
			assert.NotNil(t, conn)
		}
		err = closeFunc()
		assert.NoError(t, err)
	})
}

func TestGRPCTransport(t *testing.T) {
	mockSrv := &mockRaftServer{}
	addr, stop := startMockServer(t, mockSrv)
	defer stop()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	cfg := &api.RaftConfig{
		Timings: api.RaftTimings{
			RPCTimeout: 100 * time.Millisecond,
		},
		CBreaker: api.CircuitBreakerCfg{
			FailureThreshold: 5,
			SuccessThreshold: 3,
			ResetTimeout:     1 * time.Second,
		},
	}
	transport, err := NewGRPCTransport(cfg, []*grpc.ClientConn{conn})
	require.NoError(t, err)
	assert.Equal(t, 1, transport.PeersCount())

	ctx := context.Background()

	t.Run("SendRequestVote", func(t *testing.T) {
		req := &raftpb.RequestVoteRequest{Term: 1, CandidateId: 1}
		mockSrv.voteResp = &raftpb.RequestVoteResponse{Term: 1, VoteGranted: true}
		mockSrv.voteErr = nil

		resp, err := transport.SendRequestVote(ctx, 0, req)
		require.NoError(t, err)
		assert.True(t, cmp.Equal(mockSrv.voteResp, resp, protocmp.Transform()))
		assert.True(t, cmp.Equal(req, mockSrv.voteReq, protocmp.Transform()))
	})

	t.Run("SendAppendEntries", func(t *testing.T) {
		req := &raftpb.AppendEntriesRequest{Term: 1, LeaderId: 1}
		mockSrv.appendResp = &raftpb.AppendEntriesResponse{Term: 1, Success: true}
		mockSrv.appendErr = nil

		resp, err := transport.SendAppendEntries(ctx, 0, req)
		require.NoError(t, err)
		assert.True(t, cmp.Equal(mockSrv.appendResp, resp, protocmp.Transform()))
		assert.True(t, cmp.Equal(req, mockSrv.appendReq, protocmp.Transform()))
	})

	t.Run("SendInstallSnapshot", func(t *testing.T) {
		req := &raftpb.InstallSnapshotRequest{Term: 1, LeaderId: 1}
		mockSrv.snapResp = &raftpb.InstallSnapshotResponse{Term: 1}
		mockSrv.snapErr = nil

		resp, err := transport.SendInstallSnapshot(ctx, 0, req)
		require.NoError(t, err)
		assert.True(t, cmp.Equal(mockSrv.snapResp, resp, protocmp.Transform()))
		assert.True(t, cmp.Equal(req, mockSrv.snapReq, protocmp.Transform()))
	})

	t.Run("timeout", func(t *testing.T) {
		callErr := fmt.Errorf("should not be called")
		// Create a server that will block to simulate a timeout
		timeoutSrv := &mockRaftServer{
			voteErr:   callErr,
			appendErr: callErr,
			snapErr:   callErr,
		}
		timeoutSrv.voteFunc = func(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
			time.Sleep(200 * time.Millisecond)
			return nil, nil
		}

		addr, stop := startMockServer(t, timeoutSrv)
		defer stop()

		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		defer conn.Close()

		timeoutCfg := &api.RaftConfig{
			Timings: api.RaftTimings{
				RPCTimeout: 50 * time.Millisecond,
			},
			CBreaker: api.CircuitBreakerCfg{
				FailureThreshold: 5,
				SuccessThreshold: 3,
				ResetTimeout:     1 * time.Second,
			},
		}
		timeoutTransport, err := NewGRPCTransport(timeoutCfg, []*grpc.ClientConn{conn})
		require.NoError(t, err)

		_, err = timeoutTransport.SendRequestVote(ctx, 0, &raftpb.RequestVoteRequest{})
		assert.Error(t, err)

		s, ok := status.FromError(err)
		assert.True(t, ok, "expected error to be a gRPC status error")
		assert.Equal(t, codes.DeadlineExceeded, s.Code(), "expected DeadlineExceeded status code")
	})
}
