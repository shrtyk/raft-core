package harness_test

import (
	"testing"

	harness "github.com/shrtyk/raft-core/raft/tests/harness"
	"github.com/shrtyk/raft-core/raft/tests/simrpc"
)

type Server struct {
	killed bool
}

func newSrv() *Server {
	return &Server{}
}

func (s *Server) Kill() {
	s.killed = true
}

func (s *Server) Get(args *struct{}, reply *struct{}) {
	// The test only checks if the RPC succeeds, so the body can be empty.
}

type Test struct {
	t *testing.T
	s *Server
	*harness.Config
	clnt *harness.Clnt
	sn   string
}

func makeTest(t *testing.T, nsrv int) *Test {
	ts := &Test{t: t, sn: harness.ServerName(harness.GRP0, 0)}
	cfg := harness.MakeConfig(t, nsrv, true, ts.startServer)
	ts.Config = cfg
	ts.clnt = ts.Config.MakeClient()
	return ts
}

func (ts *Test) startServer(
	servers []*simrpc.ClientEnd,
	gid harness.Tgid,
	me int,
	persister *harness.Persister,
) []harness.IService {
	ts.s = newSrv()
	return []harness.IService{ts.s}
}

func (ts *Test) cleanup() {
	ts.Cleanup()
}

func (ts *Test) oneRPC() bool {
	if ok := ts.clnt.Call(ts.sn, "Server.Get", &struct{}{}, &struct{}{}); !ok {
		return false
	}
	return true
}

func TestBasic(t *testing.T) {
	ts := makeTest(t, 1)
	defer ts.cleanup()
	ts.oneRPC()
}

func TestShutdownServer(t *testing.T) {
	ts := makeTest(t, 1)
	defer ts.cleanup()

	ts.oneRPC()
	ts.Group(harness.GRP0).Shutdown()
	if !ts.s.killed {
		ts.Fatalf("Not killed")
	}
	if ok := ts.oneRPC(); ok {
		ts.Fatalf("RPC succeeded")
	}
}
