package testsim

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/shrtyk/raft-core/tests/harness"
)

const RaftElectionTimeout = 1000 * time.Millisecond

func TestInitialElection_3A(t *testing.T) {
	servers := 3
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	harness.AnnotateTest("TestInitialElection3A", servers)
	ts.Begin("Test (3A): initial election")

	ts.CheckOneLeader()

	time.Sleep(50 * time.Millisecond)
	term1 := ts.CheckTerms()
	if term1 < 1 {
		ts.t.Fatalf("term is %v, but should be at least 1", term1)
	}

	time.Sleep(2 * RaftElectionTimeout)
	term2 := ts.CheckTerms()
	if term1 != term2 {
		fmt.Printf("warning: term changed even though there were no failures")
	}

	ts.CheckOneLeader()
}

func TestReElection_3A(t *testing.T) {
	servers := 3
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	harness.AnnotateTest("TestReElection3A", servers)
	ts.Begin("Test (3A): election after network failure")

	leader1 := ts.CheckOneLeader()

	ts.g.DisconnectAll(leader1)
	harness.AnnotateConnection(ts.g.GetConnected())
	ts.CheckOneLeader()

	ts.g.ConnectOne(leader1)
	harness.AnnotateConnection(ts.g.GetConnected())
	leader2 := ts.CheckOneLeader()

	ts.g.DisconnectAll(leader2)
	ts.g.DisconnectAll((leader2 + 1) % servers)
	harness.AnnotateConnection(ts.g.GetConnected())
	time.Sleep(2 * RaftElectionTimeout)

	ts.CheckNoLeader()

	ts.g.ConnectOne((leader2 + 1) % servers)
	harness.AnnotateConnection(ts.g.GetConnected())
	ts.CheckOneLeader()

	ts.g.ConnectOne(leader2)
	harness.AnnotateConnection(ts.g.GetConnected())
	ts.CheckOneLeader()
}

func TestManyElections_3A(t *testing.T) {
	servers := 7
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	harness.AnnotateTest("TestManyElection3A", servers)
	ts.Begin("Test (3A): multiple elections")

	ts.CheckOneLeader()

	iters := 10
	for ii := 1; ii < iters; ii++ {
		// disconnect three nodes
		i1 := rand.Int() % servers
		i2 := rand.Int() % servers
		i3 := rand.Int() % servers
		ts.g.DisconnectAll(i1)
		ts.g.DisconnectAll(i2)
		ts.g.DisconnectAll(i3)
		harness.AnnotateConnection(ts.g.GetConnected())

		// either the current leader should still be alive,
		// or the remaining four should elect a new one.
		ts.CheckOneLeader()

		ts.g.ConnectOne(i1)
		ts.g.ConnectOne(i2)
		ts.g.ConnectOne(i3)
		harness.AnnotateConnection(ts.g.GetConnected())
	}
	ts.CheckOneLeader()
}
