package testsim

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/shrtyk/raft-core/raft/tests/harness"
	"github.com/shrtyk/raft-core/raft/tests/simgob"
)

func snapcommon(t *testing.T, name string, disconnect bool, reliable bool, crash bool) {
	iters := 30
	servers := 3
	ts := makeTest(t, servers, reliable, true)
	defer ts.cleanup()

	// Inconsistent with other test cases, but don't want to change API.
	harness.AnnotateTest(name, servers)
	ts.Begin(name)

	ts.One(rand.Int(), servers, true)
	leader1 := ts.CheckOneLeader()

	for i := 0; i < iters; i++ {
		victim := (leader1 + 1) % servers
		sender := leader1
		if i%3 == 1 {
			sender = (leader1 + 1) % servers
			victim = leader1
		}

		if disconnect {
			ts.g.DisconnectAll(victim)
			harness.AnnotateConnection(ts.g.GetConnected())
			ts.One(rand.Int(), servers-1, true)
		}
		if crash {
			ts.g.ShutdownServer(victim)
			harness.AnnotateShutdown([]int{victim})
			ts.One(rand.Int(), servers-1, true)
		}

		// perhaps send enough to get a snapshot
		start := harness.GetAnnotateTimestamp()
		nn := (SnapShotInterval / 2) + (rand.Int() % SnapShotInterval)
		for i := 0; i < nn; i++ {
			w := new(bytes.Buffer)
			e := simgob.NewEncoder(w)
			e.Encode(Command{Data: rand.Int()})
			cmdBytes := w.Bytes()
			ts.srvs[sender].raft.Submit(cmdBytes)
		}
		text := fmt.Sprintf("submitting %v commands to %v", nn, sender)
		harness.AnnotateInfoInterval(start, text, text)

		// let applier threads catch up with the Start()'s
		if disconnect == false && crash == false {
			// make sure all followers have caught up, so that
			// an InstallSnapshot RPC isn't required for
			// TestSnapshotBasic3D().
			ts.One(rand.Int(), servers, true)
		} else {
			ts.One(rand.Int(), servers-1, true)
		}

		if ts.g.LogSize() >= MAXLOGSIZE {
			ts.t.Fatalf("Log size too large")
		}
		if disconnect {
			// reconnect a follower, who maybe behind and
			// needs to rceive a snapshot to catch up.
			ts.g.ConnectOne(victim)
			harness.AnnotateConnection(ts.g.GetConnected())
			ts.One(rand.Int(), servers, true)
			leader1 = ts.CheckOneLeader()
		}
		if crash {
			ts.Restart(victim)
			harness.AnnotateRestart([]int{victim})
			ts.One(rand.Int(), servers, true)
			leader1 = ts.CheckOneLeader()
		}
	}
}

func TestSnapshotBasic_3D(t *testing.T) {
	snapcommon(t, "Test (3D): snapshots basic", false, true, false)
}

func TestSnapshotInstall_3D(t *testing.T) {
	snapcommon(t, "Test (3D): install snapshots (disconnect)", true, true, false)
}

func TestSnapshotInstallUnreliable_3D(t *testing.T) {
	snapcommon(t, "Test (3D): install snapshots (disconnect)",
		true, false, false)
}

func TestSnapshotInstallCrash_3D(t *testing.T) {
	snapcommon(t, "Test (3D): install snapshots (crash)", false, true, true)
}

func TestSnapshotInstallUnCrash_3D(t *testing.T) {
	snapcommon(t, "Test (3D): install snapshots (crash)", false, false, true)
}

func TestSnapshotAllCrash_3D(t *testing.T) {
	servers := 3
	iters := 5
	ts := makeTest(t, servers, false, true)
	defer ts.cleanup()

	harness.AnnotateTest("TestSnapshotAllCrash3D", servers)
	ts.Begin("Test (3D): crash and restart all servers")

	ts.One(rand.Int(), servers, true)

	for i := 0; i < iters; i++ {
		// perhaps enough to get a snapshot
		nn := (SnapShotInterval / 2) + (rand.Int() % SnapShotInterval)
		for i := 0; i < nn; i++ {
			ts.One(rand.Int(), servers, true)
		}

		index1 := ts.One(rand.Int(), servers, true)

		// crash all
		ts.g.Shutdown()
		harness.AnnotateShutdownAll()
		ts.g.StartServers()
		harness.AnnotateRestartAll()

		index2 := ts.One(rand.Int(), servers, true)
		if index2 < index1+1 {
			msg := fmt.Sprintf("index decreased from %v to %v", index1, index2)
			harness.AnnotateCheckerFailure("incorrect behavior: index decreased", msg)
			t.Fatalf("%s", msg)
		}
	}
}

func TestSnapshotInit_3D(t *testing.T) {
	servers := 3
	ts := makeTest(t, servers, false, true)
	defer ts.cleanup()

	harness.AnnotateTest("TestSnapshotInit3D", servers)
	ts.Begin("Test (3D): snapshot initialization after crash")
	ts.One(rand.Int(), servers, true)

	// enough ops to make a snapshot
	nn := SnapShotInterval + 1
	for i := 0; i < nn; i++ {
		ts.One(rand.Int(), servers, true)
	}

	ts.g.Shutdown()
	harness.AnnotateShutdownAll()
	ts.g.StartServers()
	harness.AnnotateRestartAll()

	// a single op, to get something to be written back to persistent storage.
	ts.One(rand.Int(), servers, true)

	ts.g.Shutdown()
	harness.AnnotateShutdownAll()
	ts.g.StartServers()
	harness.AnnotateRestartAll()

	// do another op to trigger potential bug
	ts.One(rand.Int(), servers, true)
}
