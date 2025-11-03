package testsim

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/shrtyk/raft-core/tests/harness"
	"github.com/shrtyk/raft-core/tests/simgob"
)

const (
	MAXLOGSIZE = 2000
)

func TestBasicAgree_3B(t *testing.T) {
	servers := 3
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	harness.AnnotateTest("TestBasicAgree3B", servers)
	ts.Begin("Test (3B): basic agreement")

	iters := 3
	for index := 1; index < iters+1; index++ {
		nd, _ := ts.NCommitted(index)
		if nd > 0 {
			t.Fatalf("some have committed before Start()")
		}

		xindex := ts.One(index*100, servers, false)
		if xindex != index {
			t.Fatalf("got index %v but expected %v", xindex, index)
		}
	}
}

func TestFollowerFailure_3B(t *testing.T) {
	servers := 3
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	harness.AnnotateTest("TestFollowerFailure3B", servers)
	ts.Begin("Test (3B): test progressive failure of followers")

	ts.One(101, servers, false)

	// disconnect one follower from the network.
	leader1 := ts.CheckOneLeader()
	ts.g.DisconnectAll((leader1 + 1) % servers)
	harness.AnnotateConnection(ts.g.GetConnected())

	// the leader and remaining follower should be
	// able to agree despite the disconnected follower.
	ts.One(102, servers-1, false)
	time.Sleep(RaftElectionTimeout)
	ts.One(103, servers-1, false)

	// disconnect the remaining follower
	leader2 := ts.CheckOneLeader()
	ts.g.DisconnectAll((leader2 + 1) % servers)
	ts.g.DisconnectAll((leader2 + 2) % servers)
	harness.AnnotateConnection(ts.g.GetConnected())

	// submit a command.
	w := new(bytes.Buffer)
	e := simgob.NewEncoder(w)
	e.Encode(Command{Data: 104})
	cmdBytes := w.Bytes()
	index, _, ok := ts.srvs[leader2].raft.Submit(cmdBytes)
	if ok != true {
		t.Fatalf("leader rejected Submit()")
	}
	if index != 4 {
		t.Fatalf("expected index 4, got %v", index)
	}

	time.Sleep(2 * RaftElectionTimeout)

	// check that command 104 did not commit.
	ts.CheckNoAgreement(int(index))
}

func TestLeaderFailure_3B(t *testing.T) {
	servers := 3
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	harness.AnnotateTest("TestLeaderFailure3B", servers)
	ts.Begin("Test (3B): test failure of leaders")

	ts.One(101, servers, false)

	// disconnect the first leader.
	leader1 := ts.CheckOneLeader()
	ts.g.DisconnectAll(leader1)
	harness.AnnotateConnection(ts.g.GetConnected())

	// the remaining followers should elect
	// a new leader.
	ts.One(102, servers-1, false)
	time.Sleep(RaftElectionTimeout)
	ts.One(103, servers-1, false)

	// disconnect the new leader.
	leader2 := ts.CheckOneLeader()
	ts.g.DisconnectAll(leader2)
	harness.AnnotateConnection(ts.g.GetConnected())

	// submit a command to each server.
	for i := 0; i < servers; i++ {
		w := new(bytes.Buffer)
		e := simgob.NewEncoder(w)
		e.Encode(Command{Data: 104})
		cmdBytes := w.Bytes()
		ts.srvs[i].raft.Submit(cmdBytes)
	}

	time.Sleep(2 * RaftElectionTimeout)

	// check that command 104 did not commit.
	ts.CheckNoAgreement(4)
}

func TestFailAgree_3B(t *testing.T) {
	servers := 3
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	harness.AnnotateTest("TestFailAgree3B", servers)
	ts.Begin("Test (3B): agreement after follower reconnects")

	ts.One(101, servers, false)

	// disconnect one follower from the network.
	leader := ts.CheckOneLeader()
	ts.g.DisconnectAll((leader + 1) % servers)
	harness.AnnotateConnection(ts.g.GetConnected())

	// the leader and remaining follower should be
	// able to agree despite the disconnected follower.
	ts.One(102, servers-1, false)
	ts.One(103, servers-1, false)
	time.Sleep(RaftElectionTimeout)
	ts.One(104, servers-1, false)
	ts.One(105, servers-1, false)

	// re-connect
	ts.g.ConnectOne((leader + 1) % servers)
	harness.AnnotateConnection(ts.g.GetConnected())

	// the full set of servers should preserve
	// previous agreements, and be able to agree
	// on new commands.
	ts.One(106, servers, true)
	time.Sleep(RaftElectionTimeout)
	ts.One(107, servers, true)
}

func TestFailNoAgree_3B(t *testing.T) {
	servers := 5
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	harness.AnnotateTest("TestFailNoAgree3B", servers)
	ts.Begin("Test (3B): no agreement if too many followers disconnect")

	ts.One(10, servers, false)

	// 3 of 5 followers disconnect
	leader := ts.CheckOneLeader()
	ts.g.DisconnectAll((leader + 1) % servers)
	ts.g.DisconnectAll((leader + 2) % servers)
	ts.g.DisconnectAll((leader + 3) % servers)
	harness.AnnotateConnection(ts.g.GetConnected())

	w := new(bytes.Buffer)
	e := simgob.NewEncoder(w)
	e.Encode(Command{Data: 20})
	cmdBytes := w.Bytes()
	index, _, ok := ts.srvs[leader].raft.Submit(cmdBytes)
	if ok != true {
		t.Fatalf("leader rejected Submit()")
	}
	if index != 2 {
		t.Fatalf("expected index 2, got %v", index)
	}

	time.Sleep(2 * RaftElectionTimeout)

	n, _ := ts.NCommitted(int(index))
	if n > 0 {
		t.Fatalf("%v committed but no majority", n)
	}

	// repair
	ts.g.ConnectOne((leader + 1) % servers)
	ts.g.ConnectOne((leader + 2) % servers)
	ts.g.ConnectOne((leader + 3) % servers)
	harness.AnnotateConnection(ts.g.GetConnected())

	// the disconnected majority may have chosen a leader from
	// among their own ranks, forgetting index 2.
	leader2 := ts.CheckOneLeader()

	w = new(bytes.Buffer)
	e = simgob.NewEncoder(w)
	e.Encode(Command{Data: 30})
	cmdBytes = w.Bytes()
	index2, _, ok2 := ts.srvs[leader2].raft.Submit(cmdBytes)
	if ok2 == false {
		t.Fatalf("leader2 rejected Submit()")
	}
	if index2 < 2 || index2 > 3 {
		t.Fatalf("unexpected index %v", index2)
	}

	ts.One(1000, servers, true)
}

func TestConcurrentStarts_3B(t *testing.T) {
	servers := 3
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	harness.AnnotateTest("TestConcurrentStarts3B", servers)
	ts.Begin("Test (3B): concurrent Start()s")

	var success bool
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// give solution some time to settle
			time.Sleep(3 * time.Second)
		}

		leader := ts.CheckOneLeader()
		textb := fmt.Sprintf("checking concurrent submission of commands (attempt %v)", try)
		harness.AnnotateCheckerBegin(textb)

		w := new(bytes.Buffer)
		e := simgob.NewEncoder(w)
		e.Encode(Command{Data: 1})
		cmdBytes := w.Bytes()
		_, term, ok := ts.srvs[leader].raft.Submit(cmdBytes)

		despretry := "concurrent submission failed; retry"
		if !ok {
			// leader moved on really quickly
			details := fmt.Sprintf("%v is no longer a leader", leader)
			harness.AnnotateCheckerNeutral(despretry, details)
			continue
		}

		iters := 5
		var wg sync.WaitGroup
		is := make(chan int, iters)
		for ii := 0; ii < iters; ii++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				w := new(bytes.Buffer)
				e := simgob.NewEncoder(w)
				e.Encode(Command{Data: 100 + i})
				cmdBytes := w.Bytes()
				index, term1, ok := ts.srvs[leader].raft.Submit(cmdBytes)
				if term1 != term {
					return
				}
				if ok != true {
					return
				}
				is <- int(index)
			}(ii)
		}

		wg.Wait()
		close(is)

		for j := 0; j < servers; j++ {
			if t, _ := ts.srvs[j].GetState(); int(t) != int(term) {
				// term changed -- can't expect low RPC counts
				details := fmt.Sprintf("term of server %v changed from %v to %v",
					j, term, t)
				harness.AnnotateCheckerNeutral(despretry, details)
				continue loop
			}
		}

		failed := false
		cmds := []int{}
		for index := range is {
			cmd := ts.Wait(index, servers, int(term))
			if ix, ok := cmd.(int); ok {
				if ix == -1 {
					// peers have moved on to later terms
					// so we can't expect all Start()s to
					// have succeeded
					failed = true
					details := fmt.Sprintf(
						"term changed while waiting for %v servers to commit index %v",
						servers, index)
					harness.AnnotateCheckerNeutral(despretry, details)
					break
				}
				cmds = append(cmds, ix)
			} else {
				details := fmt.Sprintf("value %v is not an int", cmd)
				harness.AnnotateCheckerFailure("read ill-typed value", details)
				t.Fatalf("%s", details)
			}
		}

		if failed {
			// avoid leaking goroutines
			go func() {
				for range is {
				}
			}()
			continue
		}

		for ii := 0; ii < iters; ii++ {
			x := 100 + ii
			ok := false
			for j := 0; j < len(cmds); j++ {
				if cmds[j] == x {
					ok = true
				}
			}
			if ok == false {
				details := fmt.Sprintf("cmd %v missing in %v", x, cmds)
				harness.AnnotateCheckerFailure("concurrent submission failed", details)
				t.Fatalf("%s", details)
			}
		}

		success = true
		break
	}

	if !success {
		harness.AnnotateCheckerFailure(
			"agreement failed under concurrent submission",
			"unable to reach agreement after 5 attempts")
		t.Fatalf("term changed too often")
	}

	text := "agreement reached under concurrent submission"
	harness.AnnotateCheckerSuccess(text, "OK")
}

func TestRejoin_3B(t *testing.T) {
	servers := 3
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	harness.AnnotateTest("TestRejoin3B", servers)
	ts.Begin("Test (3B): rejoin of partitioned leader")

	ts.One(101, servers, true)

	// leader network failure
	leader1 := ts.CheckOneLeader()
	ts.g.DisconnectAll(leader1)
	harness.AnnotateConnection(ts.g.GetConnected())

	// make old leader try to agree on some entries
	start := harness.GetAnnotateTimestamp()
	w := new(bytes.Buffer)
	e := simgob.NewEncoder(w)
	e.Encode(Command{Data: 102})
	ts.srvs[leader1].raft.Submit(w.Bytes())
	w = new(bytes.Buffer)
	e = simgob.NewEncoder(w)
	e.Encode(Command{Data: 103})
	ts.srvs[leader1].raft.Submit(w.Bytes())
	w = new(bytes.Buffer)
	e = simgob.NewEncoder(w)
	e.Encode(Command{Data: 104})
	ts.srvs[leader1].raft.Submit(w.Bytes())
	text := fmt.Sprintf("submitted commands [102 103 104] to %v", leader1)
	harness.AnnotateInfoInterval(start, text, text)

	// new leader commits, also for index=2
	ts.One(103, 2, true)

	// new leader network failure
	leader2 := ts.CheckOneLeader()
	ts.g.DisconnectAll(leader2)

	// old leader connected again
	ts.g.ConnectOne(leader1)
	harness.AnnotateConnection(ts.g.GetConnected())

	ts.One(104, 2, true)

	// all together now
	ts.g.ConnectOne(leader2)
	harness.AnnotateConnection(ts.g.GetConnected())

	ts.One(105, servers, true)
}

func TestBackup_3B(t *testing.T) {
	servers := 5
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	harness.AnnotateTest("TestBackup3B", servers)
	ts.Begin("Test (3B): leader backs up quickly over incorrect follower logs")

	ts.One(rand.Int(), servers, true)

	// put leader and one follower in a partition
	leader1 := ts.CheckOneLeader()
	ts.g.DisconnectAll((leader1 + 2) % servers)
	ts.g.DisconnectAll((leader1 + 3) % servers)
	ts.g.DisconnectAll((leader1 + 4) % servers)
	harness.AnnotateConnection(ts.g.GetConnected())

	// submit lots of commands that won't commit
	start := harness.GetAnnotateTimestamp()
	for i := 0; i < 50; i++ {
		w := new(bytes.Buffer)
		e := simgob.NewEncoder(w)
		e.Encode(Command{Data: rand.Int()})
		cmdBytes := w.Bytes()
		ts.srvs[leader1].raft.Submit(cmdBytes)
	}
	text := fmt.Sprintf("submitted 50 commands to %v", leader1)
	harness.AnnotateInfoInterval(start, text, text)

	time.Sleep(RaftElectionTimeout / 2)

	ts.g.DisconnectAll((leader1 + 0) % servers)
	ts.g.DisconnectAll((leader1 + 1) % servers)

	// allow other partition to recover
	ts.g.ConnectOne((leader1 + 2) % servers)
	ts.g.ConnectOne((leader1 + 3) % servers)
	ts.g.ConnectOne((leader1 + 4) % servers)
	harness.AnnotateConnection(ts.g.GetConnected())

	// lots of successful commands to new group.
	for i := 0; i < 50; i++ {
		ts.One(rand.Int(), 3, true)
	}

	// now another partitioned leader and one follower
	leader2 := ts.CheckOneLeader()
	other := (leader1 + 2) % servers
	if leader2 == other {
		other = (leader2 + 1) % servers
	}
	ts.g.DisconnectAll(other)
	harness.AnnotateConnection(ts.g.GetConnected())

	// lots more commands that won't commit
	start = harness.GetAnnotateTimestamp()
	for i := 0; i < 50; i++ {
		w := new(bytes.Buffer)
		e := simgob.NewEncoder(w)
		e.Encode(Command{Data: rand.Int()})
		cmdBytes := w.Bytes()
		ts.srvs[leader2].raft.Submit(cmdBytes)
	}
	text = fmt.Sprintf("submitted 50 commands to %v", leader2)
	harness.AnnotateInfoInterval(start, text, text)

	time.Sleep(RaftElectionTimeout / 2)

	// bring original leader back to life,
	for i := 0; i < servers; i++ {
		ts.g.DisconnectAll(i)
	}
	ts.g.ConnectOne((leader1 + 0) % servers)
	ts.g.ConnectOne((leader1 + 1) % servers)
	ts.g.ConnectOne(other)
	harness.AnnotateConnection(ts.g.GetConnected())

	// lots of successful commands to new group.
	for i := 0; i < 50; i++ {
		ts.One(rand.Int(), 3, true)
	}

	// now everyone
	for i := 0; i < servers; i++ {
		ts.g.ConnectOne(i)
	}
	harness.AnnotateConnection(ts.g.GetConnected())
	ts.One(rand.Int(), servers, true)
}

func TestCount_3B(t *testing.T) {
	servers := 3
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	harness.AnnotateTest("TestCount3B", servers)
	ts.Begin("Test (3B): RPC counts aren't too high")

	rpcs := func() (n int) {
		for j := 0; j < servers; j++ {
			n += ts.g.RpcCount(j)
		}
		return
	}

	ts.CheckOneLeader()

	total1 := rpcs()

	if total1 > 30 || total1 < 1 {
		text := fmt.Sprintf("too many or few RPCs (%v) to elect initial leader", total1)
		harness.AnnotateCheckerFailure(text, text)
		t.Fatalf("%s", text)
	}

	var total2 int
	var success bool
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// give solution some time to settle
			time.Sleep(3 * time.Second)
		}

		leader := ts.CheckOneLeader()
		textb := fmt.Sprintf("checking reasonable RPC counts for agreement (attempt %v)", try)
		harness.AnnotateCheckerBegin(textb)
		total1 = rpcs()

		iters := 10
		w_first := new(bytes.Buffer)
		e_first := simgob.NewEncoder(w_first)
		e_first.Encode(Command{Data: 1})
		cmdBytes_first := w_first.Bytes()
		starti, term, ok := ts.srvs[leader].raft.Submit(cmdBytes_first)
		despretry := "submission failed; retry"
		if !ok {
			// leader moved on really quickly
			details := fmt.Sprintf("%v is no longer a leader", leader)
			harness.AnnotateCheckerNeutral(despretry, details)
			continue
		}

		cmds := []int{}
		for i := 1; i < iters+2; i++ {
			x := int(rand.Int31())
			cmds = append(cmds, x)
			w_loop := new(bytes.Buffer)
			e_loop := simgob.NewEncoder(w_loop)
			e_loop.Encode(Command{Data: x})
			cmdBytes_loop := w_loop.Bytes()
			index1, term1, ok := ts.srvs[leader].raft.Submit(cmdBytes_loop)
			if term1 != term {
				// Term changed while starting
				details := fmt.Sprintf("term of the leader (%v) changed from %v to %v",
					leader, term, term1)
				harness.AnnotateCheckerNeutral(despretry, details)
				continue loop
			}
			if !ok {
				// No longer the leader, so term has changed
				details := fmt.Sprintf("%v is no longer a leader", leader)
				harness.AnnotateCheckerNeutral(despretry, details)
				continue loop
			}
			if int(starti)+i != int(index1) {
				desp := fmt.Sprintf("leader %v adds the command at the wrong index", leader)
				details := fmt.Sprintf(
					"the command should locate at index %v, but the leader puts it at %v",
					int(starti)+i, index1)
				harness.AnnotateCheckerFailure(desp, details)
				t.Fatalf("Submit() failed")
			}
		}

		for i := 1; i < iters+1; i++ {
			cmd := ts.Wait(int(starti)+i, servers, int(term))
			if ix, ok := cmd.(int); ok == false || ix != cmds[i-1] {
				if ix == -1 {
					// term changed -- try again
					details := fmt.Sprintf(
						"term changed while waiting for %v servers to commit index %v",
						servers, int(starti)+i)
					harness.AnnotateCheckerNeutral(despretry, details)
					continue loop
				}
				details := fmt.Sprintf(
					"the command submitted at index %v in term %v is %v, but read %v",
					int(starti)+i, term, cmds[i-1], cmd)
				harness.AnnotateCheckerFailure("incorrect command committed", details)
				t.Fatalf("wrong value %v committed for index %v; expected %v\n", cmd, int(starti)+i, cmds)
			}
		}

		failed := false
		total2 = 0
		for j := 0; j < servers; j++ {
			if t, _ := ts.srvs[j].GetState(); int(t) != int(term) {
				// term changed -- can't expect low RPC counts
				// need to keep going to update total2
				details := fmt.Sprintf("term of server %v changed from %v to %v", j, term, t)
				harness.AnnotateCheckerNeutral(despretry, details)
				failed = true
			}
			total2 += ts.g.RpcCount(j)
		}

		if failed {
			continue loop
		}

		if total2-total1 > (iters+1+3)*3 {
			details := fmt.Sprintf("number of RPC used for %v entries = %v > %v",
				iters, total2-total1, (iters+1+3)*3)
			harness.AnnotateCheckerFailure("used too many RPCs for agreement", details)
			t.Fatalf("too many RPCs (%v) for %v entries\n", total2-total1, iters)
		}

		details := fmt.Sprintf("number of RPC used for %v entries = %v <= %v",
			iters, total2-total1, (iters+1+3)*3)
		harness.AnnotateCheckerSuccess("used reasonable number of RPCs for agreement", details)

		success = true
		break
	}

	if !success {
		harness.AnnotateCheckerFailure(
			"agreement failed",
			"unable to reach agreement after 5 attempts")
		t.Fatalf("term changed too often")
	}

	harness.AnnotateCheckerBegin("checking reasonable RPC counts in idle")

	time.Sleep(RaftElectionTimeout)

	total3 := 0
	for j := 0; j < servers; j++ {
		total3 += ts.g.RpcCount(j)
	}

	if total3-total2 > 3*20 {
		details := fmt.Sprintf("number of RPC used for 1 second of idleness = %v > %v",
			total3-total2, 3*20)
		harness.AnnotateCheckerFailure("used too many RPCs in idle", details)
		t.Fatalf("too many RPCs (%v) for 1 second of idleness\n", total3-total2)
	}
	details := fmt.Sprintf("number of RPC used for 1 second of idleness = %v <= %v",
		total3-total2, 3*20)
	harness.AnnotateCheckerSuccess(
		"used a reasonable number of RPCs in idle", details)
}
