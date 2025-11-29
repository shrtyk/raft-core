package testsim

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shrtyk/raft-core/tests/harness"
	"github.com/shrtyk/raft-core/tests/simgob"
)

func TestPersist1_3C(t *testing.T) {
	servers := 3
	ts := makeTest(t, servers, false, false)
	defer ts.cleanup()

	harness.AnnotateTest("TestPersist13C", servers)
	ts.Begin("Test (3C): basic persistence")

	ts.One(11, servers, true)

	// crash and re-start all
	for i := 0; i < servers; i++ {
		ts.g.ShutdownServer(i)
	}
	for i := 0; i < servers; i++ {
		ts.g.StartServer(i)
	}
	ts.g.ConnectAll()

	ts.One(12, servers, true)

	leader1 := ts.CheckOneLeader()
	ts.g.ShutdownServer(leader1)
	ts.Restart(leader1)

	ts.One(13, servers, true)

	leader2 := ts.CheckOneLeader()
	ts.g.ShutdownServer(leader2)
	ts.One(14, servers-1, true)
	ts.Restart(leader2)

	// wait for leader2 to join before killing i3
	ts.Wait(4, servers, -1)

	i3 := (ts.CheckOneLeader() + 1) % servers
	ts.g.ShutdownServer(i3)
	ts.One(15, servers-1, true)
	ts.Restart(i3)

	ts.One(16, servers, true)
}

func TestPersist2_3C(t *testing.T) {
	servers := 5
	ts := makeTest(t, servers, false, false)
	defer ts.cleanup()

	harness.AnnotateTest("TestPersist23C", servers)
	ts.Begin("Test (3C): more persistence")

	index := 1
	for iters := 0; iters < 5; iters++ {
		ts.One(10+index, servers, true)
		index++

		leader1 := ts.CheckOneLeader()

		ts.g.ShutdownServer((leader1 + 1) % servers)
		ts.g.ShutdownServer((leader1 + 2) % servers)

		ts.One(10+index, servers-2, true)
		index++

		ts.g.ShutdownServer((leader1 + 0) % servers)
		ts.g.ShutdownServer((leader1 + 3) % servers)
		ts.g.ShutdownServer((leader1 + 4) % servers)

		ts.Restart((leader1 + 1) % servers)
		ts.Restart((leader1 + 2) % servers)

		time.Sleep(RaftElectionTimeout)

		ts.Restart((leader1 + 0) % servers)

		ts.One(10+index, servers-2, true)
		index++

		ts.Restart((leader1 + 3) % servers)
		ts.Restart((leader1 + 4) % servers)

		ts.One(1000, servers, true)
		index++
	}
}

func TestPersist3_3C(t *testing.T) {
	servers := 3
	ts := makeTest(t, servers, false, false)
	defer ts.cleanup()

	harness.AnnotateTest("TestPersist33C", servers)
	ts.Begin("Test (3C): partitioned leader and one follower crash, leader restarts")

	ts.One(101, 3, true)

	leader := ts.CheckOneLeader()
	ts.g.DisconnectAll((leader + 2) % servers)

	ts.One(102, 2, true)

	ts.g.ShutdownServer(leader)
	ts.g.ShutdownServer((leader + 1) % servers)
	ts.g.ConnectOne((leader + 2) % servers)
	ts.Restart(leader)

	time.Sleep(RaftElectionTimeout)

	ts.Restart((leader + 1) % servers)
	ts.One(103, 3, true)
}

func TestFigure8_3C(t *testing.T) {
	servers := 5
	ts := makeTest(t, servers, false, false)
	defer ts.cleanup()

	harness.AnnotateTest("TestFigure83C", servers)
	ts.Begin("Test (3C): Figure 8")

	ts.One(rand.Int(), 1, true)

	nup := servers
	for iters := 0; iters < 1000; iters++ {
		leader := -1
		for i := 0; i < servers; i++ {
			if !ts.srvs[i].raft.Killed() {
				w := new(bytes.Buffer)
				e := simgob.NewEncoder(w)
				e.Encode(Command{Data: rand.Int()})
				cmdBytes := w.Bytes()
				sr := ts.srvs[i].raft.Submit(cmdBytes)
				if sr.IsLeader {
					leader = i
				}
			}
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (int64(RaftElectionTimeout/time.Millisecond) / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := (rand.Int63() % 13)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader != -1 {
			ts.g.ShutdownServer(leader)
			nup -= 1
		}

		if nup < 3 {
			s := rand.Int() % servers
			if ts.srvs[s].raft.Killed() {
				ts.g.StartServer(s)
				ts.g.ConnectOne(s)
				nup += 1
			}
		}
	}

	for i := 0; i < servers; i++ {
		if ts.srvs[i].raft.Killed() {
			ts.g.StartServer(i)
			ts.g.ConnectOne(i)
		}
	}

	ts.One(rand.Int(), servers, true)
}

func TestUnreliableAgree_3C(t *testing.T) {
	servers := 3
	ts := makeTest(t, servers, false, false)
	defer ts.cleanup()

	harness.AnnotateTest("TestUnreliableAgree3C", servers)
	ts.Begin("Test (3C): agreement in unreliable network")

	var wg sync.WaitGroup

	for iters := 1; iters < 50; iters++ {
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func(iters, j int) {
				defer wg.Done()
				ts.One((100*iters)+j, 1, true)
			}(iters, j)
		}
		ts.One(iters, 1, true)
	}

	wg.Wait()

	ts.One(1000, servers, true)
}

func TestFigure8Unreliable_3C(t *testing.T) {
	servers := 5
	ts := makeTest(t, servers, false, false)
	defer ts.cleanup()

	harness.AnnotateTest("TestFigure8Unreliable3C", servers)
	ts.Begin("Test (3C): Figure 8 (unreliable)")

	ts.One(rand.Int(), 1, true)

	nup := servers
	for iters := 0; iters < 1000; iters++ {
		if iters == 500 {
			ts.SetLongReordering(true)
		}

		leader := -1
		for i := 0; i < servers; i++ {
			if !ts.srvs[i].raft.Killed() {
				w := new(bytes.Buffer)
				e := simgob.NewEncoder(w)
				e.Encode(Command{Data: rand.Int()})
				cmdBytes := w.Bytes()
				sr := ts.srvs[i].raft.Submit(cmdBytes)
				if sr.IsLeader {
					leader = i
				}
			}
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (int64(RaftElectionTimeout/time.Millisecond) / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := (rand.Int63() % 13)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader != -1 && (rand.Int()%1000) < int(RaftElectionTimeout/time.Millisecond)/2 {
			ts.g.ShutdownServer(leader)
			nup -= 1
		}

		if nup < 3 {
			s := rand.Int() % servers
			if ts.srvs[s].raft.Killed() {
				ts.g.StartServer(s)
				ts.g.ConnectOne(s)
				nup += 1
			}
		}
	}

	for i := 0; i < servers; i++ {
		if ts.srvs[i].raft.Killed() {
			ts.g.StartServer(i)
			ts.g.ConnectOne(i)
		}
	}

	ts.One(rand.Int(), servers, true)
}

func churn(t *testing.T, unreliable bool) {
	servers := 5
	ts := makeTest(t, servers, unreliable, false)
	defer ts.cleanup()

	if unreliable {
		harness.AnnotateTest("TestUnreliableChurn3C", servers)
		ts.Begin("Test (3C): unreliable churn")
	} else {
		harness.AnnotateTest("TestReliableChurn3C", servers)
		ts.Begin("Test (3C): reliable churn")
	}

	stop := int32(0)

	// create a thread to periodically start and stop servers
	var wg sync.WaitGroup
	serverLocks := make([]sync.Mutex, servers)
	wg.Go(func() {
		for atomic.LoadInt32(&stop) == 0 {
			i := rand.Int() % servers
			serverLocks[i].Lock()
			if (rand.Int() % 1000) < 200 {
				// crash
				ts.mu.Lock()
				s := ts.srvs[i]
				ts.mu.Unlock()
				if s != nil && !s.raft.Killed() {
					ts.g.ShutdownServer(i)
				}
			} else {
				// start
				ts.mu.Lock()
				s := ts.srvs[i]
				ts.mu.Unlock()
				if s != nil && s.raft.Killed() {
					ts.g.StartServer(i)
					ts.g.ConnectOne(i)
				}
			}
			serverLocks[i].Unlock()
			time.Sleep(time.Duration(rand.Int63()%20) * time.Millisecond)
		}
	})

	const n = 30
	for iters := 0; iters < n; iters++ {
		ts.One(iters, 1, true)
		time.Sleep(time.Duration(20+rand.Int63()%30) * time.Millisecond)
	}

	atomic.StoreInt32(&stop, 1)
	wg.Wait()

	// make sure all servers are running
	for i := 0; i < servers; i++ {
		if ts.srvs[i].raft.Killed() {
			ts.g.StartServer(i)
			ts.g.ConnectOne(i)
		}
	}

	ts.One(1000, servers, true)
}

func TestReliableChurn_3C(t *testing.T) {
	churn(t, false)
}

func TestUnreliableChurn_3C(t *testing.T) {
	churn(t, true)
}
