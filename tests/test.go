package testsim

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shrtyk/raft-core/api"
	"github.com/shrtyk/raft-core/pkg/logger"
	"github.com/shrtyk/raft-core/raft"
	"github.com/shrtyk/raft-core/tests/harness"
	"github.com/shrtyk/raft-core/tests/simgob"
	"github.com/shrtyk/raft-core/tests/simrpc"
)

type Command struct {
	Data interface{}
}

// rfsrv is a placeholder for the Raft server.
type rfsrv struct {
	mu        sync.Mutex
	ts        *Test
	id        int
	raft      api.Raft
	applyCh   chan *api.ApplyMessage
	persister *harness.Persister
	snapshot  bool
	logs      map[int]interface{}
	applyErr  string

	lastApplied int
}

func (rs *rfsrv) GetState() (int, bool) {
	term, isLeader := rs.raft.State()
	return int(term), isLeader
}

func (rs *rfsrv) Logs(index int) (interface{}, bool) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	v, ok := rs.logs[index]
	return v, ok
}

func (rs *rfsrv) applier() {
	for m := range rs.applyCh {
		if m.CommandValid {
			err_msg, prevok := rs.ts.CheckLogs(rs.id, *m)
			if int(m.CommandIndex) > 1 && prevok == false {
				err_msg = fmt.Sprintf("server %v apply out of order %v", rs.id, m.CommandIndex)
			}
			if err_msg != "" {
				harness.AnnotateCheckerFailureBeforeExit("apply error", err_msg)
				log.Fatalf("apply error: %v", err_msg)
				rs.applyErr = err_msg
			}
		}
	}
}

const SnapShotInterval = 10

func (rs *rfsrv) applierSnap(applyCh chan *api.ApplyMessage) {
	if rs.raft == nil {
		return
	}

	for m := range applyCh {
		err_msg := ""
		if m.SnapshotValid {
			err_msg = rs.ingestSnap(m.Snapshot, int(m.SnapshotIndex))
		} else if m.CommandValid {
			if int(m.CommandIndex) != rs.lastApplied+1 {
				err_msg = fmt.Sprintf("server %v apply out of order, expected index %v, got %v", rs.id, rs.lastApplied+1, m.CommandIndex)
			}

			if err_msg == "" {
				var prevok bool
				err_msg, prevok = rs.ts.CheckLogs(rs.id, *m)
				if int(m.CommandIndex) > 1 && prevok == false {
					err_msg = fmt.Sprintf("server %v apply out of order %v", rs.id, m.CommandIndex)
				}
			}

			rs.lastApplied = int(m.CommandIndex)

			if (int(m.CommandIndex)+1)%SnapShotInterval == 0 {
				w := new(bytes.Buffer)
				e := simgob.NewEncoder(w)
				e.Encode(m.CommandIndex)
				var xlog []interface{}
				for j := 0; j <= int(m.CommandIndex); j++ {
					xlog = append(xlog, rs.logs[j])
				}
				e.Encode(xlog)
				start := harness.GetAnnotateTimestamp()

				r := rs.raft
				if r != nil {
					r.Snapshot(m.CommandIndex, w.Bytes())
				}

				details := fmt.Sprintf(
					"snapshot created after applying the command at index %v",
					m.CommandIndex)
				harness.AnnotateInfoInterval(start, "snapshot created", details)
			}
		} else {
			// Ignore other types of ApplyMsg.
		}
		if err_msg != "" {
			harness.AnnotateCheckerFailureBeforeExit("apply error", err_msg)
			log.Fatalf("apply error: %v", err_msg)
			rs.applyErr = err_msg
		}
	}
}

func (rs *rfsrv) ingestSnap(snapshot []byte, index int) string {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if snapshot == nil {
		harness.AnnotateCheckerFailureBeforeExit("failed to ingest snapshot", "nil snapshot")
		log.Fatalf("nil snapshot")
		return "nil snapshot"
	}
	r := bytes.NewBuffer(snapshot)
	d := simgob.NewDecoder(r)
	var lastIncludedIndex int
	var xlog []interface{}
	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&xlog) != nil {
		text := "failed to decode snapshot"
		harness.AnnotateCheckerFailureBeforeExit(text, text)
		log.Fatalf("snapshot decode error")
		return "snapshot Decode() error"
	}
	if index != -1 && index != lastIncludedIndex {
		err := fmt.Sprintf("server %v snapshot doesn't match m.SnapshotIndex", rs.id)
		return err
	}
	rs.logs = map[int]interface{}{}
	for j := 0; j < len(xlog); j++ {
		rs.logs[j] = xlog[j]
	}
	rs.lastApplied = lastIncludedIndex
	return ""
}

type Test struct {
	*harness.Config
	t *testing.T
	n int
	g *harness.ServerGrp

	finished int32

	mu       sync.Mutex
	srvs     []*rfsrv
	maxIndex int
	snapshot bool
}

func makeTest(t *testing.T, n int, reliable bool, snapshot bool) *Test {
	simgob.Register(Command{})
	simgob.Register(0)
	simgob.Register("")
	ts := &Test{
		t:        t,
		n:        n,
		srvs:     make([]*rfsrv, n),
		snapshot: snapshot,
	}
	ts.Config = harness.MakeConfig(t, n, reliable, ts.Mksrv)
	ts.Config.SetLongDelays(true)
	ts.g = ts.Group(harness.GRP0)
	return ts
}

func (ts *Test) cleanup() {
	atomic.StoreInt32(&ts.finished, 1)
	ts.End()
	ts.Config.Cleanup()
	ts.CheckTimeout()
}

func (ts *Test) Mksrv(ends []*simrpc.ClientEnd, grp harness.Tgid, srv int, persister *harness.Persister) []harness.IService {
	mem_persister := &MemPersister{p: persister}

	transport := NewSimTransport(ends)

	applyCh := make(chan *api.ApplyMessage)
	cfg := raft.DefaultConfig()

	_, l := logger.NewTestLogger()
	r, _ := raft.NewNodeBuilder(srv, applyCh, nil, transport).
		WithConfig(cfg).
		WithLogger(l).
		WithPersister(mem_persister).
		Build()

	r.Start()

	s := &rfsrv{
		ts:        ts,
		id:        srv,
		raft:      r,
		applyCh:   applyCh,
		persister: persister,
		snapshot:  ts.snapshot,
		logs:      make(map[int]interface{}),
	}
	ts.mu.Lock()
	ts.srvs[srv] = s
	ts.mu.Unlock()

	if s.snapshot {
		snapshot := persister.ReadSnapshot()
		if len(snapshot) > 0 {
			err := s.ingestSnap(snapshot, -1)
			if err != "" {
				ts.t.Fatal(err)
			}
		}
		go s.applierSnap(applyCh)
	} else {
		go s.applier()
	}
	return []harness.IService{&RaftService{r.(*raft.Raft)}}
}

func (ts *Test) Restart(i int) {
	ts.g.StartServer(i)
	ts.g.ConnectOne(i)
}

func (ts *Test) CheckOneLeader() int {
	harness.AnnotateCheckerBegin("checking for a single leader")
	for iters := 0; iters < 10; iters++ {
		ms := 450 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		leaders := make(map[int][]int)
		for i := 0; i < ts.n; i++ {
			if ts.g.IsConnected(i) {
				if term, leader := ts.srvs[i].GetState(); leader {
					leaders[term] = append(leaders[term], i)
				}
			}
		}

		lastTermWithLeader := -1
		for term, leaders := range leaders {
			if len(leaders) > 1 {
				details := fmt.Sprintf("multiple leaders in term %v = %v", term, leaders)
				harness.AnnotateCheckerFailure("multiple leaders", details)
				ts.Fatalf("term %d has %d (>1) leaders", term, len(leaders))
			}
			if term > lastTermWithLeader {
				lastTermWithLeader = term
			}
		}

		if len(leaders) != 0 {
			details := fmt.Sprintf("leader in term %v = %v",
				lastTermWithLeader, leaders[lastTermWithLeader][0])
			harness.AnnotateCheckerSuccess(details, details)
			return leaders[lastTermWithLeader][0]
		}
	}
	details := "unable to find a leader"
	harness.AnnotateCheckerFailure("no leader", details)
	ts.Fatalf("expected one leader, got none")
	return -1
}

func (ts *Test) CheckTerms() int {
	harness.AnnotateCheckerBegin("checking term agreement")
	term := -1
	for i := 0; i < ts.n; i++ {
		if ts.g.IsConnected(i) {
			xterm, _ := ts.srvs[i].GetState()
			if term == -1 {
				term = xterm
			} else if term != xterm {
				details := fmt.Sprintf("node ids -> terms = { %v -> %v; %v -> %v }",
					i-1, term, i, xterm)
				harness.AnnotateCheckerFailure("term disagreed", details)
				ts.Fatalf("servers disagree on term")
			}
		}
	}
	details := fmt.Sprintf("term = %v", term)
	harness.AnnotateCheckerSuccess("term agreed", details)
	return term
}

func (ts *Test) CheckLogs(i int, m api.ApplyMessage) (string, bool) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	err_msg := ""

	var decodedCmd Command
	r := bytes.NewBuffer(m.Command)
	d := simgob.NewDecoder(r)
	if err := d.Decode(&decodedCmd); err != nil {
		log.Printf("decode error: %v", err)
		return "failed to decode command", false
	}
	cmd1 := decodedCmd.Data

	v := cmd1
	me := ts.srvs[i]
	for j, rs := range ts.srvs {
		if old, oldok := rs.Logs(int(m.CommandIndex)); oldok && old != v {
			err_msg = fmt.Sprintf("commit index=%v server=%v %v != server=%v %v",
				m.CommandIndex, i, m.Command, j, old)
		}
	}
	_, prevok := me.logs[int(m.CommandIndex)-1]
	me.logs[int(m.CommandIndex)] = v
	if int(m.CommandIndex) > ts.maxIndex {
		ts.maxIndex = int(m.CommandIndex)
	}
	return err_msg, prevok
}

func (ts *Test) CheckNoLeader() {
	harness.AnnotateCheckerBegin("checking no unexpected leader among connected servers")
	for i := 0; i < ts.n; i++ {
		if ts.g.IsConnected(i) {
			_, is_leader := ts.srvs[i].GetState()
			if is_leader {
				details := fmt.Sprintf("leader = %v", i)
				harness.AnnotateCheckerFailure("unexpected leader found", details)
				ts.Fatalf("%s", details)
			}
		}
	}
	harness.AnnotateCheckerSuccess("no unexpected leader", "no unexpected leader")
}

func (ts *Test) CheckNoAgreement(index int) {
	text := fmt.Sprintf("checking no unexpected agreement at index %v", index)
	harness.AnnotateCheckerBegin(text)
	n, _ := ts.NCommitted(index)
	if n > 0 {
		desp := fmt.Sprintf("unexpected agreement at index %v", index)
		details := fmt.Sprintf("%v server(s) commit incorrectly index", n)
		harness.AnnotateCheckerFailure(desp, details)
		ts.Fatalf("%v committed but no majority", n)
	}
	desp := fmt.Sprintf("no unexpected agreement at index %v", index)
	harness.AnnotateCheckerSuccess(desp, "OK")
}

func (ts *Test) NCommitted(index int) (int, any) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	count := 0
	var cmd any = nil
	for _, rs := range ts.srvs {
		if rs.applyErr != "" {
			harness.AnnotateCheckerFailure("apply error", rs.applyErr)
			ts.t.Fatal(rs.applyErr)
		}

		cmd1, ok := rs.Logs(index)

		if ok {
			if count > 0 && cmd != cmd1 {
				text := fmt.Sprintf("committed values at index %v do not match (%v != %v)",
					index, cmd, cmd1)
				harness.AnnotateCheckerFailure("unmatched committed values", text)
				ts.Fatalf("%s", text)
			}
			count += 1
			cmd = cmd1
		}
	}
	return count, cmd
}

func (ts *Test) One(cmd any, expectedServers int, retry bool) int {
	var textretry string
	if retry {
		textretry = "with"
	} else {
		textretry = "without"
	}

	textcmd := fmt.Sprintf("%v", cmd)
	textb := fmt.Sprintf("checking agreement of %.8s by at least %v servers %v retry",
		textcmd, expectedServers, textretry)
	harness.AnnotateCheckerBegin(textb)
	t0 := time.Now()
	starts := 0

	for time.Since(t0).Seconds() < 10 && ts.CheckFinished() == false {
		index := -1
		for range ts.srvs {
			starts = (starts + 1) % len(ts.srvs)
			var rf api.Raft
			if ts.g.IsConnected(starts) {
				ts.mu.Lock()
				if ts.srvs[starts] != nil {
					rf = ts.srvs[starts].raft
				}
				ts.mu.Unlock()
			}

			if rf != nil {
				w := new(bytes.Buffer)
				e := simgob.NewEncoder(w)
				wrappedCmd := Command{Data: cmd}
				e.Encode(wrappedCmd)
				cmdBytes := w.Bytes()
				index1, _, ok := rf.Submit(cmdBytes)

				if ok {
					index = int(index1)
					break
				}
			}
		}

		if index != -1 {
			t1 := time.Now()
			for time.Since(t1).Seconds() < 2 {
				nd, cmd1 := ts.NCommitted(index)
				if nd > 0 && nd >= expectedServers {
					if fmt.Sprintf("%v", cmd1) == fmt.Sprintf("%v", cmd) {
						desp := fmt.Sprintf("agreement of %.8s reached", textcmd)
						harness.AnnotateCheckerSuccess(desp, "OK")
						return index
					}
				}
				time.Sleep(20 * time.Millisecond)
			}

			if retry == false {
				desp := fmt.Sprintf("agreement of %.8s failed", textcmd)
				harness.AnnotateCheckerFailure(desp, "failed after submitting command")
				ts.Fatalf("one(%v) failed to reach agreement", cmd)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}

	if ts.CheckFinished() == false {
		desp := fmt.Sprintf("agreement of %.8s failed", textcmd)
		harness.AnnotateCheckerFailure(desp, "failed after 10-second timeout")
		ts.Fatalf("one(%v) failed to reach agreement", cmd)
	}

	return -1

}

func (ts *Test) CheckFinished() bool {
	z := atomic.LoadInt32(&ts.finished)
	return z != 0
}

func (ts *Test) Wait(index int, n int, startTerm int) any {
	to := 10 * time.Millisecond
	for iters := 0; iters < 30; iters++ {
		nd, _ := ts.NCommitted(index)
		if nd >= n {
			break
		}
		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
		if startTerm > -1 {
			for _, rs := range ts.srvs {
				if t, _ := rs.raft.State(); int(t) > startTerm {
					// someone has moved on
					// can no longer guarantee that we'll "win"
					return -1
				}
			}
		}
	}
	nd, cmd := ts.NCommitted(index)
	if nd < n {
		desp := fmt.Sprintf("less than %v servers commit index %v", n, index)
		details := fmt.Sprintf(
			"only %v (< %v) servers commit index %v at term %v", nd, n, index, startTerm)
		harness.AnnotateCheckerFailure(desp, details)
		ts.Fatalf("only %d decided for index %d; wanted %d",
			nd, index, n)
	}
	return cmd
}
