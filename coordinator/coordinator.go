package coordinator

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/shrtyk/raft-core/api"
	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
	"github.com/shrtyk/raft-core/internal/retry"
	"github.com/shrtyk/raft-core/pkg/logger"
	"google.golang.org/grpc"
)

const staleLeader = -1

var _ api.Coordinator = (*Coordinator)(nil)

// Coordinator is a thread-safe client for a Raft cluster.
// It discovers the leader and routes client requests to it.
type Coordinator struct {
	logger         *slog.Logger
	requestTimeout time.Duration
	clients        []raftpb.RaftServiceClient

	mu       sync.RWMutex
	leaderId int
}

func NewCoordinator(
	conns []*grpc.ClientConn,
	reqTimeout time.Duration,
	logger *slog.Logger,
) (*Coordinator, error) {
	c := &Coordinator{
		logger:         logger,
		requestTimeout: reqTimeout,
		clients:        make([]raftpb.RaftServiceClient, len(conns)),
		leaderId:       staleLeader,
	}

	for i, conn := range conns {
		c.clients[i] = raftpb.NewRaftServiceClient(conn)
	}

	return c, nil
}

func (c *Coordinator) Submit(ctx context.Context, cmd []byte) (*api.SubmitResult, error) {
	var result *api.SubmitResult

	err := retry.Do(ctx, func(ctx context.Context) error {
		leader, err := c.getLeader(ctx)
		if err != nil {
			return err
		}

		req := &raftpb.SubmitRequest{Command: cmd}
		resp, err := c.clients[leader].SubmitCommand(ctx, req)
		if err != nil {
			c.logger.Warn(
				"failed to submit command to leader",
				slog.Int("leader_id", leader),
				logger.ErrAttr(err),
			)
			c.invalidateLeader(leader)
			return err
		}

		if resp.IsLeader {
			result = &api.SubmitResult{
				Term:     resp.Term,
				LogIndex: resp.Index,
				IsLeader: true,
			}
			return nil
		}

		c.logger.Debug(
			"contacted node is not leader, retrying",
			slog.Int("node_id", leader),
		)
		c.invalidateLeader(leader)
		return errors.New("not leader, retrying")
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

func (c *Coordinator) Read(ctx context.Context, query []byte) ([]byte, error) {
	var data []byte

	err := retry.Do(ctx, func(ctx context.Context) error {
		leader, err := c.getLeader(ctx)
		if err != nil {
			return err
		}

		req := &raftpb.ReadOnlyRequest{Query: query}
		resp, err := c.clients[leader].ReadOnly(ctx, req)
		if err != nil {
			c.logger.Warn(
				"failed to send read-only to leader",
				slog.Int("leader_id", leader),
				logger.ErrAttr(err),
			)
			c.invalidateLeader(leader)
			return err
		}

		if resp.IsLeader {
			data = resp.Data
			return nil
		}

		c.logger.Debug(
			"contacted node is not leader for read-only, retrying",
			slog.Int("node_id", leader),
		)
		c.invalidateLeader(leader)
		return errors.New("not leader, retrying")
	})

	if err != nil {
		return nil, err
	}

	return data, nil
}

// getLeader returns the current leader, discovering it if necessary.
// It is safe for concurrent use.
func (c *Coordinator) getLeader(ctx context.Context) (int, error) {
	c.mu.RLock()
	leader := c.leaderId
	c.mu.RUnlock()
	if leader != staleLeader {
		return leader, nil
	}

	// Slow path: leader is unknown.
	discoveredLeader, err := c.discoverLeader(ctx)
	if err != nil {
		return -1, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Re-check leaderId. Another goroutine might have
	// updated it while we were running discoverLeader.
	if c.leaderId != staleLeader {
		return c.leaderId, nil
	}

	c.leaderId = discoveredLeader
	return c.leaderId, nil
}

// invalidateLeader marks the current leader as stale if it matches the given one.
// It is safe for concurrent use.
func (c *Coordinator) invalidateLeader(currentLeaderId int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leaderId == currentLeaderId {
		c.leaderId = staleLeader
	}
}

func (c *Coordinator) discoverLeader(ctx context.Context) (peerId int, err error) {
	var wg sync.WaitGroup
	respChan := make(chan *raftpb.IsLeaderResponse, 1)

	tctx, tcancel := context.WithTimeout(ctx, c.requestTimeout)
	defer tcancel()
	for peerId := range c.clients {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r, err := c.clients[id].IsLeader(tctx, &raftpb.IsLeaderRequest{})
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					c.logger.Warn(
						"failed to get IsLeaderResponse",
						slog.Int("peer_id", id),
						logger.ErrAttr(err),
					)
				}
				return
			}

			if r.IsLeader {
				select {
				case respChan <- r:
				default:
				}
			}
		}(peerId)
	}

	go func() {
		wg.Wait()
		close(respChan)
	}()

	select {
	case <-tctx.Done():
		err = tctx.Err()
	case r, ok := <-respChan:
		if !ok {
			err = errors.New("leader discovery failed: all nodes failed to respond")
			break
		}
		peerId = int(r.PeerId)
	}

	return
}
