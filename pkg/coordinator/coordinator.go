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

type Coordinator struct {
	logger         *slog.Logger
	requestTimeout time.Duration
	clients        []raftpb.RaftServiceClient

	leaderId int
}

func NewCoordinator(
	conns []*grpc.ClientConn,
	reqTimeout time.Duration,
	logger *slog.Logger,
) (*Coordinator, error) {
	c := &Coordinator{
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
		if c.leaderId == staleLeader {
			leader, err := c.discoverLeader(ctx)
			if err != nil {
				c.logger.Warn("failed to discover leader", logger.ErrAttr(err))
				return err
			}
			c.leaderId = leader
		}

		req := &raftpb.SubmitRequest{Command: cmd}
		resp, err := c.clients[c.leaderId].SubmitCommand(ctx, req)
		if err != nil {
			c.logger.Warn(
				"failed to submit command to leader",
				slog.Int("leader_id", c.leaderId),
				logger.ErrAttr(err),
			)
			c.leaderId = staleLeader
			return err
		}

		if resp.IsLeader {
			result = &api.SubmitResult{
				Term:     resp.Term,
				LogIndex: resp.Index,
				IsLeader: true,
			}
			return nil
		} else {
			c.logger.Debug(
				"contacted node is not leader, retrying",
				slog.Int("node_id", c.leaderId),
			)
			c.leaderId = staleLeader
			return errors.New("not leader, retrying")
		}
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

func (c *Coordinator) Read(ctx context.Context, query []byte) ([]byte, error) {
	var data []byte

	err := retry.Do(ctx, func(ctx context.Context) error {
		if c.leaderId == staleLeader {
			leader, err := c.discoverLeader(ctx)
			if err != nil {
				c.logger.Warn("failed to discover leader", logger.ErrAttr(err))
				return err
			}
			c.leaderId = leader
		}

		req := &raftpb.ReadOnlyRequest{Query: query}
		resp, err := c.clients[c.leaderId].ReadOnly(ctx, req)
		if err != nil {
			c.logger.Warn(
				"failed to send read-only to leader",
				slog.Int("leader_id", c.leaderId),
				logger.ErrAttr(err),
			)
			c.leaderId = staleLeader
			return err
		}

		if resp.IsLeader {
			data = resp.Data
			return nil
		} else {
			c.logger.Debug(
				"contacted node is not leader for read-only, retrying",
				slog.Int("node_id", c.leaderId),
			)
			c.leaderId = staleLeader
			return errors.New("not leader, retrying")
		}
	})

	if err != nil {
		return nil, err
	}

	return data, nil
}

func (c *Coordinator) discoverLeader(ctx context.Context) (peerId int, err error) {
	var wg sync.WaitGroup
	respChan := make(chan *raftpb.IsLeaderResponse)

	tctx, tcancel := context.WithTimeout(ctx, c.requestTimeout)
	defer tcancel()
	for peerId := range c.clients {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r, err := c.clients[id].IsLeader(tctx, &raftpb.IsLeaderRequest{})
			if err != nil {
				c.logger.Warn(
					"failed to get IsLeaderResponse",
					slog.Int("peer_id", id),
					logger.ErrAttr(err),
				)
			}
			if r.IsLeader {
				respChan <- r
			}
		}(peerId)
	}

	select {
	case <-tctx.Done():
		err = tctx.Err()
	case r := <-respChan:
		tcancel()
		peerId = int(r.PeerId)
	}

	go func() {
		wg.Wait()
		close(respChan)
	}()

	return
}
