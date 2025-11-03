package coordinator

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/shrtyk/raft-core/api"
	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
	"github.com/shrtyk/raft-core/pkg/logger"
	"google.golang.org/grpc"
)

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
	}

	for i, conn := range conns {
		c.clients[i] = raftpb.NewRaftServiceClient(conn)
	}

	return c, nil
}

func (*Coordinator) Submit(ctx context.Context, cmd []byte) (*api.SubmitResult, error) {
	return nil, nil
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
