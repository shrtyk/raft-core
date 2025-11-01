package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	raftpb "github.com/shrtyk/raft-core/internal/proto/gen"
	"github.com/shrtyk/raft-core/pkg/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Coordinator struct {
	logger         *slog.Logger
	requestTimeout time.Duration
	conns          []*grpc.ClientConn
	clients        []raftpb.RaftServiceClient
}

func NewCoordinator(
	peerAddrs []string,
	reqTimeout time.Duration,
	logger *slog.Logger,
) (*Coordinator, error) {
	c := &Coordinator{requestTimeout: reqTimeout}

	for i, addr := range peerAddrs {
		conn, err := grpc.NewClient(
			addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("dial peer %d: %w", i, err)
		}
		c.conns[i] = conn
		c.clients[i] = raftpb.NewRaftServiceClient(conn)
	}

	return c, nil
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

func (c *Coordinator) Shutdown() error {
	var err error
	for i, conn := range c.conns {
		if cerr := conn.Close(); cerr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("failed to close peer %d connection: %w", i, cerr))
		}
	}
	return err
}
