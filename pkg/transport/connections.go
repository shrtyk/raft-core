package transport

import (
	"errors"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func SetupConnections(peerAddrs []string) ([]*grpc.ClientConn, func() error, error) {
	var err error
	conns := make([]*grpc.ClientConn, len(peerAddrs))
	for i, addr := range peerAddrs {
		conn, clientError := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if clientError != nil {
			err = errors.Join(err, clientError)
			for j := range i {
				closeErr := conns[j].Close()
				if closeErr != nil {
					err = errors.Join(err, fmt.Errorf("failed to close peer %d connections: %w", j, closeErr))
				}
			}
			return nil, nil, err
		}
		conns[i] = conn
	}

	closeFunc := func() error {
		var cferr error
		for i, conn := range conns {
			if cerr := conn.Close(); cerr != nil {
				cferr = errors.Join(cferr, fmt.Errorf("failed to close peer %d connections: %w", i, cerr))
			}
		}
		return cferr
	}

	return conns, closeFunc, nil
}
