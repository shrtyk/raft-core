package cbreaker

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrOpenState = errors.New("circuit breaker is in open state")
)

type state int

const (
	_ state = iota
	close
	open
	halfOpen
)

type CircuitBreaker struct {
	mu    sync.RWMutex
	state state

	consecutiveFailures  int
	consecutiveSuccesses int

	failureThreshold int
	successThreshold int

	resetTimeout time.Duration
	nextProbeAt  time.Time
}

func NewCircuitBreaker(failureThreshold, successThreshold int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:            close,
		failureThreshold: failureThreshold,
		successThreshold: successThreshold,
		resetTimeout:     resetTimeout,
	}
}

type rpcCall[Response any] func(context.Context) (Response, error)

// Execute runs the given rpcCall protected by the circuit breaker.
func Do[Response any](ctx context.Context, cb *CircuitBreaker, req rpcCall[Response]) (resp Response, err error) {
	cb.mu.Lock()
	if cb.state == open {
		if time.Now().Before(cb.nextProbeAt) {
			cb.mu.Unlock()
			return resp, ErrOpenState
		}
		cb.state = halfOpen
		cb.consecutiveSuccesses = 0
	}
	cb.mu.Unlock()

	resp, err = req(ctx)

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err != nil {
		cb.consecutiveSuccesses = 0
		if cb.state == halfOpen {
			cb.open()
		} else {
			cb.consecutiveFailures++
			if cb.consecutiveFailures >= cb.failureThreshold {
				cb.open()
			}
		}
		return
	}

	if cb.state == halfOpen {
		cb.consecutiveSuccesses++
		if cb.consecutiveSuccesses >= cb.successThreshold {
			cb.reset()
		}
	} else {
		cb.consecutiveFailures = 0
	}

	return
}

func (cb *CircuitBreaker) IsClosed() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state == close || cb.state == halfOpen
}

func (cb *CircuitBreaker) open() {
	cb.state = open
	cb.nextProbeAt = time.Now().Add(cb.resetTimeout)
	cb.consecutiveFailures = 0
	cb.consecutiveSuccesses = 0
}

func (cb *CircuitBreaker) reset() {
	cb.state = close
	cb.consecutiveFailures = 0
	cb.consecutiveSuccesses = 0
}
