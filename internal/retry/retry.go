package retry

import (
	"context"
	"time"
)

// Func is a function that can be retried
type Func func(ctx context.Context) error

// DelayFunc is a closure which will return delay generator function
type DelayFunc func() func() time.Duration

type config struct {
	maxAttempts int
	delayFunc   DelayFunc
}

// Option configures the retrier
type Option func(*config)

// WithMaxAttempts sets the maximum number of attempts.
// The default is 3.
func WithMaxAttempts(n int) Option {
	return func(c *config) {
		c.maxAttempts = n
	}
}

// WithDelayFunc sets the function which will
// return timeout duration for every attempt.
// The default function will return: 150ms, 300ms, 600ms.
func WithDelayFunc(d DelayFunc) Option {
	return func(c *config) {
		c.delayFunc = d
	}
}

func Do(ctx context.Context, fn Func, opts ...Option) error {
	cfg := &config{
		maxAttempts: 3,
		delayFunc: func() func() time.Duration {
			base := 150 * time.Millisecond
			attempt := 0
			return func() time.Duration {
				delay := base << attempt
				attempt++
				return delay
			}
		},
	}

	for _, opt := range opts {
		opt(cfg)
	}

	var lastErr error
	df := cfg.delayFunc()
	for attempt := range cfg.maxAttempts {
		lastErr = fn(ctx)
		if lastErr == nil {
			return nil
		}

		if attempt == cfg.maxAttempts-1 {
			break
		}

		timer := time.NewTimer(df())
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return lastErr
}
