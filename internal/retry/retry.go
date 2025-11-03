package retry

import (
	"context"
	"time"
)

// Func is a function that can be retried
type Func func(ctx context.Context) error

// DelayFunc is a closure which will return delay generator function
type DelayFunc func(int) time.Duration

type config struct {
	maxAttempts int
	baseDelay   time.Duration
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

// WithBaseDelay sets base retry duration.
// Default value 150ms.
func WithBaseDelay(d time.Duration) Option {
	return func(c *config) {
		c.baseDelay = d
	}
}

// WithDelayFunc will return new delay based on current attempt
// By default delay will be doubled after every attempt.
func WithDelayFunc(fn func(int) time.Duration) Option {
	return func(c *config) {
		c.delayFunc = fn
	}
}

func Do(ctx context.Context, fn Func, opts ...Option) error {
	cfg := &config{
		maxAttempts: 3,
		baseDelay:   150 * time.Millisecond,
	}
	cfg.delayFunc = func(attempt int) time.Duration {
		return time.Duration(int64(cfg.baseDelay) << attempt)
	}

	for _, opt := range opts {
		opt(cfg)
	}

	var lastErr error
	for attempt := range cfg.maxAttempts {
		lastErr = fn(ctx)
		if lastErr == nil {
			return nil
		}

		if attempt == cfg.maxAttempts-1 {
			break
		}

		timer := time.NewTimer(cfg.delayFunc(attempt))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return lastErr
}
