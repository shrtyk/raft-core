package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetry(t *testing.T) {
	t.Run("success on first try", func(t *testing.T) {
		var attempts int
		fn := func(ctx context.Context) error {
			attempts++
			return nil
		}

		err := Do(context.Background(), fn, WithMaxAttempts(3))

		if err != nil {
			t.Errorf("expected no error, but got: %v", err)
		}
		if attempts != 1 {
			t.Errorf("expected 1 attempt, but got: %d", attempts)
		}
	})

	t.Run("success after a few retries", func(t *testing.T) {
		var attempts int
		fn := func(ctx context.Context) error {
			attempts++
			if attempts < 3 {
				return errors.New("transient error")
			}
			return nil
		}

		err := Do(
			context.Background(),
			fn,
			WithMaxAttempts(5),
			WithBaseDelay(1*time.Millisecond),
		)

		if err != nil {
			t.Errorf("expected no error, but got: %v", err)
		}
		if attempts != 3 {
			t.Errorf("expected 3 attempts, but got: %d", attempts)
		}
	})

	t.Run("failure after all retries", func(t *testing.T) {
		var attempts int
		expectedErr := errors.New("error")
		fn := func(ctx context.Context) error {
			attempts++
			return expectedErr
		}

		err := Do(
			context.Background(),
			fn,
			WithMaxAttempts(4),
			WithBaseDelay(1*time.Millisecond))

		if !errors.Is(err, expectedErr) {
			t.Errorf("expected error '%v', but got: %v", expectedErr, err)
		}
		if attempts != 4 {
			t.Errorf("expected 4 attempts, but got: %d", attempts)
		}
	})

	t.Run("context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var attempts int
		fn := func(ctx context.Context) error {
			attempts++
			return errors.New("error")
		}

		go func() {
			time.Sleep(5 * time.Millisecond)
			cancel()
		}()

		err := Do(
			ctx,
			fn,
			WithMaxAttempts(10),
			WithBaseDelay(10*time.Millisecond))

		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled error, but got: %v", err)
		}
		if attempts >= 10 {
			t.Errorf("expected fewer than 10 attempts, but got: %d", attempts)
		}
	})
}
