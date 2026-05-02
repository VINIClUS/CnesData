package breaker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func newTestBreaker(threshold int, reset time.Duration) *CircuitBreaker {
	return New(threshold, reset, "test")
}

func TestBreaker_ClosedToOpenAtThreshold(t *testing.T) {
	b := newTestBreaker(3, 60*time.Second)
	bang := errors.New("boom")
	for i := 0; i < 2; i++ {
		err := b.Call(context.Background(), func(_ context.Context) error { return bang })
		if !errors.Is(err, bang) {
			t.Fatalf("call %d: got %v want bang", i, err)
		}
	}
	// 3rd failure trips OPEN (return value still bang)
	err := b.Call(context.Background(), func(_ context.Context) error { return bang })
	if !errors.Is(err, bang) {
		t.Fatalf("3rd call: got %v want bang", err)
	}
	// 4th call: breaker OPEN
	err = b.Call(context.Background(), func(_ context.Context) error {
		t.Fatal("must not be called when OPEN")
		return nil
	})
	if !errors.Is(err, ErrOpen) {
		t.Fatalf("got %v want ErrOpen", err)
	}
}

func TestBreaker_OpenToHalfOpenAfterReset(t *testing.T) {
	now := time.Now()
	b := newTestBreaker(2, 30*time.Second)
	b.nowFunc = func() time.Time { return now }
	bang := errors.New("boom")
	_ = b.Call(context.Background(), func(_ context.Context) error { return bang })
	_ = b.Call(context.Background(), func(_ context.Context) error { return bang })
	// OPEN now
	if got := b.Call(context.Background(), func(_ context.Context) error { return nil }); !errors.Is(got, ErrOpen) {
		t.Fatalf("expected ErrOpen, got %v", got)
	}
	// Advance clock past reset
	b.nowFunc = func() time.Time { return now.Add(31 * time.Second) }
	called := false
	err := b.Call(context.Background(), func(_ context.Context) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("HALF_OPEN probe success returned %v", err)
	}
	if !called {
		t.Fatal("expected probe fn to be called in HALF_OPEN")
	}
	// Now CLOSED again — fresh failures must accumulate
	_ = b.Call(context.Background(), func(_ context.Context) error { return bang })
	if got := b.Call(context.Background(), func(_ context.Context) error { return nil }); errors.Is(got, ErrOpen) {
		t.Fatal("unexpected ErrOpen — counter should have reset on CLOSED")
	}
}

func TestBreaker_HalfOpenProbeFailReopens(t *testing.T) {
	now := time.Now()
	b := newTestBreaker(1, 30*time.Second)
	b.nowFunc = func() time.Time { return now }
	bang := errors.New("boom")
	_ = b.Call(context.Background(), func(_ context.Context) error { return bang })
	// OPEN
	b.nowFunc = func() time.Time { return now.Add(31 * time.Second) }
	// HALF_OPEN probe fails
	err := b.Call(context.Background(), func(_ context.Context) error { return bang })
	if !errors.Is(err, bang) {
		t.Fatalf("probe got %v want bang", err)
	}
	// Should be OPEN again immediately
	err = b.Call(context.Background(), func(_ context.Context) error { return nil })
	if !errors.Is(err, ErrOpen) {
		t.Fatalf("after failed probe: got %v want ErrOpen", err)
	}
}

func TestBreaker_ConcurrentCallsSafe(t *testing.T) {
	b := newTestBreaker(100, time.Second)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = b.Call(context.Background(), func(_ context.Context) error { return nil })
		}()
	}
	wg.Wait()
	// No assertion beyond race-detector clean run.
}

func TestBreaker_CtxCancellation(t *testing.T) {
	b := newTestBreaker(3, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := b.Call(ctx, func(c context.Context) error {
		return c.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v want context.Canceled", err)
	}
}
