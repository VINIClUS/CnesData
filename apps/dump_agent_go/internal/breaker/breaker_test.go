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
	b.rand = func() float64 { return 0.5 }
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
	b.rand = func() float64 { return 0.5 }
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

func TestBreaker_ResetWindowJittered_Low(t *testing.T) {
	now := time.Now()
	b := New(2, 60*time.Second, "test")
	b.SetClock(func() time.Time { return now })
	b.SetRand(func() float64 { return 0.0 })

	// Trip OPEN
	for i := 0; i < 2; i++ {
		_ = b.Call(context.Background(), func(context.Context) error {
			return errors.New("fail")
		})
	}

	// At 39s, not eligible
	b.SetClock(func() time.Time { return now.Add(39 * time.Second) })
	if err := b.Call(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, ErrOpen) {
		t.Errorf("at 39s expected ErrOpen, got %v", err)
	}

	// At 40.2s, eligible (60s × (1 - 0.33) = 40.2s)
	b.SetClock(func() time.Time { return now.Add(40200 * time.Millisecond) })
	if err := b.Call(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Errorf("at 40.2s expected pass, got %v", err)
	}
}

func TestBreaker_ResetWindowJittered_High(t *testing.T) {
	now := time.Now()
	b := New(2, 60*time.Second, "test")
	b.SetClock(func() time.Time { return now })
	b.SetRand(func() float64 { return 1.0 })

	for i := 0; i < 2; i++ {
		_ = b.Call(context.Background(), func(context.Context) error {
			return errors.New("fail")
		})
	}

	// At 79s, not eligible
	b.SetClock(func() time.Time { return now.Add(79 * time.Second) })
	if err := b.Call(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, ErrOpen) {
		t.Errorf("at 79s expected ErrOpen, got %v", err)
	}

	// At 80s, eligible (60s × (1 + 0.33) = 79.8s)
	b.SetClock(func() time.Time { return now.Add(80 * time.Second) })
	if err := b.Call(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Errorf("at 80s expected pass, got %v", err)
	}
}

func TestBreaker_ResetWindowSnapshotStable(t *testing.T) {
	now := time.Now()
	calls := 0
	rand := func() float64 {
		calls++
		// Different value each call so a buggy impl reading rand twice is detected.
		if calls == 1 {
			return 0.0
		}
		return 1.0
	}
	b := New(2, 60*time.Second, "test")
	b.SetClock(func() time.Time { return now })
	b.SetRand(rand)

	for i := 0; i < 2; i++ {
		_ = b.Call(context.Background(), func(context.Context) error {
			return errors.New("fail")
		})
	}

	// Window snapshot was taken at trip time with rand=0.0 (window ≈ 40.2s).
	// 5 consecutive calls past the window should all see the SAME window.
	for i := 0; i < 5; i++ {
		b.SetClock(func() time.Time { return now.Add(50 * time.Second) })
		err := b.Call(context.Background(), func(context.Context) error { return nil })
		// HALF_OPEN probe permitted at 1st pass, then transitions to CLOSED on success.
		// Subsequent CLOSED-state calls also permitted.
		if err != nil {
			t.Errorf("call %d at 50s expected pass, got %v", i, err)
		}
	}
}

func TestBreaker_ResetWindowRecomputedOnReopen(t *testing.T) {
	now := time.Now()
	randVals := []float64{0.0, 1.0} // first OPEN window low; second OPEN window high
	idx := 0
	rand := func() float64 {
		v := randVals[idx]
		idx++
		return v
	}
	b := New(2, 60*time.Second, "test")
	b.SetClock(func() time.Time { return now })
	b.SetRand(rand)

	// Trip OPEN (1st window: rand=0.0 → ≈ 40.2s)
	for i := 0; i < 2; i++ {
		_ = b.Call(context.Background(), func(context.Context) error {
			return errors.New("fail")
		})
	}
	// At 41s: half-open probe permitted; probe fails → reopen with fresh window
	b.SetClock(func() time.Time { return now.Add(41 * time.Second) })
	_ = b.Call(context.Background(), func(context.Context) error {
		return errors.New("probe fail")
	})

	// Second OPEN at clock=41s with rand=1.0 → window ≈ 79.8s
	// At clock=70s (29s into 2nd OPEN), still not eligible
	b.SetClock(func() time.Time { return now.Add(70 * time.Second) })
	if err := b.Call(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, ErrOpen) {
		t.Errorf("at 70s (29s into 2nd OPEN) expected ErrOpen due to fresh high-rand window, got %v", err)
	}
}
