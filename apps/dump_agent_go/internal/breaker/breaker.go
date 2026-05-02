// Package breaker — CLOSED→OPEN→HALF_OPEN circuit breaker for outbound calls.
package breaker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/cnesdata/dumpagent/internal/obs"
)

// ErrOpen is returned when the breaker rejects a call (state == OPEN).
var ErrOpen = errors.New("breaker: open")

type state int

const (
	stateClosed state = iota
	stateOpen
	stateHalfOpen
)

// CircuitBreaker tracks consecutive failures and blocks calls when OPEN.
// Single instance shared across drain + RegisterJob keeps fate together.
type CircuitBreaker struct {
	mu          sync.Mutex
	consecutive int
	state       state
	openedAt    time.Time
	threshold   int
	resetAfter  time.Duration
	name        string
	nowFunc     func() time.Time
}

// New constructs a CLOSED breaker. threshold is consecutive failures to trip
// OPEN; resetAfter is wait before HALF_OPEN probe.
func New(threshold int, resetAfter time.Duration, name string) *CircuitBreaker {
	return &CircuitBreaker{
		threshold:  threshold,
		resetAfter: resetAfter,
		name:       name,
		nowFunc:    time.Now,
	}
}

// Call invokes fn, tracking success/failure. Returns ErrOpen when blocked.
func (b *CircuitBreaker) Call(ctx context.Context, fn func(context.Context) error) error {
	if !b.gateBefore() {
		return ErrOpen
	}
	err := fn(ctx)
	b.gateAfter(err)
	return err
}

// gateBefore returns true if call should proceed.
func (b *CircuitBreaker) gateBefore() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case stateClosed, stateHalfOpen:
		return true
	case stateOpen:
		if b.nowFunc().Sub(b.openedAt) >= b.resetAfter {
			b.state = stateHalfOpen
			slog.Info("breaker_half_open",
				"event_id", obs.EventBreakerHalfOpen,
				"name", b.name)
			return true
		}
		return false
	}
	return false
}

// gateAfter records the outcome of a permitted call.
func (b *CircuitBreaker) gateAfter(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err == nil {
		if b.state == stateHalfOpen {
			slog.Info("breaker_closed",
				"event_id", obs.EventBreakerClosed,
				"name", b.name)
		}
		b.state = stateClosed
		b.consecutive = 0
		return
	}
	b.consecutive++
	if b.state == stateHalfOpen {
		b.state = stateOpen
		b.openedAt = b.nowFunc()
		slog.Error("breaker_reopened",
			"event_id", obs.EventBreakerOpened,
			"name", b.name)
		return
	}
	if b.consecutive >= b.threshold && b.state == stateClosed {
		b.state = stateOpen
		b.openedAt = b.nowFunc()
		slog.Error("breaker_opened",
			"event_id", obs.EventBreakerOpened,
			"name", b.name,
			"consecutive", b.consecutive)
	}
}
