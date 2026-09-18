// Package breaker — CLOSED→OPEN→HALF_OPEN circuit breaker for outbound calls.
package breaker

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/cnesdata/dumpagent/internal/obs"
)

const breakerJitterFraction = 0.33

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
	resetWindow time.Duration
	threshold   int
	resetAfter  time.Duration
	name        string
	nowFunc     func() time.Time
	rand        func() float64
}

// New constructs a CLOSED breaker. threshold is consecutive failures to trip
// OPEN; resetAfter is wait before HALF_OPEN probe (jittered ±33% per OPEN).
func New(threshold int, resetAfter time.Duration, name string) *CircuitBreaker {
	return &CircuitBreaker{
		threshold:  threshold,
		resetAfter: resetAfter,
		name:       name,
		nowFunc:    time.Now,
		rand:       rand.Float64,
	}
}

// SetClock replaces the time source. Used by tests.
func (b *CircuitBreaker) SetClock(c func() time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nowFunc = c
}

// SetRand replaces the rand source. Used by tests.
func (b *CircuitBreaker) SetRand(r func() float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.rand = r
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
		if b.nowFunc().Sub(b.openedAt) >= b.resetWindow {
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
		b.openTrip("breaker_reopened")
		return
	}
	if b.consecutive >= b.threshold && b.state == stateClosed {
		b.openTrip("breaker_opened")
	}
}

// openTrip transitions to OPEN and snapshots a fresh resetWindow.
// Caller must hold b.mu.
func (b *CircuitBreaker) openTrip(msg string) {
	b.state = stateOpen
	b.openedAt = b.nowFunc()
	b.resetWindow = obs.JitterAround(b.resetAfter, breakerJitterFraction, b.rand)
	slog.Error(msg,
		"event_id", obs.EventBreakerOpened,
		"name", b.name,
		"consecutive", b.consecutive,
		"reset_window_ms", b.resetWindow.Milliseconds())
}
