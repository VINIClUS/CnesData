package obs_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/cnesdata/dumpagent/internal/obs"
	"github.com/stretchr/testify/require"
)

func TestSafeRun_ReturnsNilOnSuccess(t *testing.T) {
	err := obs.SafeRun(func() error { return nil }, "test")
	require.NoError(t, err)
}

func TestSafeRun_PropagatesError(t *testing.T) {
	want := errors.New("boom")
	got := obs.SafeRun(func() error { return want }, "test")
	require.ErrorIs(t, got, want)
}

func TestSafeRun_RecoversPanic(t *testing.T) {
	err := obs.SafeRun(func() error {
		panic("kaboom")
	}, "scope_x")
	require.Error(t, err)
	require.Contains(t, err.Error(), "panic in scope_x")
	require.Contains(t, err.Error(), "kaboom")
}

func TestSafeGo_ReturnsErrorViaChannel(t *testing.T) {
	ch := obs.SafeGo(func() error { return errors.New("fail") }, "scope_y")
	err := <-ch
	require.Error(t, err)
	require.Equal(t, "fail", err.Error())
}

func TestSafeGo_RecoversPanicInGoroutine(t *testing.T) {
	ch := obs.SafeGo(func() error { panic("goroutine panic") }, "scope_z")
	err := <-ch
	require.Error(t, err)
	require.True(t, strings.HasPrefix(err.Error(), "panic in scope_z"))
}

func TestSafeGo_ClosesChannelAfterSuccess(t *testing.T) {
	ch := obs.SafeGo(func() error { return nil }, "ok")
	_, open := <-ch
	require.False(t, open, "channel should be closed after goroutine returns")
}
