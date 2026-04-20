package obs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cnesdata/dumpagent/internal/obs"
	"github.com/stretchr/testify/require"
)

func TestWithBackoff_SucceedsFirstTry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	calls := 0
	err := obs.WithBackoff(ctx, func() error {
		calls++
		return nil
	}, obs.DefaultBackoffConfig(), "test")

	require.NoError(t, err)
	require.Equal(t, 1, calls)
}

func TestWithBackoff_RetriesOnError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	calls := 0
	err := obs.WithBackoff(ctx, func() error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	}, obs.BackoffConfig{
		InitialInterval: 10 * time.Millisecond,
		MaxInterval:     50 * time.Millisecond,
		MaxElapsedTime:  2 * time.Second,
	}, "test")

	require.NoError(t, err)
	require.Equal(t, 3, calls)
}

func TestWithBackoff_RespectsPermanentError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	calls := 0
	permErr := errors.New("permanent")
	err := obs.WithBackoff(ctx, func() error {
		calls++
		return obs.Permanent(permErr)
	}, obs.DefaultBackoffConfig(), "test")

	require.ErrorIs(t, err, permErr)
	require.Equal(t, 1, calls)
}

func TestWithBackoff_StopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	calls := 0
	err := obs.WithBackoff(ctx, func() error {
		calls++
		return errors.New("always fails")
	}, obs.BackoffConfig{
		InitialInterval: 20 * time.Millisecond,
		MaxInterval:     20 * time.Millisecond,
		MaxElapsedTime:  10 * time.Second,
	}, "test")

	require.Error(t, err)
	require.LessOrEqual(t, calls, 5)
}
