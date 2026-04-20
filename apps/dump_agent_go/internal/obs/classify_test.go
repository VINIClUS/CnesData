package obs_test

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/cnesdata/dumpagent/internal/obs"
	"github.com/stretchr/testify/require"
)

func TestClassify_ContextCanceled_Transient(t *testing.T) {
	k := obs.Classify(context.Canceled)
	require.Equal(t, obs.FailureTransient, k)
}

func TestClassify_HTTP429_RateLimit(t *testing.T) {
	err := &obs.HTTPError{StatusCode: 429, Body: "rate limit"}
	require.Equal(t, obs.FailureRateLimit, obs.Classify(err))
}

func TestClassify_HTTP403_TimeSkew_Transient(t *testing.T) {
	err := &obs.HTTPError{StatusCode: 403, Body: "<Error>RequestTimeTooSkewed</Error>"}
	require.Equal(t, obs.FailureTransient, obs.Classify(err))
}

func TestClassify_HTTP403_Other_Permanent(t *testing.T) {
	err := &obs.HTTPError{StatusCode: 403, Body: "InvalidAccessKey"}
	require.Equal(t, obs.FailurePermanent, obs.Classify(err))
}

func TestClassify_HTTP5xx_Transient(t *testing.T) {
	err := &obs.HTTPError{StatusCode: 502}
	require.Equal(t, obs.FailureTransient, obs.Classify(err))
}

func TestClassify_HTTP400_Permanent(t *testing.T) {
	err := &obs.HTTPError{StatusCode: 400, Body: "bad request"}
	require.Equal(t, obs.FailurePermanent, obs.Classify(err))
}

func TestClassify_NetTimeout_Transient(t *testing.T) {
	var netErr net.Error = &timeoutErr{}
	require.Equal(t, obs.FailureTransient, obs.Classify(netErr))
}

func TestClassify_Default_Transient(t *testing.T) {
	require.Equal(t, obs.FailureTransient, obs.Classify(errors.New("unknown")))
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }
