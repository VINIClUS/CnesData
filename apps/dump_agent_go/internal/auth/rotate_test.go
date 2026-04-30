package auth_test

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/cnesdata/dumpagent/internal/auth"
)

// fakeRotateClient implements auth.RotateClient for tests.
type fakeRotateClient struct {
	httpClient *http.Client
}

func (f *fakeRotateClient) HTTPClient() *http.Client { return f.httpClient }
func (f *fakeRotateClient) Reload() error            { return nil }

func TestNewRotator_DefaultsApplied(t *testing.T) {
	c := &fakeRotateClient{httpClient: http.DefaultClient}
	r := auth.NewRotator(c, t.TempDir(), "https://x.example", "abc12345")
	if r == nil {
		t.Fatal("NewRotator returned nil")
	}
}

func TestSentinels_AreDistinct(t *testing.T) {
	if errors.Is(auth.ErrRotateNotDue, auth.ErrRotateTerminal) {
		t.Error("ErrRotateNotDue should not match ErrRotateTerminal")
	}
	if errors.Is(auth.ErrRotateTerminal, auth.ErrRotateRetriesExhausted) {
		t.Error("ErrRotateTerminal should not match ErrRotateRetriesExhausted")
	}
	if errors.Is(auth.ErrRotateNotDue, auth.ErrRotateRetriesExhausted) {
		t.Error("ErrRotateNotDue should not match ErrRotateRetriesExhausted")
	}
	_ = time.Now // keep import alive for follow-up tasks
}
