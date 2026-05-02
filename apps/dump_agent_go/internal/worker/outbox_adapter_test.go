package worker

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/cnesdata/dumpagent/internal/queue"
)

type mockJobAPI struct {
	registerCalls  int
	completeCalls  int
	failCalls      int
	heartbeatCalls int
	registerErr    error
	registerJob    *Job
}

func (m *mockJobAPI) RegisterJob(_ context.Context, _ JobSpec) (*Job, error) {
	m.registerCalls++
	return m.registerJob, m.registerErr
}
func (m *mockJobAPI) CompleteJob(_ context.Context, _ Job, _ int64) error {
	m.completeCalls++
	return nil
}
func (m *mockJobAPI) FailJob(_ context.Context, _ Job, _ error) error {
	m.failCalls++
	return nil
}
func (m *mockJobAPI) SendHeartbeat(_ context.Context, _ string) error {
	m.heartbeatCalls++
	return nil
}

func newOutbox(t *testing.T) *queue.Outbox {
	t.Helper()
	ob, err := queue.Open(filepath.Join(t.TempDir(), "ob.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = ob.Close() })
	return ob
}

func TestOutboxAdapter_CompleteWritesEnvelopeNotInner(t *testing.T) {
	mock := &mockJobAPI{}
	ob := newOutbox(t)
	a := NewOutboxAdapter(mock, ob)
	job := Job{ID: "uuid-1"}
	if err := a.CompleteJob(context.Background(), job, 4096); err != nil {
		t.Fatalf("CompleteJob: %v", err)
	}
	if mock.completeCalls != 0 {
		t.Errorf("inner.CompleteJob called %d times, want 0", mock.completeCalls)
	}
	items, _ := ob.Peek(10)
	if len(items) != 1 || items[0].Envelope.Type != queue.TypeComplete ||
		items[0].Envelope.JobUUID != "uuid-1" || items[0].Envelope.SizeBytes != 4096 {
		t.Fatalf("envelope mismatch: %+v", items)
	}
}

func TestOutboxAdapter_FailWritesEnvelopeNotInner(t *testing.T) {
	mock := &mockJobAPI{}
	ob := newOutbox(t)
	a := NewOutboxAdapter(mock, ob)
	job := Job{ID: "uuid-2"}
	cause := errors.New("extract_failed")
	if err := a.FailJob(context.Background(), job, cause); err != nil {
		t.Fatalf("FailJob: %v", err)
	}
	if mock.failCalls != 0 {
		t.Errorf("inner.FailJob called %d times, want 0", mock.failCalls)
	}
	items, _ := ob.Peek(10)
	if len(items) != 1 || items[0].Envelope.Type != queue.TypeFail ||
		items[0].Envelope.Cause != "extract_failed" {
		t.Fatalf("envelope mismatch: %+v", items)
	}
}

func TestOutboxAdapter_RegisterJobDelegates(t *testing.T) {
	mock := &mockJobAPI{registerJob: &Job{ID: "back"}}
	ob := newOutbox(t)
	a := NewOutboxAdapter(mock, ob)
	job, err := a.RegisterJob(context.Background(), JobSpec{})
	if err != nil {
		t.Fatalf("RegisterJob: %v", err)
	}
	if job == nil || job.ID != "back" {
		t.Fatalf("got %+v, want delegated Job", job)
	}
	if mock.registerCalls != 1 {
		t.Fatalf("inner.RegisterJob called %d times, want 1", mock.registerCalls)
	}
	items, _ := ob.Peek(10)
	if len(items) != 0 {
		t.Fatalf("unexpected envelope from RegisterJob: %+v", items)
	}
}

func TestOutboxAdapter_HeartbeatDelegates(t *testing.T) {
	mock := &mockJobAPI{}
	ob := newOutbox(t)
	a := NewOutboxAdapter(mock, ob)
	if err := a.SendHeartbeat(context.Background(), "uuid-3"); err != nil {
		t.Fatalf("SendHeartbeat: %v", err)
	}
	if mock.heartbeatCalls != 1 {
		t.Fatalf("inner.SendHeartbeat called %d times, want 1", mock.heartbeatCalls)
	}
	items, _ := ob.Peek(10)
	if len(items) != 0 {
		t.Fatalf("unexpected envelope from Heartbeat: %+v", items)
	}
}
