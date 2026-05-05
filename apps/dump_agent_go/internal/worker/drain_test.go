package worker

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/cnesdata/dumpagent/internal/breaker"
	"github.com/cnesdata/dumpagent/internal/obs"
	"github.com/cnesdata/dumpagent/internal/queue"
)

type stubJobAPI struct {
	registerResp error
	failResp     error
	registerN    int
	failN        int
	lastJob      Job
	lastSize     int64
}

func (s *stubJobAPI) MintUploadURL(_ context.Context, _ JobSpec) (*Job, error) {
	return nil, nil
}
func (s *stubJobAPI) RegisterJob(_ context.Context, job Job, sizeBytes int64) error {
	s.registerN++
	s.lastJob = job
	s.lastSize = sizeBytes
	return s.registerResp
}
func (s *stubJobAPI) FailJob(_ context.Context, _ Job, _ error) error {
	s.failN++
	return s.failResp
}
func (s *stubJobAPI) SendHeartbeat(_ context.Context, _ string) error { return nil }

func newDrainFixture(t *testing.T, registerResp error) (*Drainer, *queue.Outbox, *stubJobAPI) {
	t.Helper()
	ob, err := queue.Open(filepath.Join(t.TempDir(), "ob.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = ob.Close() })
	stub := &stubJobAPI{registerResp: registerResp}
	br := breaker.New(5, 60*time.Second, "test")
	d := NewDrainer(ob, br, stub)
	return d, ob, stub
}

func TestDrain_HappyPathDeletesEnvelope(t *testing.T) {
	d, ob, stub := newDrainFixture(t, nil)
	_ = ob.Append(queue.Envelope{Type: queue.TypeComplete, JobUUID: "uuid-1", SizeBytes: 100})
	d.tick(context.Background())
	if stub.registerN != 1 {
		t.Errorf("inner RegisterJob calls=%d want 1", stub.registerN)
	}
	items, _ := ob.Peek(10)
	if len(items) != 0 {
		t.Errorf("envelope still present after success: %+v", items)
	}
}

func TestDrain_ReplaysSha256AndMinioKey(t *testing.T) {
	d, ob, stub := newDrainFixture(t, nil)
	_ = ob.Append(queue.Envelope{
		Type:      queue.TypeComplete,
		JobUUID:   "uuid-replay",
		SizeBytes: 2048,
		SHA256:    "deadbeef",
		MinioKey:  "354130/CNES_VINCULO/2026-01-01/x.parquet.gz",
	})
	d.tick(context.Background())
	if stub.registerN != 1 {
		t.Fatalf("RegisterJob calls=%d want 1", stub.registerN)
	}
	if stub.lastJob.ID != "uuid-replay" {
		t.Errorf("Job.ID=%q want uuid-replay", stub.lastJob.ID)
	}
	if stub.lastJob.Sha256 != "deadbeef" {
		t.Errorf("Job.Sha256=%q want deadbeef", stub.lastJob.Sha256)
	}
	if stub.lastJob.MinioKey != "354130/CNES_VINCULO/2026-01-01/x.parquet.gz" {
		t.Errorf("Job.MinioKey=%q lost on replay", stub.lastJob.MinioKey)
	}
	if stub.lastSize != 2048 {
		t.Errorf("sizeBytes=%d want 2048", stub.lastSize)
	}
}

func TestDrain_TerminalDropDeletes(t *testing.T) {
	d, ob, _ := newDrainFixture(t, &obs.HTTPError{StatusCode: 404})
	_ = ob.Append(queue.Envelope{Type: queue.TypeComplete, JobUUID: "uuid-2"})
	d.tick(context.Background())
	items, _ := ob.Peek(10)
	if len(items) != 0 {
		t.Errorf("envelope still present after 404: %+v", items)
	}
}

func TestDrain_TransientRetainsEnvelope(t *testing.T) {
	d, ob, _ := newDrainFixture(t, &obs.HTTPError{StatusCode: 503})
	_ = ob.Append(queue.Envelope{Type: queue.TypeComplete, JobUUID: "uuid-3"})
	d.tick(context.Background())
	items, _ := ob.Peek(10)
	if len(items) != 1 {
		t.Fatalf("envelope dropped after transient: %+v", items)
	}
	if items[0].Envelope.Attempts != 1 {
		t.Errorf("attempts=%d want 1", items[0].Envelope.Attempts)
	}
}

func TestDrain_BreakerTripsAfterThreshold(t *testing.T) {
	d, ob, _ := newDrainFixture(t, &obs.HTTPError{StatusCode: 503})
	for i := 0; i < 10; i++ {
		_ = ob.Append(queue.Envelope{Type: queue.TypeComplete, JobUUID: "uuid-x"})
	}
	d.tick(context.Background())
	items, _ := ob.Peek(20)
	// After 5 transient failures (threshold), breaker OPEN; remaining envelopes untouched.
	if len(items) < 5 {
		t.Fatalf("expected at least 5 retained after breaker trip, got %d", len(items))
	}
}

func TestDrain_FailEnvelopeDispatched(t *testing.T) {
	d, ob, stub := newDrainFixture(t, nil)
	_ = ob.Append(queue.Envelope{
		Type: queue.TypeFail, JobUUID: "uuid-4", Cause: "oops",
	})
	d.tick(context.Background())
	if stub.failN != 1 {
		t.Errorf("inner FailJob calls=%d want 1", stub.failN)
	}
	items, _ := ob.Peek(10)
	if len(items) != 0 {
		t.Errorf("Fail envelope retained: %+v", items)
	}
}

func TestDrain_NetworkErrIsTransient(t *testing.T) {
	d, ob, _ := newDrainFixture(t, errors.New("dial: connection refused"))
	_ = ob.Append(queue.Envelope{Type: queue.TypeComplete, JobUUID: "uuid-5"})
	d.tick(context.Background())
	items, _ := ob.Peek(10)
	if len(items) != 1 {
		t.Fatalf("envelope dropped after network err: %+v", items)
	}
}

func TestDrain_RunRespectsCtxCancel(t *testing.T) {
	d, _, _ := newDrainFixture(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit on ctx cancel")
	}
}

func TestDrainer_TickIntervalJittered_Low(t *testing.T) {
	d, _, _ := newDrainFixture(t, nil)
	d.SetRand(func() float64 { return 0.0 }) // bottom of jitter window
	got := d.NextInterval()
	want := 24 * time.Second
	if got != want {
		t.Errorf("rand=0.0 got %v want %v", got, want)
	}
}

func TestDrainer_TickIntervalJittered_High(t *testing.T) {
	d, _, _ := newDrainFixture(t, nil)
	d.SetRand(func() float64 { return 1.0 })
	got := d.NextInterval()
	want := 36 * time.Second
	if got != want {
		t.Errorf("rand=1.0 got %v want %v", got, want)
	}
}

func TestDrainer_TickIntervalJittered_Mid(t *testing.T) {
	d, _, _ := newDrainFixture(t, nil)
	d.SetRand(func() float64 { return 0.5 })
	got := d.NextInterval()
	want := 30 * time.Second
	if got != want {
		t.Errorf("rand=0.5 got %v want %v", got, want)
	}
}
