package queue

import (
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func newTestOutbox(t *testing.T) (*Outbox, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "queue", "outbox.db")
	ob, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = ob.Close() })
	return ob, path
}

func TestOutbox_AppendPeekFIFO(t *testing.T) {
	ob, _ := newTestOutbox(t)
	for i := 0; i < 5; i++ {
		err := ob.Append(Envelope{
			Type: TypeComplete, JobUUID: id(i),
			EnqueuedAt: time.Date(2026, 1, 1, 0, 0, i, 0, time.UTC),
		})
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	items, err := ob.Peek(10)
	if err != nil {
		t.Fatalf("Peek: %v", err)
	}
	if len(items) != 5 {
		t.Fatalf("got %d items, want 5", len(items))
	}
	for i, it := range items {
		if it.Envelope.JobUUID != id(i) {
			t.Errorf("position %d: got %q want %q", i, it.Envelope.JobUUID, id(i))
		}
	}
}

func TestOutbox_DeleteRemovesEntry(t *testing.T) {
	ob, _ := newTestOutbox(t)
	for i := 0; i < 3; i++ {
		_ = ob.Append(Envelope{Type: TypeComplete, JobUUID: id(i)})
	}
	items, _ := ob.Peek(10)
	if err := ob.Delete(items[1].Key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	after, _ := ob.Peek(10)
	if len(after) != 2 {
		t.Fatalf("got %d after delete, want 2", len(after))
	}
	for _, it := range after {
		if it.Envelope.JobUUID == id(1) {
			t.Fatalf("middle item still present after delete")
		}
	}
}

func TestOutbox_EvictByAge(t *testing.T) {
	ob, _ := newTestOutbox(t)
	now := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)
	ob.nowFunc = func() time.Time { return now }
	for i := 0; i < 5; i++ {
		_ = ob.Append(Envelope{
			Type:       TypeComplete,
			JobUUID:    id(i),
			EnqueuedAt: now.Add(-time.Duration(i+1) * 24 * time.Hour),
		})
	}
	deleted, err := ob.Evict(2*24*time.Hour, 1000)
	if err != nil {
		t.Fatalf("Evict: %v", err)
	}
	if deleted != 3 {
		t.Fatalf("got deleted=%d want 3 (envelopes >2d old)", deleted)
	}
	items, _ := ob.Peek(10)
	if len(items) != 2 {
		t.Fatalf("got %d remaining, want 2", len(items))
	}
}

func TestOutbox_EvictByCap(t *testing.T) {
	ob, _ := newTestOutbox(t)
	now := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)
	ob.nowFunc = func() time.Time { return now }
	for i := 0; i < 10; i++ {
		_ = ob.Append(Envelope{
			Type:       TypeComplete,
			JobUUID:    id(i),
			EnqueuedAt: now,
		})
	}
	deleted, err := ob.Evict(365*24*time.Hour, 4)
	if err != nil {
		t.Fatalf("Evict: %v", err)
	}
	if deleted != 6 {
		t.Fatalf("got deleted=%d want 6 (10-4)", deleted)
	}
	items, _ := ob.Peek(20)
	if len(items) != 4 {
		t.Fatalf("got %d after cap, want 4", len(items))
	}
	for i, it := range items {
		if it.Envelope.JobUUID != id(6+i) {
			t.Errorf("position %d: got %q want %q", i, it.Envelope.JobUUID, id(6+i))
		}
	}
}

func TestOutbox_RestartSurvives(t *testing.T) {
	ob, path := newTestOutbox(t)
	for i := 0; i < 3; i++ {
		_ = ob.Append(Envelope{Type: TypeComplete, JobUUID: id(i)})
	}
	if err := ob.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ob2, err := Open(path)
	if err != nil {
		t.Fatalf("Re-open: %v", err)
	}
	defer ob2.Close()
	items, _ := ob2.Peek(10)
	if len(items) != 3 {
		t.Fatalf("after re-open: got %d items, want 3", len(items))
	}
}

func TestOutbox_ConcurrentAppendRaceClean(t *testing.T) {
	ob, _ := newTestOutbox(t)
	var wg sync.WaitGroup
	for w := 0; w < 10; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = ob.Append(Envelope{
					Type:    TypeComplete,
					JobUUID: idDeep(worker, j),
				})
			}
		}(w)
	}
	wg.Wait()
	items, _ := ob.Peek(10000)
	if len(items) != 1000 {
		t.Fatalf("got %d items after concurrent append, want 1000", len(items))
	}
}

func TestOutbox_CloseIdempotent(t *testing.T) {
	ob, _ := newTestOutbox(t)
	if err := ob.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := ob.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func id(i int) string             { return string(rune('a' + i)) }
func idDeep(worker, j int) string { return string([]rune{rune('a' + worker), rune('0' + (j % 10))}) }
