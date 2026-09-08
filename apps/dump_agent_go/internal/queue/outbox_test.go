package queue

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"go.etcd.io/bbolt"
)

func rawEnvelope(t *testing.T, job string, fence uint64) Envelope {
	t.Helper()
	payload := fmt.Sprintf(`{"type":"raw_manifest","job_id":%q,"fencing_token":%d,
		"source_key":{"Source":"CNES","Intent":"VINCULO","Competencia":"2026-01"},
		"manifest_json":"e30=","manifest_sha256":"abc"}`, job, fence)
	var env Envelope
	if err := json.Unmarshal([]byte(payload), &env); err != nil {
		t.Fatal(err)
	}
	return env
}

func TestReciboTerminalSobreviveReinicioEDeletaJuntoDoEnvelope(t *testing.T) {
	out, path := newTestOutbox(t)
	require.NoError(t, out.Append(rawEnvelope(t, "job", 1)))
	items, err := out.Peek(10)
	require.NoError(t, err)
	require.NoError(t, out.MarkRawTerminal(items[0].Key))
	require.NoError(t, out.Close())
	out, err = Open(path)
	require.NoError(t, err)
	defer out.Close()
	terminal, err := out.RawTerminal(items[0].Key)
	require.NoError(t, err)
	require.True(t, terminal)
	require.NoError(t, out.Delete(items[0].Key))
	terminal, err = out.RawTerminal(items[0].Key)
	require.NoError(t, err)
	require.False(t, terminal)
	require.Error(t, out.MarkRawTerminal(items[0].Key))
}

func TestEnvelopeRawNaoExpiraPorTTLOuLimite(t *testing.T) {
	ob, _ := newTestOutbox(t)
	now := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)
	ob.nowFunc = func() time.Time { return now }
	old := rawEnvelope(t, "old", 1)
	old.EnqueuedAt = now.Add(-100 * 24 * time.Hour)
	for _, env := range []Envelope{
		old, rawEnvelope(t, "new", 2),
		{Type: TypeComplete, JobUUID: "expired", EnqueuedAt: old.EnqueuedAt},
		{Type: TypeComplete, JobUUID: "cap"}, {Type: TypeComplete, JobUUID: "kept"},
	} {
		if err := ob.Append(env); err != nil {
			t.Fatal(err)
		}
	}
	deleted, err := ob.Evict(90*24*time.Hour, 1)
	if err != nil || deleted != 2 {
		t.Fatalf("deleted=%d error=%v want=2", deleted, err)
	}
	items, err := ob.Peek(10)
	if err != nil || len(items) != 3 {
		t.Fatalf("remaining=%d error=%v want=3", len(items), err)
	}
	if items[2].Envelope.JobUUID != "kept" {
		t.Fatal("legacy_fifo_changed=true")
	}
}

func TestNovoFenceCriaEnvelopeImutavel(t *testing.T) {
	ob, path := newTestOutbox(t)
	env := rawEnvelope(t, "job", 1)
	if err := ob.Append(env); err != nil {
		t.Fatal(err)
	}
	before, _ := ob.Peek(10)
	if err := ob.Close(); err != nil {
		t.Fatal(err)
	}
	ob, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ob.Close()
	env.Attempts = 3
	if err := ob.Append(env); err != nil {
		t.Fatal(err)
	}
	if err := ob.Append(rawEnvelope(t, "job", 2)); err != nil {
		t.Fatal(err)
	}
	after, _ := ob.Peek(10)
	if len(after) != 2 || !reflect.DeepEqual(before[0], after[0]) {
		t.Fatalf("immutable_replay_failed=true count=%d", len(after))
	}
}

func TestRejeitaReplayRawComIdentidadeDivergente(t *testing.T) {
	for _, field := range []string{"source_key", "manifest_json", "manifest_sha256"} {
		t.Run(field, func(t *testing.T) {
			ob, _ := newTestOutbox(t)
			env := rawEnvelope(t, "job", 1)
			if err := ob.Append(env); err != nil {
				t.Fatal(err)
			}
			payload, _ := json.Marshal(env)
			fields := map[string]json.RawMessage{}
			_ = json.Unmarshal(payload, &fields)
			fields[field] = json.RawMessage(`"different"`)
			if field == "source_key" {
				fields[field] = json.RawMessage(`{"Source":"SIHD"}`)
			}
			if field == "manifest_json" {
				fields[field] = json.RawMessage(`"eyJ4IjoxfQ=="`)
			}
			payload, _ = json.Marshal(fields)
			_ = json.Unmarshal(payload, &env)
			if err := ob.Append(env); err == nil {
				t.Fatal("conflicting_replay_accepted=true")
			}
		})
	}
}

func TestApagarEnvelopeRawLiberaIndice(t *testing.T) {
	ob, _ := newTestOutbox(t)
	env := rawEnvelope(t, "job", 1)
	if err := ob.Append(env); err != nil {
		t.Fatal(err)
	}
	items, _ := ob.Peek(10)
	if err := ob.Delete(items[0].Key); err != nil {
		t.Fatal(err)
	}
	if err := ob.Append(env); err != nil {
		t.Fatal(err)
	}
	items, _ = ob.Peek(10)
	if len(items) != 1 {
		t.Fatalf("remaining=%d want=1", len(items))
	}
}

func TestAppendLegadoNaoSobrescreveRawAposReinicio(t *testing.T) {
	ob, path := newTestOutbox(t)
	now := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)
	env := rawEnvelope(t, "job", 1)
	env.EnqueuedAt = now
	if err := ob.Append(env); err != nil {
		t.Fatal(err)
	}
	if err := ob.Close(); err != nil {
		t.Fatal(err)
	}
	ob, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ob.Close()
	for i := 0; i < 3; i++ {
		if err := ob.Append(Envelope{Type: TypeComplete, JobUUID: id(i), EnqueuedAt: now}); err != nil {
			t.Fatal(err)
		}
	}
	items, err := ob.Peek(10)
	if err != nil || len(items) != 4 {
		t.Fatalf("remaining=%d error=%v want=4", len(items), err)
	}
	if !reflect.DeepEqual(env, items[0].Envelope) {
		t.Fatal("raw_overwritten=true")
	}
}

func TestRejeitaEnvelopeRawSemIdentidadeCompleta(t *testing.T) {
	for _, field := range []string{
		"job_id", "fencing_token", "source_key", "manifest_json", "manifest_sha256",
	} {
		ob, _ := newTestOutbox(t)
		payload, _ := json.Marshal(rawEnvelope(t, "job", 1))
		fields := map[string]json.RawMessage{}
		_ = json.Unmarshal(payload, &fields)
		delete(fields, field)
		payload, _ = json.Marshal(fields)
		var env Envelope
		if err := json.Unmarshal(payload, &env); err != nil {
			t.Fatal(err)
		}
		if err := ob.Append(env); err == nil {
			t.Errorf("missing_identity_accepted=%s", field)
		}
	}
}

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

func TestOutbox_RejeitaOverflowDaSequencia(t *testing.T) {
	ob, _ := newTestOutbox(t)
	err := ob.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket([]byte(bucketName)).SetSequence(uint64(^uint32(0)))
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := ob.Append(Envelope{JobUUID: "overflow"}); err == nil {
		t.Fatal("expected sequence overflow")
	}
}

func id(i int) string { return string(rune('a' + i)) }
func idDeep(worker, j int) string {
	return string([]rune{rune('a' + worker), rune('0' + (j % 10))})
}
