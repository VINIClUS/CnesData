package queue

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEnvelope_RoundTripComplete(t *testing.T) {
	now := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)
	in := Envelope{
		Type:       TypeComplete,
		JobUUID:    "abc-123",
		SizeBytes:  4096,
		EnqueuedAt: now,
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out Envelope
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Type != TypeComplete || out.JobUUID != in.JobUUID ||
		out.SizeBytes != in.SizeBytes || !out.EnqueuedAt.Equal(now) {
		t.Fatalf("round-trip lost fields: %+v", out)
	}
}

func TestEnvelope_RoundTripFail(t *testing.T) {
	in := Envelope{
		Type:    TypeFail,
		JobUUID: "abc-123",
		Cause:   "extract_failed: cnes connection lost",
	}
	b, _ := json.Marshal(in)
	var out Envelope
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Type != TypeFail || out.Cause != in.Cause {
		t.Fatalf("Fail envelope lost fields: %+v", out)
	}
}

func TestEnvelope_OmitsZeroFields(t *testing.T) {
	in := Envelope{Type: TypeComplete, JobUUID: "x"}
	b, _ := json.Marshal(in)
	s := string(b)
	for _, banned := range []string{"size_bytes", "cause", "last_error"} {
		if contains(s, banned) {
			t.Errorf("expected omitempty for %q in %s", banned, s)
		}
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
