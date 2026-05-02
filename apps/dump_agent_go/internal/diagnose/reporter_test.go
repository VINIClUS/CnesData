package diagnose

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestReporter_TextRendersAllSeverities(t *testing.T) {
	checks := []Check{
		{Name: "cert", Severity: SeverityPass, Message: "valid",
			Fields: map[string]any{"days_remaining": 91}},
		{Name: "outbox", Severity: SeverityWarn, Message: "approaching_cap",
			Fields: map[string]any{"count": 8123}},
		{Name: "minio", Severity: SeverityFail, Message: "dial_failed",
			Fields: map[string]any{"endpoint": "minio:9000"}},
	}
	var buf bytes.Buffer
	if err := Text(&buf, checks); err != nil {
		t.Fatalf("Text: %v", err)
	}
	out := buf.String()
	for _, want := range []string{"[PASS] cert", "[WARN] outbox", "[FAIL] minio",
		"days_remaining=91", "count=8123", "endpoint=minio:9000",
		"Summary: 1 PASS, 1 WARN, 1 FAIL"} {
		if !strings.Contains(out, want) {
			t.Errorf("text output missing %q\nfull output:\n%s", want, out)
		}
	}
}

func TestReporter_JSONRoundTrip(t *testing.T) {
	in := []Check{
		{Name: "cert", Severity: SeverityPass, Message: "valid",
			Fields: map[string]any{"days_remaining": 91}},
	}
	var buf bytes.Buffer
	if err := JSON(&buf, in); err != nil {
		t.Fatalf("JSON: %v", err)
	}
	var out []Check
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(out) != 1 || out[0].Name != "cert" || out[0].Severity != SeverityPass {
		t.Errorf("round-trip mismatch: %+v", out)
	}
}

func TestReporter_AnyFailTrueOnFail(t *testing.T) {
	checks := []Check{
		{Severity: SeverityPass},
		{Severity: SeverityWarn},
		{Severity: SeverityFail},
	}
	if !AnyFail(checks) {
		t.Error("expected true when FAIL present")
	}
}

func TestReporter_AnyFailFalseOnPassWarn(t *testing.T) {
	checks := []Check{
		{Severity: SeverityPass},
		{Severity: SeverityWarn},
	}
	if AnyFail(checks) {
		t.Error("expected false when only PASS+WARN")
	}
}
