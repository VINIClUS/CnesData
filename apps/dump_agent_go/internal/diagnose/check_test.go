package diagnose

import (
	"encoding/json"
	"testing"
)

func TestCheck_JSONRoundTrip(t *testing.T) {
	in := Check{
		Name:     "cert",
		Severity: SeverityPass,
		Message:  "valid",
		Fields:   map[string]any{"days_remaining": 91},
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out Check
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Name != in.Name || out.Severity != in.Severity || out.Message != in.Message {
		t.Errorf("round-trip mismatch: got %+v want %+v", out, in)
	}
}

func TestSeverityConstants(t *testing.T) {
	cases := []struct {
		s    string
		want string
	}{
		{SeverityPass, "PASS"},
		{SeverityWarn, "WARN"},
		{SeverityFail, "FAIL"},
	}
	for _, c := range cases {
		if c.s != c.want {
			t.Errorf("got %q want %q", c.s, c.want)
		}
	}
}

func TestConfigDefaults(t *testing.T) {
	cfg := Config{}
	if cfg.Probe {
		t.Error("default Probe should be false")
	}
	if cfg.JSON {
		t.Error("default JSON should be false")
	}
}
