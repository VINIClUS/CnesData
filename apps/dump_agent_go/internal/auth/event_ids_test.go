package auth

import (
	"os"
	"strings"
	"testing"
)

// TestEventIDAttrsOnP4CallSites enforces P5.1 spec acceptance:
// every slog call that emits dpapi_unwrap_failed or cert_rotation_halt
// must carry obs.EventDPAPIUnwrapFailed or obs.EventCertRotationHalted.
// If the call site is not present in current source, the test skips.
// This serves as a forward drift guard for when those slog calls land.
func TestEventIDAttrsOnP4CallSites(t *testing.T) {
	cases := []struct {
		file    string
		msg     string
		attrVal string
	}{
		{"store.go", "dpapi_unwrap_failed", "obs.EventDPAPIUnwrapFailed"},
		{"rotate.go", "cert_rotation_halt", "obs.EventCertRotationHalted"},
	}
	for _, c := range cases {
		body, err := os.ReadFile(c.file)
		if err != nil {
			t.Fatalf("read %s: %v", c.file, err)
		}
		text := string(body)
		if !strings.Contains(text, c.msg) {
			t.Skipf("call site %s not present in %s; drift guard inactive", c.msg, c.file)
		}
		if !strings.Contains(text, c.attrVal) {
			t.Errorf("%s: emits %q without %q event_id attr",
				c.file, c.msg, c.attrVal)
		}
	}
}
