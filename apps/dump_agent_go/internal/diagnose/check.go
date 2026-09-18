// Package diagnose runs read-only health checks against the local edge agent install.
package diagnose

// Severity values returned per Check.
const (
	SeverityPass = "PASS"
	SeverityWarn = "WARN"
	SeverityFail = "FAIL"
)

// Check is the result of a single diagnose probe.
type Check struct {
	Name     string         `json:"name"`
	Severity string         `json:"severity"`
	Message  string         `json:"message"`
	Fields   map[string]any `json:"fields,omitempty"`
}

// Config drives Run. Probe gates network checks. JSON selects output format.
type Config struct {
	Probe            bool
	JSON             bool
	AuthDir          string
	AppData          string
	BaseURL          string
	FBDsn            string
	MinIOEP          string
	DiscoverYAMLPath string
	DeltaDBPath      string
}
