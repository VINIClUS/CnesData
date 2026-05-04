// Package audit emits an HMAC-signed JSONL append-only log of edge
// extraction lifecycle events for LGPD-compliant tamper-evident
// auditing.
package audit

import (
	"encoding/json"
	"time"
)

// Lifecycle identifies one stage in the extraction → upload → commit
// pipeline.
type Lifecycle int

const (
	LifecycleUnknown Lifecycle = iota
	LifecycleExtracted
	LifecycleUploaded
	LifecycleCommitted
	LifecycleAborted
)

// String returns the canonical lowercase token for a lifecycle value.
func (l Lifecycle) String() string {
	switch l {
	case LifecycleExtracted:
		return "extracted"
	case LifecycleUploaded:
		return "uploaded"
	case LifecycleCommitted:
		return "committed"
	case LifecycleAborted:
		return "aborted"
	default:
		return "unknown"
	}
}

// Event is a single audit log entry.
type Event struct {
	Ts           time.Time `json:"ts"`
	MachineID    string    `json:"machine_id"`
	TenantID     string    `json:"tenant_id"`
	Source       string    `json:"source"`
	Intent       string    `json:"intent"`
	Competencia  string    `json:"competencia"`
	ExtractionID string    `json:"extraction_id"`
	JobID        string    `json:"job_id"`
	SHA256       string    `json:"sha256"`
	SizeBytes    int64     `json:"size_bytes"`
	Lifecycle    Lifecycle `json:"lifecycle"`
	HMAC         string    `json:"hmac,omitempty"`
}

// CanonicalJSON returns sorted-key JSON of the event with the HMAC
// field excluded — input to HMAC-SHA256 computation.
func (e Event) CanonicalJSON() ([]byte, error) {
	m := map[string]any{
		"ts":            e.Ts.UTC().Format(time.RFC3339Nano),
		"machine_id":    e.MachineID,
		"tenant_id":     e.TenantID,
		"source":        e.Source,
		"intent":        e.Intent,
		"competencia":   e.Competencia,
		"extraction_id": e.ExtractionID,
		"job_id":        e.JobID,
		"sha256":        e.SHA256,
		"size_bytes":    e.SizeBytes,
		"lifecycle":     e.Lifecycle.String(),
	}
	return json.Marshal(m)
}
