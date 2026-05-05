// Package queue — bbolt-backed persistent outbox + classification.
package queue

import "time"

// EnvelopeType discriminates persisted outbound API calls.
type EnvelopeType string

const (
	TypeComplete EnvelopeType = "complete"
	TypeFail     EnvelopeType = "fail"
)

// Envelope is a single outbound API call persisted in the outbox.
// SizeBytes is set for complete; Cause is set for fail.
// SHA256 + MinioKey are set for TypeComplete (FU1 RegisterJob dispatch)
// so drain replay after agent restart can rebuild the full Job payload.
type Envelope struct {
	Type       EnvelopeType `json:"type"`
	JobUUID    string       `json:"job_uuid"`
	SizeBytes  int64        `json:"size_bytes,omitempty"`
	SHA256     string       `json:"sha256,omitempty"`
	MinioKey   string       `json:"minio_key,omitempty"`
	Cause      string       `json:"cause,omitempty"`
	EnqueuedAt time.Time    `json:"enqueued_at"`
	Attempts   int          `json:"attempts"`
	LastError  string       `json:"last_error,omitempty"`
}
