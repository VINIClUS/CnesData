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
type Envelope struct {
	Type       EnvelopeType `json:"type"`
	JobUUID    string       `json:"job_uuid"`
	SizeBytes  int64        `json:"size_bytes,omitempty"`
	Cause      string       `json:"cause,omitempty"`
	EnqueuedAt time.Time    `json:"enqueued_at"`
	Attempts   int          `json:"attempts"`
	LastError  string       `json:"last_error,omitempty"`
}
