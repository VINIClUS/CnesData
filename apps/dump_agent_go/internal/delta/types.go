// Package delta detects insert/update/delete changes between extraction
// cycles and emits CDC-style deltas for incremental transmission.
package delta

// Op identifies a delta action.
type Op int

const (
	OpUnknown Op = iota
	OpInsert
	OpUpdate
	OpDelete
)

// String returns the canonical _op column token.
func (o Op) String() string {
	switch o {
	case OpInsert:
		return "I"
	case OpUpdate:
		return "U"
	case OpDelete:
		return "D"
	default:
		return "?"
	}
}

// SourceKey identifies one (source, intent, competencia) extraction unit.
type SourceKey struct {
	Source      string
	Intent      string
	Competencia string
}

// BucketPath returns the bbolt sub-bucket path under committed/ or pending/.
func (k SourceKey) BucketPath() string {
	return k.Source + "/" + k.Intent + "/" + k.Competencia
}

// Row is one extracted row. Payload carries source columns;
// PK is computed via Profile.PKExtractor.
type Row map[string]any

// Set bundles per-action row slices for one cycle.
type Set struct {
	Inserts []Row
	Updates []Row
	Deletes []Row
}

// TotalCount returns total rows across all three action buckets.
func (d Set) TotalCount() int {
	return len(d.Inserts) + len(d.Updates) + len(d.Deletes)
}

// Stats summarizes a cycle for slog output.
type Stats struct {
	Total          int
	NewCount       int
	ChangedCount   int
	DeletedCount   int
	CommittedCount int
}
