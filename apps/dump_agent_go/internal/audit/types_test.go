package audit

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLifecycle_String(t *testing.T) {
	require.Equal(t, "extracted", LifecycleExtracted.String())
	require.Equal(t, "uploaded", LifecycleUploaded.String())
	require.Equal(t, "committed", LifecycleCommitted.String())
	require.Equal(t, "aborted", LifecycleAborted.String())
	require.Equal(t, "unknown", Lifecycle(99).String())
}

func TestEvent_CanonicalJSON_SortedKeys(t *testing.T) {
	ev := Event{
		Ts:           time.Date(2026, 5, 4, 10, 0, 0, 0, time.UTC),
		MachineID:    "8a3f0c11",
		TenantID:     "presidente-epitacio",
		Source:       "cnes",
		Intent:       "profissionais",
		Competencia:  "202605",
		ExtractionID: "9f3a2b1c-4e5d-6f78-90ab-cdef01234567",
		JobID:        "job-abc",
		SHA256:       "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		SizeBytes:    12345,
		Lifecycle:    LifecycleUploaded,
	}
	got, err := ev.CanonicalJSON()
	require.NoError(t, err)
	var parsed map[string]any
	require.NoError(t, json.Unmarshal(got, &parsed))
	require.NotContains(t, parsed, "hmac")
	require.Contains(t, parsed, "sha256")
	require.Contains(t, parsed, "lifecycle")
}

func TestEvent_CanonicalJSON_ExcludesHMACField(t *testing.T) {
	ev := Event{Lifecycle: LifecycleExtracted, HMAC: "should-not-appear"}
	got, err := ev.CanonicalJSON()
	require.NoError(t, err)
	require.NotContains(t, string(got), "should-not-appear")
	require.NotContains(t, string(got), "hmac")
}

func TestEvent_CanonicalJSON_KeysAreSorted(t *testing.T) {
	ev := Event{Lifecycle: LifecycleExtracted, MachineID: "z", Source: "a"}
	got, err := ev.CanonicalJSON()
	require.NoError(t, err)
	s := string(got)
	require.Less(t, indexOfSub(s, "lifecycle"), indexOfSub(s, "machine_id"))
	require.Less(t, indexOfSub(s, "machine_id"), indexOfSub(s, "source"))
}

func indexOfSub(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
