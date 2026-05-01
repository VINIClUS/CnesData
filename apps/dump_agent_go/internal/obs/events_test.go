package obs

import (
	"testing"
)

func TestEventID_NoDuplicates(t *testing.T) {
	all := allEventIDs()
	seen := map[EventID]string{}
	for name, id := range all {
		if prior, ok := seen[id]; ok {
			t.Fatalf("duplicate event id %d: %s and %s", id, prior, name)
		}
		seen[id] = name
	}
}

func TestEventID_P4ReservationsPreserved(t *testing.T) {
	if EventDPAPIUnwrapFailed != 1003 {
		t.Fatalf("EventDPAPIUnwrapFailed must remain 1003 (P4 reserved), got %d", EventDPAPIUnwrapFailed)
	}
	if EventCertRotationHalted != 1004 {
		t.Fatalf("EventCertRotationHalted must remain 1004 (P4 reserved), got %d", EventCertRotationHalted)
	}
}

func TestEventID_BandsRespected(t *testing.T) {
	bands := []struct {
		name string
		min  EventID
		max  EventID
		ids  []EventID
	}{
		{"auth", 1000, 1999, []EventID{
			EventDPAPIUnwrapFailed, EventCertRotationHalted, EventCertExpiringSoon,
			EventOAuthRefreshDenied, EventClockSkewExcessive,
		}},
		{"queue", 2000, 2999, []EventID{
			EventQueueOpenFailed, EventQueueCorrupted, EventQueueDrained,
		}},
		{"extract", 3000, 3999, []EventID{
			EventExtractFailed, EventExtractEmpty, EventFirebirdConnFailed,
		}},
		{"upload", 4000, 4999, []EventID{
			EventUploadFailed, EventUploadRetryExceeded, EventPresignedURLExpired,
		}},
		{"diagnose", 5000, 5999, []EventID{EventDiagnoseFailed}},
		{"generic", 9000, 9999, []EventID{
			EventStartup, EventShutdown, EventPanicRecovered, EventUnknown,
		}},
	}
	for _, b := range bands {
		for _, id := range b.ids {
			if id < b.min || id > b.max {
				t.Errorf("band=%s id=%d out of [%d..%d]", b.name, id, b.min, b.max)
			}
		}
	}
}

// allEventIDs is exported via the test for duplicate detection.
// Keep in sync with constants in events.go.
func allEventIDs() map[string]EventID {
	return map[string]EventID{
		"EventDPAPIUnwrapFailed":   EventDPAPIUnwrapFailed,
		"EventCertRotationHalted":  EventCertRotationHalted,
		"EventCertExpiringSoon":    EventCertExpiringSoon,
		"EventOAuthRefreshDenied":  EventOAuthRefreshDenied,
		"EventClockSkewExcessive":  EventClockSkewExcessive,
		"EventQueueOpenFailed":     EventQueueOpenFailed,
		"EventQueueCorrupted":      EventQueueCorrupted,
		"EventQueueDrained":        EventQueueDrained,
		"EventExtractFailed":       EventExtractFailed,
		"EventExtractEmpty":        EventExtractEmpty,
		"EventFirebirdConnFailed":  EventFirebirdConnFailed,
		"EventUploadFailed":        EventUploadFailed,
		"EventUploadRetryExceeded": EventUploadRetryExceeded,
		"EventPresignedURLExpired": EventPresignedURLExpired,
		"EventDiagnoseFailed":      EventDiagnoseFailed,
		"EventStartup":             EventStartup,
		"EventShutdown":            EventShutdown,
		"EventPanicRecovered":      EventPanicRecovered,
		"EventUnknown":             EventUnknown,
	}
}
