package obs

// EventID identifies a DumpAgent event in Windows Event Log.
// Bands are append-only; never recycle. ~95 IDs slack per band.
//
//	1xxx  auth (P4 — OAuth, mTLS, cert lifecycle)
//	2xxx  queue (P5.2 — bbolt persistence)
//	3xxx  extraction (Firebird/SIA/BPA jobs)
//	4xxx  upload (MinIO PUT, presigned URL)
//	5xxx  diagnose (P5.4)
//	9xxx  generic (panic, startup, shutdown, unknown)
type EventID uint32

const (
	// 1xxx auth.
	EventDPAPIUnwrapFailed  EventID = 1003
	EventCertRotationHalted EventID = 1004
	EventCertExpiringSoon   EventID = 1005
	EventOAuthRefreshDenied EventID = 1006
	EventClockSkewExcessive EventID = 1007

	// 2xxx queue (placeholders for P5.2).
	EventQueueOpenFailed EventID = 2001
	EventQueueCorrupted  EventID = 2002
	EventQueueDrained    EventID = 2003

	// 3xxx extraction.
	EventExtractFailed      EventID = 3001
	EventExtractEmpty       EventID = 3002
	EventFirebirdConnFailed EventID = 3003

	// 4xxx upload.
	EventUploadFailed        EventID = 4001
	EventUploadRetryExceeded EventID = 4002
	EventPresignedURLExpired EventID = 4003

	// 5xxx diagnose (placeholders for P5.4).
	EventDiagnoseFailed EventID = 5001

	// 9xxx generic.
	EventStartup        EventID = 9001
	EventShutdown       EventID = 9002
	EventPanicRecovered EventID = 9003
	EventUnknown        EventID = 9999
)
