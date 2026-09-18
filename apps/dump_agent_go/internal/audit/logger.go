package audit

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Logger appends HMAC-signed audit events to daily-rotated JSONL files.
type Logger struct {
	hmacKey   []byte
	dir       string
	machineID string
	tenantID  string
	timeNow   func() time.Time
}

// New returns a Logger writing under dir; machineID + tenantID stamped on
// each event. Caller pre-creates dir if needed (Append also MkdirAll).
func New(dir, machineID, tenantID string, key []byte) *Logger {
	return &Logger{
		hmacKey:   key,
		dir:       dir,
		machineID: machineID,
		tenantID:  tenantID,
		timeNow:   time.Now,
	}
}

// SetTimeNow injects a test clock. Production callers should not invoke.
func (l *Logger) SetTimeNow(fn func() time.Time) {
	l.timeNow = fn
}

// Append writes one event line to today's JSONL file. ts/machineID/
// tenantID are filled in from Logger if zero. HMAC computed over
// CanonicalJSON of the event-without-hmac and appended as final field.
func (l *Logger) Append(ev Event) error {
	now := l.timeNow().UTC()
	if ev.Ts.IsZero() {
		ev.Ts = now
	}
	if ev.MachineID == "" {
		ev.MachineID = l.machineID
	}
	if ev.TenantID == "" {
		ev.TenantID = l.tenantID
	}
	canonical, err := ev.CanonicalJSON()
	if err != nil {
		return fmt.Errorf("audit_canonical: %w", err)
	}
	mac := hmac.New(sha256.New, l.hmacKey)
	mac.Write(canonical)
	ev.HMAC = hex.EncodeToString(mac.Sum(nil))
	line, err := serializeWithHMAC(canonical, ev.HMAC)
	if err != nil {
		return err
	}
	return l.writeLine(now, line)
}

func serializeWithHMAC(canonical []byte, hmacHex string) ([]byte, error) {
	var m map[string]any
	if err := json.Unmarshal(canonical, &m); err != nil {
		return nil, fmt.Errorf("audit_unmarshal: %w", err)
	}
	m["hmac"] = hmacHex
	out, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("audit_marshal: %w", err)
	}
	return append(out, '\n'), nil
}

func (l *Logger) writeLine(now time.Time, line []byte) error {
	if err := os.MkdirAll(l.dir, 0o755); err != nil {
		return fmt.Errorf("audit_mkdir: %w", err)
	}
	path := filepath.Join(l.dir,
		"events-"+now.Format("2006-01-02")+".jsonl")
	f, err := os.OpenFile(path,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("audit_open: %w", err)
	}
	defer f.Close()
	if _, err := f.Write(line); err != nil {
		return fmt.Errorf("audit_write: %w", err)
	}
	return nil
}
