package audit

import (
	"bufio"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLogger_AppendWritesValidLine(t *testing.T) {
	dir := t.TempDir()
	key := []byte("0123456789abcdef0123456789abcdef")
	logger := New(dir, "machine-1", "tenant-1", key)
	logger.SetTimeNow(func() time.Time {
		return time.Date(2026, 5, 4, 10, 0, 0, 0, time.UTC)
	})
	ev := Event{
		Source: "cnes", Intent: "profissionais",
		Competencia: "202605", ExtractionID: "ext-1",
		JobID: "job-1", SHA256: "abc", SizeBytes: 42,
		Lifecycle: LifecycleUploaded,
	}
	require.NoError(t, logger.Append(ev))

	expectedFile := filepath.Join(dir, "events-2026-05-04.jsonl")
	require.FileExists(t, expectedFile)
	content, err := os.ReadFile(expectedFile)
	require.NoError(t, err)
	require.Contains(t, string(content), "uploaded")
	require.Contains(t, string(content), "machine-1")
	var parsed map[string]any
	require.NoError(t, json.Unmarshal(content[:len(content)-1], &parsed))
	require.Contains(t, parsed, "hmac")
}

func TestLogger_AppendComputesHMACOverCanonicalJSON(t *testing.T) {
	dir := t.TempDir()
	key := []byte("0123456789abcdef0123456789abcdef")
	logger := New(dir, "machine-1", "tenant-1", key)
	logger.SetTimeNow(func() time.Time {
		return time.Date(2026, 5, 4, 10, 0, 0, 0, time.UTC)
	})
	ev := Event{
		Source: "cnes", Intent: "profissionais",
		Competencia: "202605", ExtractionID: "ext-1",
		JobID: "job-1", SHA256: "abc", SizeBytes: 42,
		Lifecycle: LifecycleUploaded,
	}
	require.NoError(t, logger.Append(ev))

	f, err := os.Open(filepath.Join(dir, "events-2026-05-04.jsonl"))
	require.NoError(t, err)
	defer f.Close()
	scanner := bufio.NewScanner(f)
	require.True(t, scanner.Scan())
	var got map[string]any
	require.NoError(t, json.Unmarshal(scanner.Bytes(), &got))
	hmacHex, _ := got["hmac"].(string)
	delete(got, "hmac")
	canonical, err := json.Marshal(got)
	require.NoError(t, err)
	mac := hmac.New(sha256.New, key)
	mac.Write(canonical)
	expected := hex.EncodeToString(mac.Sum(nil))
	require.Equal(t, expected, hmacHex)
}

func TestLogger_DailyRotation(t *testing.T) {
	dir := t.TempDir()
	logger := New(dir, "m", "t", []byte("k"))
	called := 0
	logger.SetTimeNow(func() time.Time {
		called++
		if called == 1 {
			return time.Date(2026, 5, 4, 23, 59, 59, 0, time.UTC)
		}
		return time.Date(2026, 5, 5, 0, 0, 1, 0, time.UTC)
	})
	require.NoError(t, logger.Append(Event{Lifecycle: LifecycleExtracted}))
	require.NoError(t, logger.Append(Event{Lifecycle: LifecycleUploaded}))
	require.FileExists(t, filepath.Join(dir, "events-2026-05-04.jsonl"))
	require.FileExists(t, filepath.Join(dir, "events-2026-05-05.jsonl"))
}

func TestLogger_AppendOpensWithO_APPEND(t *testing.T) {
	dir := t.TempDir()
	logger := New(dir, "m", "t", []byte("k"))
	logger.SetTimeNow(func() time.Time {
		return time.Date(2026, 5, 4, 10, 0, 0, 0, time.UTC)
	})
	require.NoError(t, logger.Append(Event{Lifecycle: LifecycleExtracted}))
	require.NoError(t, logger.Append(Event{Lifecycle: LifecycleUploaded}))
	content, _ := os.ReadFile(filepath.Join(dir, "events-2026-05-04.jsonl"))
	lines := 0
	for _, b := range content {
		if b == '\n' {
			lines++
		}
	}
	require.Equal(t, 2, lines)
}
