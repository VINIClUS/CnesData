package audit

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func writeEvents(t *testing.T, key []byte, dir string, events []Event) string {
	logger := New(dir, "m", "t", key)
	fixed := time.Date(2026, 5, 4, 10, 0, 0, 0, time.UTC)
	logger.SetTimeNow(func() time.Time { return fixed })
	for _, ev := range events {
		require.NoError(t, logger.Append(ev))
	}
	return filepath.Join(dir, "events-2026-05-04.jsonl")
}

func TestVerifyFile_AllValid(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	path := writeEvents(t, key, t.TempDir(), []Event{
		{Lifecycle: LifecycleExtracted},
		{Lifecycle: LifecycleUploaded, SHA256: "abc"},
	})
	valid, errs, err := VerifyFile(path, key)
	require.NoError(t, err)
	require.Len(t, valid, 2)
	require.Empty(t, errs)
}

func TestVerifyFile_DetectsTamper(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	path := writeEvents(t, key, t.TempDir(), []Event{
		{Lifecycle: LifecycleExtracted},
	})
	content, _ := os.ReadFile(path)
	tampered := strings.Replace(string(content), "extracted", "tampered_op", 1)
	require.NoError(t, os.WriteFile(path, []byte(tampered), 0o644))
	_, errs, err := VerifyFile(path, key)
	require.NoError(t, err)
	require.Len(t, errs, 1)
}

func TestVerifyFile_TolerateCorruptedTrailingLine(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	path := writeEvents(t, key, t.TempDir(), []Event{
		{Lifecycle: LifecycleExtracted},
	})
	require.NoError(t, os.WriteFile(path,
		append(mustRead(t, path), []byte("garbage-no-newline")...),
		0o644))
	valid, errs, err := VerifyFile(path, key)
	require.NoError(t, err)
	require.Len(t, valid, 1)
	require.Len(t, errs, 1, "garbage line counts as invalid")
}

func TestVerifyFile_WrongKeyAllInvalid(t *testing.T) {
	key1 := []byte("0123456789abcdef0123456789abcdef")
	key2 := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	path := writeEvents(t, key1, t.TempDir(), []Event{
		{Lifecycle: LifecycleExtracted},
		{Lifecycle: LifecycleUploaded},
	})
	_, errs, err := VerifyFile(path, key2)
	require.NoError(t, err)
	require.Len(t, errs, 2)
}

func mustRead(t *testing.T, path string) []byte {
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return b
}
