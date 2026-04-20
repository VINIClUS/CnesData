package obs_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/cnesdata/dumpagent/internal/obs"
	"github.com/stretchr/testify/require"
)

func TestNewJSONHandler_WritesStructuredFields(t *testing.T) {
	var buf bytes.Buffer
	h := obs.NewJSONHandler(&buf, slog.LevelInfo)
	logger := slog.New(h)

	logger.Info("event", "key", "value", "count", 42)

	var out map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &out))
	require.Equal(t, "event", out["msg"])
	require.Equal(t, "value", out["key"])
	require.InDelta(t, 42.0, out["count"].(float64), 0.01)
	require.NotEmpty(t, out["time"])
}

func TestNewRotatingHandler_WritesToFile(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "test.log")

	h, closer := obs.NewRotatingHandler(logPath, slog.LevelInfo)
	logger := slog.New(h)

	logger.Info("rotated_event", "field", "xyz")
	closer()

	data, err := os.ReadFile(logPath)
	require.NoError(t, err)
	require.Contains(t, string(data), "rotated_event")
	require.Contains(t, string(data), "xyz")
}

func TestNewStdoutHandler_ReturnsHandler(t *testing.T) {
	h := obs.NewStdoutHandler(slog.LevelDebug)
	require.NotNil(t, h)
	// smoke: use the handler, ensure no panic
	slog.New(h).Debug("smoke")
}
