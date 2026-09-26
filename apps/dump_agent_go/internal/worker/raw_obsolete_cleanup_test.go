package worker

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/cnesdata/dumpagent/internal/queue"
	"github.com/stretchr/testify/require"
)

func TestFalhaNaLimpezaDeFenceObsoletoPreservaEnvelope(t *testing.T) {
	f := newRawDrainFixture(t)
	path := filepath.Join(f.spoolDirectory, f.env.SpoolName)
	calls := 0
	client := rawClientFunc(func(context.Context, queue.Envelope) (RawManifestResponse, error) {
		calls++
		require.NoError(t, os.Rename(path, path+".retained"))
		require.NoError(t, os.Mkdir(path, 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(path, "busy"), []byte("busy"), 0o600))
		return RawManifestResponse{StatusCode: 409, Reason: "job_fence_rejected"}, nil
	})
	drainer := f.drainer(client)
	require.Error(t, drainer.Drain(context.Background(), f.out))
	items, err := f.out.Peek(10)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.NoError(t, os.Remove(filepath.Join(path, "busy")))
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.Rename(path+".retained", path))
	reopenRawFixture(t, &f)
	drainer = f.drainer(client)
	require.NoError(t, drainer.Drain(context.Background(), f.out))
	require.Equal(t, 1, calls)
	items, err = f.out.Peek(10)
	require.NoError(t, err)
	require.Empty(t, items)
}
