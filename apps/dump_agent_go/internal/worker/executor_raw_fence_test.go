package worker_test

import (
	"context"
	"io"
	"testing"

	"github.com/cnesdata/dumpagent/internal/upload"
	"github.com/stretchr/testify/require"
)

func TestSegundaTentativaUsaSnapshotComFence(t *testing.T) {
	exe, job := newRawExecutor(t), rawJob()
	job.Attempt = 2
	exe.RawUploader = rawUploadFunc(func(
		_ context.Context, request upload.RawPutRequest,
	) (int64, error) {
		require.Equal(t,
			"raw/tenant/CNES_LOCAL/2026-01/full-job-f7/data.parquet", request.ObjectKey)
		return io.Copy(io.Discard, request.Body)
	})

	_, err := exe.RunRaw(context.Background(), &job)

	require.NoError(t, err)
}
