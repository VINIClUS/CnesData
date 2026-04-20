package writer_test

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"

	"github.com/cnesdata/dumpagent/internal/writer"
	pq "github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

type sampleRow struct {
	Name string `parquet:"name"`
	Age  int64  `parquet:"age"`
}

func TestParquetGzipWriter_RoundTrip(t *testing.T) {
	var buf bytes.Buffer
	w := writer.NewParquetGzip[sampleRow](&buf)

	rows := []sampleRow{
		{Name: "alice", Age: 30},
		{Name: "bob", Age: 25},
	}
	for _, r := range rows {
		require.NoError(t, w.Write(r))
	}
	require.NoError(t, w.Close())

	gzr, err := gzip.NewReader(&buf)
	require.NoError(t, err)
	raw, err := io.ReadAll(gzr)
	require.NoError(t, err)
	require.NoError(t, gzr.Close())

	pr := pq.NewGenericReader[sampleRow](bytes.NewReader(raw))
	got := make([]sampleRow, 2)
	n, err := pr.Read(got)
	if err != nil && err != io.EOF {
		require.NoError(t, err)
	}
	require.Equal(t, 2, n)
	require.Equal(t, rows, got)
}
