package worker_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/cnesdata/dumpagent/internal/extractor"
	"github.com/cnesdata/dumpagent/internal/worker"
	pq "github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

func TestIntentPipeline_ProduzParquetValido(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	cols := []string{"cnes", "nome_fanta", "tp_unid_id", "codmungest", "cnpj_mant"}
	mock.ExpectQuery("SELECT est.CNES").
		WithArgs("354130").
		WillReturnRows(sqlmock.NewRows(cols).
			AddRow("0001", "UBS", "05", "354130", "12345"))

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	fn, ok := worker.PipelineFor(extractor.IntentCnesEstabelecimentos)
	require.True(t, ok)

	var buf bytes.Buffer
	err = fn(context.Background(), conn,
		extractor.ExtractionParams{
			Intent: extractor.IntentCnesEstabelecimentos, CodMunGest: "354130",
		}, &buf)
	require.NoError(t, err)

	gzr, err := gzip.NewReader(&buf)
	require.NoError(t, err)
	raw, err := io.ReadAll(gzr)
	require.NoError(t, err)
	require.NoError(t, gzr.Close())

	pr := pq.NewGenericReader[extractor.CnesEstabelecimentoRow](bytes.NewReader(raw))
	var dst [1]extractor.CnesEstabelecimentoRow
	n, err := pr.Read(dst[:])
	if err != nil && err != io.EOF {
		require.NoError(t, err)
	}
	require.Equal(t, 1, n)
	require.Equal(t, "UBS", dst[0].NomeFanta)
}
