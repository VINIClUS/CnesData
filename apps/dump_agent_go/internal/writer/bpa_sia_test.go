package writer_test

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"

	"github.com/cnesdata/dumpagent/internal/extractor"
	"github.com/cnesdata/dumpagent/internal/writer"
	pq "github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

func readBPAParquet(t *testing.T, payload []byte) *pq.File {
	t.Helper()
	gzr, err := gzip.NewReader(bytes.NewReader(payload))
	require.NoError(t, err)
	raw, err := io.ReadAll(gzr)
	require.NoError(t, err)
	file, err := pq.OpenFile(bytes.NewReader(raw), int64(len(raw)))
	require.NoError(t, err)
	return file
}

func TestWriteBPA_EmiteContratoRawSPRD(t *testing.T) {
	quantidade := 10.0
	payload, err := writer.WriteBPACParquetGzip([]extractor.BPARow{{
		Uid: "2269481", Competencia: "202608", Org: "BPA", Folha: "001", Sequencia: "01",
		Procedimento: "0301010056", Cbo: "225125", Idade: "045", Quantidade: &quantidade,
	}})
	require.NoError(t, err)

	file := readBPAParquet(t, payload)

	names := []string{}
	for _, field := range file.Schema().Fields() {
		names = append(names, field.Name())
	}
	require.ElementsMatch(t, []string{
		"prd_uid", "prd_cmp", "prd_org", "prd_flh", "prd_seq", "prd_pa", "prd_cbo",
		"prd_cid", "prd_idade", "prd_dtaten", "prd_cnsmed", "prd_qt_p",
	}, names)
	require.Equal(t, int64(1), file.NumRows())
	quantity, ok := file.Schema().Lookup("prd_qt_p")
	require.True(t, ok)
	require.True(t, quantity.Node.Optional())
}

func TestWriteBPA_SubtipoVazioGeraParquetComSchema(t *testing.T) {
	payload, err := writer.WriteBPAIParquetGzip(nil)
	require.NoError(t, err)

	file := readBPAParquet(t, payload)

	require.Equal(t, int64(0), file.NumRows())
	require.Len(t, file.Schema().Fields(), 12)
}
