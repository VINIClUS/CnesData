package writer_test

import (
	"bytes"
	"compress/gzip"
	"flag"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/cnesdata/dumpagent/internal/extractor"
	"github.com/cnesdata/dumpagent/internal/writer"
	pq "github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

// -update-sia-golden regrava os Parquet lidos por
// apps/data_processor/tests/sources/sia/test_edge_contract.py.
var updateSIAGolden = flag.Bool("update-sia-golden", false, "regrava golden SIA do contrato Edge")

var siaGoldenDir = filepath.Join(
	"..", "..", "..", "data_processor", "tests", "fixtures", "sia", "edge_golden")

var siaPIIColumns = []string{
	"apa_cnspct", "apa_cpfpct", "apa_nmpcn", "apa_dtnasc", "prd_cnspcn", "prd_cpfpct",
	"bpi_cnspac", "bpi_cpfpct", "bpi_nmpac", "bpi_dtnasc",
}

func siaPayloads(t *testing.T) map[string][]byte {
	t.Helper()
	dir := filepath.Join("..", "..", "test", "integration", "fixtures", "sia_synthetic")
	r, err := extractor.ExtractSIA(dir, "202601",
		[]string{"SIA_APA", "SIA_BPI", "SIA_BPIHST", "DIM_SIGTAP", "DIM_MUNICIPIO"})
	require.NoError(t, err)
	out := map[string][]byte{}
	for subtype, write := range map[string]func() ([]byte, error){
		"SIA_APA":       func() ([]byte, error) { return writer.WriteSIAAPAParquetGzip(r.APA) },
		"SIA_BPI":       func() ([]byte, error) { return writer.WriteSIABPIParquetGzip(r.BPI) },
		"SIA_BPIHST":    func() ([]byte, error) { return writer.WriteSIABPIParquetGzip(r.BPIHST) },
		"DIM_SIGTAP":    func() ([]byte, error) { return writer.WriteSIGTAPParquetGzip(r.SIGTAP) },
		"DIM_MUNICIPIO": func() ([]byte, error) { return writer.WriteCADMUNParquetGzip(r.CADMUN) },
	} {
		payload, err := write()
		require.NoError(t, err)
		gzr, err := gzip.NewReader(bytes.NewReader(payload))
		require.NoError(t, err)
		out[subtype], err = io.ReadAll(gzr)
		require.NoError(t, err)
	}
	return out
}

func openParquet(t *testing.T, raw []byte) *pq.File {
	t.Helper()
	file, err := pq.OpenFile(bytes.NewReader(raw), int64(len(raw)))
	require.NoError(t, err)
	return file
}

func TestSIAContrato_SchemaIgualAoGoldenDoProcessor(t *testing.T) {
	for subtype, raw := range siaPayloads(t) {
		path := filepath.Join(siaGoldenDir, subtype+".parquet")
		if *updateSIAGolden {
			require.NoError(t, os.WriteFile(path, raw, 0o644))
			continue
		}
		golden, err := os.ReadFile(path)
		require.NoError(t, err, "rode go test ./internal/writer -update-sia-golden")
		require.Equal(t, openParquet(t, golden).Schema().String(), openParquet(t, raw).Schema().String(),
			"schema %s divergiu do golden do data_processor", subtype)
	}
}

func TestSIAContrato_SemColunasDePIIDoPaciente(t *testing.T) {
	for subtype, raw := range siaPayloads(t) {
		for _, column := range siaPIIColumns {
			_, found := openParquet(t, raw).Schema().Lookup(column)
			require.False(t, found, "subtype=%s column=%s", subtype, column)
		}
	}
}

func TestSIAContrato_QuantidadesEValoresOpcionais(t *testing.T) {
	payloads := siaPayloads(t)
	for subtype, columns := range map[string][]string{
		"SIA_APA":       {"prd_qt_p", "prd_qt_a", "prd_vl_p", "prd_vl_a"},
		"SIA_BPI":       {"bpi_qt_p", "bpi_qt_a"},
		"DIM_MUNICIPIO": {"tetopab", "calcpab"},
	} {
		schema := openParquet(t, payloads[subtype]).Schema()
		for _, column := range columns {
			leaf, ok := schema.Lookup(column)
			require.True(t, ok, "subtype=%s column=%s", subtype, column)
			require.True(t, leaf.Node.Optional(), "subtype=%s column=%s", subtype, column)
		}
	}
}

func TestSIAContrato_SubtipoVazioMantemSchema(t *testing.T) {
	payload, err := writer.WriteSIGTAPParquetGzip(nil)
	require.NoError(t, err)

	file := readBPAParquet(t, payload)

	require.Equal(t, int64(0), file.NumRows())
	require.Len(t, file.Schema().Fields(), 5)
}
