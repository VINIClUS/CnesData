package writer_test

import (
	"bytes"
	"errors"
	"io"
	"slices"
	"testing"

	"github.com/cnesdata/dumpagent/internal/delta"
	"github.com/cnesdata/dumpagent/internal/writer"
	pq "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	"github.com/stretchr/testify/require"
)

//nolint:misspell // NOME_PROFISSIONAL is a frozen CNES field name.
var frozenColumns = []string{
	"CPF", "CNS", "NOME_PROFISSIONAL", "NOME_SOCIAL", "SEXO", "CBO", "CNES",
	"TIPO_VINCULO", "SUS", "CH_TOTAL", "CH_AMBULATORIAL", "CH_OUTRAS", "CH_HOSPITALAR", "FONTE",
}

//nolint:misspell // NOME_PROFISSIONAL is a frozen CNES field name.
func frozenRow() delta.Row {
	return delta.Row{
		"CPF": "00000000001", "CNS": "000000000000001", "NOME_PROFISSIONAL": "Fixture",
		"NOME_SOCIAL": nil, "SEXO": nil, "CBO": "000001", "CNES": "0000001",
		"TIPO_VINCULO": "000001", "SUS": "S", "CH_TOTAL": int64(40),
		"CH_AMBULATORIAL": int64(20), "CH_OUTRAS": int64(5), "CH_HOSPITALAR": int64(15),
		"FONTE": "NACIONAL",
	}
}

func assertFrozenParquet(t *testing.T, data []byte, deltaMode bool) *pq.File {
	t.Helper()
	require.Equal(t, "PAR1", string(data[:4]))
	require.Equal(t, "PAR1", string(data[len(data)-4:]))
	file, err := pq.OpenFile(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	columns := slices.Clone(frozenColumns)
	if deltaMode {
		columns = append(columns, "_op")
	}
	fields := file.Schema().Fields()
	require.Len(t, fields, len(columns))
	for i, col := range columns {
		require.Equal(t, col, fields[i].Name())
		if i >= 9 && i <= 12 {
			require.Equal(t, pq.Int64, fields[i].Type().Kind())
		} else {
			require.Equal(t, pq.ByteArray, fields[i].Type().Kind())
			require.NotNil(t, fields[i].Type().LogicalType().UTF8)
		}
		require.Equal(t, col != "_op", fields[i].Optional())
	}
	require.Equal(t, "Polars", file.Metadata().CreatedBy)
	for _, group := range file.Metadata().RowGroups {
		require.LessOrEqual(t, group.NumRows, int64(64000))
		for _, col := range group.Columns {
			require.Equal(t, format.Zstd, col.MetaData.Codec)
			stats := col.MetaData.Statistics
			require.True(t, len(stats.MinValue) > 0 || stats.NullCount == group.NumRows)
		}
	}
	if len(file.RowGroups()) > 0 {
		assertRawPageStatistics(t, file.RowGroups()[0].ColumnChunks()[0])
	}
	return file
}

func assertRawPageStatistics(t *testing.T, chunk pq.ColumnChunk) {
	t.Helper()
	pages := chunk.Pages()
	defer pages.Close()
	page, err := pages.ReadPage()
	require.NoError(t, err)
	minValue, maxValue, ok := page.Bounds()
	require.True(t, ok)
	require.Equal(t, "00000000001", minValue.String())
	require.Equal(t, "00000000001", maxValue.String())
}

func TestFullUsaParquetCongeladoSemOp(t *testing.T) {
	rows := make([]delta.Row, 64001)
	for i := range rows {
		rows[i] = frozenRow()
	}
	var buf bytes.Buffer
	require.NoError(t, writer.WriteRawFullParquet(&buf, rows))
	file := assertFrozenParquet(t, buf.Bytes(), false)
	require.Equal(t, int64(64001), file.NumRows())
	require.Len(t, file.RowGroups(), 2)
	require.Equal(t, int64(64000), file.RowGroups()[0].NumRows())
	require.Equal(t, int64(1), file.RowGroups()[1].NumRows())
	require.NotContains(t, readDeltaRows(t, buf.Bytes())[0], "_op")
}

//nolint:misspell // NOME_PROFISSIONAL is asserted as part of the frozen schema.
func TestDeltaUsaParquetCongeladoComIUD(t *testing.T) {
	ds := delta.Set{
		Inserts: []delta.Row{frozenRow()}, Updates: []delta.Row{frozenRow()},
		Deletes: []delta.Row{{"CPF": "00000000001", "CNES": "0000001"}},
	}
	var buf bytes.Buffer
	require.NoError(t, writer.WriteRawDeltaParquet(&buf, ds))
	file := assertFrozenParquet(t, buf.Bytes(), true)
	require.Equal(t, int64(3), file.NumRows())
	rows := readDeltaRows(t, buf.Bytes())
	for i, op := range []string{"I", "U", "D"} {
		require.Equal(t, op, rows[i]["_op"])
	}
	require.Equal(t, "40", rows[0]["CH_TOTAL"])
	require.Nil(t, rows[2]["CH_TOTAL"])
	require.Nil(t, rows[2]["NOME_PROFISSIONAL"])
}

func TestRawOrdenaColunasComNumerosENulosSemAlterarEntrada(t *testing.T) {
	input := []delta.Row{
		{"CPF": nil}, {"CPF": "b"}, {"CPF": "a", "CH_TOTAL": int64(10)},
		{"CPF": "a", "CH_TOTAL": int64(2)}, {"CPF": "a"},
		{"CPF": "a", "CNS": "z"}, {"CPF": "a", "CNS": "a"},
	}
	var first, second bytes.Buffer
	require.NoError(t, writer.WriteRawFullParquet(&first, input))
	require.Nil(t, input[0]["CPF"])
	reordered := slices.Clone(input)
	slices.Reverse(reordered)
	require.NoError(t, writer.WriteRawFullParquet(&second, reordered))
	require.Equal(t, first.Bytes(), second.Bytes())
	rows := readDeltaRows(t, first.Bytes())
	require.Equal(t, "a", rows[0]["CNS"])
	require.Equal(t, "z", rows[1]["CNS"])
	require.Equal(t, "2", rows[2]["CH_TOTAL"])
	require.Equal(t, "10", rows[3]["CH_TOTAL"])
	require.Nil(t, rows[4]["CH_TOTAL"])
	require.Equal(t, "b", rows[5]["CPF"])
	require.Nil(t, rows[6]["CPF"])
}

func TestDeltaOrdenaCadaBucketEPreservaDuplicatas(t *testing.T) {
	input := []delta.Row{{"CPF": nil}, {"CPF": "b"}, {"CPF": "a"}, {"CPF": "a"}}
	reversed := slices.Clone(input)
	slices.Reverse(reversed)
	var first, second bytes.Buffer
	require.NoError(t, writer.WriteRawDeltaParquet(&first, delta.Set{
		Inserts: input, Updates: input, Deletes: input,
	}))
	require.NoError(t, writer.WriteRawDeltaParquet(&second, delta.Set{
		Inserts: reversed, Updates: reversed, Deletes: reversed,
	}))
	require.Equal(t, first.Bytes(), second.Bytes())
	rows := readDeltaRows(t, first.Bytes())
	require.Len(t, rows, 12)
	for i, op := range []string{"I", "U", "D"} {
		for j, cpf := range []any{"a", "a", "b", nil} {
			require.Equal(t, op, rows[4*i+j]["_op"])
			require.Equal(t, cpf, rows[4*i+j]["CPF"])
		}
	}
}

func TestRawRejeitaTiposEOpsInvalidosAntesDeEmitirArquivo(t *testing.T) {
	for _, row := range []delta.Row{
		{"CPF": 1}, {"CH_TOTAL": "40"}, {"CH_TOTAL": 1.5},
		{"_op": "X"}, {"_op": "D"}, {"_op": nil},
	} {
		var buf bytes.Buffer
		err := writer.WriteRawDeltaParquet(&buf, delta.Set{Inserts: []delta.Row{row}})
		require.Error(t, err)
		require.Empty(t, buf.Bytes())
	}
	var buf bytes.Buffer
	require.Error(t, writer.WriteRawFullParquet(&buf, []delta.Row{{"_op": "I"}}))
	require.Empty(t, buf.Bytes())
}

func TestRawVazioProduzArquivoValidoEPropagaErroDoDestino(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, writer.WriteRawFullParquet(&buf, nil))
	file := assertFrozenParquet(t, buf.Bytes(), false)
	require.Zero(t, file.NumRows())
	buf.Reset()
	require.NoError(t, writer.WriteRawDeltaParquet(&buf, delta.Set{}))
	file = assertFrozenParquet(t, buf.Bytes(), true)
	require.Zero(t, file.NumRows())
	err := writer.WriteRawFullParquet(rawFailWriter{}, []delta.Row{frozenRow()})
	require.ErrorContains(t, err, "destination_failed")
}

type rawFailWriter struct{}

func (rawFailWriter) Write([]byte) (int, error) {
	return 0, errors.New("destination_failed=true")
}

func readDeltaRows(t *testing.T, raw []byte) []map[string]any {
	t.Helper()
	pr := pq.NewReader(bytes.NewReader(raw))
	defer pr.Close()
	cols := pr.Schema().Columns()
	out := []map[string]any{}
	rowBuf := make([]pq.Row, 1)
	for {
		n, err := pr.ReadRows(rowBuf)
		if n > 0 {
			out = append(out, decodeRow(cols, rowBuf[0]))
		}
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		if n == 0 {
			break
		}
	}
	return out
}

func decodeRow(cols [][]string, row pq.Row) map[string]any {
	m := map[string]any{}
	row.Range(func(idx int, vals []pq.Value) bool {
		if idx >= len(cols) || len(vals) == 0 {
			return true
		}
		v := vals[0]
		name := cols[idx][len(cols[idx])-1]
		if v.IsNull() {
			m[name] = nil
		} else {
			m[name] = v.String()
		}
		return true
	})
	return m
}

func TestWriteDeltaParquet_InsertsAndUpdatesAndDeletes(t *testing.T) {
	ds := delta.Set{
		Inserts: []delta.Row{{"CNES": "1", "NOME_FANTA": "A"}},
		Updates: []delta.Row{{"CNES": "2", "NOME_FANTA": "B"}},
		Deletes: []delta.Row{{"CNES": "3"}},
	}
	var buf bytes.Buffer
	err := writer.WriteDeltaParquet(&buf, ds, []string{"CNES", "NOME_FANTA"})
	require.NoError(t, err)
	require.NotEmpty(t, buf.Bytes())

	rows := readDeltaRows(t, buf.Bytes())
	require.Len(t, rows, 3)

	byOp := map[string]map[string]any{}
	for _, r := range rows {
		op, _ := r["_op"].(string)
		byOp[op] = r
	}
	require.Equal(t, "1", byOp["I"]["CNES"])
	require.Equal(t, "A", byOp["I"]["NOME_FANTA"])
	require.Equal(t, "2", byOp["U"]["CNES"])
	require.Equal(t, "B", byOp["U"]["NOME_FANTA"])
	require.Equal(t, "3", byOp["D"]["CNES"])
	require.Nil(t, byOp["D"]["NOME_FANTA"])
}

func TestWriteDeltaParquet_EmptyProducesValidOutput(t *testing.T) {
	var buf bytes.Buffer
	err := writer.WriteDeltaParquet(&buf, delta.Set{}, []string{"CNES"})
	require.NoError(t, err)
	require.NotEmpty(t, buf.Bytes())
	rows := readDeltaRows(t, buf.Bytes())
	require.Empty(t, rows)
}

func TestWriteDeltaParquet_StringifiesNonStringValues(t *testing.T) {
	ds := delta.Set{
		Inserts: []delta.Row{{"CNES": 42, "NOME_FANTA": nil}},
	}
	var buf bytes.Buffer
	err := writer.WriteDeltaParquet(&buf, ds, []string{"CNES", "NOME_FANTA"})
	require.NoError(t, err)

	rows := readDeltaRows(t, buf.Bytes())
	require.Len(t, rows, 1)
	require.Equal(t, "42", rows[0]["CNES"])
	require.Nil(t, rows[0]["NOME_FANTA"])
	require.Equal(t, "I", rows[0]["_op"])
}
