package writer_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/cnesdata/dumpagent/internal/delta"
	"github.com/cnesdata/dumpagent/internal/writer"
	pq "github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

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
