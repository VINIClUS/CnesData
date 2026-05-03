package writer

import (
	"fmt"
	"io"

	"github.com/cnesdata/dumpagent/internal/delta"
	pq "github.com/parquet-go/parquet-go"
)

const opColumnName = "_op"

type deltaSchema struct {
	schema *pq.Schema
	idx    map[string]int
	cols   []string
}

// WriteDeltaParquet emits a single Parquet stream containing all
// I/U/D rows from ds. All columns are typed string for v1.
// I/U rows include schemaColumns + _op; D rows have only PK columns
// populated, others null + _op="D".
func WriteDeltaParquet(w io.Writer, ds delta.DeltaSet, schemaColumns []string) error {
	ds2 := buildDeltaSchema(schemaColumns)
	pw := pq.NewWriter(w, ds2.schema)
	if err := ds2.writeOp(pw, ds.Inserts, "I"); err != nil {
		return err
	}
	if err := ds2.writeOp(pw, ds.Updates, "U"); err != nil {
		return err
	}
	if err := ds2.writeOp(pw, ds.Deletes, "D"); err != nil {
		return err
	}
	return pw.Close()
}

func buildDeltaSchema(cols []string) *deltaSchema {
	g := pq.Group{}
	for _, c := range cols {
		g[c] = pq.Optional(pq.String())
	}
	g[opColumnName] = pq.String()
	schema := pq.NewSchema("delta", g)
	idx := make(map[string]int, len(schema.Columns()))
	for i, path := range schema.Columns() {
		idx[path[len(path)-1]] = i
	}
	return &deltaSchema{schema: schema, idx: idx, cols: cols}
}

func (d *deltaSchema) writeOp(pw *pq.Writer, rows []delta.Row, op string) error {
	for _, r := range rows {
		if _, err := pw.WriteRows([]pq.Row{d.buildRow(r, op)}); err != nil {
			return err
		}
	}
	return nil
}

func (d *deltaSchema) buildRow(r delta.Row, op string) pq.Row {
	b := pq.NewRowBuilder(d.schema)
	for _, c := range d.cols {
		v, ok := r[c]
		if !ok || v == nil {
			continue
		}
		b.Add(d.idx[c], pq.ValueOf(fmt.Sprintf("%v", v)))
	}
	b.Add(d.idx[opColumnName], pq.ValueOf(op))
	return b.Row()
}
