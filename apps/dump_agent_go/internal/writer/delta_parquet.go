package writer

import (
	"cmp"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/cnesdata/dumpagent/internal/delta"
	pq "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
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
func WriteDeltaParquet(w io.Writer, ds delta.Set, schemaColumns []string) error {
	ds2 := buildDeltaSchema(schemaColumns)
	//nolint:staticcheck // SA1019: dynamic schema requires pq.Writer.
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

//nolint:staticcheck // SA1019: dynamic schema requires pq.Writer.
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

type rawColumn struct {
	name string
	kind pq.Kind
}

var rawColumns = []rawColumn{
	{"CPF", pq.ByteArray}, {"CNS", pq.ByteArray}, {"NOME_PROFISSIONAL", pq.ByteArray},
	{"NOME_SOCIAL", pq.ByteArray}, {"SEXO", pq.ByteArray}, {"CBO", pq.ByteArray},
	{"CNES", pq.ByteArray}, {"TIPO_VINCULO", pq.ByteArray}, {"SUS", pq.ByteArray},
	{"CH_TOTAL", pq.Int64}, {"CH_AMBULATORIAL", pq.Int64}, {"CH_OUTRAS", pq.Int64},
	{"CH_HOSPITALAR", pq.Int64}, {"FONTE", pq.ByteArray},
}

type rawBucket struct {
	rows []delta.Row
	op   string
}

type orderedRawGroup struct {
	pq.Group
	fields []pq.Field
}

func (g orderedRawGroup) Fields() []pq.Field { return g.fields }

// WriteRawFullParquet emite todas as linhas no schema raw congelado, sem _op.
func WriteRawFullParquet(dst io.Writer, rows []delta.Row) error {
	return writeRawParquet(dst, []rawBucket{{rows: rows}}, false)
}

// WriteRawDeltaParquet emite os buckets ordenados I/U/D no schema raw congelado.
func WriteRawDeltaParquet(dst io.Writer, set delta.Set) error {
	buckets := []rawBucket{{set.Inserts, "I"}, {set.Updates, "U"}, {set.Deletes, "D"}}
	return writeRawParquet(dst, buckets, true)
}

func writeRawParquet(dst io.Writer, buckets []rawBucket, deltaMode bool) (err error) {
	for _, bucket := range buckets {
		if err := validateRawBucket(bucket); err != nil {
			return err
		}
	}
	schema := buildRawSchema(deltaMode)
	pw := pq.NewWriter(dst, schema, &pq.WriterConfig{CreatedBy: "Polars"},
		pq.Compression(&zstd.Codec{Level: zstd.SpeedDefault}),
		pq.MaxRowsPerRowGroup(64000), pq.DataPageStatistics(true))
	defer func() { err = errors.Join(err, pw.Close()) }()
	for _, bucket := range buckets {
		if err := writeRawBucket(pw, schema, bucket); err != nil {
			return err
		}
	}
	return nil
}

func buildRawSchema(deltaMode bool) *pq.Schema {
	group := orderedRawGroup{Group: pq.Group{}}
	for _, col := range rawColumns {
		node := pq.String()
		if col.kind == pq.Int64 {
			node = pq.Int(64)
		}
		group.Group[col.name] = pq.Optional(node)
		field := pq.Group{col.name: group.Group[col.name]}.Fields()[0]
		group.fields = append(group.fields, field)
	}
	if deltaMode {
		group.Group[opColumnName] = pq.String()
		group.fields = append(group.fields, pq.Group{opColumnName: pq.String()}.Fields()[0])
	}
	return pq.NewSchema("raw", group)
}

func validateRawBucket(bucket rawBucket) error {
	for _, row := range bucket.rows {
		if op, exists := row[opColumnName]; exists && (bucket.op == "" || op != bucket.op) {
			return errors.New("raw_operation_invalid=true")
		}
		for _, col := range rawColumns {
			if _, err := rawValue(col, row[col.name]); err != nil {
				return err
			}
		}
	}
	return nil
}

func rawValue(col rawColumn, value any) (pq.Value, error) {
	if value == nil {
		return pq.NullValue(), nil
	}
	if col.kind == pq.ByteArray {
		if text, ok := value.(string); ok {
			return pq.ValueOf(text), nil
		}
	} else {
		switch number := value.(type) {
		case int:
			return pq.Int64Value(int64(number)), nil
		case int32:
			return pq.Int64Value(int64(number)), nil
		case int64:
			return pq.Int64Value(number), nil
		}
	}
	return pq.Value{}, fmt.Errorf("raw_column_type_invalid=%s", col.name)
}

func writeRawBucket(pw *pq.Writer, schema *pq.Schema, bucket rawBucket) error {
	rows := slices.Clone(bucket.rows)
	slices.SortStableFunc(rows, compareRawRows)
	for _, row := range rows {
		builder := pq.NewRowBuilder(schema)
		for i, col := range rawColumns {
			value, _ := rawValue(col, row[col.name])
			if !value.IsNull() {
				builder.Add(i, value)
			}
		}
		if bucket.op != "" {
			builder.Add(len(rawColumns), pq.ValueOf(bucket.op))
		}
		if _, err := pw.WriteRows([]pq.Row{builder.Row()}); err != nil {
			return err
		}
	}
	return nil
}

func compareRawRows(left, right delta.Row) int {
	for _, col := range rawColumns {
		l, _ := rawValue(col, left[col.name])
		r, _ := rawValue(col, right[col.name])
		if result := compareRawValues(l, r); result != 0 {
			return result
		}
	}
	return 0
}

func compareRawValues(left, right pq.Value) int {
	if left.IsNull() && right.IsNull() {
		return 0
	}
	if left.IsNull() {
		return 1
	}
	if right.IsNull() {
		return -1
	}
	if left.Kind() == pq.Int64 {
		return cmp.Compare(left.Int64(), right.Int64())
	}
	return cmp.Compare(left.String(), right.String())
}
