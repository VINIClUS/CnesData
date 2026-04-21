package writer_test

import (
	"bytes"
	"testing"

	"github.com/cnesdata/dumpagent/internal/writer"
)

type benchRow struct {
	ID   int64   `parquet:"id"`
	Name string  `parquet:"name"`
	Val  float64 `parquet:"val"`
}

func BenchmarkParquetGzip1000Rows(b *testing.B) {
	rows := make([]benchRow, 1000)
	for i := range rows {
		rows[i] = benchRow{
			ID:   int64(i),
			Name: "Name" + string(rune(i%26+'A')),
			Val:  float64(i) * 1.5,
		}
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		w := writer.NewParquetGzip[benchRow](&buf)
		for _, r := range rows {
			if err := w.Write(r); err != nil {
				b.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
