// Package writer streaming Parquet+gzip para io.Writer destino.
package writer

import (
	"compress/gzip"
	"io"

	pq "github.com/parquet-go/parquet-go"
)

// ParquetGzip writer genérico — cada chamada Write serializa 1 row.
// Close() flush parquet → gzip → underlying writer, nessa ordem.
type ParquetGzip[T any] struct {
	gzw *gzip.Writer
	pqw *pq.GenericWriter[T]
}

// NewParquetGzip envolve dst com gzip + parquet writer.
func NewParquetGzip[T any](dst io.Writer) *ParquetGzip[T] {
	gzw := gzip.NewWriter(dst)
	return &ParquetGzip[T]{
		gzw: gzw,
		pqw: pq.NewGenericWriter[T](gzw),
	}
}

// Write serializa 1 row.
func (p *ParquetGzip[T]) Write(row T) error {
	_, err := p.pqw.Write([]T{row})
	return err
}

// WriteBatch serializa várias rows em uma chamada.
func (p *ParquetGzip[T]) WriteBatch(rows []T) error {
	_, err := p.pqw.Write(rows)
	return err
}

// Close flush parquet footer + gzip. Não fecha o underlying writer.
func (p *ParquetGzip[T]) Close() error {
	if err := p.pqw.Close(); err != nil {
		return err
	}
	return p.gzw.Close()
}
