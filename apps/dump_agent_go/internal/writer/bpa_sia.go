// Package writer helpers per-row-type para serializar slices em Parquet+gzip.
package writer

import (
	"bytes"

	"github.com/cnesdata/dumpagent/internal/extractor"
)

// WriteBPACParquetGzip serializa BPA_C rows em Parquet+gzip in-memory.
func WriteBPACParquetGzip(rows []extractor.BPACRow) ([]byte, error) {
	return writeRowsParquetGzip(rows)
}

// WriteBPAIParquetGzip serializa BPA_I rows em Parquet+gzip in-memory.
func WriteBPAIParquetGzip(rows []extractor.BPAIRow) ([]byte, error) {
	return writeRowsParquetGzip(rows)
}

// WriteSIAAPAParquetGzip serializa SIA APA rows em Parquet+gzip in-memory.
func WriteSIAAPAParquetGzip(rows []extractor.SIAAPARow) ([]byte, error) {
	return writeRowsParquetGzip(rows)
}

// WriteSIABPIParquetGzip serializa SIA BPI rows em Parquet+gzip in-memory.
func WriteSIABPIParquetGzip(rows []extractor.SIABPIRow) ([]byte, error) {
	return writeRowsParquetGzip(rows)
}

// WriteCDNParquetGzip serializa SIA CDN (DIM_SIGTAP) rows em Parquet+gzip.
func WriteCDNParquetGzip(rows []extractor.SIACDNRow) ([]byte, error) {
	return writeRowsParquetGzip(rows)
}

// WriteCADMUNParquetGzip serializa CADMUN (DIM_MUNICIPIO) rows em Parquet+gzip.
func WriteCADMUNParquetGzip(rows []extractor.CADMUNRow) ([]byte, error) {
	return writeRowsParquetGzip(rows)
}

func writeRowsParquetGzip[T any](rows []T) ([]byte, error) {
	var buf bytes.Buffer
	w := NewParquetGzip[T](&buf)
	if len(rows) > 0 {
		if err := w.WriteBatch(rows); err != nil {
			return nil, err
		}
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
