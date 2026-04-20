package extractor_test

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/cnesdata/dumpagent/internal/extractor"
	"github.com/stretchr/testify/require"
)

func TestExtractSihdProducao_ReturnsRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	cols := []string{"aih_numero", "procedimento", "competencia", "cnes", "valor"}
	mock.ExpectQuery("SELECT .* FROM SIHD").
		WithArgs("2026-01", "354130").
		WillReturnRows(sqlmock.NewRows(cols).
			AddRow("0001234567", "0301010014", "2026-01", "0001234", 12500))

	conn, _ := db.Conn(context.Background())
	defer conn.Close()

	ch := make(chan extractor.SihdProducaoRow, 10)
	go func() {
		defer close(ch)
		err := extractor.ExtractSihdProducao(context.Background(), conn,
			extractor.ExtractionParams{Competencia: "2026-01", CodMunGest: "354130"}, ch)
		require.NoError(t, err)
	}()

	var rows []extractor.SihdProducaoRow
	for r := range ch {
		rows = append(rows, r)
	}
	require.Len(t, rows, 1)
	require.Equal(t, int64(12500), rows[0].Valor)
}
