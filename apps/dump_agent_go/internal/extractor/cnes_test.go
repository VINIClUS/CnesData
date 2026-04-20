package extractor_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/cnesdata/dumpagent/internal/extractor"
	"github.com/stretchr/testify/require"
)

func TestExtractCnesProfissionais_MergesThreeQueries(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	profCols := []string{"cpf_prof", "cod_cns", "nome_prof", "no_social", "sexo", "data_nasc",
		"cod_cbo", "ind_vinc", "tp_sus_nao_sus", "carga_horaria_total",
		"cg_horaamb", "cghoraoutr", "cghorahosp", "cnes", "nome_fanta", "tp_unid_id", "codmungest"}
	mock.ExpectQuery("SELECT prof.CPF_PROF").
		WithArgs("354130").
		WillReturnRows(sqlmock.NewRows(profCols).
			AddRow("12345678900", "700000000000001", "João", "", "M", "1990-01-01",
				"225125", "1", "S", 40, 20, 10, 10, "0001234", "UBS", "05", "354130"))

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	ch := make(chan extractor.CnesProfissionalRow, 10)
	go func() {
		defer close(ch)
		err := extractor.ExtractCnesProfissionais(context.Background(), conn,
			extractor.ExtractionParams{CodMunGest: "354130"}, ch)
		require.NoError(t, err)
	}()

	var rows []extractor.CnesProfissionalRow
	for r := range ch {
		rows = append(rows, r)
	}
	require.Len(t, rows, 1)
	require.Equal(t, "João", rows[0].NomeProf)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExtractCnesProfissionais_EmptyResult(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery("SELECT prof.CPF_PROF").
		WithArgs("999999").
		WillReturnRows(sqlmock.NewRows([]string{"cpf_prof"}))

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	ch := make(chan extractor.CnesProfissionalRow, 1)
	err = extractor.ExtractCnesProfissionais(context.Background(), conn,
		extractor.ExtractionParams{CodMunGest: "999999"}, ch)
	close(ch)
	require.NoError(t, err)

	count := 0
	for range ch {
		count++
	}
	require.Equal(t, 0, count)
}

func TestExtractCnesProfissionais_SanitizesEncoding(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	profCols := []string{"cpf_prof", "cod_cns", "nome_prof", "no_social", "sexo", "data_nasc",
		"cod_cbo", "ind_vinc", "tp_sus_nao_sus", "carga_horaria_total",
		"cg_horaamb", "cghoraoutr", "cghorahosp", "cnes", "nome_fanta", "tp_unid_id", "codmungest"}
	mock.ExpectQuery("SELECT prof.CPF_PROF").
		WithArgs("354130").
		WillReturnRows(sqlmock.NewRows(profCols).
			AddRow("123", "700", "Aten\xE7\xE3o", "", "F", "1980-01-01",
				"225125", "1", "S", 40, 20, 10, 10, "0001", "UBS", "05", "354130"))

	conn, _ := db.Conn(context.Background())
	defer conn.Close()

	ch := make(chan extractor.CnesProfissionalRow, 1)
	err = extractor.ExtractCnesProfissionais(context.Background(), conn,
		extractor.ExtractionParams{CodMunGest: "354130"}, ch)
	close(ch)
	require.NoError(t, err)

	row := <-ch
	require.Equal(t, "Aten??o", row.NomeProf, "invalid bytes replaced by ?")
}

// keep sql import used
var _ = sql.Drivers
