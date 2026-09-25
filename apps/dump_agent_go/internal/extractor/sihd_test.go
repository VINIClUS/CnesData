package extractor_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
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

type capturedSQL struct{ queries []string }

func (c *capturedSQL) Match(_, actual string) error {
	c.queries = append(c.queries, actual)
	return nil
}

func newSihdRawMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock, *capturedSQL) {
	t.Helper()
	captured := &capturedSQL{}
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(captured))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db, mock, captured
}

func sihdRawNames(subtype string) []string {
	var names []string
	for _, column := range extractor.SihdRawColumns(subtype) {
		names = append(names, column.Name)
	}
	return names
}

func TestSihdInternacaoLeTBHAIHDaCompetenciaComColunasCruas(t *testing.T) {
	db, mock, captured := newSihdRawMock(t)
	values := make([]driver.Value, 17)
	values[0], values[1], values[2], values[3] = "3526100012345 ", "3541300001", int64(12), nil
	values[4], values[5] = "202601", "0303010037"
	mock.ExpectQuery("").WithArgs("202601").
		WillReturnRows(sqlmock.NewRows(sihdRawNames("SIHD_INTERNACAO")).AddRow(values...))

	rows, err := extractor.ExtractSihdRaw(context.Background(), db, "2026-01", "SIHD_INTERNACAO")

	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "3526100012345", rows[0]["AH_NUM_AIH"])
	require.Equal(t, int64(12), rows[0]["AH_SEQ"])
	require.Nil(t, rows[0]["AH_CNES"])
	require.Nil(t, rows[0]["AH_PACIENTE_MUN_ORIGEM"])
	require.Len(t, rows[0], 17)
	require.Contains(t, captured.queries[0], "FROM TB_HAIH")
	require.Contains(t, captured.queries[0], "WHERE AH_CMPT = ?")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSihdProcAIHConverteValorDuploEmTextoDecimal(t *testing.T) {
	db, mock, captured := newSihdRawMock(t)
	mock.ExpectQuery("").WithArgs("202601").
		WillReturnRows(sqlmock.NewRows(sihdRawNames("SIHD_PROC_AIH")).AddRow(
			"3526100012345", "3541300001", int64(12), int64(2), "2077000", "202601",
			"0303010037", int64(3), 1234.56, "225125"))

	rows, err := extractor.ExtractSihdRaw(context.Background(), db, "202601", "SIHD_PROC_AIH")

	require.NoError(t, err)
	require.Equal(t, "1234.56", rows[0]["PA_VALOR"])
	require.Equal(t, int64(2), rows[0]["PA_INDX"])
	require.Equal(t, int64(3), rows[0]["PA_PROCEDIMENTO_QTD"])
	require.Contains(t, captured.queries[0], "FROM TB_HPA")
	require.Contains(t, captured.queries[0], "WHERE PA_CMPT = ?")
}

func TestSihdRawNaoSelecionaPIIDePacienteNemDocumentoProfissional(t *testing.T) {
	pii := []string{"AH_PACIENTE_NOME", "AH_PACIENTE_NUMERO_CNS", "AH_PACIENTE_DT_NASCIMENTO",
		"AH_MED_SOL_DOC", "AH_MED_RESP_DOC", "PA_PF_DOC"}
	for _, subtype := range []string{"SIHD_INTERNACAO", "SIHD_PROC_AIH"} {
		db, mock, captured := newSihdRawMock(t)
		mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows(sihdRawNames(subtype)))
		_, err := extractor.ExtractSihdRaw(context.Background(), db, "202601", subtype)
		require.NoError(t, err)
		for _, column := range pii {
			require.NotContains(t, captured.queries[0], column)
			require.NotContains(t, sihdRawNames(subtype), column)
		}
		for _, column := range sihdRawNames(subtype) {
			require.Contains(t, captured.queries[0], column)
		}
	}
}

func TestSihdRawTabelaVaziaRetornaZeroLinhas(t *testing.T) {
	db, mock, _ := newSihdRawMock(t)
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows(sihdRawNames("SIHD_PROC_AIH")))
	rows, err := extractor.ExtractSihdRaw(context.Background(), db, "202601", "SIHD_PROC_AIH")
	require.NoError(t, err)
	require.Empty(t, rows)
}

func TestSihdRawPropagaErrosDeSubtipoQueryEScan(t *testing.T) {
	db, mock, _ := newSihdRawMock(t)
	_, err := extractor.ExtractSihdRaw(context.Background(), db, "202601", "SIHD_PRODUCAO")
	require.EqualError(t, err, "unknown_sihd_subtype=SIHD_PRODUCAO")
	require.Empty(t, extractor.SihdRawColumns("SIHD_PRODUCAO"))

	mock.ExpectQuery("").WillReturnError(errors.New("boom"))
	_, err = extractor.ExtractSihdRaw(context.Background(), db, "202601", "SIHD_PROC_AIH")
	require.ErrorContains(t, err, "sihd_query subtype=SIHD_PROC_AIH")

	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"PA_NUM_AIH"}).AddRow("1"))
	_, err = extractor.ExtractSihdRaw(context.Background(), db, "202601", "SIHD_PROC_AIH")
	require.ErrorContains(t, err, "sihd_scan subtype=SIHD_PROC_AIH")

	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows(sihdRawNames("SIHD_PROC_AIH")).
		AddRow("1", "2", int64(1), int64(1), "3", "202601", "4", int64(1), 1.0, "5").
		RowError(0, errors.New("cursor")))
	_, err = extractor.ExtractSihdRaw(context.Background(), db, "202601", "SIHD_PROC_AIH")
	require.ErrorContains(t, err, "sihd_rows subtype=SIHD_PROC_AIH")
}
