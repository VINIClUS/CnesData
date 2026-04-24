package extractor

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestBPA_ExtractCAndI(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rowsC := sqlmock.NewRows([]string{
		"NU_COMPETENCIA", "CO_CNES", "CO_PROCEDIMENTO",
		"QT_APROVADA", "CO_CBO", "TP_IDADE", "NU_IDADE",
	}).AddRow("202601", "2269481", "0301010056", 10, "225125", 3, 45)

	rowsI := sqlmock.NewRows([]string{
		"NU_COMPETENCIA", "CO_CNES", "NU_CNS_PAC", "NU_CPF_PAC",
		"CO_PROCEDIMENTO", "CO_CBO", "CO_CID10", "DT_ATENDIMENTO",
		"QT_APROVADA", "NU_CNS_PROF",
	}).AddRow("202601", "2269481", "700123456789012", "12345678901",
		"0301010064", "225125", "J00",
		time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC),
		1, "700987654321098")

	mock.ExpectQuery("FROM BPA_C_LINHAS").WillReturnRows(rowsC)
	mock.ExpectQuery("FROM BPA_I_LINHAS").WillReturnRows(rowsI)

	result, err := ExtractBPA(context.Background(), db, "202601")
	if err != nil {
		t.Fatalf("extract err=%v", err)
	}

	if len(result.BPA_C) != 1 {
		t.Errorf("BPA_C count=%d want=1", len(result.BPA_C))
	}
	if len(result.BPA_I) != 1 {
		t.Errorf("BPA_I count=%d want=1", len(result.BPA_I))
	}
	if result.BPA_C[0].Procedimento != "0301010056" {
		t.Errorf("proc=%q", result.BPA_C[0].Procedimento)
	}
}

func TestBPA_EmptyCompetencia(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	mock.ExpectQuery("FROM BPA_C_LINHAS").
		WillReturnRows(sqlmock.NewRows([]string{
			"NU_COMPETENCIA", "CO_CNES", "CO_PROCEDIMENTO",
			"QT_APROVADA", "CO_CBO", "TP_IDADE", "NU_IDADE",
		}))
	mock.ExpectQuery("FROM BPA_I_LINHAS").
		WillReturnRows(sqlmock.NewRows([]string{
			"NU_COMPETENCIA", "CO_CNES", "NU_CNS_PAC", "NU_CPF_PAC",
			"CO_PROCEDIMENTO", "CO_CBO", "CO_CID10", "DT_ATENDIMENTO",
			"QT_APROVADA", "NU_CNS_PROF",
		}))

	result, err := ExtractBPA(context.Background(), db, "202601")
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if len(result.BPA_C) != 0 || len(result.BPA_I) != 0 {
		t.Errorf("expected empty, got C=%d I=%d",
			len(result.BPA_C), len(result.BPA_I))
	}
}

func TestBPA_QueryError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	mock.ExpectQuery("FROM BPA_C_LINHAS").
		WillReturnError(sql.ErrConnDone)

	_, err := ExtractBPA(context.Background(), db, "202601")
	if err == nil {
		t.Fatal("expected error")
	}
}
