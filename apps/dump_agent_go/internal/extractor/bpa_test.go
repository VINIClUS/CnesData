package extractor

import (
	"context"
	"database/sql"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

var bpaColumns = []string{
	"PRD_UID", "PRD_CMP", "PRD_ORG", "PRD_FLH", "PRD_SEQ", "PRD_PA", "PRD_CBO",
	"PRD_CID", "PRD_IDADE", "PRD_DTATEN", "PRD_CNSMED", "PRD_QT_P",
}

var sPrdQuery = regexp.QuoteMeta("FROM S_PRD")

func TestBPA_ExtraiBPACeBPAIDeSPRDPorOrigem(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rowsC := sqlmock.NewRows(bpaColumns).AddRow(
		"2269481", "202608", "BPA", "001", "01", "0301010056", "225125",
		"    ", "045", "        ", "               ", 10.0)
	rowsI := sqlmock.NewRows(bpaColumns).AddRow(
		"2269481", "202608", "BPI", "177", "01", "0301010072", "225125",
		"J00 ", "046", "20260805", "999000000000101", 1.0)

	mock.ExpectQuery(sPrdQuery).WithArgs("202608", "BPA").WillReturnRows(rowsC)
	mock.ExpectQuery(sPrdQuery).WithArgs("202608", "BPI").WillReturnRows(rowsI)

	result, err := ExtractBPA(context.Background(), db, "202608")
	if err != nil {
		t.Fatalf("extract err=%v", err)
	}
	if len(result.BPA_C) != 1 || len(result.BPA_I) != 1 {
		t.Fatalf("counts C=%d I=%d want 1/1", len(result.BPA_C), len(result.BPA_I))
	}
	c, i := result.BPA_C[0], result.BPA_I[0]
	if c.Org != "BPA" || c.Procedimento != "0301010056" || *c.Quantidade != 10 {
		t.Errorf("bpa_c row=%+v", c)
	}
	if i.Org != "BPI" || i.Folha != "177" || i.DtAtendimento != "20260805" {
		t.Errorf("bpa_i row=%+v", i)
	}
	if i.CnsProfissional != "999000000000101" || i.Cid != "J00 " {
		t.Errorf("bpa_i row=%+v", i)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestBPA_QuantidadeNulaPermaneceNula(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	rows := sqlmock.NewRows(bpaColumns).AddRow(
		"2269481", "202608", "BPA", "001", "01", "0301010056", "225125",
		"", "045", "", "", nil)
	mock.ExpectQuery(sPrdQuery).WillReturnRows(rows)
	mock.ExpectQuery(sPrdQuery).WillReturnRows(sqlmock.NewRows(bpaColumns))

	result, err := ExtractBPA(context.Background(), db, "202608")
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if result.BPA_C[0].Quantidade != nil {
		t.Errorf("quantidade=%v want nil", *result.BPA_C[0].Quantidade)
	}
}

func TestBPA_SQLNaoSelecionaPIIDePaciente(t *testing.T) {
	for _, column := range []string{"PRD_CNSPAC", "PRD_NMPAC", "PRD_DTNASC", "PRD_CPF_PCNTE"} {
		if strings.Contains(sqlBPA, column) {
			t.Errorf("sqlBPA selects PII column %s", column)
		}
	}
}

func TestBPA_CompetenciaVazia(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	mock.ExpectQuery(sPrdQuery).WillReturnRows(sqlmock.NewRows(bpaColumns))
	mock.ExpectQuery(sPrdQuery).WillReturnRows(sqlmock.NewRows(bpaColumns))

	result, err := ExtractBPA(context.Background(), db, "202608")
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if len(result.BPA_C) != 0 || len(result.BPA_I) != 0 {
		t.Errorf("expected empty, got C=%d I=%d", len(result.BPA_C), len(result.BPA_I))
	}
}

func TestBPA_ErroDeQueryBPAC(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	mock.ExpectQuery(sPrdQuery).WillReturnError(sql.ErrConnDone)

	_, err := ExtractBPA(context.Background(), db, "202608")
	if err == nil || !strings.Contains(err.Error(), "bpa_c_query") {
		t.Fatalf("err=%v, want bpa_c_query", err)
	}
}

func TestBPA_ErroDeQueryBPAI(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	mock.ExpectQuery(sPrdQuery).WillReturnRows(sqlmock.NewRows(bpaColumns))
	mock.ExpectQuery(sPrdQuery).WillReturnError(sql.ErrConnDone)

	_, err := ExtractBPA(context.Background(), db, "202608")
	if err == nil || !strings.Contains(err.Error(), "bpa_i_query") {
		t.Fatalf("err=%v, want bpa_i_query", err)
	}
}

func TestBPA_ErroDeScan(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	rows := sqlmock.NewRows(bpaColumns).AddRow(
		"2269481", "202608", "BPA", "001", "01", "0301010056", "225125",
		"", "045", "", "", "not-a-number")
	mock.ExpectQuery(sPrdQuery).WillReturnRows(rows)

	_, err := ExtractBPA(context.Background(), db, "202608")
	if err == nil || !strings.Contains(err.Error(), "bpa_c_scan") {
		t.Fatalf("err=%v, want bpa_c_scan", err)
	}
}

func TestBPA_SanitizaCp1252Invalido(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	rows := sqlmock.NewRows(bpaColumns).AddRow(
		"2269481", "202608", "BPA", "001", "01", "0301010056", "225\xc3125",
		"", "045", "", "", 1.0)
	mock.ExpectQuery(sPrdQuery).WillReturnRows(rows)
	mock.ExpectQuery(sPrdQuery).WillReturnRows(sqlmock.NewRows(bpaColumns))

	result, err := ExtractBPA(context.Background(), db, "202608")
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if result.BPA_C[0].Cbo != "225?125" {
		t.Errorf("cbo=%q want 225?125", result.BPA_C[0].Cbo)
	}
}
