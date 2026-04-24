// Package extractor SIA DBF reader via LindsayBradford/go-dbf.
package extractor

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/LindsayBradford/go-dbf/godbf"
)

type SIAAPARow struct {
	Competencia     string    `parquet:"apa_cmp"`
	Cnes            string    `parquet:"apa_cnes"`
	CnsPaciente     string    `parquet:"apa_cnspct"`
	CnsProfissional string    `parquet:"apa_cnsexe"`
	Procedimento    string    `parquet:"apa_proc"`
	Cbo             string    `parquet:"apa_cbo"`
	Cid10           string    `parquet:"apa_cid"`
	DtInicio        time.Time `parquet:"apa_dtini"`
	DtFim           time.Time `parquet:"apa_dtfin"`
	Quantidade      int32     `parquet:"apa_qtapr"`
	ValorCents      int64     `parquet:"apa_vlapr"`
}

type SIABPIRow struct {
	Competencia     string    `parquet:"bpi_cmp"`
	Cnes            string    `parquet:"bpi_cnes"`
	CnsPaciente     string    `parquet:"bpi_cnspac"`
	CnsProfissional string    `parquet:"bpi_cnsmed"`
	Cbo             string    `parquet:"bpi_cbo"`
	Procedimento    string    `parquet:"bpi_proc"`
	Cid10           string    `parquet:"bpi_cid"`
	DtAtendimento   time.Time `parquet:"bpi_dtaten"`
	Quantidade      int32     `parquet:"bpi_qt"`
	Folha           int16     `parquet:"bpi_folha"`
	Seq             int16     `parquet:"bpi_seq"`
}

type SIACDNRow struct {
	Tabela    string `parquet:"cdn_tb"`
	Item      string `parquet:"cdn_it"`
	Descricao string `parquet:"cdn_dscr"`
	Checksum  string `parquet:"cdn_chksm"`
}

type CADMUNRow struct {
	CodUF   string `parquet:"coduf"`
	CodMun  string `parquet:"codmunic"`
	Nome    string `parquet:"nome"`
	Condic  string `parquet:"condic"`
	TetoPab int64  `parquet:"tetopab"`
	CalcPab string `parquet:"calcpab"`
}

type SIAResult struct {
	APA    []SIAAPARow
	BPI    []SIABPIRow
	BPIHST []SIABPIRow
	CDN    []SIACDNRow
	CADMUN []CADMUNRow
}

// ExtractSIA lê os 5 DBFs SIA em dir. Arquivos ausentes são tolerados;
// erros de parse são agregados via errors.Join.
func ExtractSIA(dir string) (*SIAResult, error) {
	if _, err := os.Stat(dir); err != nil {
		return nil, fmt.Errorf("sia_dir_missing: %w", err)
	}
	result := &SIAResult{}
	errs := readSIAFiles(dir, result)
	if len(errs) > 0 {
		return result, errors.Join(errs...)
	}
	return result, nil
}

func readSIAFiles(dir string, result *SIAResult) []error {
	var errs []error
	if apa, err := readAPA(filepath.Join(dir, "S_APA.DBF")); err == nil {
		result.APA = apa
	} else if !errors.Is(err, os.ErrNotExist) {
		errs = append(errs, err)
	}
	if bpi, err := readBPI(filepath.Join(dir, "S_BPI.DBF")); err == nil {
		result.BPI = bpi
	} else if !errors.Is(err, os.ErrNotExist) {
		errs = append(errs, err)
	}
	if bpihst, err := readBPI(filepath.Join(dir, "S_BPIHST.DBF")); err == nil {
		result.BPIHST = bpihst
	} else if !errors.Is(err, os.ErrNotExist) {
		errs = append(errs, err)
	}
	if cdn, err := readCDN(filepath.Join(dir, "S_CDN.DBF")); err == nil {
		result.CDN = cdn
	} else if !errors.Is(err, os.ErrNotExist) {
		errs = append(errs, err)
	}
	if cadmun, err := readCADMUN(filepath.Join(dir, "CADMUN.DBF")); err == nil {
		result.CADMUN = cadmun
	} else if !errors.Is(err, os.ErrNotExist) {
		errs = append(errs, err)
	}
	return errs
}

func openDBF(path string) (*godbf.DbfTable, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	t, err := godbf.NewFromFile(path, "windows-1252")
	if err != nil {
		return nil, fmt.Errorf("dbf_open path=%s: %w", filepath.Base(path), err)
	}
	return t, nil
}

func readAPA(path string) ([]SIAAPARow, error) {
	t, err := openDBF(path)
	if err != nil {
		return nil, err
	}
	rows := make([]SIAAPARow, 0, t.NumberOfRecords())
	for i := 0; i < t.NumberOfRecords(); i++ {
		qt, _ := t.Int64FieldValueByName(i, "APA_QTAPR")
		vl, _ := t.Int64FieldValueByName(i, "APA_VLAPR")
		rows = append(rows, SIAAPARow{
			Competencia:     sanitizeDBF(t, i, "APA_CMP"),
			Cnes:            sanitizeDBF(t, i, "APA_CNES"),
			CnsPaciente:     sanitizeDBF(t, i, "APA_CNSPCT"),
			CnsProfissional: sanitizeDBF(t, i, "APA_CNSEXE"),
			Procedimento:    sanitizeDBF(t, i, "APA_PROC"),
			Cbo:             sanitizeDBF(t, i, "APA_CBO"),
			Cid10:           sanitizeDBF(t, i, "APA_CID"),
			DtInicio:        parseDBFDate(t, i, "APA_DTINI"),
			DtFim:           parseDBFDate(t, i, "APA_DTFIN"),
			Quantidade:      int32(qt),
			ValorCents:      vl,
		})
	}
	return rows, nil
}

func readBPI(path string) ([]SIABPIRow, error) {
	t, err := openDBF(path)
	if err != nil {
		return nil, err
	}
	rows := make([]SIABPIRow, 0, t.NumberOfRecords())
	for i := 0; i < t.NumberOfRecords(); i++ {
		rows = append(rows, buildBPIRow(t, i))
	}
	return rows, nil
}

func buildBPIRow(t *godbf.DbfTable, i int) SIABPIRow {
	qt, _ := t.Int64FieldValueByName(i, "BPI_QT")
	folha, _ := t.Int64FieldValueByName(i, "BPI_FOLHA")
	seq, _ := t.Int64FieldValueByName(i, "BPI_SEQ")
	return SIABPIRow{
		Competencia:     sanitizeDBF(t, i, "BPI_CMP"),
		Cnes:            sanitizeDBF(t, i, "BPI_CNES"),
		CnsPaciente:     sanitizeDBF(t, i, "BPI_CNSPAC"),
		CnsProfissional: sanitizeDBF(t, i, "BPI_CNSMED"),
		Cbo:             sanitizeDBF(t, i, "BPI_CBO"),
		Procedimento:    sanitizeDBF(t, i, "BPI_PROC"),
		Cid10:           sanitizeDBF(t, i, "BPI_CID"),
		DtAtendimento:   parseDBFDate(t, i, "BPI_DTATEN"),
		Quantidade:      int32(qt),
		Folha:           int16(folha),
		Seq:             int16(seq),
	}
}

func readCDN(path string) ([]SIACDNRow, error) {
	t, err := openDBF(path)
	if err != nil {
		return nil, err
	}
	rows := make([]SIACDNRow, 0, t.NumberOfRecords())
	for i := 0; i < t.NumberOfRecords(); i++ {
		rows = append(rows, SIACDNRow{
			Tabela:    sanitizeDBF(t, i, "CDN_TB"),
			Item:      sanitizeDBF(t, i, "CDN_IT"),
			Descricao: sanitizeDBF(t, i, "CDN_DSCR"),
			Checksum:  sanitizeDBF(t, i, "CDN_CHKSM"),
		})
	}
	return rows, nil
}

func readCADMUN(path string) ([]CADMUNRow, error) {
	t, err := openDBF(path)
	if err != nil {
		return nil, err
	}
	rows := make([]CADMUNRow, 0, t.NumberOfRecords())
	for i := 0; i < t.NumberOfRecords(); i++ {
		teto, _ := t.Int64FieldValueByName(i, "TETOPAB")
		rows = append(rows, CADMUNRow{
			CodUF:   sanitizeDBF(t, i, "CODUF"),
			CodMun:  sanitizeDBF(t, i, "CODMUNIC"),
			Nome:    sanitizeDBF(t, i, "NOME"),
			Condic:  sanitizeDBF(t, i, "CONDIC"),
			TetoPab: teto,
			CalcPab: sanitizeDBF(t, i, "CALCPAB"),
		})
	}
	return rows, nil
}

func sanitizeDBF(t *godbf.DbfTable, row int, col string) string {
	v, _ := t.FieldValueByName(row, col)
	clean, _ := SanitizeString(strings.TrimSpace(v))
	return clean
}

func parseDBFDate(t *godbf.DbfTable, row int, col string) time.Time {
	v, _ := t.FieldValueByName(row, col)
	s := strings.TrimSpace(v)
	if s == "" {
		return time.Time{}
	}
	d, err := time.Parse("20060102", s)
	if err != nil {
		return time.Time{}
	}
	return d
}
