// Package extractor SIA DBF reader via LindsayBradford/go-dbf.
package extractor

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// SIAAPARow é uma linha de produção APAC: S_PRD com PRD_APANUM, enriquecida
// com S_APA por (APA_NUM, APA_CMP). Campos apa_* ficam vazios sem APAC.
type SIAAPARow struct {
	Uid              string `parquet:"prd_uid"`
	Competencia      string `parquet:"prd_cmp"`
	Apanum           string `parquet:"prd_apanum"`
	Procedimento     string `parquet:"prd_pa"`
	Cbo              string `parquet:"prd_cbo"`
	CidPrincipal     string `parquet:"prd_cidpri"`
	QtProduzida      *int64 `parquet:"prd_qt_p,optional"`
	QtAprovada       *int64 `parquet:"prd_qt_a,optional"`
	ValorProduzCents *int64 `parquet:"prd_vl_p,optional"`
	ValorAprovCents  *int64 `parquet:"prd_vl_a,optional"`
	DtInicio         string `parquet:"apa_dtinic"`
	DtFim            string `parquet:"apa_dtfim"`
	CnsExecutante    string `parquet:"apa_cnsexe"`
}

// SIABPIRow é uma linha de S_BPI/S_BPIHST sem dados do paciente.
type SIABPIRow struct {
	Uid             string `parquet:"bpi_uid"`
	Competencia     string `parquet:"bpi_cmp"`
	CnsProfissional string `parquet:"bpi_cnsmed"`
	Cbo             string `parquet:"bpi_cbo"`
	Folha           string `parquet:"bpi_flh"`
	Seq             string `parquet:"bpi_seq"`
	Procedimento    string `parquet:"bpi_pa"`
	Cid             string `parquet:"bpi_cid"`
	DtAtendimento   string `parquet:"bpi_dtaten"`
	QtProduzida     *int64 `parquet:"bpi_qt_p,optional"`
	QtAprovada      *int64 `parquet:"bpi_qt_a,optional"`
}

// SIASIGTAPRow segue o layout tb_procedimento do SIGTAP, derivado de S_PA.
type SIASIGTAPRow struct {
	CoProcedimento  string `parquet:"co_procedimento"`
	NoProcedimento  string `parquet:"no_procedimento"`
	TpComplexidade  string `parquet:"tp_complexidade"`
	CoFinanciamento string `parquet:"co_financiamento"`
	DtCompetencia   string `parquet:"dt_competencia"`
}

// CADMUNRow é uma linha de CADMUN; CODMUNIC tem 4 dígitos (IBGE6 = CODUF+CODMUNIC).
type CADMUNRow struct {
	CodUF   string   `parquet:"coduf"`
	CodMun  string   `parquet:"codmunic"`
	Nome    string   `parquet:"nome"`
	Condic  string   `parquet:"condic"`
	TetoPab *float64 `parquet:"tetopab,optional"`
	CalcPab *float64 `parquet:"calcpab,optional"`
}

// SIAResult contém só os subtipos pedidos ao ExtractSIA.
type SIAResult struct {
	APA    []SIAAPARow
	BPI    []SIABPIRow
	BPIHST []SIABPIRow
	SIGTAP []SIASIGTAPRow
	CADMUN []CADMUNRow
}

var (
	fieldsPRD = []string{
		"PRD_UID", "PRD_CMP", "PRD_APANUM", "PRD_PA", "PRD_CBO", "PRD_CIDPRI",
		"PRD_QT_P", "PRD_QT_A", "PRD_VL_P", "PRD_VL_A",
	}
	fieldsAPA = []string{"APA_NUM", "APA_CMP", "APA_DTINIC", "APA_DTFIM", "APA_CNSEXE"}
	fieldsBPI = []string{
		"BPI_UID", "BPI_CMP", "BPI_CNSMED", "BPI_CBO", "BPI_FLH", "BPI_SEQ", "BPI_PA",
		"BPI_CID", "BPI_DTATEN", "BPI_QT_P", "BPI_QT_A",
	}
	fieldsPA     = []string{"PA_CMP", "PA_ID", "PA_DV", "PA_DC", "PA_CPX", "PA_CTF"}
	fieldsCADMUN = []string{"CODUF", "CODMUNIC", "NOME", "CONDIC", "TETOPAB", "CALCPAB"}
)

// ExtractSIA lê de dir só os DBFs necessários aos subtipos pedidos.
//
// Args: dir (pasta SIASUS), competencia AAAAMM, subtypes (fato_subtype do job).
// S_BPIHST.DBF ausente vira SIA_BPIHST vazio; demais DBFs ausentes falham.
// Raises: sia_file_missing, sia_field_missing, sia_sigtap_empty, unknown_sia_subtype.
func ExtractSIA(dir, competencia string, subtypes []string) (*SIAResult, error) {
	if _, err := os.Stat(dir); err != nil {
		return nil, fmt.Errorf("sia_dir_missing: %w", err)
	}
	competencia = strings.ReplaceAll(competencia, "-", "")
	result := &SIAResult{}
	for _, subtype := range subtypes {
		if err := extractSIASubtype(dir, competencia, subtype, result); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func extractSIASubtype(dir, competencia, subtype string, r *SIAResult) error {
	var err error
	switch subtype {
	case "SIA_APA":
		r.APA, err = readAPA(dir)
	case "SIA_BPI":
		r.BPI, err = readBPI(dir, "S_BPI.DBF")
	case "SIA_BPIHST":
		r.BPIHST, err = readOptionalBPI(dir, "S_BPIHST.DBF")
	case "DIM_SIGTAP":
		r.SIGTAP, err = readSIGTAP(dir, competencia)
	case "DIM_MUNICIPIO":
		r.CADMUN, err = readCADMUN(dir)
	default:
		err = fmt.Errorf("unknown_sia_subtype=%s", subtype)
	}
	return err
}

type apaKey struct{ num, cmp string }

type apaHeader struct{ dtInicio, dtFim, cnsExecutante string }

func readAPA(dir string) ([]SIAAPARow, error) {
	headers, err := readAPAHeaders(dir)
	if err != nil {
		return nil, err
	}
	t, err := openSIADBF(dir, "S_PRD.DBF", fieldsPRD)
	if err != nil {
		return nil, err
	}
	rows := make([]SIAAPARow, 0)
	for i := 0; i < t.NumberOfRecords(); i++ {
		apanum := t.text(i, "PRD_APANUM")
		if t.RowIsDeleted(i) || apanum == "" {
			continue
		}
		row, err := buildAPARow(t, i, apanum)
		if err != nil {
			return nil, err
		}
		header := headers[apaKey{apanum, row.Competencia}]
		row.DtInicio, row.DtFim, row.CnsExecutante = header.dtInicio, header.dtFim, header.cnsExecutante
		rows = append(rows, row)
	}
	return rows, nil
}

func buildAPARow(t *siaTable, i int, apanum string) (SIAAPARow, error) {
	row := SIAAPARow{
		Uid: t.text(i, "PRD_UID"), Competencia: t.text(i, "PRD_CMP"), Apanum: apanum,
		Procedimento: t.text(i, "PRD_PA"), Cbo: t.text(i, "PRD_CBO"),
		CidPrincipal: t.text(i, "PRD_CIDPRI"),
	}
	var err error
	if row.QtProduzida, err = t.integer(i, "PRD_QT_P"); err != nil {
		return row, err
	}
	if row.QtAprovada, err = t.integer(i, "PRD_QT_A"); err != nil {
		return row, err
	}
	if row.ValorProduzCents, err = t.cents(i, "PRD_VL_P"); err != nil {
		return row, err
	}
	row.ValorAprovCents, err = t.cents(i, "PRD_VL_A")
	return row, err
}

func readAPAHeaders(dir string) (map[apaKey]apaHeader, error) {
	t, err := openSIADBF(dir, "S_APA.DBF", fieldsAPA)
	if err != nil {
		return nil, err
	}
	headers := make(map[apaKey]apaHeader, t.NumberOfRecords())
	for i := 0; i < t.NumberOfRecords(); i++ {
		if t.RowIsDeleted(i) {
			continue
		}
		headers[apaKey{t.text(i, "APA_NUM"), t.text(i, "APA_CMP")}] = apaHeader{
			dtInicio:      t.text(i, "APA_DTINIC"),
			dtFim:         t.text(i, "APA_DTFIM"),
			cnsExecutante: t.text(i, "APA_CNSEXE"),
		}
	}
	return headers, nil
}

func readBPI(dir, file string) ([]SIABPIRow, error) {
	t, err := openSIADBF(dir, file, fieldsBPI)
	if err != nil {
		return nil, err
	}
	rows := make([]SIABPIRow, 0, t.NumberOfRecords())
	for i := 0; i < t.NumberOfRecords(); i++ {
		if t.RowIsDeleted(i) {
			continue
		}
		row := SIABPIRow{
			Uid: t.text(i, "BPI_UID"), Competencia: t.text(i, "BPI_CMP"),
			CnsProfissional: t.text(i, "BPI_CNSMED"), Cbo: t.text(i, "BPI_CBO"),
			Folha: t.text(i, "BPI_FLH"), Seq: t.text(i, "BPI_SEQ"),
			Procedimento: t.text(i, "BPI_PA"), Cid: t.text(i, "BPI_CID"),
			DtAtendimento: t.text(i, "BPI_DTATEN"),
		}
		if row.QtProduzida, err = t.integer(i, "BPI_QT_P"); err != nil {
			return nil, err
		}
		if row.QtAprovada, err = t.integer(i, "BPI_QT_A"); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// Histórico BPI só existe depois de fechamentos; ausência é slot zero-row, não dependência.
func readOptionalBPI(dir, file string) ([]SIABPIRow, error) {
	if _, err := os.Stat(filepath.Join(dir, file)); errors.Is(err, os.ErrNotExist) {
		return []SIABPIRow{}, nil
	}
	return readBPI(dir, file)
}

func readSIGTAP(dir, competencia string) ([]SIASIGTAPRow, error) {
	t, err := openSIADBF(dir, "S_PA.DBF", fieldsPA)
	if err != nil {
		return nil, err
	}
	rows := make([]SIASIGTAPRow, 0)
	for i := 0; i < t.NumberOfRecords(); i++ {
		if t.RowIsDeleted(i) || t.text(i, "PA_CMP") != competencia {
			continue
		}
		rows = append(rows, SIASIGTAPRow{
			CoProcedimento:  t.text(i, "PA_ID") + t.text(i, "PA_DV"),
			NoProcedimento:  t.text(i, "PA_DC"),
			TpComplexidade:  t.text(i, "PA_CPX"),
			CoFinanciamento: t.text(i, "PA_CTF"),
			DtCompetencia:   competencia,
		})
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("sia_sigtap_empty competencia=%s", competencia)
	}
	return rows, nil
}

func readCADMUN(dir string) ([]CADMUNRow, error) {
	t, err := openSIADBF(dir, "CADMUN.DBF", fieldsCADMUN)
	if err != nil {
		return nil, err
	}
	rows := make([]CADMUNRow, 0, t.NumberOfRecords())
	for i := 0; i < t.NumberOfRecords(); i++ {
		if t.RowIsDeleted(i) {
			continue
		}
		row := CADMUNRow{
			CodUF: t.text(i, "CODUF"), CodMun: t.text(i, "CODMUNIC"),
			Nome: t.text(i, "NOME"), Condic: t.text(i, "CONDIC"),
		}
		if row.TetoPab, err = t.decimal(i, "TETOPAB"); err != nil {
			return nil, err
		}
		if row.CalcPab, err = t.decimal(i, "CALCPAB"); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}
