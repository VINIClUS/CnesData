package extractor

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/cnesdata/dumpagent/internal/delta"
)

const sqlSihdProducao = `
	SELECT
		aih.NUMERO, proc.CODIGO, aih.COMPETENCIA,
		est.CNES, aih.VALOR
	FROM       SIHD_AIH aih
	INNER JOIN SIHD_PROC proc ON proc.AIH_ID = aih.ID
	INNER JOIN LFCES004 est   ON est.UNIDADE_ID = aih.UNIDADE_ID
	WHERE aih.COMPETENCIA = ? AND est.CODMUNGEST = ?
`

// ExtractSihdProducao stream rows SIHD para channel out.
// Ajustar query conforme schema real do SIHD (ver data-dictionary-sihd-hospital.md).
func ExtractSihdProducao(
	ctx context.Context,
	conn *sql.Conn,
	params ExtractionParams,
	out chan<- SihdProducaoRow,
) error {
	rows, err := conn.QueryContext(ctx, sqlSihdProducao, params.Competencia, params.CodMunGest)
	if err != nil {
		return fmt.Errorf("query_sihd: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var r SihdProducaoRow
		if err := rows.Scan(&r.AIHNumero, &r.Procedimento, &r.Competencia, &r.CNES, &r.Valor); err != nil {
			return fmt.Errorf("scan_sihd: %w", err)
		}
		select {
		case out <- r:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return rows.Err()
}

// SihdKind é o tipo lido do Firebird para uma coluna raw SIHD.
type SihdKind int

const (
	SihdText SihdKind = iota
	SihdInteger
	SihdDecimal
)

// SihdColumn declara uma coluna crua de TB_HAIH/TB_HPA na ordem do SELECT.
type SihdColumn struct {
	Name string
	Kind SihdKind
}

// SihdQueryer cobre *sql.DB e *sql.Conn.
type SihdQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// Só tabelas históricas (pós-fechamento) e só colunas mapeadas pelo data_processor;
// nome, CNS e nascimento do paciente e documentos de profissionais nunca saem do Edge.
// No SIHD2 real AH_DIAG_SEC é legado ('0000') e AH_PACIENTE_MUN_ORIGEM fica NULL (#295).
const sqlSihdInternacao = `
	SELECT AH_NUM_AIH, AH_OE_GESTOR, AH_SEQ, AH_CNES, AH_CMPT,
	       AH_PROC_SOLICITADO, AH_PROC_REALIZADO, AH_DIAG_PRI, AH_DIAG_SEC_1,
	       AH_DT_INTERNACAO, AH_DT_SAIDA, AH_CAR_INTERNACAO, AH_SITUACAO, AH_IDENT,
	       AH_MODALIDADE_INTERNACAO, AH_PACIENTE_SEXO, AH_PACIENTE_LOGR_MUNICIPIO
	FROM TB_HAIH
	WHERE AH_CMPT = ?
	ORDER BY AH_OE_GESTOR, AH_SEQ
`

const sqlSihdProcAIH = `
	SELECT PA_NUM_AIH, PA_OE_GESTOR, PA_SEQ_PRINC, PA_INDX, PA_CNES, PA_CMPT,
	       PA_PROCEDIMENTO, PA_PROCEDIMENTO_QTD, PA_VALOR, PA_PF_CBO
	FROM TB_HPA
	WHERE PA_CMPT = ?
	ORDER BY PA_OE_GESTOR, PA_SEQ_PRINC, PA_INDX
`

var sihdRawQueries = map[string]string{
	"SIHD_INTERNACAO": sqlSihdInternacao,
	"SIHD_PROC_AIH":   sqlSihdProcAIH,
}

var sihdRawColumns = map[string][]SihdColumn{
	"SIHD_INTERNACAO": {
		{"AH_NUM_AIH", SihdText}, {"AH_OE_GESTOR", SihdText}, {"AH_SEQ", SihdInteger},
		{"AH_CNES", SihdText}, {"AH_CMPT", SihdText}, {"AH_PROC_SOLICITADO", SihdText},
		{"AH_PROC_REALIZADO", SihdText}, {"AH_DIAG_PRI", SihdText}, {"AH_DIAG_SEC_1", SihdText},
		{"AH_DT_INTERNACAO", SihdText}, {"AH_DT_SAIDA", SihdText},
		{"AH_CAR_INTERNACAO", SihdText}, {"AH_SITUACAO", SihdText}, {"AH_IDENT", SihdText},
		{"AH_MODALIDADE_INTERNACAO", SihdText}, {"AH_PACIENTE_SEXO", SihdText},
		{"AH_PACIENTE_LOGR_MUNICIPIO", SihdText},
	},
	"SIHD_PROC_AIH": {
		{"PA_NUM_AIH", SihdText}, {"PA_OE_GESTOR", SihdText}, {"PA_SEQ_PRINC", SihdInteger},
		{"PA_INDX", SihdInteger}, {"PA_CNES", SihdText}, {"PA_CMPT", SihdText},
		{"PA_PROCEDIMENTO", SihdText}, {"PA_PROCEDIMENTO_QTD", SihdInteger},
		{"PA_VALOR", SihdDecimal}, {"PA_PF_CBO", SihdText},
	},
}

// SihdRawColumns devolve o schema raw do subtipo SIHD, vazio se desconhecido.
func SihdRawColumns(subtype string) []SihdColumn {
	return slices.Clone(sihdRawColumns[subtype])
}

// ExtractSihdRaw lê TB_HAIH (SIHD_INTERNACAO) ou TB_HPA (SIHD_PROC_AIH) da competência.
//
// Args: db, competencia (AAAAMM ou AAAA-MM), subtype.
// Returns: linhas com colunas cruas AH_*/PA_*; NULL vira nil, texto sem espaços de CHAR.
// Raises: unknown_sihd_subtype, sihd_query, sihd_scan, sihd_rows.
func ExtractSihdRaw(
	ctx context.Context, db SihdQueryer, competencia, subtype string,
) ([]delta.Row, error) {
	query, ok := sihdRawQueries[subtype]
	if !ok {
		return nil, fmt.Errorf("unknown_sihd_subtype=%s", subtype)
	}
	rows, err := db.QueryContext(ctx, query, strings.ReplaceAll(competencia, "-", ""))
	if err != nil {
		return nil, fmt.Errorf("sihd_query subtype=%s: %w", subtype, err)
	}
	defer rows.Close()
	columns := sihdRawColumns[subtype]
	out := make([]delta.Row, 0)
	for rows.Next() {
		row, err := scanSihdRaw(rows, columns)
		if err != nil {
			return nil, fmt.Errorf("sihd_scan subtype=%s: %w", subtype, err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sihd_rows subtype=%s: %w", subtype, err)
	}
	return out, nil
}

func scanSihdRaw(rows *sql.Rows, columns []SihdColumn) (delta.Row, error) {
	dest := make([]any, len(columns))
	for i, column := range columns {
		switch column.Kind {
		case SihdInteger:
			dest[i] = &sql.NullInt64{}
		case SihdDecimal:
			dest[i] = &sql.NullFloat64{}
		default:
			dest[i] = &sql.NullString{}
		}
	}
	if err := rows.Scan(dest...); err != nil {
		return nil, err
	}
	row := make(delta.Row, len(columns))
	for i, column := range columns {
		row[column.Name] = sihdRawValue(dest[i])
	}
	return row, nil
}

func sihdRawValue(scanned any) any {
	switch value := scanned.(type) {
	case *sql.NullInt64:
		if value.Valid {
			return value.Int64
		}
	case *sql.NullFloat64:
		if value.Valid {
			return strconv.FormatFloat(value.Float64, 'f', -1, 64)
		}
	case *sql.NullString:
		if value.Valid {
			clean, _ := SanitizeString(strings.TrimSpace(value.String))
			return clean
		}
	}
	return nil
}
