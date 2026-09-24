// Package extractor BPA reader against FB 1.5 BPAMAG.GDB via nakagami/firebirdsql.
package extractor

import (
	"context"
	"database/sql"
	"fmt"
)

// Origem PRD_ORG em S_PRD: 'BPA' = consolidado (BPA_C), 'BPI' = individualizado (BPA_I).
const (
	bpaOrigemConsolidado     = "BPA"
	bpaOrigemIndividualizado = "BPI"
)

// BPARow linha raw de S_PRD, no contrato `prd_*` consumido pelo data_processor.
// Colunas de PII de paciente (CNS, nome, nascimento, CPF, endereço) não são extraídas.
type BPARow struct {
	Uid             string   `parquet:"prd_uid"`
	Competencia     string   `parquet:"prd_cmp"`
	Org             string   `parquet:"prd_org"`
	Folha           string   `parquet:"prd_flh"`
	Sequencia       string   `parquet:"prd_seq"`
	Procedimento    string   `parquet:"prd_pa"`
	Cbo             string   `parquet:"prd_cbo"`
	Cid             string   `parquet:"prd_cid"`
	Idade           string   `parquet:"prd_idade"`
	DtAtendimento   string   `parquet:"prd_dtaten"`
	CnsProfissional string   `parquet:"prd_cnsmed"`
	Quantidade      *float64 `parquet:"prd_qt_p,optional"`
}

// BPAResult agregado dos dois subtipos BPA para uma competência.
type BPAResult struct {
	BPA_C []BPARow
	BPA_I []BPARow
}

// Strings nulas viram string vazia; o data_processor trata branco como nulo.
// PRD_QT_P (DOUBLE) permanece nulo para virar quality issue explícita.
const sqlBPA = `
	SELECT COALESCE(PRD_UID, '') AS PRD_UID,
	       COALESCE(PRD_CMP, '') AS PRD_CMP,
	       COALESCE(PRD_ORG, '') AS PRD_ORG,
	       COALESCE(PRD_FLH, '') AS PRD_FLH,
	       COALESCE(PRD_SEQ, '') AS PRD_SEQ,
	       COALESCE(PRD_PA, '') AS PRD_PA,
	       COALESCE(PRD_CBO, '') AS PRD_CBO,
	       COALESCE(PRD_CID, '') AS PRD_CID,
	       COALESCE(PRD_IDADE, '') AS PRD_IDADE,
	       COALESCE(PRD_DTATEN, '') AS PRD_DTATEN,
	       COALESCE(PRD_CNSMED, '') AS PRD_CNSMED,
	       PRD_QT_P
	FROM S_PRD
	WHERE PRD_CMP = ? AND PRD_ORG = ?
	ORDER BY PRD_UID, PRD_FLH, PRD_SEQ
`

// ExtractBPA lê S_PRD da competência separando BPA_C e BPA_I por PRD_ORG.
// Args: ctx, db (FB 1.5 BPAMAG.GDB), competencia AAAAMM (ex: "202608").
// Returns: *BPAResult com BPA_C + BPA_I.
// Raises: erro propagado se query/scan falhar.
func ExtractBPA(ctx context.Context, db *sql.DB, competencia string) (*BPAResult, error) {
	consolidado, err := extractBPARows(ctx, db, competencia, bpaOrigemConsolidado)
	if err != nil {
		return nil, fmt.Errorf("bpa_c_%w", err)
	}
	individualizado, err := extractBPARows(ctx, db, competencia, bpaOrigemIndividualizado)
	if err != nil {
		return nil, fmt.Errorf("bpa_i_%w", err)
	}
	return &BPAResult{BPA_C: consolidado, BPA_I: individualizado}, nil
}

func extractBPARows(
	ctx context.Context, db *sql.DB, competencia, origem string,
) ([]BPARow, error) {
	rows, err := db.QueryContext(ctx, sqlBPA, competencia, origem)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var out []BPARow
	for rows.Next() {
		r, err := scanBPARow(rows)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	return out, nil
}

func scanBPARow(rows *sql.Rows) (BPARow, error) {
	var r BPARow
	var quantidade sql.NullFloat64
	fields := []*string{
		&r.Uid, &r.Competencia, &r.Org, &r.Folha, &r.Sequencia, &r.Procedimento,
		&r.Cbo, &r.Cid, &r.Idade, &r.DtAtendimento, &r.CnsProfissional,
	}
	dest := make([]any, 0, len(fields)+1)
	for _, field := range fields {
		dest = append(dest, field)
	}
	if err := rows.Scan(append(dest, &quantidade)...); err != nil {
		return BPARow{}, err
	}
	for _, field := range fields {
		*field, _ = SanitizeString(*field)
	}
	if quantidade.Valid {
		r.Quantidade = &quantidade.Float64
	}
	return r, nil
}
