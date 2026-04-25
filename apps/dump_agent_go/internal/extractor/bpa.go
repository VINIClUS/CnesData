// Package extractor BPA reader against FB 1.5 BPAMAG.GDB via nakagami/firebirdsql.
package extractor

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// BPACRow raw row para BPA_C_LINHAS (consolidado).
type BPACRow struct {
	Competencia  string `parquet:"nu_competencia"`
	Cnes         string `parquet:"co_cnes"`
	Procedimento string `parquet:"co_procedimento"`
	Quantidade   int32  `parquet:"qt_aprovada"`
	Cbo          string `parquet:"co_cbo"`
	TpIdade      int16  `parquet:"tp_idade"`
	NuIdade      int16  `parquet:"nu_idade"`
}

// BPAIRow raw row para BPA_I_LINHAS (individualizado).
type BPAIRow struct {
	Competencia     string    `parquet:"nu_competencia"`
	Cnes            string    `parquet:"co_cnes"`
	CnsPaciente     string    `parquet:"nu_cns_pac"`
	CpfPaciente     string    `parquet:"nu_cpf_pac"`
	Procedimento    string    `parquet:"co_procedimento"`
	Cbo             string    `parquet:"co_cbo"`
	Cid10           string    `parquet:"co_cid10"`
	DtAtendimento   time.Time `parquet:"dt_atendimento"`
	Quantidade      int32     `parquet:"qt_aprovada"`
	CnsProfissional string    `parquet:"nu_cns_prof"`
}

// BPAResult agregado das duas tabelas BPA para uma competência.
type BPAResult struct {
	BPA_C []BPACRow
	BPA_I []BPAIRow
}

const sqlBPAC = `
	SELECT NU_COMPETENCIA, CO_CNES, CO_PROCEDIMENTO, QT_APROVADA,
	       CO_CBO, TP_IDADE, NU_IDADE
	FROM BPA_C_LINHAS
	WHERE NU_COMPETENCIA = ?
`

const sqlBPAI = `
	SELECT NU_COMPETENCIA, CO_CNES, NU_CNS_PAC,
	       COALESCE(NU_CPF_PAC, '') AS NU_CPF_PAC,
	       COALESCE(CO_PROCEDIMENTO, '') AS CO_PROCEDIMENTO,
	       COALESCE(CO_CBO, '') AS CO_CBO,
	       COALESCE(CO_CID10, '') AS CO_CID10,
	       DT_ATENDIMENTO,
	       COALESCE(QT_APROVADA, 0) AS QT_APROVADA,
	       COALESCE(NU_CNS_PROF, '') AS NU_CNS_PROF
	FROM BPA_I_LINHAS
	WHERE NU_COMPETENCIA = ?
`

// ExtractBPA executa as duas queries BPA e retorna BPAResult agregado.
// Args: ctx, db (FB 1.5 BPAMAG.GDB), competencia AAAAMM (ex: "202601").
// Returns: *BPAResult com BPA_C + BPA_I.
// Raises: erro propagado se query/scan falhar.
func ExtractBPA(ctx context.Context, db *sql.DB, competencia string) (*BPAResult, error) {
	result := &BPAResult{}
	if err := extractBPAC(ctx, db, competencia, result); err != nil {
		return nil, err
	}
	if err := extractBPAI(ctx, db, competencia, result); err != nil {
		return nil, err
	}
	return result, nil
}

func extractBPAC(ctx context.Context, db *sql.DB, competencia string, result *BPAResult) error {
	rows, err := db.QueryContext(ctx, sqlBPAC, competencia)
	if err != nil {
		return fmt.Errorf("bpa_c_query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var r BPACRow
		if err := rows.Scan(
			&r.Competencia, &r.Cnes, &r.Procedimento, &r.Quantidade,
			&r.Cbo, &r.TpIdade, &r.NuIdade,
		); err != nil {
			return fmt.Errorf("bpa_c_scan: %w", err)
		}
		r.Procedimento, _ = SanitizeString(r.Procedimento)
		r.Cbo, _ = SanitizeString(r.Cbo)
		result.BPA_C = append(result.BPA_C, r)
	}
	return rows.Err()
}

func extractBPAI(ctx context.Context, db *sql.DB, competencia string, result *BPAResult) error {
	rows, err := db.QueryContext(ctx, sqlBPAI, competencia)
	if err != nil {
		return fmt.Errorf("bpa_i_query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var r BPAIRow
		if err := rows.Scan(
			&r.Competencia, &r.Cnes, &r.CnsPaciente, &r.CpfPaciente,
			&r.Procedimento, &r.Cbo, &r.Cid10, &r.DtAtendimento,
			&r.Quantidade, &r.CnsProfissional,
		); err != nil {
			return fmt.Errorf("bpa_i_scan: %w", err)
		}
		r.Procedimento, _ = SanitizeString(r.Procedimento)
		r.Cbo, _ = SanitizeString(r.Cbo)
		r.Cid10, _ = SanitizeString(r.Cid10)
		result.BPA_I = append(result.BPA_I, r)
	}
	return rows.Err()
}
