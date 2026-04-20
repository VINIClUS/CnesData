package extractor

import (
	"context"
	"database/sql"
	"fmt"
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
