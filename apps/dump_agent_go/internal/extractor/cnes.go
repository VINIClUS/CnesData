package extractor

import (
	"context"
	"database/sql"
	"fmt"
)

// ExtractionParams parâmetros de job de extração.
type ExtractionParams struct {
	Intent      string
	Competencia string
	CodMunGest  string // IBGE 6-dígitos
}

const sqlProfissionais = `
	SELECT
		prof.CPF_PROF, prof.COD_CNS, prof.NOME_PROF,
		prof.NO_SOCIAL, prof.SEXO, prof.DATA_NASC,
		vinc.COD_CBO, vinc.IND_VINC, vinc.TP_SUS_NAO_SUS,
		(COALESCE(vinc.CG_HORAAMB, 0)
		 + COALESCE(vinc.CGHORAOUTR, 0)
		 + COALESCE(vinc.CGHORAHOSP, 0)) AS CARGA_HORARIA_TOTAL,
		COALESCE(vinc.CG_HORAAMB, 0) AS CG_HORAAMB,
		COALESCE(vinc.CGHORAOUTR, 0) AS CGHORAOUTR,
		COALESCE(vinc.CGHORAHOSP, 0) AS CGHORAHOSP,
		est.CNES, est.NOME_FANTA, est.TP_UNID_ID,
		est.CODMUNGEST
	FROM       LFCES021 vinc
	INNER JOIN LFCES004 est  ON est.UNIDADE_ID = vinc.UNIDADE_ID
	INNER JOIN LFCES018 prof ON prof.PROF_ID   = vinc.PROF_ID
	WHERE est.CODMUNGEST = ?
	ORDER BY prof.NOME_PROF, vinc.COD_CBO
`

// ExtractCnesProfissionais executa query de profissionais e envia rows
// sanitizadas pelo channel. Versão Python atual não faz LEFT JOIN a
// LFCES060 aqui (equipes é intent separado), portanto bug -501 não se
// aplica a esta query específica. Se validação em banco real indicar
// necessidade de merge com LFCES060, adicionar 3-query workaround.
func ExtractCnesProfissionais(
	ctx context.Context,
	conn *sql.Conn,
	params ExtractionParams,
	out chan<- CnesProfissionalRow,
) error {
	rows, err := conn.QueryContext(ctx, sqlProfissionais, params.CodMunGest)
	if err != nil {
		return fmt.Errorf("query_profissionais: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var r CnesProfissionalRow
		if err := rows.Scan(
			&r.CPFProf, &r.CodCNS, &r.NomeProf, &r.NoSocial, &r.Sexo, &r.DataNasc,
			&r.CodCBO, &r.IndVinc, &r.TPSUSNaoSUS, &r.CargaHorariaTotal,
			&r.CGHoraAmb, &r.CGHoraOutr, &r.CGHoraHosp,
			&r.CNES, &r.NomeFanta, &r.TPUnidID, &r.CodMunGest,
		); err != nil {
			return fmt.Errorf("scan_profissionais: %w", err)
		}
		sanitizeCnesProfissionalRow(&r)
		select {
		case out <- r:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return rows.Err()
}

func sanitizeCnesProfissionalRow(r *CnesProfissionalRow) {
	r.NomeProf, _ = SanitizeString(r.NomeProf)
	r.NoSocial, _ = SanitizeString(r.NoSocial)
	r.NomeFanta, _ = SanitizeString(r.NomeFanta)
}
