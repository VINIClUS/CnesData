"""Extractor CNES — Firebird cursor para raw Parquet."""

import logging
from pathlib import Path

import polars as pl

from cnes_domain.models.extraction import (
    ExtractionIntent,
    ExtractionParams,
)
from dump_agent.io_guard import SpoolGuard

logger = logging.getLogger(__name__)

_SQL_PROFISSIONAIS: str = """
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
"""

_SQL_ESTABELECIMENTOS: str = """
    SELECT
        est.CNES, est.NOME_FANTA, est.TP_UNID_ID,
        est.CODMUNGEST, est.CNPJ_MANT
    FROM LFCES004 est
    WHERE est.CODMUNGEST = ?
"""

_SQL_EQUIPES: str = """
    SELECT
        eq.SEQ_EQUIPE, eq.INE, eq.DS_AREA,
        eq.TP_EQUIPE, eq.COD_MUN
    FROM LFCES060 eq
    WHERE eq.COD_MUN = ?
"""

_INTENT_SQL: dict[ExtractionIntent, str] = {
    ExtractionIntent.PROFISSIONAIS: _SQL_PROFISSIONAIS,
    ExtractionIntent.ESTABELECIMENTOS: _SQL_ESTABELECIMENTOS,
    ExtractionIntent.EQUIPES: _SQL_EQUIPES,
}


class CnesExtractor:
    """Extrai dados CNES do Firebird para Parquet raw."""

    def extract(
        self,
        params: ExtractionParams,
        con: object,
        tmp_dir: Path,
        guard: SpoolGuard,
        batch_size: int = 5000,
    ) -> Path:
        sql = _INTENT_SQL[params.intent]
        output = tmp_dir / f"{params.intent.value}.parquet"

        cur = con.cursor()
        try:
            cur.execute(sql, (params.cod_municipio,))
            columns: list[str] = [
                d[0] for d in cur.description
            ]
            frames: list[pl.DataFrame] = []

            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break
                batch_df = pl.DataFrame(
                    rows, schema=columns, orient="row",
                )
                frames.append(batch_df)
                guard.track(batch_df.estimated_size())
        finally:
            cur.close()

        if not frames:
            empty = pl.DataFrame(
                schema=dict.fromkeys(columns, pl.Utf8),
            )
            empty.write_parquet(output)
        else:
            combined = pl.concat(frames)
            combined.write_parquet(output)

        logger.info(
            "extract_done intent=%s rows=%d path=%s",
            params.intent.value,
            sum(len(f) for f in frames),
            output,
        )
        return output
