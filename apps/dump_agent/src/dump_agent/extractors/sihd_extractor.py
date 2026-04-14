"""Extractor SIHD — Firebird cursor para raw Parquet."""

import logging
from pathlib import Path

import polars as pl

from cnes_domain.models.extraction import ExtractionParams
from dump_agent.io_guard import SpoolGuard

logger = logging.getLogger(__name__)

_SQL_SIHD_PRODUCAO: str = """
    SELECT
        AH_NUM_AIH, AH_CNES, AH_CMPT,
        AH_PACIENTE_NOME, AH_PACIENTE_NUMERO_CNS,
        AH_PACIENTE_SEXO, AH_PACIENTE_DT_NASCIMENTO,
        AH_PACIENTE_MUN_ORIGEM,
        AH_DIAG_PRI, AH_DIAG_SEC,
        AH_PROC_SOLICITADO, AH_PROC_REALIZADO,
        AH_DT_INTERNACAO, AH_DT_SAIDA,
        AH_MOT_SAIDA, AH_CAR_INTERNACAO,
        AH_ESPECIALIDADE, AH_SITUACAO,
        AH_MED_SOL_DOC, AH_MED_RESP_DOC,
        AH_OE_GESTOR, AH_SEQ
    FROM TB_HAIH
    WHERE AH_CMPT = ?
    ORDER BY AH_NUM_AIH
"""


class SihdExtractor:
    def extract(
        self,
        params: ExtractionParams,
        con: object,
        tmp_dir: Path,
        guard: SpoolGuard,
        batch_size: int = 5000,
    ) -> Path:
        competencia_aaaamm = params.competencia.replace("-", "")
        output = tmp_dir / f"{params.intent.value}.parquet"

        cur = con.cursor()
        try:
            cur.execute(_SQL_SIHD_PRODUCAO, (competencia_aaaamm,))
            columns: list[str] = [d[0] for d in cur.description]
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
            "extract_done intent=%s rows=%d",
            params.intent.value,
            sum(len(f) for f in frames),
        )
        return output
