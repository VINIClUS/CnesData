"""PostgresAdapter — implementação de StoragePort para PostgreSQL via SQLAlchemy Core."""

import logging
import time

import polars as pl
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine

from storage.schema import dim_estabelecimento, dim_profissional, fato_vinculo

logger = logging.getLogger(__name__)

_CHUNK_SIZE: int = 1000
_SUS_MAP: dict[str, bool] = {"S": True, "N": False}


def _chunked(lst: list, size: int) -> list[list]:
    return [lst[i:i + size] for i in range(0, len(lst), size)]


class PostgresAdapter:
    """Persiste dados do pipeline CNES no schema gold do PostgreSQL."""

    def __init__(self, engine: Engine, tenant_id: str) -> None:
        self._engine = engine
        self._tenant_id = tenant_id

    def gravar_profissionais(self, competencia: str, df: pl.DataFrame) -> None:
        """Persiste profissionais e vínculos no schema gold.

        Args:
            competencia: Competência no formato 'YYYY-MM'.
            df: DataFrame Polars com colunas SCHEMA_PROFISSIONAL.
        """
        t0 = time.perf_counter()
        prof_rows = self._build_profissional_rows(df)
        vinculo_rows = self._build_vinculo_rows(competencia, df)
        with self._engine.begin() as con:
            self._upsert_chunks(con, dim_profissional, prof_rows, "profissional")
            self._upsert_chunks(con, fato_vinculo, vinculo_rows, "vinculo")
        elapsed = time.perf_counter() - t0
        logger.info(
            "gravar_profissionais dim=%d fato=%d elapsed=%.2fs",
            len(prof_rows), len(vinculo_rows), elapsed,
        )

    def gravar_estabelecimentos(self, competencia: str, df: pl.DataFrame) -> None:
        """Persiste estabelecimentos no schema gold.

        Args:
            competencia: Competência no formato 'YYYY-MM'.
            df: DataFrame Polars com colunas SCHEMA_ESTABELECIMENTO.
        """
        t0 = time.perf_counter()
        rows = self._build_estabelecimento_rows(df)
        with self._engine.begin() as con:
            for chunk in _chunked(rows, _CHUNK_SIZE):
                con.execute(
                    insert(dim_estabelecimento)
                    .values(chunk)
                    .on_conflict_do_update(
                        index_elements=["tenant_id", "cnes"],
                        set_={
                            "fontes": text(
                                "dim_estabelecimento.fontes || EXCLUDED.fontes"
                            ),
                            "atualizado_em": text("NOW()"),
                        },
                    ),
                )
        elapsed = time.perf_counter() - t0
        logger.info(
            "gravar_estabelecimentos rows=%d elapsed=%.2fs", len(rows), elapsed,
        )

    def registrar_pipeline_run(self, competencia: str, estado: dict) -> None:
        logger.debug("pipeline_run competencia=%s", competencia)

    def _upsert_chunks(self, con, table, rows: list[dict], kind: str) -> None:
        conflict_keys = {
            "profissional": {
                "index_elements": ["tenant_id", "cpf"],
                "set_": {
                    "fontes": text(f"{table.name}.fontes || EXCLUDED.fontes"),
                    "atualizado_em": text("NOW()"),
                },
            },
            "vinculo": {
                "index_elements": [
                    "tenant_id", "competencia", "cpf", "cnes", "cbo",
                ],
                "set_": {
                    "tipo_vinculo": text(
                        "COALESCE(EXCLUDED.tipo_vinculo, fato_vinculo.tipo_vinculo)"
                    ),
                    "sus": text("COALESCE(EXCLUDED.sus, fato_vinculo.sus)"),
                    "ch_total": text("EXCLUDED.ch_total"),
                    "ch_ambulatorial": text("EXCLUDED.ch_ambulatorial"),
                    "ch_outras": text("EXCLUDED.ch_outras"),
                    "ch_hospitalar": text("EXCLUDED.ch_hospitalar"),
                    "fontes": text("fato_vinculo.fontes || EXCLUDED.fontes"),
                    "atualizado_em": text("NOW()"),
                },
            },
        }
        cfg = conflict_keys[kind]
        for chunk in _chunked(rows, _CHUNK_SIZE):
            con.execute(
                insert(table)
                .values(chunk)
                .on_conflict_do_update(**cfg),
            )

    def _build_profissional_rows(self, df: pl.DataFrame) -> list[dict]:
        dedup = df.unique(subset=["CPF"])
        out = dedup.select("CPF", "CNS", "NOME_PROFISSIONAL", "SEXO", "FONTE")
        out = out.rename({
            "CPF": "cpf", "CNS": "cns",
            "NOME_PROFISSIONAL": "nome_profissional",
            "SEXO": "sexo", "FONTE": "fonte",
        })
        out = out.with_columns(
            pl.lit(self._tenant_id).alias("tenant_id"),
            pl.col("fonte").map_elements(
                lambda f: {f: True}, return_dtype=pl.Object
            ).alias("fontes"),
        )
        return _df_to_records(
            out.select(
                "tenant_id", "cpf", "cns", "nome_profissional", "sexo", "fontes"
            )
        )

    def _build_vinculo_rows(
        self, competencia: str, df: pl.DataFrame,
    ) -> list[dict]:
        out = df.select(
            "CPF", "CNES", "CBO", "TIPO_VINCULO", "SUS",
            "CH_TOTAL", "CH_AMBULATORIAL", "CH_OUTRAS", "CH_HOSPITALAR", "FONTE",
        ).rename({
            "CPF": "cpf", "CNES": "cnes", "CBO": "cbo",
            "TIPO_VINCULO": "tipo_vinculo", "SUS": "sus",
            "CH_TOTAL": "ch_total", "CH_AMBULATORIAL": "ch_ambulatorial",
            "CH_OUTRAS": "ch_outras", "CH_HOSPITALAR": "ch_hospitalar",
            "FONTE": "fonte",
        })
        out = out.with_columns(
            pl.lit(self._tenant_id).alias("tenant_id"),
            pl.lit(competencia).alias("competencia"),
            pl.when(pl.col("sus") == "S").then(True)
            .when(pl.col("sus") == "N").then(False)
            .otherwise(None).alias("sus"),
            pl.col("fonte").map_elements(
                lambda f: {f: True}, return_dtype=pl.Object
            ).alias("fontes"),
        )
        return _df_to_records(
            out.select(
                "tenant_id", "competencia", "cpf", "cnes", "cbo",
                "tipo_vinculo", "sus", "ch_total", "ch_ambulatorial",
                "ch_outras", "ch_hospitalar", "fontes",
            )
        )

    def _build_estabelecimento_rows(self, df: pl.DataFrame) -> list[dict]:
        out = df.select(
            "CNES", "NOME_FANTASIA", "TIPO_UNIDADE", "CNPJ_MANTENEDORA",
            "NATUREZA_JURIDICA", "VINCULO_SUS", "FONTE",
        ).rename({
            "CNES": "cnes", "NOME_FANTASIA": "nome_fantasia",
            "TIPO_UNIDADE": "tipo_unidade",
            "CNPJ_MANTENEDORA": "cnpj_mantenedora",
            "NATUREZA_JURIDICA": "natureza_juridica",
            "VINCULO_SUS": "vinculo_sus", "FONTE": "fonte",
        })
        out = out.with_columns(
            pl.lit(self._tenant_id).alias("tenant_id"),
            pl.when(pl.col("vinculo_sus") == "S").then(True)
            .when(pl.col("vinculo_sus") == "N").then(False)
            .otherwise(None).alias("vinculo_sus"),
            pl.col("fonte").map_elements(
                lambda f: {f: True}, return_dtype=pl.Object
            ).alias("fontes"),
        )
        return _df_to_records(
            out.select(
                "tenant_id", "cnes", "nome_fantasia", "tipo_unidade",
                "cnpj_mantenedora", "natureza_juridica", "vinculo_sus",
                "fontes",
            )
        )


def _df_to_records(df: pl.DataFrame) -> list[dict]:
    rows = df.to_dicts()
    for row in rows:
        for k, v in row.items():
            if v is not None and isinstance(v, float) and v != v:
                row[k] = None
    return rows
