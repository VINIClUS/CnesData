"""PostgresAdapter — implementação de StoragePort para PostgreSQL via SQLAlchemy Core."""
import logging
import time

import pandas as pd
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine

from storage.schema import dim_estabelecimento, dim_profissional, fato_vinculo

logger = logging.getLogger(__name__)

_CHUNK_SIZE: int = 1000
_SUS_MAP: dict[str, bool] = {"S": True, "N": False}


def _chunked(lst: list, size: int) -> list[list]:
    return [lst[i:i + size] for i in range(0, len(lst), size)]


def _map_sus(series: pd.Series) -> pd.Series:
    return series.map(_SUS_MAP)


class PostgresAdapter:
    """Persiste dados do pipeline CNES no schema gold do PostgreSQL."""

    def __init__(self, engine: Engine, tenant_id: str) -> None:
        self._engine = engine
        self._tenant_id = tenant_id

    def gravar_profissionais(self, competencia: str, df: pd.DataFrame) -> None:
        t0 = time.perf_counter()
        prof_rows = self._build_profissional_rows(df)
        vinculo_rows = self._build_vinculo_rows(competencia, df)
        with self._engine.begin() as con:
            for chunk in _chunked(prof_rows, _CHUNK_SIZE):
                con.execute(
                    insert(dim_profissional)
                    .values(chunk)
                    .on_conflict_do_update(
                        index_elements=["tenant_id", "cpf"],
                        set_={
                            "fontes": text("dim_profissional.fontes || EXCLUDED.fontes"),
                            "atualizado_em": text("NOW()"),
                        },
                    ),
                )
            for chunk in _chunked(vinculo_rows, _CHUNK_SIZE):
                con.execute(
                    insert(fato_vinculo)
                    .values(chunk)
                    .on_conflict_do_update(
                        index_elements=["tenant_id", "competencia", "cpf", "cnes", "cbo"],
                        set_={
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
                    ),
                )
        elapsed = time.perf_counter() - t0
        logger.info(
            "gravar_profissionais dim=%d fato=%d elapsed=%.2fs",
            len(prof_rows), len(vinculo_rows), elapsed,
        )

    def gravar_estabelecimentos(self, competencia: str, df: pd.DataFrame) -> None:
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
                            "fontes": text("dim_estabelecimento.fontes || EXCLUDED.fontes"),
                            "atualizado_em": text("NOW()"),
                        },
                    ),
                )
        elapsed = time.perf_counter() - t0
        logger.info(
            "gravar_estabelecimentos rows=%d elapsed=%.2fs",
            len(rows), elapsed,
        )

    def registrar_pipeline_run(self, competencia: str, estado: dict) -> None:
        logger.debug("pipeline_run competencia=%s", competencia)

    def _build_profissional_rows(self, df: pd.DataFrame) -> list[dict]:
        dedup = df.drop_duplicates(subset=["CPF"])
        out = dedup[["CPF", "CNS", "NOME_PROFISSIONAL", "SEXO", "FONTE"]].copy()
        out.columns = ["cpf", "cns", "nome_profissional", "sexo", "fonte"]
        out["tenant_id"] = self._tenant_id
        out["fontes"] = out["fonte"].apply(lambda f: {f: True})
        out = out.where(out.notna(), None)
        return out[
            ["tenant_id", "cpf", "cns", "nome_profissional", "sexo", "fontes"]
        ].to_dict(orient="records")

    def _build_vinculo_rows(self, competencia: str, df: pd.DataFrame) -> list[dict]:
        out = df[
            ["CPF", "CNES", "CBO", "TIPO_VINCULO", "SUS",
             "CH_TOTAL", "CH_AMBULATORIAL", "CH_OUTRAS", "CH_HOSPITALAR", "FONTE"]
        ].copy()
        out.columns = [
            "cpf", "cnes", "cbo", "tipo_vinculo", "sus",
            "ch_total", "ch_ambulatorial", "ch_outras", "ch_hospitalar", "fonte",
        ]
        out["tenant_id"] = self._tenant_id
        out["competencia"] = competencia
        out["sus"] = _map_sus(out["sus"])
        out["fontes"] = out["fonte"].apply(lambda f: {f: True})
        out = out.where(out.notna(), None)
        return out[
            ["tenant_id", "competencia", "cpf", "cnes", "cbo", "tipo_vinculo",
             "sus", "ch_total", "ch_ambulatorial", "ch_outras", "ch_hospitalar",
             "fontes"]
        ].to_dict(orient="records")

    def _build_estabelecimento_rows(self, df: pd.DataFrame) -> list[dict]:
        out = df[
            ["CNES", "NOME_FANTASIA", "TIPO_UNIDADE", "CNPJ_MANTENEDORA",
             "NATUREZA_JURIDICA", "VINCULO_SUS", "FONTE"]
        ].copy()
        out.columns = [
            "cnes", "nome_fantasia", "tipo_unidade", "cnpj_mantenedora",
            "natureza_juridica", "vinculo_sus", "fonte",
        ]
        out["tenant_id"] = self._tenant_id
        out["vinculo_sus"] = _map_sus(out["vinculo_sus"])
        out["fontes"] = out["fonte"].apply(lambda f: {f: True})
        out = out.where(out.notna(), None)
        return out[
            ["tenant_id", "cnes", "nome_fantasia", "tipo_unidade",
             "cnpj_mantenedora", "natureza_juridica", "vinculo_sus", "fontes"]
        ].to_dict(orient="records")
