"""PostgresAdapter — implementação de StoragePort para PostgreSQL via SQLAlchemy Core."""
import logging
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine

from storage.schema import dim_estabelecimento, dim_profissional, fato_vinculo

logger = logging.getLogger(__name__)

_SUS_MAP: dict[Any, Any] = {"S": True, "N": False}


def _sus(value: Any) -> bool | None:
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return None
    return _SUS_MAP.get(value)


def _clean_none(value: Any) -> Any:
    if isinstance(value, float) and pd.isna(value):
        return None
    return value


class PostgresAdapter:
    """Persiste dados do pipeline CNES no schema gold do PostgreSQL."""

    def __init__(self, engine: Engine, tenant_id: str) -> None:
        self._engine = engine
        self._tenant_id = tenant_id

    def gravar_profissionais(self, competencia: str, df: pd.DataFrame) -> None:
        prof_rows = self._build_profissional_rows(df)
        vinculo_rows = self._build_vinculo_rows(competencia, df)
        with self._engine.begin() as con:
            con.execute(
                insert(dim_profissional)
                .values(prof_rows)
                .on_conflict_do_update(
                    index_elements=["tenant_id", "cpf"],
                    set_={
                        "fontes": text("dim_profissional.fontes || EXCLUDED.fontes"),
                        "atualizado_em": text("NOW()"),
                    },
                ),
                prof_rows,
            )
            con.execute(
                insert(fato_vinculo)
                .values(vinculo_rows)
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
                        # WHERE guard omitido: SQLAlchemy Core não suporta WHERE em
                        # on_conflict_do_update; atualiza sempre por correção.
                    },
                ),
                vinculo_rows,
            )

    def gravar_estabelecimentos(self, competencia: str, df: pd.DataFrame) -> None:
        rows = self._build_estabelecimento_rows(df)
        with self._engine.begin() as con:
            con.execute(
                insert(dim_estabelecimento)
                .values(rows)
                .on_conflict_do_update(
                    index_elements=["tenant_id", "cnes"],
                    set_={
                        "fontes": text("dim_estabelecimento.fontes || EXCLUDED.fontes"),
                        "atualizado_em": text("NOW()"),
                    },
                ),
                rows,
            )

    def registrar_pipeline_run(self, competencia: str, estado: dict) -> None:
        logger.debug("pipeline_run competencia=%s", competencia)

    def _build_profissional_rows(self, df: pd.DataFrame) -> list[dict]:
        seen: set[str] = set()
        rows: list[dict] = []
        for row in df.itertuples(index=False):
            cpf = row.CPF
            if cpf in seen:
                continue
            seen.add(cpf)
            rows.append(
                {
                    "tenant_id": self._tenant_id,
                    "cpf": cpf,
                    "cns": _clean_none(row.CNS),
                    "nome_profissional": _clean_none(row.NOME_PROFISSIONAL),
                    "sexo": _clean_none(row.SEXO),
                    "fontes": {row.FONTE: True},
                }
            )
        return rows

    def _build_vinculo_rows(self, competencia: str, df: pd.DataFrame) -> list[dict]:
        rows: list[dict] = []
        for row in df.itertuples(index=False):
            rows.append(
                {
                    "tenant_id": self._tenant_id,
                    "competencia": competencia,
                    "cpf": row.CPF,
                    "cnes": row.CNES,
                    "cbo": row.CBO,
                    "tipo_vinculo": _clean_none(row.TIPO_VINCULO),
                    "sus": _sus(row.SUS),
                    "ch_total": _clean_none(row.CH_TOTAL),
                    "ch_ambulatorial": _clean_none(row.CH_AMBULATORIAL),
                    "ch_outras": _clean_none(row.CH_OUTRAS),
                    "ch_hospitalar": _clean_none(row.CH_HOSPITALAR),
                    "fontes": {row.FONTE: True},
                }
            )
        return rows

    def _build_estabelecimento_rows(self, df: pd.DataFrame) -> list[dict]:
        rows: list[dict] = []
        for row in df.itertuples(index=False):
            rows.append(
                {
                    "tenant_id": self._tenant_id,
                    "cnes": row.CNES,
                    "nome_fantasia": _clean_none(row.NOME_FANTASIA),
                    "tipo_unidade": _clean_none(row.TIPO_UNIDADE),
                    "cnpj_mantenedora": _clean_none(row.CNPJ_MANTENEDORA),
                    "natureza_juridica": _clean_none(row.NATUREZA_JURIDICA),
                    "vinculo_sus": _sus(row.VINCULO_SUS),
                    "fontes": {row.FONTE: True},
                }
            )
        return rows
