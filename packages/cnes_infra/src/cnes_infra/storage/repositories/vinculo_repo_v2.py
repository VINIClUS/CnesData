"""vinculo_repo_v2: write gold.fato_vinculo_cnes using surrogate keys (Gold v2)."""
from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

    from cnes_contracts.fatos import VinculoCNES


def gravar(conn: Connection, v: VinculoCNES) -> None:
    """Insere vinculo em gold.fato_vinculo_cnes.

    Gold v2 semantic: N rows per (sk_prof, sk_estab, sk_cbo, sk_competencia)
    tuple when multiple fonte_sistema values arrive — no JSONB merge.
    Consumer aggregates via SUM/MAX at query time.
    """
    conn.execute(
        text("""
            INSERT INTO gold.fato_vinculo_cnes (
                sk_profissional, sk_estabelecimento, sk_cbo, sk_competencia,
                carga_horaria_sem, ind_vinc, sk_equipe,
                job_id, fonte_sistema, extracao_ts
            ) VALUES (
                :sk_profissional, :sk_estabelecimento, :sk_cbo, :sk_competencia,
                :carga_horaria_sem, :ind_vinc, :sk_equipe,
                :job_id, :fonte_sistema, :extracao_ts
            )
        """),
        v.model_dump(mode="python"),
    )
