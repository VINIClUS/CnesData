"""producao_ambulatorial_repo: upsert idempotente em fato_producao_ambulatorial."""
from __future__ import annotations

import json
from typing import TYPE_CHECKING

from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

    from cnes_contracts.fatos import ProducaoAmbulatorial


def gravar(conn: Connection, p: ProducaoAmbulatorial) -> None:
    conn.execute(
        text("""
            INSERT INTO gold.fato_producao_ambulatorial (
                sk_profissional, sk_estabelecimento, sk_procedimento,
                sk_competencia, sk_cid_principal, qtd, valor_aprov_cents,
                dt_atendimento, job_id, fonte_sistema, extracao_ts,
                fontes_reportadas
            ) VALUES (
                :sk_prof, :sk_estab, :sk_proc, :sk_comp, :sk_cid,
                :qtd, :valor, :dt, :job, :fonte, :ts,
                CAST(:fontes AS jsonb)
            )
            ON CONFLICT (
                sk_competencia, sk_profissional, sk_estabelecimento,
                sk_procedimento, job_id
            ) DO UPDATE SET
                qtd = EXCLUDED.qtd,
                valor_aprov_cents = EXCLUDED.valor_aprov_cents,
                fontes_reportadas = COALESCE(
                    fato_producao_ambulatorial.fontes_reportadas,
                    '{}'::jsonb
                ) || COALESCE(EXCLUDED.fontes_reportadas, '{}'::jsonb)
        """),
        {
            "sk_prof": p.sk_profissional,
            "sk_estab": p.sk_estabelecimento,
            "sk_proc": p.sk_procedimento,
            "sk_comp": p.sk_competencia,
            "sk_cid": p.sk_cid_principal,
            "qtd": p.qtd,
            "valor": p.valor_aprov_cents,
            "dt": p.dt_atendimento,
            "job": str(p.job_id),
            "fonte": p.fonte_sistema,
            "ts": p.extracao_ts,
            "fontes": json.dumps(p.fontes_reportadas or {}),
        },
    )
