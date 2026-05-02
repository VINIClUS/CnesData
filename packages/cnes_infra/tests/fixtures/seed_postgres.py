"""Seed script para banco de integração. Invocado pelo serviço pg-seed do docker-compose.

Schema v2 (dimensional). Insere quantidade mínima de registros gold para que
testes/queries downstream tenham dados consistentes. Não cobre fatos
ambulatoriais/AIH — apenas dim_* + fato_vinculo_cnes.
"""
import logging
import os
import uuid
from datetime import datetime, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.dialects.postgresql import insert

from cnes_infra.storage.schema_v2 import (
    dim_cbo,
    dim_competencia,
    dim_estabelecimento,
    dim_municipio,
    dim_profissional,
    fato_vinculo_cnes,
    metadata,
)

_URL = os.environ.get(
    "PG_TEST_URL",
    "postgresql+psycopg://cnesdata:cnesdata_test@localhost:5433/cnesdata_test",
)


def _pad(n: int, length: int) -> str:
    return str(n).zfill(length)


def _lookup(con, table, key_col, key_val, sk_col):
    return con.execute(
        select(table.c[sk_col]).where(table.c[key_col] == key_val),
    ).scalar_one()


def seed(engine) -> None:
    metadata.create_all(engine)
    job_id = uuid.uuid4()
    extracao_ts = datetime.now(timezone.utc)

    with engine.begin() as con:
        con.execute(insert(dim_municipio).values([
            {"ibge6": "354130", "ibge7": "3541308",
             "nome": "Presidente Epitacio", "uf": "SP"},
        ]).on_conflict_do_nothing(index_elements=["ibge6"]))
        con.execute(insert(dim_cbo).values([
            {"cod_cbo": "515105", "descricao": "Agente comunitario de saude"},
            {"cod_cbo": "223505", "descricao": "Medico clinico"},
        ]).on_conflict_do_nothing(index_elements=["cod_cbo"]))
        con.execute(insert(dim_competencia).values([
            {"competencia": 202601, "ano": 2026, "mes": 1},
            {"competencia": 202602, "ano": 2026, "mes": 2},
        ]).on_conflict_do_nothing(index_elements=["competencia"]))

        sk_mun = _lookup(con, dim_municipio, "ibge6", "354130", "sk_municipio")

        con.execute(insert(dim_estabelecimento).values([
            {"cnes": _pad(i, 7), "nome": f"UBS {i}",
             "tp_unid": 1, "sk_municipio": sk_mun,
             "fontes": {"LOCAL": True}}
            for i in [1234567, 2345678, 3456789]
        ]).on_conflict_do_nothing(index_elements=["cnes"]))
        con.execute(insert(dim_profissional).values([
            {"cpf_hash": _pad(i, 11), "nome": f"Profissional {i}",
             "fontes": {"LOCAL": True}}
            for i in [11111111111, 22222222222, 33333333333]
        ]).on_conflict_do_nothing(index_elements=["cpf_hash"]))

        sk_cbo = _lookup(con, dim_cbo, "cod_cbo", "515105", "sk_cbo")
        sk_comp = _lookup(con, dim_competencia, "competencia", 202601, "sk_competencia")
        sk_estab = _lookup(con, dim_estabelecimento, "cnes", _pad(1234567, 7), "sk_estabelecimento")
        sk_profs = [
            _lookup(con, dim_profissional, "cpf_hash", _pad(c, 11), "sk_profissional")
            for c in [11111111111, 22222222222, 33333333333]
        ]
        already_seeded = con.execute(
            select(fato_vinculo_cnes.c.sk_vinculo)
            .where(fato_vinculo_cnes.c.fonte_sistema == "CNES_LOCAL")
            .where(fato_vinculo_cnes.c.sk_competencia == sk_comp)
            .limit(1)
        ).first()
        if already_seeded is None:
            con.execute(insert(fato_vinculo_cnes).values([
                {"sk_profissional": sk_p, "sk_estabelecimento": sk_estab,
                 "sk_cbo": sk_cbo, "sk_competencia": sk_comp,
                 "carga_horaria_sem": 40, "ind_vinc": "ESTAT ",
                 "job_id": job_id, "fonte_sistema": "CNES_LOCAL",
                 "extracao_ts": extracao_ts}
                for sk_p in sk_profs
            ]))

    logging.getLogger(__name__).info(
        "seed dims=municipio+cbo+competencia+estabelecimento+profissional "
        "fato_vinculo_cnes=%d", len(sk_profs),
    )


if __name__ == "__main__":
    engine = create_engine(_URL)
    seed(engine)
    engine.dispose()
