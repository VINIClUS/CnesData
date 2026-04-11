"""Seed script para banco de integração. Invocado pelo serviço pg-seed do docker-compose."""
import logging
import os
import random

from sqlalchemy import create_engine, insert

from storage.schema import dim_estabelecimento, dim_profissional, fato_vinculo, gold_metadata

_URL = os.environ.get(
    "PG_TEST_URL",
    "postgresql+psycopg://cnesdata:cnesdata_test@localhost:5433/cnesdata_test",
)
_TENANT_ID = "355030"

random.seed(42)


def _pad(n: int, length: int) -> str:
    return str(n).zfill(length)


def seed(engine) -> None:
    gold_metadata.create_all(engine)

    estabs = [
        {
            "tenant_id": _TENANT_ID, "cnes": _pad(i, 7),
            "nome_fantasia": f"UBS {i}", "tipo_unidade": "01",
            "vinculo_sus": True, "fontes": {"LOCAL": True},
        }
        for i in [1234567, 2345678, 3456789]
    ]

    profs = [
        {
            "tenant_id": _TENANT_ID, "cpf": _pad(i, 11),
            "nome_profissional": f"Profissional {i}", "fontes": {"LOCAL": True},
        }
        for i in [11111111111, 22222222222, 33333333333, 44444444444, 55555555555]
    ]

    seen: set = set()
    vinculos = []
    for comp in ["2026-01", "2026-02"]:
        for p in profs[:4]:
            e = estabs[random.randint(0, 2)]
            key = (_TENANT_ID, comp, p["cpf"], e["cnes"], "515105")
            if key in seen:
                continue
            seen.add(key)
            vinculos.append({
                "tenant_id": _TENANT_ID, "competencia": comp,
                "cpf": p["cpf"], "cnes": e["cnes"], "cbo": "515105",
                "sus": True, "ch_total": random.randint(20, 40),
                "ch_ambulatorial": 20, "ch_outras": 0, "ch_hospitalar": 0,
                "fontes": {"LOCAL": True},
            })

    with engine.begin() as con:
        con.execute(insert(dim_estabelecimento), estabs)
        con.execute(insert(dim_profissional), profs)
        con.execute(insert(fato_vinculo), vinculos)

    logging.getLogger(__name__).info(
        "seed estabs=%d profs=%d vinculos=%d", len(estabs), len(profs), len(vinculos)
    )


if __name__ == "__main__":
    engine = create_engine(_URL)
    seed(engine)
    engine.dispose()
