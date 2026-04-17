"""Fixtures para testes de integração dos repositórios."""
import os

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text

from cnes_domain.tenant import set_tenant_id
from cnes_infra.storage.repositories import PostgresUnitOfWork

_PG_URL = os.getenv(
    "PG_TEST_URL",
    "postgresql+psycopg://cnesdata:cnesdata_test@localhost:5433/cnesdata_test",
)
_TENANT_ID = "355030"


@pytest.fixture(scope="session")
def pg_engine():
    engine = create_engine(_PG_URL)
    try:
        with engine.connect() as con:
            con.execute(text("SELECT 1"))
    except Exception:
        pytest.skip(f"postgres indisponível em {_PG_URL}; rode 'docker compose up -d' primeiro")
    cfg = Config()
    cfg.set_main_option("script_location", "cnes_infra:alembic")
    cfg.set_main_option("sqlalchemy.url", _PG_URL)
    command.upgrade(cfg, "head")
    yield engine
    engine.dispose()


@pytest.fixture
def uow(pg_engine) -> PostgresUnitOfWork:
    set_tenant_id(_TENANT_ID)
    yield PostgresUnitOfWork(pg_engine)
    with pg_engine.begin() as con:
        con.execute(text(
            "TRUNCATE gold.fato_vinculo, gold.dim_profissional, "
            "gold.dim_estabelecimento RESTART IDENTITY CASCADE"
        ))
