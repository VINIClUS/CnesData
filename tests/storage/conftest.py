"""Fixtures para testes de integração do PostgresAdapter."""
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text

from storage.postgres_adapter import PostgresAdapter

_PG_URL = "postgresql+psycopg://cnesdata:cnesdata_test@localhost:5433/cnesdata_test"
_TENANT_ID = "355030"


def _is_pg_ready() -> bool:
    try:
        engine = create_engine(_PG_URL)
        with engine.connect() as con:
            con.execute(text("SELECT 1"))
        engine.dispose()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def pg_service(docker_services):
    docker_services.wait_until_responsive(
        timeout=30.0,
        pause=0.5,
        check=_is_pg_ready,
    )


@pytest.fixture(scope="session")
def pg_engine(pg_service):
    engine = create_engine(_PG_URL)
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", _PG_URL)
    command.upgrade(cfg, "head")
    yield engine
    with engine.connect() as con:
        con.execute(text("DROP SCHEMA IF EXISTS gold CASCADE"))
        con.commit()
    engine.dispose()


@pytest.fixture()
def adapter(pg_engine) -> PostgresAdapter:
    return PostgresAdapter(pg_engine, tenant_id=_TENANT_ID)


@pytest.fixture(autouse=True)
def truncate_tables(pg_engine):
    yield
    with pg_engine.begin() as con:
        con.execute(text(
            "TRUNCATE gold.fato_vinculo, gold.dim_profissional, "
            "gold.dim_estabelecimento RESTART IDENTITY CASCADE"
        ))
