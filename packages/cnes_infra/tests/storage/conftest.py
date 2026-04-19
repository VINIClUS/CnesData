"""Fixtures para testes de integração dos repositórios."""
import pytest
from sqlalchemy import text

from cnes_domain.tenant import set_tenant_id
from cnes_infra.storage.repositories import PostgresUnitOfWork

_TENANT_ID = "355030"


@pytest.fixture
def uow(pg_engine) -> PostgresUnitOfWork:
    set_tenant_id(_TENANT_ID)
    yield PostgresUnitOfWork(pg_engine)
    with pg_engine.begin() as con:
        con.execute(text(
            "TRUNCATE gold.fato_vinculo, gold.dim_profissional, "
            "gold.dim_estabelecimento RESTART IDENTITY CASCADE"
        ))
