"""ProvisionedCertsRepo: append-only audit log w/ RLS."""
import datetime as dt

import pytest
from sqlalchemy import text

from cnes_infra.auth.provisioned_certs import ProvisionedCertsRepo

_AGENT_IDS = ("agent-101", "agent-102", "agent-103")


@pytest.fixture(autouse=True)
def _limpar_linhas(pg_engine):
    with pg_engine.begin() as conn:
        conn.execute(
            text("DELETE FROM auth_provisioned_certs WHERE agent_id = ANY(:ids)"),
            {"ids": list(_AGENT_IDS)},
        )


@pytest.mark.postgres
def test_record_grava_linha_em_auth_provisioned_certs(pg_engine):
    repo = ProvisionedCertsRepo(pg_engine)
    expires = dt.datetime.now(dt.UTC) + dt.timedelta(days=90)
    repo.record(
        agent_id="agent-101",
        tenant_id="presidente-epitacio",
        subject_cn="agent-machine-101",
        ca_serial="abcdef0123456789",
        expires_at=expires,
    )
    with pg_engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT agent_id, tenant_id, subject_cn, ca_serial "
                "FROM auth_provisioned_certs WHERE agent_id = :a"
            ),
            {"a": "agent-101"},
        ).one()
    assert row.tenant_id == "presidente-epitacio"
    assert row.subject_cn == "agent-machine-101"
    assert row.ca_serial == "abcdef0123456789"


@pytest.mark.postgres
def test_record_chamado_duas_vezes_grava_duas_linhas(pg_engine):
    """Append-only: no UNIQUE constraint."""
    repo = ProvisionedCertsRepo(pg_engine)
    expires = dt.datetime.now(dt.UTC) + dt.timedelta(days=90)
    repo.record(
        agent_id="agent-102", tenant_id="t1",
        subject_cn="cn-x", ca_serial="serial-1", expires_at=expires,
    )
    repo.record(
        agent_id="agent-102", tenant_id="t1",
        subject_cn="cn-x", ca_serial="serial-2", expires_at=expires,
    )
    with pg_engine.connect() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM auth_provisioned_certs WHERE agent_id = :a"),
            {"a": "agent-102"},
        ).scalar()
    assert count == 2


@pytest.mark.postgres
def test_record_persiste_expires_at(pg_engine):
    repo = ProvisionedCertsRepo(pg_engine)
    expires = dt.datetime(2026, 7, 26, 12, 0, 0, tzinfo=dt.UTC)
    repo.record(
        agent_id="agent-103", tenant_id="t1",
        subject_cn="cn", ca_serial="s", expires_at=expires,
    )
    with pg_engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT expires_at FROM auth_provisioned_certs WHERE agent_id = :a"
            ),
            {"a": "agent-103"},
        ).one()
    assert row.expires_at == expires
