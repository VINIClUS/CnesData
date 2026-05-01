"""ProvisionedCertsRepo: append-only audit log w/ RLS."""
import datetime as dt

import pytest
from sqlalchemy import text

from cnes_infra.auth.provisioned_certs import ProvisionedCertsRepo

_AGENT_IDS = (
    "agent-101", "agent-102", "agent-103",
    "agent-201-sem-linhas", "agent-202", "agent-203", "agent-204",
    "agent-205-expirado",
)


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


@pytest.mark.postgres
def test_find_active_retorna_none_quando_agent_sem_linhas(pg_engine):
    repo = ProvisionedCertsRepo(pg_engine)
    assert repo.find_active_by_agent_id("agent-201-sem-linhas") is None


@pytest.mark.postgres
def test_find_active_retorna_mais_recente_quando_multiplas_linhas(pg_engine):
    repo = ProvisionedCertsRepo(pg_engine)
    expires = dt.datetime.now(dt.UTC) + dt.timedelta(days=90)
    repo.record(
        agent_id="agent-202", tenant_id="t1",
        subject_cn="cn-old", ca_serial="serial-old", expires_at=expires,
    )
    repo.record(
        agent_id="agent-202", tenant_id="t1",
        subject_cn="cn-new", ca_serial="serial-new", expires_at=expires,
    )
    row = repo.find_active_by_agent_id("agent-202")
    assert row is not None
    assert row.ca_serial == "serial-new"


@pytest.mark.postgres
def test_find_active_ignora_linhas_revogadas(pg_engine):
    repo = ProvisionedCertsRepo(pg_engine)
    expires = dt.datetime.now(dt.UTC) + dt.timedelta(days=90)
    repo.record(
        agent_id="agent-203", tenant_id="t1",
        subject_cn="cn-rev", ca_serial="serial-rev", expires_at=expires,
    )
    with pg_engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE auth_provisioned_certs SET revoked_at = now() "
                "WHERE agent_id = 'agent-203'",
            ),
        )
    repo.record(
        agent_id="agent-203", tenant_id="t1",
        subject_cn="cn-active", ca_serial="serial-active", expires_at=expires,
    )
    row = repo.find_active_by_agent_id("agent-203")
    assert row is not None
    assert row.ca_serial == "serial-active"


@pytest.mark.postgres
def test_find_active_retorna_none_quando_todas_revogadas(pg_engine):
    repo = ProvisionedCertsRepo(pg_engine)
    expires = dt.datetime.now(dt.UTC) + dt.timedelta(days=90)
    repo.record(
        agent_id="agent-204", tenant_id="t1",
        subject_cn="cn", ca_serial="serial-only", expires_at=expires,
    )
    with pg_engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE auth_provisioned_certs SET revoked_at = now() "
                "WHERE agent_id = 'agent-204'",
            ),
        )
    assert repo.find_active_by_agent_id("agent-204") is None


@pytest.mark.postgres
def test_find_active_ignora_linhas_expiradas(pg_engine):
    """Critical regression: cert past expires_at MUST NOT be returned."""
    repo = ProvisionedCertsRepo(pg_engine)
    expired = dt.datetime.now(dt.UTC) - dt.timedelta(days=1)
    repo.record(
        agent_id="agent-205-expirado", tenant_id="t1",
        subject_cn="cn-old", ca_serial="serial-expired", expires_at=expired,
    )
    assert repo.find_active_by_agent_id("agent-205-expirado") is None
