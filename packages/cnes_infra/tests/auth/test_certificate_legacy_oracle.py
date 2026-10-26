"""Oráculo sintético do repositório PostgreSQL de certificados legado."""

import datetime as dt
import json
import os
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url

from cnes_domain.tenant import set_tenant_id, tenant_id_ctx
from cnes_infra.auth.provisioned_certs import ProvisionedCertsRepo

_ORACLE_PATH = Path(__file__).resolve().parents[1] / "fixtures/auth/certificate_legacy_oracle.json"


@pytest.fixture
def legacy_oracle(request: pytest.FixtureRequest) -> Any:
    raw_url = os.environ.get("PG_TEST_URL")
    assert raw_url is not None, "PG_TEST_URL_missing"
    url = make_url(raw_url)
    assert url.drivername.startswith("postgresql"), "PG_TEST_URL_not_postgresql"
    assert url.host in {"localhost", "127.0.0.1"}, "PG_TEST_URL_not_local"
    assert url.port is not None, "PG_TEST_URL_port_missing"
    assert url.database is not None, "PG_TEST_URL_database_missing"
    assert url.database.endswith("_test"), "PG_TEST_URL_not_test"
    assert not url.query, "PG_TEST_URL_query_forbidden"
    expected = json.loads(_ORACLE_PATH.read_text(encoding="utf-8"))
    token = tenant_id_ctx.set(expected["tenant_id"])
    set_tenant_id(expected["tenant_id"])
    engine = request.getfixturevalue("pg_engine")
    try:
        yield engine, expected
    finally:
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM auth_provisioned_certs "
                     "WHERE tenant_id = :tenant AND ca_serial LIKE :prefix"),
                {"tenant": expected["tenant_id"], "prefix": "cert-oracle-%"},
            )
        tenant_id_ctx.reset(token)


def _record(repo: ProvisionedCertsRepo, record: dict[str, Any]) -> dt.datetime:
    expires_at = dt.datetime.now(dt.UTC) + dt.timedelta(days=record["expires_after_days"])
    repo.record(
        agent_id=record["agent_id"], tenant_id=record["tenant_id"],
        subject_cn=record["subject_cn"], ca_serial=record["ca_serial"],
        expires_at=expires_at,
    )
    return expires_at


@pytest.mark.postgres
def test_legado_preserva_overlap_e_rejeita_serial_invalido(legacy_oracle: Any) -> None:
    engine, expected = legacy_oracle
    repo = ProvisionedCertsRepo(engine)
    for record in expected["records"]:
        expires_at = _record(repo, record)
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT * FROM auth_provisioned_certs "
                     "WHERE tenant_id = :tenant AND ca_serial = :serial"),
                {"tenant": record["tenant_id"], "serial": record["ca_serial"]},
            ).one()
        assert set(row._mapping) == set(expected["legacy_columns"])
        for field in ("agent_id", "tenant_id", "subject_cn", "ca_serial"):
            assert row._mapping[field] == record[field]
        assert row.id > 0
        assert row.issued_at.tzinfo is not None
        assert row.expires_at == expires_at
        assert row.revoked_at is None
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE auth_provisioned_certs SET revoked_at = now() "
                 "WHERE tenant_id = :tenant AND ca_serial = :serial"),
            {"tenant": expected["tenant_id"], "serial": "cert-oracle-revoked"},
        )
        conn.execute(
            text("UPDATE auth_provisioned_certs SET expires_at = now() "
                 "WHERE tenant_id = :tenant AND ca_serial = :serial"),
            {"tenant": expected["tenant_id"], "serial": "cert-oracle-expired"},
        )
    for observation in expected["observations"]:
        assert repo.is_serial_active(
            observation["agent_id"], observation["ca_serial"]
        ) is observation["active"]


@pytest.mark.postgres
def test_legado_aceita_duas_linhas_com_mesmo_serial(legacy_oracle: Any) -> None:
    engine, expected = legacy_oracle
    repo = ProvisionedCertsRepo(engine)
    duplicate = expected["duplicate_record"]
    _record(repo, duplicate)
    _record(repo, duplicate)
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT agent_id, tenant_id, subject_cn, ca_serial "
                 "FROM auth_provisioned_certs WHERE tenant_id = :tenant AND ca_serial = :serial"),
            {"tenant": duplicate["tenant_id"], "serial": duplicate["ca_serial"]},
        ).all()
    assert len(rows) == duplicate["expected_count"]
    assert all(dict(row._mapping) == {
        field: duplicate[field] for field in ("agent_id", "tenant_id", "subject_cn", "ca_serial")
    } for row in rows)
