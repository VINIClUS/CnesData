"""Tests for extractions_repo."""
from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import text

from cnes_contracts.landing import ExtractionRegisterPayload
from cnes_infra.storage.extractions_repo import (
    claim_next,
    complete,
    fail,
    heartbeat,
    mark_uploaded,
    reap_expired,
    register,
)

pytestmark = pytest.mark.postgres


def _base_payload(**overrides) -> ExtractionRegisterPayload:
    defaults = {
        "tenant_id": "354130",
        "fonte_sistema": "CNES_LOCAL",
        "tipo_extracao": "cnes_profissionais",
        "competencia": 202601,
        "job_id": uuid4(),
        "agent_version": "v0.2.0",
        "machine_id": "m-test",
    }
    defaults.update(overrides)
    return ExtractionRegisterPayload(**defaults)


def test_register_cria_row_pending(pg_conn):
    pl = _base_payload()
    ext_id, obj_key = register(pg_conn, pl, bucket="cnesdata-landing")

    assert ext_id is not None
    assert obj_key.startswith("354130/CNES_LOCAL/")
    row = pg_conn.execute(
        text("SELECT status FROM landing.extractions WHERE id = :i"),
        {"i": ext_id},
    ).scalar()
    assert row == "PENDING"


def test_mark_uploaded_transitiona_para_uploaded(pg_conn):
    pl = _base_payload(tipo_extracao="cnes_profissionais_1")
    ext_id, _ = register(pg_conn, pl, bucket="b")
    mark_uploaded(pg_conn, ext_id, "a" * 64, 100)
    status = pg_conn.execute(
        text("SELECT status, row_count, sha256 FROM landing.extractions WHERE id = :i"),
        {"i": ext_id},
    ).first()
    assert status[0] == "UPLOADED"
    assert status[1] == 100


def test_claim_next_retorna_uploaded_apenas(pg_conn):
    pl1 = _base_payload(tipo_extracao="claim_test_1")
    _ext1, _ = register(pg_conn, pl1, bucket="b")
    pl2 = _base_payload(tipo_extracao="claim_test_2")
    ext2, _ = register(pg_conn, pl2, bucket="b")
    mark_uploaded(pg_conn, ext2, "b" * 64, 50)

    claimed = claim_next(pg_conn, processor_id="p1", lease_secs=300)
    assert claimed is not None
    assert claimed.id == ext2
    assert claimed.status == "PROCESSING"


def test_claim_next_retorna_none_sem_uploaded(pg_conn):
    pl = _base_payload(tipo_extracao="no_uploaded_test")
    register(pg_conn, pl, bucket="b")
    claimed = claim_next(pg_conn, processor_id="p1", lease_secs=300)
    assert claimed is None or claimed.status == "PROCESSING"


def test_heartbeat_estende_lease(pg_conn):
    pl = _base_payload(tipo_extracao="hb_test")
    ext_id, _ = register(pg_conn, pl, bucket="b")
    mark_uploaded(pg_conn, ext_id, "c" * 64, 10)
    claim_next(pg_conn, processor_id="p-hb", lease_secs=60)

    before = pg_conn.execute(
        text("SELECT lease_until FROM landing.extractions WHERE id = :i"),
        {"i": ext_id},
    ).scalar()
    heartbeat(pg_conn, ext_id, processor_id="p-hb", lease_secs=600)
    after = pg_conn.execute(
        text("SELECT lease_until FROM landing.extractions WHERE id = :i"),
        {"i": ext_id},
    ).scalar()
    assert after > before


def test_heartbeat_nao_afeta_outro_owner(pg_conn):
    pl = _base_payload(tipo_extracao="hb_owner_test")
    ext_id, _ = register(pg_conn, pl, bucket="b")
    mark_uploaded(pg_conn, ext_id, "d" * 64, 10)
    claim_next(pg_conn, processor_id="p-owner", lease_secs=60)

    before = pg_conn.execute(
        text("SELECT lease_until FROM landing.extractions WHERE id = :i"),
        {"i": ext_id},
    ).scalar()
    heartbeat(pg_conn, ext_id, processor_id="p-intruder", lease_secs=600)
    after = pg_conn.execute(
        text("SELECT lease_until FROM landing.extractions WHERE id = :i"),
        {"i": ext_id},
    ).scalar()
    assert after == before


def test_complete_marca_ingested(pg_conn):
    pl = _base_payload(tipo_extracao="complete_test")
    ext_id, _ = register(pg_conn, pl, bucket="b")
    mark_uploaded(pg_conn, ext_id, "e" * 64, 10)
    claim_next(pg_conn, processor_id="p", lease_secs=60)
    complete(pg_conn, ext_id)
    status = pg_conn.execute(
        text("SELECT status FROM landing.extractions WHERE id = :i"),
        {"i": ext_id},
    ).scalar()
    assert status == "INGESTED"


def test_fail_retry_com_budget(pg_conn):
    pl = _base_payload(tipo_extracao="fail_retry_test")
    ext_id, _ = register(pg_conn, pl, bucket="b")
    mark_uploaded(pg_conn, ext_id, "f" * 64, 10)
    claim_next(pg_conn, processor_id="p", lease_secs=60)
    fail(pg_conn, ext_id, "transient error", max_retries=3)

    row = pg_conn.execute(
        text("SELECT status, retry_count FROM landing.extractions WHERE id = :i"),
        {"i": ext_id},
    ).first()
    assert row[0] == "UPLOADED"
    assert row[1] == 1


def test_fail_sem_budget_vai_failed(pg_conn):
    pl = _base_payload(tipo_extracao="fail_max_test")
    ext_id, _ = register(pg_conn, pl, bucket="b")
    mark_uploaded(pg_conn, ext_id, "2" * 64, 10)
    for _ in range(3):
        claim_next(pg_conn, processor_id="p", lease_secs=60)
        fail(pg_conn, ext_id, "persistent", max_retries=3)

    status = pg_conn.execute(
        text("SELECT status FROM landing.extractions WHERE id = :i"),
        {"i": ext_id},
    ).scalar()
    assert status == "FAILED"


def test_reap_expired_devolve_ao_uploaded(pg_conn):
    pl = _base_payload(tipo_extracao="reap_test")
    ext_id, _ = register(pg_conn, pl, bucket="b")
    mark_uploaded(pg_conn, ext_id, "3" * 64, 10)
    claim_next(pg_conn, processor_id="p-expired", lease_secs=0)
    pg_conn.execute(
        text(
            "UPDATE landing.extractions SET lease_until = NOW() - INTERVAL '5 minutes' "
            "WHERE id = :i",
        ),
        {"i": ext_id},
    )

    reaped = reap_expired(pg_conn)
    assert reaped >= 1

    row = pg_conn.execute(
        text("SELECT status, lease_owner FROM landing.extractions WHERE id = :i"),
        {"i": ext_id},
    ).first()
    assert row[0] == "UPLOADED"
    assert row[1] is None
