"""Backup/restore verificado do runtime local: usuários, memberships, agents, decisões."""
from __future__ import annotations

import shutil
import sqlite3
import tarfile
from datetime import UTC, datetime
from hashlib import sha256
from io import BytesIO
from pathlib import Path

import pytest

from central_api.composition import build_local_runtime
from central_api.local_backup import create_backup, restore_backup
from cnes_domain.control_plane.entities import AccessRequest, Agent, Membership, OutboxEvent
from cnes_domain.control_plane.enums import AccessRequestState, AgentState
from cnes_domain.profiles import local_state_db, parse_profile
from cnes_infra.audit.local_sink import LocalAuditSink
from cnes_infra.auth.local_credentials import LocalCredentialStore, build_user

pytestmark = [pytest.mark.chaos, pytest.mark.local_profile]

_TENANT = "354130"
_NOW = datetime(2026, 1, 15, 12, tzinfo=UTC)
_PASSWORD = "correct-horse-battery"  # noqa: S105
_FINGERPRINT = "a" * 64


def _utc_now() -> datetime:
    return _NOW


def _build_runtime(data_dir: Path):
    settings = parse_profile({"TENANT_ID": _TENANT, "DATA_DIR": str(data_dir)})
    return build_local_runtime(settings, _utc_now)


def _seed_credentials(data_dir: Path) -> tuple[str, str]:
    credentials = LocalCredentialStore(local_state_db(data_dir))
    credentials.initialize()
    user_id, email = "user-1", "gestor@epitacio.sp.gov.br"
    record = build_user(user_id, email, _PASSWORD, _NOW)
    credentials.put_user(record)
    return user_id, email


def _seed_membership(control_plane, user_id: str) -> Membership:
    membership = Membership(
        tenant_id=_TENANT, user_id=user_id, role="gestor", created_at=_NOW, oidc_issuer=None,
    )
    control_plane.put_membership(membership)
    return membership


def _seed_agent(control_plane) -> Agent:
    agent = Agent(
        tenant_id=_TENANT, agent_id="agent-a", state=AgentState.ACTIVE, version="1.0.0",
        certificate_fingerprint=_FINGERPRINT, last_seen_at=_NOW, created_at=_NOW,
    )
    control_plane.put_agent(agent)
    return agent


def _seed_decided_access_request(control_plane) -> AccessRequest:
    pending = AccessRequest(
        tenant_id=_TENANT, request_id="request-a", user_id="user-2",
        state=AccessRequestState.PENDING, decided_by=None, decided_at=None,
    )
    control_plane.put_access_request(pending, _event("access-requested"))
    approved = pending.model_copy(update={
        "state": AccessRequestState.APPROVED, "decided_by": "admin-a", "decided_at": _NOW,
    })
    control_plane.decide_access_request(approved, _event("access-approved"))
    return approved


def _event(event_id: str) -> OutboxEvent:
    return OutboxEvent(
        tenant_id=_TENANT, event_id=event_id, event_type=event_id, aggregate_id="request-a",
        payload={}, created_at=_NOW, delivered_at=None,
    )


def _seed_serving_object(object_store) -> tuple[str, bytes]:
    key = f"serving/{_TENANT}/run-1/overview.json"
    body = b'{"schema_version": "cnes-serving-v1"}'
    object_store.put(key, BytesIO(body), sha256(body).hexdigest())
    return key, body


def test_backup_restaura_usuarios_memberships_agents_e_access_decisions(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    runtime = _build_runtime(data_dir)
    user_id, email = _seed_credentials(data_dir)
    membership = _seed_membership(runtime.control_plane, user_id)
    agent = _seed_agent(runtime.control_plane)
    decided = _seed_decided_access_request(runtime.control_plane)
    object_key, object_body = _seed_serving_object(runtime.object_store)
    target = tmp_path / "backup.tar"

    create_backup(data_dir, target, _NOW)
    shutil.rmtree(data_dir)
    assert not data_dir.exists()

    restored_data_dir = tmp_path / "restored"
    restored_state_db = local_state_db(restored_data_dir)
    restore_backup(target, restored_data_dir, _TENANT)

    restored_settings = parse_profile({"TENANT_ID": _TENANT, "DATA_DIR": str(restored_data_dir)})
    restored_runtime = build_local_runtime(restored_settings, _utc_now)
    restored_credentials = LocalCredentialStore(restored_state_db)

    restored_user = restored_credentials.find_user_by_email(email)
    assert restored_user is not None
    assert restored_user.user_id == user_id
    from cnes_infra.auth.local_credentials import hash_password
    assert hash_password(_PASSWORD, restored_user.salt) == restored_user.password_hash

    assert restored_runtime.control_plane.get_membership(_TENANT, user_id) == membership
    assert restored_runtime.control_plane.get_agent(_TENANT, "agent-a") == agent
    restored_request = restored_runtime.control_plane.get_access_request(_TENANT, "request-a")
    assert restored_request == decided
    assert restored_request.decided_by == "admin-a"

    with restored_runtime.object_store.open(object_key) as stream:
        assert stream.read() == object_body


def _audit_index_event_ids(data_dir: Path) -> set[str]:
    database = sqlite3.connect(data_dir / "audit" / "index.sqlite3")
    try:
        rows = database.execute("SELECT event_id FROM events").fetchall()
    finally:
        database.close()
    return {row[0] for row in rows}


def test_restore_reconstroi_indice_de_auditoria_a_partir_do_jsonl(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    runtime = _build_runtime(data_dir)
    event = OutboxEvent(
        tenant_id=_TENANT, event_id="event-audit-1", event_type="job.created",
        aggregate_id="job-1", payload={}, created_at=_NOW, delivered_at=None,
    )
    runtime.audit_sink.append(event)
    original_event_ids = _audit_index_event_ids(data_dir)
    assert original_event_ids == {"event-audit-1"}
    target = tmp_path / "backup.tar"
    create_backup(data_dir, target, _NOW)

    with tarfile.open(target, "r") as tar:
        names = tar.getnames()
    assert "audit/index.sqlite3" not in names
    assert ".sink.lock" not in {Path(name).name for name in names}
    assert any(name.endswith("events.jsonl") for name in names)

    shutil.rmtree(data_dir)
    restored_data_dir = tmp_path / "restored"
    restore_backup(target, restored_data_dir, _TENANT)

    assert not (restored_data_dir / "audit" / "index.sqlite3").exists()
    LocalAuditSink(restored_data_dir)
    assert _audit_index_event_ids(restored_data_dir) == original_event_ids
