"""Crash entre o CAS do ponteiro e o sink de auditoria reentrega sem republicar."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from io import BytesIO
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest

from cnes_contracts.manifests.outputs import OutputManifest
from cnes_domain.control_plane.entities import ManifestRef, OutboxEvent, Run, RunDependency, RunUnit
from cnes_domain.control_plane.enums import RunStage, RunState, RunUnitState
from cnes_domain.outbox_dispatcher import DispatchResult, dispatch_once
from cnes_infra.audit.local_sink import LocalAuditSink
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from cnes_infra.object_store.filesystem import FilesystemObjectStore
from data_processor.orchestration.attempt_store import attempt_object_key, unit_attempt_prefix
from data_processor.orchestration.publisher import DatasetPublisher, PublishRequest

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = [pytest.mark.chaos]

_TENANT = "354130"
_COMPETENCIA = "2026-01"
_NOW = datetime(2026, 1, 15, 12, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class _Publicacao:
    run: Run
    units: tuple[RunUnit, ...]


@dataclass
class _SinkIndisponivel:
    delegate: LocalAuditSink
    falhas: int = field(default=0)
    disponivel: bool = field(default=False)

    def append(self, event: OutboxEvent) -> None:
        if not self.disponivel:
            self.falhas += 1
            raise OSError("audit_sink=indisponivel")
        self.delegate.append(event)


class _SinkComFalhaUnica(LocalAuditSink):
    def __init__(self, root: Path) -> None:
        self._disparado = False
        super().__init__(root)

    def _fault(self, boundary: str) -> None:
        if boundary == "after_file_write" and not self._disparado:
            self._disparado = True
            raise OSError("audit_sink=falha_apos_escrita")


def _control_plane(root: Path) -> SQLiteControlPlane:
    adapter = SQLiteControlPlane(root / "cp.sqlite3", lambda: _NOW)
    adapter.initialize()
    return adapter


def _chave_run_manifest(run_id: str) -> str:
    return f"reconciliation/{_TENANT}/{_COMPETENCIA}/{run_id}/run-manifest.json"


def _semear(store: FilesystemObjectStore, run_id: str) -> _Publicacao:
    run = Run(
        tenant_id=_TENANT, run_id=run_id, competencia=_COMPETENCIA, dataset_name="gold",
        state=RunState.PUBLISHING,
        dependencies=(RunDependency(source_type="CNES", file_subtype="ST", required=True),),
        missing_sources=(), created_at=_NOW,
    )
    body = f"payload-{run_id}".encode()
    digest = hashlib.sha256(body).hexdigest()
    object_key = f"reconciliation/{_TENANT}/{_COMPETENCIA}/{run_id}/part-a.parquet"
    manifest = OutputManifest(
        manifest_version=1, manifest_id=f"manifest-{run_id}", tenant_id=_TENANT,
        layer="reconciliation", source_type=None, competencia=_COMPETENCIA,
        run_id=run_id, unit_id="unit-m", attempt=1,
        schema_version="gold-v1", object_key=object_key, object_sha256=digest,
        row_count=10, created_at=_NOW,
    )
    prefix = unit_attempt_prefix(
        SimpleNamespace(tenant_id=_TENANT, run_id=run_id, unit_id="unit-m", attempt=1)
    )
    source_key = attempt_object_key(prefix, object_key)
    store.put(source_key, BytesIO(body), digest)
    sidecar_key = attempt_object_key(prefix, f"manifests/{manifest.manifest_id}/manifest.json")
    payload = manifest.model_dump_json(exclude_none=False, by_alias=False).encode()
    store.put(sidecar_key, BytesIO(payload), hashlib.sha256(payload).hexdigest())
    ref = ManifestRef(manifest_id=manifest.manifest_id, manifest_key=sidecar_key)
    unit = RunUnit(
        tenant_id=_TENANT, run_id=run_id, unit_id="unit-m", stage=RunStage.MATERIALIZE,
        source_type=None, file_subtype=None, partition="all",
        depends_on_unit_ids=("unit-upstream",), input_manifests=(),
        state=RunUnitState.SUCCEEDED, attempt=1, fencing_token=1, lease_owner=None,
        lease_until=None, dispatch_id=None, output_manifests=(ref,), error_code=None,
    )
    return _Publicacao(run=run, units=(unit,))


def _publicar(store: FilesystemObjectStore, adapter: SQLiteControlPlane, run_id: str):
    publicacao = _semear(store, run_id)
    adapter.put_run(publicacao.run)
    publisher = DatasetPublisher(store=store, control_plane=adapter)
    request = PublishRequest(
        run=publicacao.run, units=publicacao.units, expected_version_id=None, now=_NOW
    )
    return publisher.publish(request)


def test_crash_apos_pointer_antes_do_sink_reenvia_sem_republicar(tmp_path: Path) -> None:
    store = FilesystemObjectStore(tmp_path / "objects")
    database_path = tmp_path / "cp.sqlite3"
    audit_root = tmp_path / "audit-root"
    adapter = SQLiteControlPlane(database_path, lambda: _NOW)
    adapter.initialize()
    run_id = "run-audit"
    resultado = _publicar(store, adapter, run_id)
    manifest_stat_antes = store.stat(_chave_run_manifest(run_id))

    indisponivel = _SinkIndisponivel(delegate=LocalAuditSink(audit_root))
    primeiro = dispatch_once(adapter, indisponivel, _NOW)
    assert primeiro == DispatchResult(delivered=0, failed=1)
    assert indisponivel.falhas == 1

    reopened_adapter = SQLiteControlPlane(database_path, lambda: _NOW)
    reopened_adapter.initialize()
    segundo = dispatch_once(reopened_adapter, LocalAuditSink(audit_root), _NOW)
    terceiro = dispatch_once(reopened_adapter, LocalAuditSink(audit_root), _NOW)

    assert segundo == DispatchResult(delivered=1, failed=0)
    assert terceiro == DispatchResult(delivered=0, failed=0)
    assert reopened_adapter.pending_outbox(10) == ()

    log_path = audit_root / "audit" / _TENANT / "2026" / "01" / "15" / "events.jsonl"
    linhas = log_path.read_text(encoding="utf-8").splitlines()
    assert len(linhas) == 1
    assert json.loads(linhas[0])["event_id"] == f"reconciliation.published:{_TENANT}:{run_id}"
    assert reopened_adapter.get_dataset_pointer(_TENANT, "gold") == resultado.pointer
    assert store.stat(_chave_run_manifest(run_id)) == manifest_stat_antes


def test_log_parcial_do_sink_nao_duplica_evento_apos_reabertura(tmp_path: Path) -> None:
    store = FilesystemObjectStore(tmp_path / "objects")
    database_path = tmp_path / "cp.sqlite3"
    audit_root = tmp_path / "audit-root"
    adapter = SQLiteControlPlane(database_path, lambda: _NOW)
    adapter.initialize()
    run_id = "run-audit-torn"
    _publicar(store, adapter, run_id)

    sink_com_falha = _SinkComFalhaUnica(audit_root)
    primeira = dispatch_once(adapter, sink_com_falha, _NOW)
    assert primeira == DispatchResult(delivered=0, failed=1)

    reopened_adapter = SQLiteControlPlane(database_path, lambda: _NOW)
    reopened_adapter.initialize()
    segunda = dispatch_once(reopened_adapter, LocalAuditSink(audit_root), _NOW)

    assert segunda == DispatchResult(delivered=1, failed=0)
    assert reopened_adapter.pending_outbox(10) == ()
    log_path = audit_root / "audit" / _TENANT / "2026" / "01" / "15" / "events.jsonl"
    assert len(log_path.read_text(encoding="utf-8").splitlines()) == 1
