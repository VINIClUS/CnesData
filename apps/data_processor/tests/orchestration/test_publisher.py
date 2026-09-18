"""TDD do DatasetPublisher: promoção atômica + CAS do DatasetPointer."""
from __future__ import annotations

import hashlib
import json
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import UTC, datetime
from io import BytesIO
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest

from cnes_contracts.manifests.outputs import OutputManifest
from cnes_domain.control_plane.commands import PublicationPermit
from cnes_domain.control_plane.entities import ManifestRef, Run, RunDependency, RunUnit
from cnes_domain.control_plane.enums import RunStage, RunState, RunUnitState
from cnes_domain.control_plane.errors import Conflict, ControlPlaneErrorCode
from cnes_domain.ports.object_store import ObjectStat
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from data_processor.orchestration.attempt_store import attempt_object_key, unit_attempt_prefix
from data_processor.orchestration.publisher import DatasetPublisher, PublishRequest

if TYPE_CHECKING:
    from collections.abc import BinaryIO
    from contextlib import AbstractContextManager as ContextManager

_TENANT = "354130"
_RUN_ID = "run-a"
_RUN_ID_B = "run-b"
_COMPETENCIA = "2026-01"
_NOW = datetime(2026, 1, 15, 12, tzinfo=UTC)


@dataclass
class _FakeObjectStore:
    objects: dict[str, bytes] = field(default_factory=dict)
    fail_after_promotions: int | None = None
    _promotions: int = field(default=0, init=False, repr=False)

    def put(self, key: str, body: BinaryIO, expected_sha256: str) -> ObjectStat:
        data = body.read()
        digest = hashlib.sha256(data).hexdigest()
        if digest != expected_sha256:
            raise ValueError(f"sha256_mismatch key={key}")
        self.objects[key] = data
        return ObjectStat(key=key, size_bytes=len(data), sha256=digest)

    def open(self, key: str) -> ContextManager[BinaryIO]:
        return nullcontext(BytesIO(self.objects[key]))

    def stat(self, key: str) -> ObjectStat | None:
        data = self.objects.get(key)
        if data is None:
            return None
        return ObjectStat(key=key, size_bytes=len(data), sha256=hashlib.sha256(data).hexdigest())

    def delete(self, key: str) -> None:
        self.objects.pop(key, None)

    def promote(self, source_key: str, destination_key: str, expected_sha256: str) -> ObjectStat:
        limit = self.fail_after_promotions
        if limit is not None and self._promotions >= limit:
            raise ValueError("sha256=mismatch")
        self._promotions += 1
        data = self.objects.pop(source_key)
        self.objects[destination_key] = data
        return ObjectStat(key=destination_key, size_bytes=len(data), sha256=expected_sha256)


@pytest.fixture
def adapter(tmp_path) -> SQLiteControlPlane:
    control_plane = SQLiteControlPlane(tmp_path / "cp.db", lambda: _NOW)
    control_plane.initialize()
    return control_plane


@pytest.fixture
def store() -> _FakeObjectStore:
    return _FakeObjectStore()


def _run(
    run_id: str = _RUN_ID, *,
    state: RunState = RunState.PUBLISHING, missing_sources: tuple[str, ...] = (),
) -> Run:
    return Run(
        tenant_id=_TENANT, run_id=run_id, competencia=_COMPETENCIA, dataset_name="gold",
        state=state,
        dependencies=(RunDependency(source_type="CNES", file_subtype="ST", required=True),),
        missing_sources=missing_sources, created_at=_NOW,
    )


def _input_ref(name: str) -> ManifestRef:
    key = f"raw/{_TENANT}/CNES/{_COMPETENCIA}/{name}/manifest.json"
    return ManifestRef(manifest_id=name, manifest_key=key)


def _bare_ref(manifest_id: str = "manifest-x") -> ManifestRef:
    key = f"tmp/{_TENANT}/{_RUN_ID}/unit-x/1/manifests/{manifest_id}/manifest.json"
    return ManifestRef(manifest_id=manifest_id, manifest_key=key)


def _materialize_unit(
    refs: tuple[ManifestRef, ...], *,
    unit_id: str = "unit-m", attempt: int = 1, run_id: str = _RUN_ID,
) -> RunUnit:
    return RunUnit(
        tenant_id=_TENANT, run_id=run_id, unit_id=unit_id, stage=RunStage.MATERIALIZE,
        source_type=None, file_subtype=None, partition="all",
        depends_on_unit_ids=("unit-upstream",), input_manifests=(),
        state=RunUnitState.SUCCEEDED, attempt=attempt, fencing_token=1, lease_owner=None,
        lease_until=None, dispatch_id=None, output_manifests=refs, error_code=None,
    )


def _reconcile_unit(
    refs: tuple[ManifestRef, ...], *,
    unit_id: str = "unit-r", attempt: int = 1, run_id: str = _RUN_ID,
) -> RunUnit:
    return RunUnit(
        tenant_id=_TENANT, run_id=run_id, unit_id=unit_id, stage=RunStage.RECONCILE,
        source_type=None, file_subtype=None, partition="all",
        depends_on_unit_ids=("unit-upstream",), input_manifests=(),
        state=RunUnitState.SUCCEEDED, attempt=attempt, fencing_token=1, lease_owner=None,
        lease_until=None, dispatch_id=None, output_manifests=refs, error_code=None,
    )


def _failed_unit(unit_id: str = "unit-f") -> RunUnit:
    return RunUnit(
        tenant_id=_TENANT, run_id=_RUN_ID, unit_id=unit_id, stage=RunStage.NORMALIZE,
        source_type="CNES", file_subtype="ST", partition="all",
        depends_on_unit_ids=(), input_manifests=(_input_ref(unit_id),),
        state=RunUnitState.FAILED_FINAL, attempt=1, fencing_token=1, lease_owner=None,
        lease_until=None, dispatch_id=None, output_manifests=(), error_code="boom",
    )


def _degraded_unit_with_outputs(unit_id: str = "unit-d") -> RunUnit:
    base = RunUnit(
        tenant_id=_TENANT, run_id=_RUN_ID, unit_id=unit_id, stage=RunStage.NORMALIZE,
        source_type="CNES", file_subtype="ST", partition="all",
        depends_on_unit_ids=(), input_manifests=(_input_ref(unit_id),),
        state=RunUnitState.SUCCEEDED_DEGRADED, attempt=1, fencing_token=1, lease_owner=None,
        lease_until=None, dispatch_id=None, output_manifests=(), error_code="boom",
    )
    return base.model_copy(update={"output_manifests": (_bare_ref(),)})


def _seed_manifest(
    store: _FakeObjectStore, unit_id: str, attempt: int, suffix: str = "a", *,
    run_id: str = _RUN_ID, manifest_id: str | None = None, object_key: str | None = None,
    stored_manifest_id: str | None = None, canonical: bool = True,
    object_sha256: str | None = None, body: bytes | None = None,
) -> tuple[OutputManifest, ManifestRef]:
    body = body if body is not None else f"payload-{suffix}".encode()
    digest = object_sha256 if object_sha256 is not None else hashlib.sha256(body).hexdigest()
    key = object_key or f"reconciliation/{_TENANT}/{_COMPETENCIA}/{run_id}/part-{suffix}.parquet"
    manifest = OutputManifest(
        manifest_version=1, manifest_id=manifest_id or f"manifest-{suffix}", tenant_id=_TENANT,
        layer="reconciliation", source_type=None, competencia=_COMPETENCIA,
        run_id=run_id, unit_id=unit_id, attempt=attempt,
        schema_version="gold-v1", object_key=key, object_sha256=digest,
        row_count=10, created_at=_NOW,
    )
    prefix = unit_attempt_prefix(
        SimpleNamespace(tenant_id=_TENANT, run_id=run_id, unit_id=unit_id, attempt=attempt)
    )
    store.objects[attempt_object_key(prefix, manifest.object_key)] = body
    stored = manifest
    if stored_manifest_id is not None:
        stored = manifest.model_copy(update={"manifest_id": stored_manifest_id})
    sidecar_key = attempt_object_key(prefix, f"manifests/{manifest.manifest_id}/manifest.json")
    payload = stored.model_dump_json(exclude_none=False, by_alias=False).encode()
    if not canonical:
        payload = json.dumps(json.loads(payload), indent=2).encode()
    store.objects[sidecar_key] = payload
    return manifest, ManifestRef(manifest_id=manifest.manifest_id, manifest_key=sidecar_key)


def _request(
    run: Run, units: tuple[RunUnit, ...], *, expected_version_id: str | None = None,
) -> PublishRequest:
    return PublishRequest(run=run, units=units, expected_version_id=expected_version_id, now=_NOW)


def test_run_fora_de_publishing_falha(adapter, store):
    run = _run(state=RunState.PROCESSING)
    adapter.put_run(run)
    publisher = DatasetPublisher(store=store, control_plane=adapter)

    with pytest.raises(ValueError, match="run_not_publishing"):
        publisher.publish(_request(run, ()))


def test_unit_nao_satisfeita_falha(adapter, store):
    run = _run()
    adapter.put_run(run)
    publisher = DatasetPublisher(store=store, control_plane=adapter)

    with pytest.raises(ValueError, match="unit_not_succeeded"):
        publisher.publish(_request(run, (_failed_unit(),)))


def test_succeeded_sem_outputs_falha(adapter, store):
    run = _run()
    adapter.put_run(run)
    publisher = DatasetPublisher(store=store, control_plane=adapter)

    with pytest.raises(ValueError, match="succeeded_unit_missing_outputs"):
        publisher.publish(_request(run, (_materialize_unit(()),)))


def test_degraded_com_outputs_falha(adapter, store):
    run = _run()
    adapter.put_run(run)
    publisher = DatasetPublisher(store=store, control_plane=adapter)

    with pytest.raises(ValueError, match="degraded_unit_outputs_forbidden"):
        publisher.publish(_request(run, (_degraded_unit_with_outputs(),)))


def test_zero_materialize_falha(adapter, store):
    run = _run()
    adapter.put_run(run)
    unit = _reconcile_unit((_bare_ref(),))
    publisher = DatasetPublisher(store=store, control_plane=adapter)

    with pytest.raises(ValueError, match="materialize_unit_count_invalid"):
        publisher.publish(_request(run, (unit,)))


def test_dois_materialize_falha(adapter, store):
    run = _run()
    adapter.put_run(run)
    units = (
        _materialize_unit((_bare_ref("manifest-1"),), unit_id="unit-m1"),
        _materialize_unit((_bare_ref("manifest-2"),), unit_id="unit-m2"),
    )
    publisher = DatasetPublisher(store=store, control_plane=adapter)

    with pytest.raises(ValueError, match="materialize_unit_count_invalid"):
        publisher.publish(_request(run, units))


def test_manifest_id_divergente_do_sidecar_falha(adapter, store):
    run = _run()
    adapter.put_run(run)
    _, ref = _seed_manifest(store, "unit-m", 1, stored_manifest_id="manifest-other")
    publisher = DatasetPublisher(store=store, control_plane=adapter)

    with pytest.raises(ValueError, match="manifest_id_mismatch"):
        publisher.publish(_request(run, (_materialize_unit((ref,)),)))


def test_sidecar_nao_canonico_falha(adapter, store):
    run = _run()
    adapter.put_run(run)
    _, ref = _seed_manifest(store, "unit-m", 1, canonical=False)
    publisher = DatasetPublisher(store=store, control_plane=adapter)

    with pytest.raises(ValueError, match="manifest_not_canonical"):
        publisher.publish(_request(run, (_materialize_unit((ref,)),)))


def test_object_key_duplicado_entre_units_falha(adapter, store):
    run = _run()
    adapter.put_run(run)
    shared_key = f"reconciliation/{_TENANT}/{_COMPETENCIA}/{_RUN_ID}/shared.parquet"
    _, ref_a = _seed_manifest(store, "unit-r", 1, object_key=shared_key, manifest_id="manifest-a")
    _, ref_b = _seed_manifest(store, "unit-m", 1, object_key=shared_key, manifest_id="manifest-b")
    units = (_reconcile_unit((ref_a,)), _materialize_unit((ref_b,)))
    publisher = DatasetPublisher(store=store, control_plane=adapter)

    with pytest.raises(ValueError, match="duplicate_object_key"):
        publisher.publish(_request(run, units))


def test_hash_divergente_apos_promote_falha(adapter, store):
    run = _run()
    adapter.put_run(run)
    _, ref = _seed_manifest(store, "unit-m", 1, object_sha256="0" * 64, body=b"payload-real")
    publisher = DatasetPublisher(store=store, control_plane=adapter)

    with pytest.raises(ValueError, match="promoted_object_hash_mismatch"):
        publisher.publish(_request(run, (_materialize_unit((ref,)),)))


def test_policy_com_identidade_divergente_falha(adapter, store):
    run = _run()
    adapter.put_run(run)
    _, ref = _seed_manifest(store, "unit-m", 1)
    bad_permit = PublicationPermit(
        tenant_id=_TENANT, run_id="other-run", policy_version=0, fencing_token=0
    )
    publisher = DatasetPublisher(
        store=store, control_plane=adapter, publication_policy=lambda _: bad_permit
    )

    with pytest.raises(ValueError, match="publication_permit_identity_mismatch"):
        publisher.publish(_request(run, (_materialize_unit((ref,)),)))


def test_falha_antes_do_cas_preserva_pointer(adapter, store):
    run = _run()
    adapter.put_run(run)
    _, ref_a = _seed_manifest(store, "unit-r", 1, suffix="a")
    _, ref_b = _seed_manifest(store, "unit-m", 1, suffix="b")
    units = (_reconcile_unit((ref_a,)), _materialize_unit((ref_b,)))
    store.fail_after_promotions = 1
    publisher = DatasetPublisher(store=store, control_plane=adapter)

    with pytest.raises(ValueError, match="sha256=mismatch"):
        publisher.publish(_request(run, units))

    assert adapter.get_dataset_pointer(_TENANT, "gold") is None
    assert adapter.get_run(_TENANT, _RUN_ID).state is RunState.PUBLISHING


def test_policy_forte_roda_imediatamente_antes_da_transacao(adapter, store):
    run = _run()
    adapter.put_run(run)
    _, ref = _seed_manifest(store, "unit-m", 1)
    permit = PublicationPermit(tenant_id=_TENANT, run_id=_RUN_ID, policy_version=7, fencing_token=3)
    policy = Mock(return_value=permit)
    control_plane = Mock(wraps=adapter)
    publisher = DatasetPublisher(
        store=store, control_plane=control_plane, publication_policy=policy
    )

    publisher.publish(_request(run, (_materialize_unit((ref,)),)))

    policy.assert_called_once_with(run)
    control_plane.publish_dataset.assert_called_once()
    command = control_plane.publish_dataset.call_args.args[0]
    assert command.publication_permit is permit


def test_publica_dataset_e_avanca_pointer(adapter, store):
    run = _run()
    adapter.put_run(run)
    _, ref_a = _seed_manifest(store, "unit-r", 1, suffix="a")
    _, ref_b = _seed_manifest(store, "unit-m", 1, suffix="b")
    units = (_reconcile_unit((ref_a,)), _materialize_unit((ref_b,)))
    publisher = DatasetPublisher(store=store, control_plane=adapter)

    result = publisher.publish(_request(run, units))

    assert result.pointer.version_id == _RUN_ID
    assert result.pointer.dataset_name == "gold"
    assert result.version.run_manifest_key == (
        f"reconciliation/{_TENANT}/{_COMPETENCIA}/{_RUN_ID}/run-manifest.json"
    )
    assert len(result.run_manifest.outputs) == 2
    assert adapter.get_run(_TENANT, _RUN_ID).state is RunState.PUBLISHED
    assert store.stat(f"reconciliation/{_TENANT}/{_COMPETENCIA}/{_RUN_ID}/part-a.parquet")
    assert store.stat(result.version.run_manifest_key)


def test_missing_sources_publica_degradado(adapter, store):
    run = _run(missing_sources=("CNES:OPT",))
    adapter.put_run(run)
    _, ref = _seed_manifest(store, "unit-m", 1)
    publisher = DatasetPublisher(store=store, control_plane=adapter)

    result = publisher.publish(_request(run, (_materialize_unit((ref,)),)))

    assert adapter.get_run(_TENANT, _RUN_ID).state is RunState.PUBLISHED_DEGRADED
    assert result.run_manifest.missing_sources == ("CNES:OPT",)


def test_conflito_de_cas_propaga_sem_reverter_pointer(adapter, store):
    run_a = _run()
    adapter.put_run(run_a)
    _, ref_a = _seed_manifest(store, "unit-m", 1, suffix="a")
    publisher = DatasetPublisher(store=store, control_plane=adapter)
    winner = publisher.publish(_request(run_a, (_materialize_unit((ref_a,)),)))

    run_b = _run(_RUN_ID_B)
    adapter.put_run(run_b)
    _, ref_b = _seed_manifest(store, "unit-m", 1, suffix="b", run_id=_RUN_ID_B)
    unit_b = _materialize_unit((ref_b,), run_id=_RUN_ID_B)

    with pytest.raises(Conflict) as excinfo:
        publisher.publish(_request(run_b, (unit_b,)))

    assert excinfo.value.code is ControlPlaneErrorCode.POINTER_CAS
    pointer = adapter.get_dataset_pointer(_TENANT, "gold")
    assert pointer.version_id == winner.version.version_id == _RUN_ID
