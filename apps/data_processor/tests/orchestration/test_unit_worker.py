"""TDD do UnitWorker: claim -> process -> validate -> commit fenced."""
from __future__ import annotations

import hashlib
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from io import BytesIO
from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest

from cnes_contracts.manifests.outputs import OutputManifest
from cnes_contracts.manifests.raw import SourceType
from cnes_domain.control_plane.commands import ClaimRunUnit, PutRunUnits, ReserveRunDispatch
from cnes_domain.control_plane.entities import ManifestRef, Run, RunDependency, RunUnit
from cnes_domain.control_plane.enums import RunStage, RunState, RunUnitState
from cnes_domain.control_plane.errors import FenceRejected, LeaseLost
from cnes_domain.ports.object_store import ObjectStat
from cnes_domain.ports.processing import RunUnitMessage
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from data_processor.orchestration.attempt_store import AttemptObjectStore, unit_attempt_prefix
from data_processor.orchestration.unit_handler import RunUnitCommandHandler
from data_processor.orchestration.unit_worker import (
    UnitWorker,
    UnitWorkerDependencies,
    UnitWorkerPolicy,
)

if TYPE_CHECKING:
    from collections.abc import BinaryIO
    from contextlib import AbstractContextManager as ContextManager

_TENANT = "354130"
_RUN_ID = "run-a"
_NOW = datetime(2026, 1, 15, 12, tzinfo=UTC)
_WAVE_A = "a" * 16
_WAVE_B = "b" * 16


@dataclass(slots=True)
class _MutableClock:
    instant: datetime

    def now(self) -> datetime:
        return self.instant

    def advance(self, delta: timedelta) -> None:
        self.instant += delta


@dataclass
class _FakeObjectStore:
    objects: dict[str, bytes] = field(default_factory=dict)

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
        data = self.objects.pop(source_key)
        self.objects[destination_key] = data
        return ObjectStat(key=destination_key, size_bytes=len(data), sha256=expected_sha256)


def _run() -> Run:
    return Run(
        tenant_id=_TENANT, run_id=_RUN_ID, competencia="2026-01", dataset_name="gold",
        state=RunState.PROCESSING,
        dependencies=(
            RunDependency(source_type="CNES", file_subtype="ST", required=True),
            RunDependency(source_type="CNES", file_subtype="OPT", required=False),
        ),
        missing_sources=(), created_at=_NOW,
    )


def _input_ref(name: str) -> ManifestRef:
    key = f"raw/{_TENANT}/CNES/2026-01/{name}/manifest.json"
    return ManifestRef(manifest_id=name, manifest_key=key)


def _unit(unit_id: str = "unit-a", file_subtype: str = "ST") -> RunUnit:
    return RunUnit(
        tenant_id=_TENANT, run_id=_RUN_ID, unit_id=unit_id, stage=RunStage.NORMALIZE,
        source_type="CNES", file_subtype=file_subtype, partition="all",
        depends_on_unit_ids=(), input_manifests=(_input_ref(f"input-{unit_id}"),),
        state=RunUnitState.PENDING, attempt=0, fencing_token=0, lease_owner=None,
        lease_until=None, dispatch_id=None, output_manifests=(), error_code=None,
    )


def _put_units(adapter: SQLiteControlPlane, units: tuple[RunUnit, ...]) -> None:
    adapter.put_run_units(PutRunUnits(
        tenant_id=_TENANT, run_id=_RUN_ID, expected_run_state=RunState.PROCESSING, units=units,
    ))


def _reserve(adapter, clock: _MutableClock, wave: str, unit_ids: tuple[str, ...]):
    return adapter.reserve_run_dispatch(ReserveRunDispatch(
        tenant_id=_TENANT, run_id=_RUN_ID, wave_id=wave, unit_ids=unit_ids,
        now=clock.now(), lease_seconds=30,
    ))


def _prepare(
    adapter, clock: _MutableClock, *,
    unit_id: str = "unit-a", file_subtype: str = "ST", wave: str = _WAVE_A,
):
    adapter.put_run(_run())
    _put_units(adapter, (_unit(unit_id, file_subtype),))
    return _reserve(adapter, clock, wave, (unit_id,))


def _claim_command(
    dispatch_id: str, clock: _MutableClock, owner: str = "worker-a", unit_id: str = "unit-a",
) -> ClaimRunUnit:
    return ClaimRunUnit(
        tenant_id=_TENANT, run_id=_RUN_ID, unit_id=unit_id, dispatch_id=dispatch_id,
        owner=owner, now=clock.now(), lease_seconds=30,
    )


def _make_manifest(unit: RunUnit, store, suffix: str = "a") -> OutputManifest:
    body = f"payload-{suffix}".encode()
    digest = hashlib.sha256(body).hexdigest()
    object_key = (
        f"normalized/{unit.tenant_id}/{SourceType.CNES_LOCAL.value}/2026-01/"
        f"{unit.run_id}/part-{suffix}.parquet"
    )
    store.put(object_key, BytesIO(body), digest)
    return OutputManifest(
        manifest_version=1, manifest_id=f"manifest-{suffix}", tenant_id=unit.tenant_id,
        layer="normalized", source_type=SourceType.CNES_LOCAL, competencia="2026-01",
        run_id=unit.run_id, unit_id=unit.unit_id, attempt=unit.attempt,
        schema_version="etl-v1", object_key=object_key, object_sha256=digest,
        row_count=1, created_at=_NOW,
    )


def _default_processor(unit: RunUnit, store) -> tuple[OutputManifest, ...]:
    return (_make_manifest(unit, store),)


def _always_raises(unit: RunUnit, store) -> tuple[OutputManifest, ...]:
    raise RuntimeError("boom")


def _returns_empty(unit: RunUnit, store) -> tuple[OutputManifest, ...]:
    return ()


def _dependencies(adapter, store, processor, clock: _MutableClock) -> UnitWorkerDependencies:
    return UnitWorkerDependencies(
        control_plane=adapter, store=store, processor=processor, clock=clock.now,
    )


@pytest.fixture
def clock() -> _MutableClock:
    return _MutableClock(_NOW)


@pytest.fixture
def adapter(tmp_path, clock: _MutableClock) -> SQLiteControlPlane:
    control_plane = SQLiteControlPlane(tmp_path / "cp.db", clock.now)
    control_plane.initialize()
    return control_plane


@pytest.fixture
def store() -> _FakeObjectStore:
    return _FakeObjectStore()


def test_handler_converte_ids_em_claim_exato():
    worker = Mock()
    handler = RunUnitCommandHandler(worker)
    message = RunUnitMessage(
        tenant_id=_TENANT, run_id=_RUN_ID, wave_id=_WAVE_A, dispatch_id=_WAVE_B,
        unit_id="unit-a", owner="worker-a", now=_NOW, lease_seconds=30,
    )

    handler.handle(message)

    worker.execute.assert_called_once_with(ClaimRunUnit(
        tenant_id=_TENANT, run_id=_RUN_ID, unit_id="unit-a", dispatch_id=_WAVE_B,
        owner="worker-a", now=_NOW, lease_seconds=30,
    ))


def _supersede_then_succeed(adapter, clock: _MutableClock):
    def _processor(unit: RunUnit, attempt_store) -> tuple[OutputManifest, ...]:
        clock.advance(timedelta(seconds=60))
        new_dispatch = _reserve(adapter, clock, _WAVE_B, ("unit-a",))
        adapter.claim_run_unit(_claim_command(new_dispatch.dispatch_id, clock, owner="worker-b"))
        return (_make_manifest(unit, attempt_store),)
    return _processor


def test_dispatch_antigo_nao_commita_apos_supersede(adapter, clock, store):
    dispatch = _prepare(adapter, clock)
    claim = _claim_command(dispatch.dispatch_id, clock)
    dependencies = _dependencies(adapter, store, _supersede_then_succeed(adapter, clock), clock)
    worker = UnitWorker(dependencies)

    with pytest.raises((FenceRejected, LeaseLost), match="dispatch"):
        worker.execute(claim)


def test_worker_atrasado_nao_commita_output(adapter, clock, store):
    dispatch = _prepare(adapter, clock)
    claim = _claim_command(dispatch.dispatch_id, clock)
    dependencies = _dependencies(adapter, store, _supersede_then_succeed(adapter, clock), clock)
    worker = UnitWorker(dependencies)

    with pytest.raises((FenceRejected, LeaseLost)):
        worker.execute(claim)

    reclaimed = adapter.list_run_units(_TENANT, _RUN_ID)[0]
    assert reclaimed.output_manifests == ()
    assert reclaimed.state == RunUnitState.LEASED
    assert all(key.startswith("tmp/") for key in store.objects)


def test_attempt_object_store_confina_escritas_ao_prefixo_da_tentativa(store):
    unit = _unit().model_copy(update={"attempt": 1})
    wrapped = AttemptObjectStore(delegate=store, prefix=unit_attempt_prefix(unit))
    body = b"payload"
    digest = hashlib.sha256(body).hexdigest()

    stat = wrapped.put("normalized/output.parquet", BytesIO(body), digest)

    assert stat.key == "normalized/output.parquet"
    assert f"tmp/{_TENANT}/{_RUN_ID}/unit-a/1/normalized/output.parquet" in store.objects
    assert wrapped.stat("normalized/output.parquet").sha256 == digest


def test_attempt_object_store_rejeita_leitura_nao_allowlisted(store):
    unit = _unit().model_copy(update={"attempt": 1})
    wrapped = AttemptObjectStore(delegate=store, prefix=unit_attempt_prefix(unit))

    with pytest.raises(ValueError, match="input_not_allowlisted"):
        wrapped.open("raw/some/input.parquet")


def test_attempt_object_store_permite_leitura_apos_with_inputs(store):
    unit = _unit().model_copy(update={"attempt": 1})
    body = b"raw-input"
    store.objects["raw/some/input.parquet"] = body
    wrapped = AttemptObjectStore(
        delegate=store, prefix=unit_attempt_prefix(unit),
    ).with_inputs({"raw/some/input.parquet": "raw/some/input.parquet"})

    with wrapped.open("raw/some/input.parquet") as handle:
        assert handle.read() == body


def test_attempt_object_store_rejeita_input_sob_prefixo_da_propria_tentativa(store):
    unit = _unit().model_copy(update={"attempt": 1})
    prefix = unit_attempt_prefix(unit)
    wrapped = AttemptObjectStore(delegate=store, prefix=prefix)

    with pytest.raises(ValueError, match="input_under_attempt_prefix"):
        wrapped.with_inputs({"logical": f"{prefix}/own-output.parquet"})


def test_attempt_object_store_promote_e_proibido(store):
    unit = _unit().model_copy(update={"attempt": 1})
    wrapped = AttemptObjectStore(delegate=store, prefix=unit_attempt_prefix(unit))

    with pytest.raises(RuntimeError, match="promote_forbidden"):
        wrapped.promote("a", "b", "c" * 64)


def test_after_persist_recebe_unit_apos_commit_bem_sucedido(adapter, clock, store):
    dispatch = _prepare(adapter, clock)
    calls: list[RunUnit] = []
    dependencies = _dependencies(adapter, store, _default_processor, clock)
    worker = UnitWorker(dependencies, UnitWorkerPolicy(after_persist=calls.append))

    result = worker.execute(_claim_command(dispatch.dispatch_id, clock))

    assert result.state == RunUnitState.SUCCEEDED
    assert calls == [result]


def test_after_persist_recebe_unit_apos_falha_persistida(adapter, clock, store):
    dispatch = _prepare(adapter, clock)
    calls: list[RunUnit] = []
    dependencies = _dependencies(adapter, store, _always_raises, clock)
    policy = UnitWorkerPolicy(max_attempts=3, after_persist=calls.append)
    worker = UnitWorker(dependencies, policy)

    result = worker.execute(_claim_command(dispatch.dispatch_id, clock))

    assert result.state == RunUnitState.FAILED_RETRYABLE
    assert calls == [result]


def test_after_persist_nao_e_chamado_quando_fence_rejeita(adapter, clock, store):
    dispatch = _prepare(adapter, clock)
    claim = _claim_command(dispatch.dispatch_id, clock)
    calls: list[RunUnit] = []
    dependencies = _dependencies(adapter, store, _supersede_then_succeed(adapter, clock), clock)
    worker = UnitWorker(dependencies, UnitWorkerPolicy(after_persist=calls.append))

    with pytest.raises((FenceRejected, LeaseLost)):
        worker.execute(claim)

    assert calls == []


def test_worker_falha_retryable_ate_esgotar_tentativas_depois_final(adapter, clock, store):
    dispatch = _prepare(adapter, clock)
    dependencies = _dependencies(adapter, store, _always_raises, clock)
    worker = UnitWorker(dependencies, UnitWorkerPolicy(max_attempts=3))

    first = worker.execute(_claim_command(dispatch.dispatch_id, clock, owner="worker-a"))
    assert first.state == RunUnitState.FAILED_RETRYABLE

    second = worker.execute(_claim_command(dispatch.dispatch_id, clock, owner="worker-b"))
    assert second.state == RunUnitState.FAILED_RETRYABLE

    third = worker.execute(_claim_command(dispatch.dispatch_id, clock, owner="worker-c"))
    assert third.state == RunUnitState.FAILED_FINAL

    with pytest.raises(LeaseLost):
        worker.execute(_claim_command(dispatch.dispatch_id, clock, owner="worker-d"))


def test_worker_falha_definitiva_em_dependencia_opcional_gera_degradado(adapter, clock, store):
    dispatch = _prepare(adapter, clock, unit_id="unit-b", file_subtype="OPT")
    dependencies = _dependencies(adapter, store, _always_raises, clock)
    worker = UnitWorker(dependencies, UnitWorkerPolicy(max_attempts=1))

    result = worker.execute(_claim_command(dispatch.dispatch_id, clock, unit_id="unit-b"))

    assert result.state == RunUnitState.SUCCEEDED_DEGRADED
    assert result.output_manifests == ()
    parent = adapter.get_run(_TENANT, _RUN_ID)
    assert parent.missing_sources == ("CNES/OPT",)


def test_saida_invalida_falha_imediatamente_sem_retry(adapter, clock, store):
    dispatch = _prepare(adapter, clock)
    dependencies = _dependencies(adapter, store, _returns_empty, clock)
    worker = UnitWorker(dependencies, UnitWorkerPolicy(max_attempts=5))

    result = worker.execute(_claim_command(dispatch.dispatch_id, clock))

    assert result.state == RunUnitState.FAILED_FINAL
    assert result.attempt == 1


def test_policy_rejeita_max_attempts_nao_positivo():
    with pytest.raises(ValueError, match="max_attempts_must_be_positive"):
        UnitWorkerPolicy(max_attempts=0)


def _duplicate_manifest_id_processor(unit: RunUnit, store) -> tuple[OutputManifest, ...]:
    manifest = _make_manifest(unit, store, suffix="a")
    return (manifest, manifest.model_copy(update={"object_key": manifest.object_key + ".dup"}))


def test_manifest_id_duplicado_falha_sem_retry(adapter, clock, store):
    dispatch = _prepare(adapter, clock)
    dependencies = _dependencies(adapter, store, _duplicate_manifest_id_processor, clock)
    worker = UnitWorker(dependencies, UnitWorkerPolicy(max_attempts=5))

    result = worker.execute(_claim_command(dispatch.dispatch_id, clock))

    assert result.state == RunUnitState.FAILED_FINAL


def _duplicate_object_key_processor(unit: RunUnit, store) -> tuple[OutputManifest, ...]:
    manifest = _make_manifest(unit, store, suffix="a")
    return (manifest, manifest.model_copy(update={"manifest_id": "manifest-other"}))


def test_object_key_duplicado_falha_sem_retry(adapter, clock, store):
    dispatch = _prepare(adapter, clock)
    dependencies = _dependencies(adapter, store, _duplicate_object_key_processor, clock)
    worker = UnitWorker(dependencies, UnitWorkerPolicy(max_attempts=5))

    result = worker.execute(_claim_command(dispatch.dispatch_id, clock))

    assert result.state == RunUnitState.FAILED_FINAL


def _identity_mismatch_processor(unit: RunUnit, store) -> tuple[OutputManifest, ...]:
    manifest = _make_manifest(unit, store, suffix="a")
    return (manifest.model_copy(update={"unit_id": "other-unit"}),)


def test_identidade_divergente_falha_sem_retry(adapter, clock, store):
    dispatch = _prepare(adapter, clock)
    dependencies = _dependencies(adapter, store, _identity_mismatch_processor, clock)
    worker = UnitWorker(dependencies, UnitWorkerPolicy(max_attempts=5))

    result = worker.execute(_claim_command(dispatch.dispatch_id, clock))

    assert result.state == RunUnitState.FAILED_FINAL


def _hash_mismatch_processor(unit: RunUnit, store) -> tuple[OutputManifest, ...]:
    manifest = _make_manifest(unit, store, suffix="a")
    return (manifest.model_copy(update={"object_sha256": "0" * 64}),)


def test_hash_divergente_falha_sem_retry(adapter, clock, store):
    dispatch = _prepare(adapter, clock)
    dependencies = _dependencies(adapter, store, _hash_mismatch_processor, clock)
    worker = UnitWorker(dependencies, UnitWorkerPolicy(max_attempts=5))

    result = worker.execute(_claim_command(dispatch.dispatch_id, clock))

    assert result.state == RunUnitState.FAILED_FINAL


def test_attempt_object_store_rejeita_chave_logica_invalida(store):
    unit = _unit().model_copy(update={"attempt": 1})
    wrapped = AttemptObjectStore(delegate=store, prefix=unit_attempt_prefix(unit))

    with pytest.raises(ValueError, match="invalid_logical_key"):
        wrapped.put("/leading-slash", BytesIO(b"x"), "0" * 64)


def test_attempt_object_store_rejeita_segmento_de_travessia(store):
    unit = _unit().model_copy(update={"attempt": 1})
    wrapped = AttemptObjectStore(delegate=store, prefix=unit_attempt_prefix(unit))

    with pytest.raises(ValueError, match="invalid_logical_key"):
        wrapped.put("normalized/../escape", BytesIO(b"x"), "0" * 64)


def test_attempt_object_store_rejeita_alvo_fisico_duplicado(store):
    unit = _unit().model_copy(update={"attempt": 1})
    wrapped = AttemptObjectStore(delegate=store, prefix=unit_attempt_prefix(unit))

    with pytest.raises(ValueError, match="duplicate_input_target"):
        wrapped.with_inputs({"logical-a": "raw/shared.parquet", "logical-b": "raw/shared.parquet"})


def test_attempt_object_store_stat_usa_input_quando_sem_output_da_tentativa(store):
    unit = _unit().model_copy(update={"attempt": 1})
    body = b"raw-input"
    store.objects["raw/some/input.parquet"] = body
    wrapped = AttemptObjectStore(
        delegate=store, prefix=unit_attempt_prefix(unit),
    ).with_inputs({"logical-input": "raw/some/input.parquet"})

    stat = wrapped.stat("logical-input")

    assert stat is not None
    assert stat.key == "logical-input"
    assert stat.sha256 == hashlib.sha256(body).hexdigest()


def test_attempt_object_store_stat_retorna_none_para_chave_desconhecida(store):
    unit = _unit().model_copy(update={"attempt": 1})
    wrapped = AttemptObjectStore(delegate=store, prefix=unit_attempt_prefix(unit))

    assert wrapped.stat("unknown-key") is None


def test_attempt_object_store_delete_remove_apenas_output_da_tentativa(store):
    unit = _unit().model_copy(update={"attempt": 1})
    wrapped = AttemptObjectStore(delegate=store, prefix=unit_attempt_prefix(unit))
    body = b"payload"
    wrapped.put("normalized/output.parquet", BytesIO(body), hashlib.sha256(body).hexdigest())

    wrapped.delete("normalized/output.parquet")

    assert wrapped.stat("normalized/output.parquet") is None
