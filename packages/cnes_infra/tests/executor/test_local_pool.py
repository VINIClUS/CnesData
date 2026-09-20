"""Concorrência, cancelamento e mapeamento de status do pool local."""

from __future__ import annotations

import threading
from datetime import UTC, datetime

import pytest

from cnes_domain.control_plane.entities import RunUnit
from cnes_domain.control_plane.enums import RunStage, RunUnitState
from cnes_domain.ports.processing import (
    CancelRunExecution,
    ExecutionStatus,
    ProcessorExecutorPort,
    RunUnitMessage,
    StartRunExecution,
)
from cnes_domain.tenant import get_tenant_id, set_tenant_id
from cnes_infra.executor.local_pool import LocalWorkerPool

_TENANT = "354130"
_WAVE_ID = "0123456789abcdef"
_DISPATCH_ID = "fedcba9876543210"


def _utc_now() -> datetime:
    return datetime(2026, 7, 15, 12, tzinfo=UTC)


def _reconcile_unit(unit_id: str, run_id: str) -> RunUnit:
    return RunUnit(
        tenant_id=_TENANT,
        run_id=run_id,
        unit_id=unit_id,
        stage=RunStage.RECONCILE,
        source_type=None,
        file_subtype=None,
        partition="all",
        depends_on_unit_ids=("normalize-1",),
        input_manifests=(),
        state=RunUnitState.SUCCEEDED,
        attempt=1,
        fencing_token=1,
        lease_owner=None,
        lease_until=None,
        dispatch_id=None,
        output_manifests=(),
        error_code=None,
    )


def _request(
    unit_ids: tuple[str, ...] = ("unit-1",),
    max_concurrency: int = 1,
    run_id: str = "run-1",
) -> StartRunExecution:
    return StartRunExecution(
        tenant_id=_TENANT,
        run_id=run_id,
        wave_id=_WAVE_ID,
        dispatch_id=_DISPATCH_ID,
        unit_ids=unit_ids,
        max_concurrency=max_concurrency,
    )


class ConcurrencyProbe:
    """Registra o pico de execuções simultâneas observadas pelo handler."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._current = 0
        self.maximum = 0

    def enter(self) -> None:
        with self._lock:
            self._current += 1
            self.maximum = max(self.maximum, self._current)

    def leave(self) -> None:
        with self._lock:
            self._current -= 1


def recording_handler(probe: ConcurrencyProbe, barrier: threading.Barrier):
    def handler(message: RunUnitMessage) -> RunUnit:
        probe.enter()
        try:
            barrier.wait(timeout=5)
        finally:
            probe.leave()
        return _reconcile_unit(message.unit_id, message.run_id)

    return handler


def test_pool_nunca_excede_limite() -> None:
    probe = ConcurrencyProbe()
    barrier = threading.Barrier(2)
    handler = recording_handler(probe, barrier)
    pool = LocalWorkerPool(handler, "local-worker", _utc_now, lease_seconds=300)

    ref = pool.start(
        _request(unit_ids=("unit-1", "unit-2", "unit-3", "unit-4"), max_concurrency=2)
    )
    pool.close()

    assert ref == "local:run-1:fedcba9876543210"
    assert probe.maximum == 2


def test_start_e_idempotente_para_o_mesmo_dispatch() -> None:
    calls: list[str] = []

    def handler(message: RunUnitMessage) -> RunUnit:
        calls.append(message.unit_id)
        return _reconcile_unit(message.unit_id, message.run_id)

    pool = LocalWorkerPool(handler, "local-worker", _utc_now, lease_seconds=300)
    request = _request(unit_ids=("unit-1", "unit-2"))

    first = pool.start(request)
    second = pool.start(request)
    pool.close()

    assert first == second
    assert calls == ["unit-1", "unit-2"]


def test_mensagem_carrega_owner_lease_e_unidades_ordenadas() -> None:
    received: list[RunUnitMessage] = []

    def handler(message: RunUnitMessage) -> RunUnit:
        received.append(message)
        return _reconcile_unit(message.unit_id, message.run_id)

    clock_value = datetime(2026, 7, 15, 12, tzinfo=UTC)
    pool = LocalWorkerPool(handler, "local-worker", lambda: clock_value, lease_seconds=120)
    request = _request(unit_ids=("unit-b", "unit-a"))

    pool.start(request)
    pool.close()

    assert [message.unit_id for message in received] == ["unit-a", "unit-b"]
    assert all(message.owner == "local-worker" for message in received)
    assert all(message.now == clock_value for message in received)
    assert all(message.lease_seconds == 120 for message in received)
    assert all(message.tenant_id == request.tenant_id for message in received)
    assert all(message.wave_id == request.wave_id for message in received)
    assert all(message.dispatch_id == request.dispatch_id for message in received)


def test_propaga_tenant_para_as_threads() -> None:
    observed: list[str] = []

    def handler(message: RunUnitMessage) -> RunUnit:
        observed.append(get_tenant_id())
        return _reconcile_unit(message.unit_id, message.run_id)

    set_tenant_id(_TENANT)
    pool = LocalWorkerPool(handler, "local-worker", _utc_now, lease_seconds=300)
    pool.start(_request())
    pool.close()

    assert observed == [_TENANT]


def test_status_reporta_running_enquanto_unidade_viva() -> None:
    release = threading.Event()

    def handler(message: RunUnitMessage) -> RunUnit:
        assert release.wait(timeout=5)
        return _reconcile_unit(message.unit_id, message.run_id)

    pool = LocalWorkerPool(handler, "local-worker", _utc_now, lease_seconds=300)
    ref = pool.start(_request())

    assert pool.status(ref) is ExecutionStatus.RUNNING
    release.set()
    pool.close()

    assert pool.status(ref) is ExecutionStatus.SUCCEEDED


def test_status_reporta_failed_quando_handler_levanta() -> None:
    def handler(message: RunUnitMessage) -> RunUnit:
        raise RuntimeError("boom")

    pool = LocalWorkerPool(handler, "local-worker", _utc_now, lease_seconds=300)
    ref = pool.start(_request())
    pool.close()

    assert pool.status(ref) is ExecutionStatus.FAILED


def test_status_recolhe_pool_apos_estado_terminal() -> None:
    pool = LocalWorkerPool(
        lambda message: _reconcile_unit(message.unit_id, message.run_id),
        "local-worker",
        _utc_now,
        lease_seconds=300,
    )
    ref = pool.start(_request())

    assert pool.status(ref) is ExecutionStatus.SUCCEEDED
    assert pool._batches == {}


def test_cancel_interrompe_batch_em_andamento() -> None:
    started = threading.Event()
    release = threading.Event()
    calls: list[str] = []

    def handler(message: RunUnitMessage) -> RunUnit:
        calls.append(message.unit_id)
        started.set()
        assert release.wait(timeout=5)
        return _reconcile_unit(message.unit_id, message.run_id)

    pool = LocalWorkerPool(handler, "local-worker", _utc_now, lease_seconds=300)
    request = _request(unit_ids=("unit-1", "unit-2"), max_concurrency=1)

    ref = pool.start(request)
    assert started.wait(timeout=5)
    pool.cancel(CancelRunExecution(tenant_id=_TENANT, run_id="run-1", execution_ref=ref))
    release.set()
    pool.close()

    assert calls == ["unit-1"]
    assert pool.status(ref) is ExecutionStatus.CANCELED


def test_cancel_antes_do_start_suprime_execucao() -> None:
    calls: list[str] = []

    def handler(message: RunUnitMessage) -> RunUnit:
        calls.append(message.unit_id)
        return _reconcile_unit(message.unit_id, message.run_id)

    pool = LocalWorkerPool(handler, "local-worker", _utc_now, lease_seconds=300)
    pool.cancel(CancelRunExecution(tenant_id=_TENANT, run_id="run-1", execution_ref=None))

    ref = pool.start(_request())
    pool.close()

    assert calls == []
    assert pool.status(ref) is ExecutionStatus.CANCELED


def test_rejeita_execution_ref_desconhecido() -> None:
    pool = LocalWorkerPool(
        lambda message: _reconcile_unit(message.unit_id, message.run_id),
        "local-worker",
        _utc_now,
        lease_seconds=300,
    )

    with pytest.raises(ValueError, match="execution_ref=unknown"):
        pool.status("local:missing:0000000000000000")


def test_rejeita_owner_em_branco() -> None:
    with pytest.raises(ValueError, match="owner=blank"):
        LocalWorkerPool(
            lambda message: _reconcile_unit(message.unit_id, message.run_id),
            " ",
            _utc_now,
            lease_seconds=300,
        )


def test_rejeita_lease_seconds_invalido() -> None:
    with pytest.raises(ValueError, match="lease_seconds=invalid"):
        LocalWorkerPool(
            lambda message: _reconcile_unit(message.unit_id, message.run_id),
            "local-worker",
            _utc_now,
            lease_seconds=0,
        )


def test_pool_preserva_run_unit_do_handler_sem_mutacao() -> None:
    produced: list[RunUnit] = []

    def handler(message: RunUnitMessage) -> RunUnit:
        unit = _reconcile_unit(message.unit_id, message.run_id)
        produced.append(unit)
        return unit

    pool = LocalWorkerPool(handler, "local-worker", _utc_now, lease_seconds=300)
    ref = pool.start(_request())
    pool.close()

    assert pool.status(ref) is ExecutionStatus.SUCCEEDED
    assert produced[0].state is RunUnitState.SUCCEEDED
    assert produced[0].fencing_token == 1


def test_implementa_processor_executor_port() -> None:
    pool = LocalWorkerPool(
        lambda message: _reconcile_unit(message.unit_id, message.run_id),
        "local-worker",
        _utc_now,
        lease_seconds=300,
    )

    assert isinstance(pool, ProcessorExecutorPort)
