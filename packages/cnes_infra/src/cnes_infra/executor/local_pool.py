"""Adapter local do executor do processador, baseado em ThreadPoolExecutor."""

from __future__ import annotations

import threading
from collections import OrderedDict
from concurrent.futures import Future, ThreadPoolExecutor
from contextvars import copy_context
from dataclasses import dataclass
from typing import TYPE_CHECKING

from cnes_domain.ports.processing import ExecutionStatus, RunUnitMessage

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from cnes_domain.control_plane.entities import RunUnit
    from cnes_domain.ports.processing import CancelRunExecution, StartRunExecution

type RunUnitHandler = Callable[[RunUnitMessage], "RunUnit"]
type Clock = Callable[[], "datetime"]

_COMPLETED_STATUS_LIMIT = 1024


@dataclass(frozen=True, slots=True)
class _Batch:
    pool: ThreadPoolExecutor
    futures: tuple[Future[RunUnit | None], ...]


class LocalWorkerPool:
    """Executa unidades de um dispatch em threads, sem mutar `Run`/`RunUnit`."""

    def __init__(
        self, handler: RunUnitHandler, owner: str, clock: Clock, lease_seconds: int
    ) -> None:
        if not owner.strip():
            raise ValueError("owner=blank")
        if lease_seconds < 1:
            raise ValueError("lease_seconds=invalid")
        self._handler = handler
        self._owner = owner
        self._clock = clock
        self._lease_seconds = lease_seconds
        self._lock = threading.Lock()
        self._batches: dict[str, _Batch] = {}
        self._completed: OrderedDict[str, ExecutionStatus] = OrderedDict()
        self._cancel_events: dict[str, threading.Event] = {}

    def start(self, request: StartRunExecution) -> str:
        """Inicia (ou reencontra, se já iniciado) um dispatch. Idempotente."""
        ref = f"local:{request.run_id}:{request.dispatch_id}"
        with self._lock:
            if ref in self._batches or ref in self._completed:
                return ref
            cancel = self._cancel_events.setdefault(request.run_id, threading.Event())
            pool = ThreadPoolExecutor(max_workers=request.max_concurrency)
            futures = tuple(
                pool.submit(copy_context().run, self._run_unit, message, cancel)
                for message in self._ordered_messages(request)
            )
            self._batches[ref] = _Batch(pool=pool, futures=futures)
        return ref

    def cancel(self, request: CancelRunExecution) -> None:
        """Sinaliza cancelamento best-effort para o run; não interrompe threads em curso."""
        with self._lock:
            self._cancel_events.setdefault(request.run_id, threading.Event()).set()

    def status(self, execution_ref: str) -> ExecutionStatus:
        """Reporta o estado agregado do dispatch a partir das futures registradas."""
        with self._lock:
            completed = self._completed.get(execution_ref)
            batch = self._batches.get(execution_ref)
        if completed is not None:
            return completed
        if batch is None:
            raise ValueError(f"execution_ref=unknown ref={execution_ref}")
        status = self._batch_status(batch)
        if status is ExecutionStatus.RUNNING:
            return status
        batch.pool.shutdown(wait=True)
        self._remember_completed(execution_ref, status)
        return status

    def close(self) -> None:
        """Encerra todos os pools criados, aguardando as threads em curso."""
        with self._lock:
            batches = tuple(self._batches.items())
            self._batches.clear()
        for ref, batch in batches:
            batch.pool.shutdown(wait=True)
            self._remember_completed(ref, self._batch_status(batch))

    def _remember_completed(self, execution_ref: str, status: ExecutionStatus) -> None:
        with self._lock:
            self._batches.pop(execution_ref, None)
            self._completed[execution_ref] = status
            self._completed.move_to_end(execution_ref)
            while len(self._completed) > _COMPLETED_STATUS_LIMIT:
                self._completed.popitem(last=False)

    @staticmethod
    def _batch_status(batch: _Batch) -> ExecutionStatus:
        if any(not future.done() for future in batch.futures):
            return ExecutionStatus.RUNNING
        if any(future.exception() is not None for future in batch.futures):
            return ExecutionStatus.FAILED
        if any(future.result() is None for future in batch.futures):
            return ExecutionStatus.CANCELED
        return ExecutionStatus.SUCCEEDED

    def _ordered_messages(self, request: StartRunExecution) -> tuple[RunUnitMessage, ...]:
        now = self._clock()
        return tuple(
            RunUnitMessage(
                tenant_id=request.tenant_id,
                run_id=request.run_id,
                wave_id=request.wave_id,
                dispatch_id=request.dispatch_id,
                unit_id=unit_id,
                owner=self._owner,
                now=now,
                lease_seconds=self._lease_seconds,
            )
            for unit_id in sorted(request.unit_ids)
        )

    def _run_unit(self, message: RunUnitMessage, cancel: threading.Event) -> RunUnit | None:
        if cancel.is_set():
            return None
        return self._handler(message)
