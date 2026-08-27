"""Autocertificação das suites compartilhadas de contrato."""

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from cnes_domain.control_plane.commands import IdempotencyOutcome
from cnes_domain.control_plane.entities import DatasetPointer, IdempotencyRecord, RunDispatch
from cnes_domain.control_plane.enums import (
    AgentState,
    DispatchState,
    JobState,
    RunState,
    RunUnitState,
)
from cnes_domain.control_plane.errors import Conflict, FenceRejected, LeaseLost
from cnes_domain.control_plane.transitions import transition_run, transition_run_unit
from packages.cnes_infra.tests.contracts.clock import MutableClock, _HarnessState
from packages.cnes_infra.tests.contracts.control_plane_contract import (
    ControlPlaneCase,
    control_plane_cases,
)
from packages.cnes_infra.tests.contracts.object_store_contract import (
    ObjectStoreCase,
    _MemoryObjectStore,
    object_store_cases,
)

_NOW = datetime(2026, 7, 15, 12, tzinfo=UTC)


class FakeControlPlane(_HarnessState):
    def list_claimable_jobs(self, tenant_id: str, agent_id: str, limit: int) -> tuple[Any, ...]:
        agent = self.get_agent(tenant_id, agent_id)
        if agent is None or agent.state is AgentState.REVOKED:
            return ()
        values = [
            job
            for job in self.jobs.values()
            if job.tenant_id == tenant_id
            and job.agent_id == agent_id
            and self._job_claimable(job)
        ]
        return tuple(sorted(values, key=lambda item: (item.created_at, item.job_id))[:limit])

    def _job_claimable(self, job: Any) -> bool:
        retryable = job.state in {JobState.PENDING, JobState.FAILED_RETRYABLE}
        expired = job.state is JobState.LEASED and job.lease_until <= self.clock.now()
        return retryable or expired

    def claim_job(self, command: Any) -> Any | None:
        job = self.get_job(command.tenant_id, command.job_id)
        agent = None if job is None else self.get_agent(command.tenant_id, job.agent_id)
        if job is None or agent is None or agent.state is AgentState.REVOKED:
            return None
        if not self._job_claimable(job):
            return None
        claimed = job.model_copy(
            update={
                "state": JobState.LEASED,
                "attempt": job.attempt + 1,
                "fencing_token": job.fencing_token + 1,
                "lease_owner": command.owner,
                "lease_until": command.now + timedelta(seconds=command.lease_seconds),
            }
        )
        self.jobs[(job.tenant_id, job.job_id)] = claimed
        return claimed

    def _validate_job_fence(self, command: Any) -> Any:
        job = self.get_job(command.tenant_id, command.job_id)
        if job is None or job.state is not JobState.LEASED:
            raise LeaseLost("job_not_leased")
        if job.lease_owner != command.owner:
            raise LeaseLost("owner_mismatch")
        if job.fencing_token != command.fencing_token:
            raise FenceRejected("fence_mismatch")
        if job.lease_until <= self.clock.now():
            raise LeaseLost("lease_expired")
        return job

    def renew_job_lease(self, command: Any) -> Any:
        job = self._validate_job_fence(command)
        renewed = job.model_copy(
            update={"lease_until": command.now + timedelta(seconds=command.lease_seconds)}
        )
        self.jobs[(job.tenant_id, job.job_id)] = renewed
        return renewed

    def fail_job(self, command: Any, event: Any) -> Any:
        job = self._validate_job_fence(command)
        state = JobState.FAILED_RETRYABLE if command.retryable else JobState.FAILED_FINAL
        failed = job.model_copy(
            update={
                "state": state,
                "lease_owner": None,
                "lease_until": None,
                "error_code": command.error_code,
            }
        )
        self.jobs[(job.tenant_id, job.job_id)] = failed
        self.outbox[event.event_id] = event
        return failed

    def complete_job(self, command: Any, event: Any) -> Any:
        job = self._validate_job_fence(command)
        manifest = command.manifest
        completed = job.model_copy(
            update={
                "state": JobState.SUCCEEDED,
                "lease_owner": None,
                "lease_until": None,
                "result_manifest_id": manifest.manifest_id,
                "result_manifest_key": manifest.manifest_key,
            }
        )
        self.jobs[(job.tenant_id, job.job_id)] = completed
        self.raw_records.append(manifest)
        self.outbox[event.event_id] = event
        return completed

    def list_raw_manifest_chain(self, *args: Any) -> tuple[Any, ...]:
        tenant_id, source_type, file_subtype, competencia, limit = args
        identity = (tenant_id, source_type, file_subtype, competencia)
        records = [
            item
            for item in self.raw_records
            if (item.tenant_id, item.source_type, item.file_subtype, item.competencia) == identity
        ]
        if not records:
            return ()
        if self.mutation == "raw_chains":
            selected = sorted(records, key=lambda item: item.created_at)
        else:
            selected = self._select_raw_chain(records)
        bounded = selected if len(selected) <= limit else []
        return tuple(self._manifest_ref(item) for item in bounded)

    @staticmethod
    def _manifest_ref(record: Any) -> Any:
        from cnes_domain.control_plane.entities import ManifestRef

        return ManifestRef(manifest_id=record.manifest_id, manifest_key=record.manifest_key)

    def list_waiting_runs_for_dependency(self, *args: Any) -> tuple[Any, ...]:
        tenant_id, source_type, file_subtype, competencia, limit = args
        values = [
            run
            for run in self.runs.values()
            if run.tenant_id == tenant_id
            and run.competencia == competencia
            and run.state is RunState.WAITING_INPUTS
            and any(
                (dep.source_type, dep.file_subtype) == (source_type, file_subtype)
                for dep in run.dependencies
            )
        ]
        if self.mutation == "run_discovery":
            values = list(self.runs.values())
        return tuple(sorted(values, key=lambda item: (item.created_at, item.run_id))[:limit])

    def list_recoverable_runs(self, now: datetime, limit: int = 100) -> tuple[Any, ...]:
        states = {
            RunState.WAITING_INPUTS,
            RunState.PROCESSING,
            RunState.PUBLISHING,
            RunState.CANCEL_REQUESTED,
        }
        values = [run for run in self.runs.values() if run.state in states]
        return tuple(sorted(values, key=lambda item: (item.created_at, item.run_id))[:limit])

    def put_run_units(self, command: Any) -> tuple[Any, ...]:
        run = self.get_run(command.tenant_id, command.run_id)
        if run is None or run.state is not command.expected_run_state:
            raise Conflict("run_state_conflict")
        existing = self.list_run_units(command.tenant_id, command.run_id)
        if existing:
            if existing != command.units:
                if self.mutation == "run_units_atomic":
                    self._write_units(command.units)
                raise Conflict("units_conflict")
            return existing
        self._write_units(command.units)
        return command.units

    def _write_units(self, units: tuple[Any, ...]) -> None:
        for unit in units:
            self.units[(unit.tenant_id, unit.run_id, unit.unit_id)] = unit

    def claim_run_unit(self, command: Any) -> Any | None:
        key = (command.tenant_id, command.run_id, command.unit_id)
        unit = self.units.get(key)
        run = self.get_run(command.tenant_id, command.run_id)
        dispatch = self.dispatches.get((command.tenant_id, command.run_id))
        valid_parent = run is not None and run.state is RunState.PROCESSING
        valid_dispatch = (
            dispatch is not None
            and dispatch.dispatch_id == command.dispatch_id
            and dispatch.state is not DispatchState.TERMINAL and dispatch.lease_until > command.now
            and command.unit_id in dispatch.unit_ids
        )
        if self.mutation == "unit_claim":
            valid_parent = True
        if unit is None or not valid_parent or not valid_dispatch:
            return None
        claimable = unit.state in {RunUnitState.PENDING, RunUnitState.FAILED_RETRYABLE}
        claimable |= unit.state is RunUnitState.LEASED and unit.lease_until <= command.now
        if not claimable:
            return None
        claimed = unit.model_copy(
            update={
                "state": RunUnitState.LEASED,
                "attempt": unit.attempt + 1,
                "fencing_token": unit.fencing_token + 1,
                "lease_owner": command.owner,
                "lease_until": command.now + timedelta(seconds=command.lease_seconds),
                "dispatch_id": command.dispatch_id,
            }
        )
        self.units[key] = claimed
        return claimed

    def _validate_unit_fence(self, command: Any) -> tuple[Any, Any]:
        unit = self.units.get((command.tenant_id, command.run_id, command.unit_id))
        run = self.get_run(command.tenant_id, command.run_id)
        dispatch = self.dispatches.get((command.tenant_id, command.run_id))
        if self.mutation != "unit_fences":
            if run is None or run.state is not RunState.PROCESSING:
                raise LeaseLost("parent_not_processing")
            if dispatch is None or dispatch.dispatch_id != command.dispatch_id:
                raise LeaseLost("dispatch_mismatch")
            if unit is None or unit.lease_owner != command.owner:
                raise LeaseLost("owner_mismatch")
            if unit.fencing_token != command.fencing_token:
                raise FenceRejected("fence_mismatch")
            if unit.lease_until <= self.clock.now():
                raise LeaseLost("lease_expired")
        if unit is None:
            raise LeaseLost("unit_missing")
        return unit, run

    def commit_run_unit(self, command: Any, event: Any) -> Any:
        unit, run = self._validate_unit_fence(command)
        completed = transition_run_unit(unit, RunUnitState.SUCCEEDED, run).model_copy(
            update={
                "lease_owner": None,
                "lease_until": None,
                "output_manifests": command.output_manifests,
            }
        )
        self.units[(unit.tenant_id, unit.run_id, unit.unit_id)] = completed
        self.outbox[event.event_id] = event
        return completed

    def fail_run_unit(self, command: Any, event: Any) -> Any:
        unit, run = self._validate_unit_fence(command)
        optional = any(
            (dep.source_type, dep.file_subtype) == (unit.source_type, unit.file_subtype)
            and not dep.required
            for dep in run.dependencies
        )
        if command.retryable:
            state = RunUnitState.FAILED_RETRYABLE
        elif optional and self.mutation != "degraded_unit":
            state = RunUnitState.SUCCEEDED_DEGRADED
        else:
            state = RunUnitState.FAILED_FINAL
        failed = unit.model_copy(
            update={"error_code": command.error_code, "lease_owner": None, "lease_until": None}
        )
        failed = transition_run_unit(failed, state, run)
        self.units[(unit.tenant_id, unit.run_id, unit.unit_id)] = failed
        if state is RunUnitState.SUCCEEDED_DEGRADED:
            source = f"{unit.source_type}/{unit.file_subtype}"
            self.put_run(run.model_copy(update={"missing_sources": (*run.missing_sources, source)}))
        self.outbox[event.event_id] = event
        return failed

    def finalize_run_cancellation(self, command: Any, event: Any) -> Any:
        run = self.get_run(command.tenant_id, command.run_id)
        if run is not None and run.state is RunState.CANCELED:
            return run
        if run is None or run.state is not RunState.CANCEL_REQUESTED:
            raise Conflict("run_not_canceling")
        terminal_states = {
            RunUnitState.SUCCEEDED,
            RunUnitState.SUCCEEDED_DEGRADED,
            RunUnitState.FAILED_FINAL,
            RunUnitState.CANCELED,
        }
        for key, unit in tuple(self.units.items()):
            if key[:2] != (command.tenant_id, command.run_id):
                continue
            if unit.state not in terminal_states or self.mutation == "cancellation":
                self.units[key] = unit.model_copy(
                    update={
                        "state": RunUnitState.CANCELED,
                        "lease_owner": None,
                        "lease_until": None,
                    }
                )
        canceled = transition_run(run, RunState.CANCELED)
        self.put_run(canceled)
        self.outbox.setdefault(event.event_id, event)
        return canceled

    def reserve_run_dispatch(self, command: Any) -> Any:
        key = (command.tenant_id, command.run_id)
        active = self.dispatches.get(key)
        replay = active and active.wave_id == command.wave_id and active.lease_until > command.now
        if replay and self.mutation != "dispatch":
            return active
        live = active and active.state is not DispatchState.TERMINAL
        if live and (
            active.lease_until > command.now or self._has_live_unit_lease(active, command.now)
        ):
            raise Conflict("dispatch_live")
        generation = 1 if active is None else active.generation + 1
        dispatch = RunDispatch(
            tenant_id=command.tenant_id,
            run_id=command.run_id,
            wave_id=command.wave_id,
            dispatch_id=f"{generation:016x}",
            generation=generation,
            unit_ids=command.unit_ids,
            state=DispatchState.RESERVED,
            lease_until=command.now + timedelta(seconds=command.lease_seconds),
        )
        self.dispatches[key] = dispatch
        return dispatch

    def _has_live_unit_lease(self, dispatch: Any, now: datetime) -> bool:
        if self.mutation == "dispatch_expiry":
            return False
        return any(
            unit.dispatch_id == dispatch.dispatch_id
            and unit.state is RunUnitState.LEASED
            and unit.lease_until > now
            for unit in self.units.values()
        )

    def bind_run_dispatch(self, command: Any) -> Any:
        key = (command.tenant_id, command.run_id)
        dispatch = self.dispatches.get(key)
        if dispatch is None or dispatch.dispatch_id != command.dispatch_id:
            raise Conflict("dispatch_stale")
        if dispatch.state is DispatchState.STARTED:
            if dispatch.execution_ref != command.execution_ref:
                raise Conflict("execution_conflict")
            return dispatch
        if dispatch.state is not DispatchState.RESERVED:
            raise Conflict("dispatch_terminal")
        started = dispatch.model_copy(
            update={
                "state": DispatchState.STARTED,
                "execution_ref": command.execution_ref,
                "lease_until": command.now + timedelta(seconds=command.lease_seconds),
            }
        )
        self.dispatches[key] = started
        return started

    def finish_run_dispatch(self, command: Any) -> Any:
        key = (command.tenant_id, command.run_id)
        dispatch = self.dispatches.get(key)
        if dispatch is None or dispatch.dispatch_id != command.dispatch_id:
            raise Conflict("dispatch_stale")
        if dispatch.state is DispatchState.TERMINAL:
            if dispatch.terminal_outcome != command.outcome:
                raise Conflict("outcome_conflict")
            return dispatch
        finished = dispatch.model_copy(
            update={"state": DispatchState.TERMINAL, "terminal_outcome": command.outcome}
        )
        self.dispatches[key] = finished
        return finished

    def begin_idempotency(self, command: Any) -> Any:
        key = (command.tenant_id, command.scope, command.key)
        current = self.idempotency.get(key)
        if current and current.expires_at > command.now:
            if current.request_hash != command.request_hash:
                raise Conflict("idempotency_hash_conflict")
            if self.mutation == "idempotency":
                current = current.model_copy(update={"resource_id": command.resource_id})
            return IdempotencyOutcome(record=current, created=False)
        record = IdempotencyRecord(
            tenant_id=command.tenant_id,
            scope=command.scope,
            key=command.key,
            request_hash=command.request_hash,
            status="CREATED",
            resource_id=command.resource_id,
            created_at=command.now,
            expires_at=command.expires_at,
        )
        self.idempotency[key] = record
        return IdempotencyOutcome(record=record, created=True)

    def publish_dataset(self, command: Any) -> Any:
        version_key = (
            command.version.tenant_id,
            command.version.dataset_name,
            command.version.version_id,
        )
        pointer_key = (
            command.version.tenant_id,
            command.version.dataset_name,
            command.pointer_name,
        )
        current_version = self.versions.get(version_key)
        pointer = self.pointers.get(pointer_key)
        if current_version is not None:
            if current_version != command.version:
                raise Conflict("version_immutable")
            if pointer and pointer.version_id == command.version.version_id:
                return pointer
        actual = None if pointer is None else pointer.version_id
        if actual != command.expected_version_id:
            if self.mutation == "publication":
                self.versions[version_key] = command.version
            raise Conflict("pointer_cas")
        run = self.get_run(command.version.tenant_id, command.version.run_id)
        if run is None or run.state is not RunState.PUBLISHING:
            raise Conflict("run_not_publishing")
        updated = run.model_copy(
            update={"state": command.final_state, "missing_sources": command.missing_sources}
        )
        result = DatasetPointer(
            tenant_id=command.version.tenant_id,
            dataset_name=command.version.dataset_name,
            pointer_name=command.pointer_name,
            version_id=command.version.version_id,
            updated_at=self.clock.now(),
        )
        self.versions[version_key] = command.version
        self.pointers[pointer_key] = result
        self.put_run(updated)
        self.outbox.setdefault(command.event.event_id, command.event)
        return result

@pytest.fixture
def clock() -> MutableClock:
    return MutableClock(_NOW)


@pytest.fixture
def fake_control_plane(clock: MutableClock) -> FakeControlPlane:
    return FakeControlPlane(clock)


@pytest.fixture
def fake_object_store() -> _MemoryObjectStore:
    return _MemoryObjectStore()

@pytest.mark.parametrize("case", control_plane_cases(), ids=lambda case: case.name)
def test_fake_control_plane_conforme(case, fake_control_plane, clock):
    case.run(fake_control_plane, clock)


@pytest.mark.parametrize("case", object_store_cases(), ids=lambda case: case.name)
def test_fake_object_store_conforme(case, fake_object_store, clock):
    case.run(fake_object_store, clock)


@pytest.mark.parametrize("case", control_plane_cases(), ids=lambda case: case.name)
def test_mutacao_do_control_plane_e_detectada(case: ControlPlaneCase, clock: MutableClock) -> None:
    with pytest.raises(AssertionError, match=f"case={case.name}"):
        case.run(FakeControlPlane(clock, case.name), clock)


@pytest.mark.parametrize("case", object_store_cases(), ids=lambda case: case.name)
def test_mutacao_do_object_store_e_detectada(case: ObjectStoreCase, clock: MutableClock) -> None:
    with pytest.raises(AssertionError, match=f"case={case.name}"):
        case.run(_MemoryObjectStore(case.name), clock)


@pytest.mark.parametrize(
    ("catalog", "case_type"),
    [(control_plane_cases(), ControlPlaneCase), (object_store_cases(), ObjectStoreCase)],
)
def test_catalogo_e_estavel_e_congelado(
    catalog: tuple[Any, ...], case_type: type[Any]
) -> None:
    assert catalog == tuple(catalog)
    assert len({case.name for case in catalog}) == len(catalog)
    assert all(isinstance(case, case_type) for case in catalog)
    with pytest.raises(FrozenInstanceError):
        catalog[0].name = "alterado"


def test_rejeita_adapter_parcial(clock: MutableClock, fake_control_plane: FakeControlPlane) -> None:
    class Parcial:
        def __getattr__(self, name: str) -> Any:
            return getattr(fake_control_plane, name)

    with pytest.raises(AssertionError, match="case=authorization_jobs"):
        control_plane_cases()[0].run(Parcial(), clock)
