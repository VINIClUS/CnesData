"""DynamoDB lease, fence, unit, and dispatch transactions."""

from __future__ import annotations

from datetime import timedelta
from hashlib import sha256
from typing import TYPE_CHECKING, Any

from botocore.exceptions import ConnectionClosedError, ReadTimeoutError

from cnes_domain.control_plane.entities import Agent, Job, Run, RunDispatch, RunUnit
from cnes_domain.control_plane.enums import (
    AgentState,
    DispatchState,
    JobState,
    RunState,
    RunUnitState,
)
from cnes_domain.control_plane.errors import Conflict, FenceRejected, LeaseLost, NotFound
from cnes_domain.control_plane.transitions import transition_job, transition_run_unit
from cnes_infra.control_plane.dynamodb_codec import (
    Action,
    Item,
    check_action,
    decode_model,
    encode_model,
    payload,
    put_action,
)
from cnes_infra.control_plane.dynamodb_keys import (
    dispatch_key,
    entity_key,
    key_component,
    run_entity_key,
    unit_key,
)

if TYPE_CHECKING:
    from cnes_domain.control_plane.commands import (
        BindRunDispatch,
        ClaimJob,
        ClaimRunUnit,
        CommitRunUnit,
        CompleteJob,
        FailJob,
        FailRunUnit,
        FinishRunDispatch,
        RenewJobLease,
        ReserveRunDispatch,
    )


class DynamoDBClaims:
    """Implementa decisões protegidas por lease e fencing token."""

    def _dispatch_item(self, dispatch: RunDispatch) -> Item:
        attributes = {
            "gsi5pk": (
                f"RUN_ITEMS#{key_component(dispatch.tenant_id)}#{key_component(dispatch.run_id)}"
            ),
            "gsi5sk": "DISPATCH#ACTIVE",
        }
        key = dispatch_key(dispatch.tenant_id, dispatch.run_id)
        return encode_model(dispatch, "RUNDISPATCH", key, attributes)
    def claim_job(self, command: ClaimJob) -> Job | None:
        """Reivindica um job elegível com novo fence."""
        now = self._clock()
        key = entity_key(command.tenant_id, "JOB", command.job_id)
        item = self._get_item(key)
        if item is None:
            return None
        job = decode_model(item, Job)
        agent_item = self._get_item(entity_key(command.tenant_id, "AGENT", job.agent_id))
        if agent_item is None or decode_model(agent_item, Agent).state is not AgentState.ACTIVE:
            return None
        if not self._job_is_claimable(job, now):
            return None
        leased = transition_job(job, JobState.LEASED) if job.state is not JobState.LEASED else job
        updated = leased.model_copy(
            update={
                "attempt": job.attempt + 1,
                "fencing_token": job.fencing_token + 1,
                "lease_owner": command.owner,
                "lease_until": now + timedelta(seconds=command.lease_seconds),
                "error_code": None,
            }
        )
        actions = (
            check_action(self._table_name, agent_item),
            put_action(self._table_name, self._job_item(updated), payload(item)),
        )
        try:
            self._transact(actions)
        except Conflict:
            return None
        return updated
    def renew_job_lease(self, command: RenewJobLease) -> Job:
        """Renova o lease de um job fenced."""
        now = self._clock()
        item, job = self._leased_job(command.tenant_id, command.job_id)
        self._validate_job_fence(job, command.owner, command.fencing_token, now)
        agent_item = self._active_agent_item(job)
        updated = job.model_copy(
            update={"lease_until": now + timedelta(seconds=command.lease_seconds)}
        )
        self._transact(
            (
                check_action(self._table_name, agent_item),
                put_action(self._table_name, self._job_item(updated), payload(item)),
            )
        )
        return updated
    def _complete_job_actions(
        self, command: CompleteJob, event: Any
    ) -> tuple[Job, tuple[Action, ...]]:
        item, job = self._leased_job(command.tenant_id, command.job_id)
        self._validate_job_fence(job, command.owner, command.fencing_token, self._clock())
        agent_item = self._active_agent_item(job)
        identity = (job.tenant_id, job.agent_id, job.source_type, job.file_subtype, job.competencia)
        manifest_identity = (command.manifest.tenant_id, command.manifest.agent_id,
                             command.manifest.source_type, command.manifest.file_subtype,
                             command.manifest.competencia)
        if identity != manifest_identity:
            raise Conflict("manifest_identity_conflict")
        updated = Job.model_validate(
            job.model_dump() | {
                "state": JobState.SUCCEEDED,
                "lease_owner": None,
                "lease_until": None,
                "result_manifest_id": command.manifest.manifest_id,
                "result_manifest_key": command.manifest.manifest_key,
            }
        )
        actions = (check_action(self._table_name, agent_item),
            put_action(self._table_name, self._job_item(updated), payload(item)),
            *self._raw_actions(command.manifest),
            self._latest_job_action(updated),
            self._event_action(job.tenant_id, event),
        )
        return updated, actions
    def _complete_job_replay(self, command: CompleteJob, event: Any, updated: Job) -> bool:
        raw_item = self._raw_item(command.manifest)
        return (
            self.get_job(command.tenant_id, command.job_id) == updated
            and self._get_model(
                (raw_item["pk"]["S"], raw_item["sk"]["S"]), type(command.manifest)
            ) == command.manifest
            and self.latest_succeeded_job(
                command.tenant_id, command.manifest.agent_id, command.manifest.source_type,
                command.manifest.file_subtype, command.manifest.competencia,
            ) == updated
            and self._event_replay_matches(self._get_outbox_event(event.event_id), event)
        )
    def complete_job(self, command: CompleteJob, event: Any) -> Job:
        """Conclui job, manifesto e evento atomicamente."""
        conflict = None
        for _ in range(3):
            updated, actions = self._complete_job_actions(command, event)
            try:
                self._transact(actions)
            except Conflict as error:
                conflict = error
                if self._complete_job_replay(command, event, updated):
                    return updated
                continue
            return updated
        raise TimeoutError("transaction_contention") from conflict
    def fail_job(self, command: FailJob, event: Any) -> Job:
        """Falha um job leased e grava seu evento."""
        item, job = self._leased_job(command.tenant_id, command.job_id)
        self._validate_job_fence(job, command.owner, command.fencing_token, self._clock())
        agent_item = self._active_agent_item(job)
        state = JobState.FAILED_RETRYABLE if command.retryable else JobState.FAILED_FINAL
        updated = transition_job(job, state).model_copy(
            update={
                "lease_owner": None,
                "lease_until": None,
                "error_code": command.error_code,
            }
        )
        self._transact(
            (
                check_action(self._table_name, agent_item),
                put_action(self._table_name, self._job_item(updated), payload(item)),
                self._event_action(job.tenant_id, event),
            )
        )
        return updated
    def _leased_job(self, tenant_id: str, job_id: str) -> tuple[Item, Job]:
        item = self._get_item(entity_key(tenant_id, "JOB", job_id))
        if item is None:
            raise NotFound("job_missing")
        job = decode_model(item, Job)
        if job.state is not JobState.LEASED:
            raise LeaseLost("job_not_leased")
        return item, job
    @staticmethod
    def _validate_job_fence(job: Job, owner: str, fence: int, now: Any) -> None:
        if job.fencing_token != fence:
            raise FenceRejected("job_fence_rejected")
        if job.lease_owner != owner:
            raise LeaseLost("job_owner_lost")
        if job.lease_until is None or job.lease_until <= now:
            raise LeaseLost("job_lease_expired")
    def _active_agent_item(self, job: Job) -> Item:
        item = self._get_item(entity_key(job.tenant_id, "AGENT", job.agent_id))
        if item is None or decode_model(item, Agent).state is not AgentState.ACTIVE:
            raise LeaseLost("agent_revoked")
        return item
    def claim_run_unit(self, command: ClaimRunUnit) -> RunUnit | None:
        """Reivindica uma unidade despachada com novo fence."""
        context = self._unit_claim_context(command)
        if context is None:
            return None
        run_item, dispatch_item, unit_item, unit = context
        leased = (
            transition_run_unit(unit, RunUnitState.LEASED)
            if unit.state is not RunUnitState.LEASED
            else unit
        )
        updated = leased.model_copy(
            update={
                "attempt": unit.attempt + 1,
                "fencing_token": unit.fencing_token + 1,
                "lease_owner": command.owner,
                "lease_until": command.now + timedelta(seconds=command.lease_seconds),
                "dispatch_id": command.dispatch_id,
                "error_code": None,
            }
        )
        actions = (
            check_action(self._table_name, run_item),
            check_action(self._table_name, dispatch_item),
            put_action(self._table_name, self._unit_item(updated), payload(unit_item)),
        )
        try:
            self._transact(actions)
        except Conflict:
            return None
        return updated
    def _unit_claim_context(self, command: ClaimRunUnit) -> tuple[Any, ...] | None:
        run_item = self._get_item(run_entity_key(command.tenant_id, command.run_id))
        dispatch_item = self._get_item(dispatch_key(command.tenant_id, command.run_id))
        unit_item = self._get_item(unit_key(command.tenant_id, command.run_id, command.unit_id))
        if run_item is None or dispatch_item is None or unit_item is None:
            return None
        run = decode_model(run_item, Run)
        dispatch = decode_model(dispatch_item, RunDispatch)
        unit = decode_model(unit_item, RunUnit)
        if (
            run.state is not RunState.PROCESSING
            or dispatch.dispatch_id != command.dispatch_id
            or dispatch.state not in {DispatchState.RESERVED, DispatchState.STARTED}
            or dispatch.lease_until <= command.now
            or command.unit_id not in dispatch.unit_ids
            or not self._unit_is_claimable(unit, command.now)
        ):
            return None
        return run_item, dispatch_item, unit_item, unit
    def commit_run_unit(self, command: CommitRunUnit, event: Any) -> RunUnit:
        """Confirma a saída de uma unidade fenced."""
        context = self._leased_unit_context(command)
        run_item, dispatch_item, unit_item, run, unit = context
        candidate = unit.model_copy(
            update={
                "output_manifests": command.output_manifests,
                "lease_owner": None,
                "lease_until": None,
            }
        )
        updated = transition_run_unit(candidate, RunUnitState.SUCCEEDED, run)
        actions = (
            check_action(self._table_name, run_item),
            check_action(self._table_name, dispatch_item),
            put_action(self._table_name, self._unit_item(updated), payload(unit_item)),
            self._event_action(command.tenant_id, event),
        )
        token = self._commit_client_request_token(command, event)
        try:
            self._transact(actions, token)
        except (ConnectionClosedError, ReadTimeoutError):
            try:
                self._transact(actions, token)
            except (Conflict, ConnectionClosedError, ReadTimeoutError):
                if self._commit_run_unit_replay(command, event, updated):
                    return updated
                raise
        except Conflict:
            if self._commit_run_unit_replay(command, event, updated):
                return updated
            raise
        return updated

    @staticmethod
    def _commit_client_request_token(command: CommitRunUnit, event: Any) -> str:
        values = (command.tenant_id, command.run_id, command.unit_id, event.event_id)
        return sha256("\x1f".join(values).encode()).hexdigest()[:36]

    def _commit_run_unit_replay(
        self, command: CommitRunUnit, event: Any, updated: RunUnit
    ) -> bool:
        winner = self._get_model(
            unit_key(command.tenant_id, command.run_id, command.unit_id), RunUnit
        )
        return winner == updated and self._event_replay_matches(
            self._get_outbox_event(event.event_id), event
        )
    def _fail_run_unit_actions(
        self, command: FailRunUnit, event: Any
    ) -> tuple[RunUnit, tuple[Action, ...]]:
        context = self._leased_unit_context(command)
        run_item, dispatch_item, unit_item, run, unit = context
        state = self._failed_unit_state(run, unit, command.retryable)
        candidate = unit.model_copy(
            update={
                "error_code": command.error_code,
                "lease_owner": None,
                "lease_until": None,
            }
        )
        updated = transition_run_unit(candidate, state, run)
        actions: list[Action] = [
            check_action(self._table_name, dispatch_item),
            put_action(self._table_name, self._unit_item(updated), payload(unit_item)),
        ]
        if state is RunUnitState.SUCCEEDED_DEGRADED:
            source = f"{unit.source_type}/{unit.file_subtype}"
            missing = tuple(sorted({*run.missing_sources, source}))
            updated_run = run.model_copy(update={"missing_sources": missing})
            actions.append(
                put_action(self._table_name, self._run_item(updated_run), payload(run_item))
            )
        else:
            actions.append(check_action(self._table_name, run_item))
        actions.append(self._event_action(command.tenant_id, event))
        return updated, tuple(actions)
    def fail_run_unit(self, command: FailRunUnit, event: Any) -> RunUnit:
        """Falha ou degrada uma unidade fenced."""
        updated, actions = self._fail_run_unit_actions(command, event)
        try:
            self._transact(actions)
        except Conflict:
            updated, actions = self._fail_run_unit_actions(command, event)
            self._transact(actions)
        return updated
    def _leased_unit_context(self, command: Any) -> tuple[Any, ...]:
        run_item = self._get_item(run_entity_key(command.tenant_id, command.run_id))
        dispatch_item = self._get_item(dispatch_key(command.tenant_id, command.run_id))
        unit_item = self._get_item(unit_key(command.tenant_id, command.run_id, command.unit_id))
        if run_item is None or dispatch_item is None or unit_item is None:
            raise NotFound("unit_context_missing")
        run = decode_model(run_item, Run)
        dispatch = decode_model(dispatch_item, RunDispatch)
        unit = decode_model(unit_item, RunUnit)
        if run.state is not RunState.PROCESSING:
            raise LeaseLost("run_not_processing")
        self._validate_dispatch_lease(dispatch, command.dispatch_id, self._clock())
        self._validate_unit_fence(unit, command, self._clock())
        return run_item, dispatch_item, unit_item, run, unit
    @staticmethod
    def _validate_dispatch_lease(dispatch: RunDispatch, dispatch_id: str, now: Any) -> None:
        if dispatch.dispatch_id != dispatch_id:
            raise FenceRejected("dispatch_fence_rejected")
        if dispatch.state not in {DispatchState.RESERVED, DispatchState.STARTED}:
            raise LeaseLost("dispatch_terminal")
        if dispatch.lease_until <= now:
            raise LeaseLost("dispatch_expired")
    @staticmethod
    def _validate_unit_fence(unit: RunUnit, command: Any, now: Any) -> None:
        if unit.fencing_token != command.fencing_token:
            raise FenceRejected("unit_fence_rejected")
        if unit.dispatch_id != command.dispatch_id:
            raise FenceRejected("unit_dispatch_rejected")
        if unit.lease_owner != command.owner:
            raise LeaseLost("unit_owner_lost")
        if unit.lease_until is None or unit.lease_until <= now:
            raise LeaseLost("unit_lease_expired")

    @staticmethod
    def _failed_unit_state(run: Run, unit: RunUnit, retryable: bool) -> RunUnitState:
        if retryable:
            return RunUnitState.FAILED_RETRYABLE
        dependency = next(
            (
                item
                for item in run.dependencies
                if (item.source_type, item.file_subtype) == (unit.source_type, unit.file_subtype)
            ),
            None,
        )
        if dependency is not None and not dependency.required:
            return RunUnitState.SUCCEEDED_DEGRADED
        return RunUnitState.FAILED_FINAL
    def reserve_run_dispatch(self, command: ReserveRunDispatch) -> RunDispatch:
        """Reserva uma geração de dispatch do run."""
        run_item = self._get_item(run_entity_key(command.tenant_id, command.run_id))
        if run_item is None or decode_model(run_item, Run).state is not RunState.PROCESSING:
            raise Conflict("run_not_processing")
        current_item = self._get_item(dispatch_key(command.tenant_id, command.run_id))
        current = decode_model(current_item, RunDispatch) if current_item else None
        replay = self._dispatch_replay(current, command)
        if replay is not None:
            return replay
        prior_unit_items = ()
        if current is not None:
            prior_unit_items = self._replacement_unit_items(current, command.now)
        unit_items = self._dispatch_unit_items(command)
        generation = 1 if current is None else current.generation + 1
        raw_id = f"{command.tenant_id}\x1f{command.run_id}\x1f{command.wave_id}\x1f{generation}"
        dispatch = RunDispatch(
            tenant_id=command.tenant_id,
            run_id=command.run_id,
            wave_id=command.wave_id,
            dispatch_id=sha256(raw_id.encode()).hexdigest()[:16],
            generation=generation,
            unit_ids=command.unit_ids,
            state=DispatchState.RESERVED,
            lease_until=command.now + timedelta(seconds=command.lease_seconds),
        )
        expected = payload(current_item) if current_item is not None else None
        checked_items = {
            (item["pk"]["S"], item["sk"]["S"]): item for item in (*prior_unit_items, *unit_items)
        }
        actions = [check_action(self._table_name, run_item)]
        actions.extend(check_action(self._table_name, item) for item in checked_items.values())
        actions.append(put_action(self._table_name, self._dispatch_item(dispatch), expected))
        self._transact(tuple(actions))
        return dispatch
    @staticmethod
    def _dispatch_replay(
        current: RunDispatch | None, command: ReserveRunDispatch
    ) -> RunDispatch | None:
        if current is None:
            return None
        if current.state is DispatchState.TERMINAL or current.lease_until <= command.now:
            return None
        if current.wave_id != command.wave_id or current.unit_ids != command.unit_ids:
            raise Conflict("active_dispatch_conflict")
        return current
    def _replacement_unit_items(self, dispatch: RunDispatch, now: Any) -> tuple[Item, ...]:
        items = []
        for unit_id in dispatch.unit_ids:
            item = self._get_item(unit_key(dispatch.tenant_id, dispatch.run_id, unit_id))
            if item is None:
                raise Conflict("dispatch_unit_missing")
            unit = decode_model(item, RunUnit)
            live = (
                unit.state is RunUnitState.LEASED
                and unit.lease_until is not None
                and unit.lease_until > now
            )
            if live:
                raise Conflict("dispatch_unit_unavailable")
            items.append(item)
        return tuple(items)
    def _dispatch_unit_items(self, command: ReserveRunDispatch) -> tuple[Item, ...]:
        items = []
        for unit_id in command.unit_ids:
            item = self._get_item(unit_key(command.tenant_id, command.run_id, unit_id))
            if item is None:
                raise Conflict("dispatch_unit_missing")
            unit = decode_model(item, RunUnit)
            if not self._unit_is_claimable(unit, command.now):
                raise Conflict("dispatch_unit_unavailable")
            items.append(item)
        return tuple(items)
    @staticmethod
    def _unit_is_claimable(unit: RunUnit, now: Any) -> bool:
        return unit.state in {RunUnitState.PENDING, RunUnitState.FAILED_RETRYABLE} or (
            unit.state is RunUnitState.LEASED
            and unit.lease_until is not None
            and unit.lease_until <= now
        )
    def bind_run_dispatch(self, command: BindRunDispatch) -> RunDispatch:
        """Vincula o dispatch a uma execução externa."""
        run_item = self._get_item(run_entity_key(command.tenant_id, command.run_id))
        if run_item is None or decode_model(run_item, Run).state is not RunState.PROCESSING:
            raise Conflict("run_not_processing")
        item, dispatch = self._required_dispatch(command.tenant_id, command.run_id)
        self._validate_dispatch_lease(dispatch, command.dispatch_id, command.now)
        if dispatch.state is DispatchState.STARTED:
            if dispatch.execution_ref == command.execution_ref:
                return dispatch
            raise Conflict("dispatch_binding_conflict")
        updated = dispatch.model_copy(
            update={
                "state": DispatchState.STARTED,
                "execution_ref": command.execution_ref,
                "lease_until": command.now + timedelta(seconds=command.lease_seconds),
            }
        )
        self._transact(
            (
                check_action(self._table_name, run_item),
                put_action(self._table_name, self._dispatch_item(updated), payload(item)),
            )
        )
        return updated
    def finish_run_dispatch(self, command: FinishRunDispatch) -> RunDispatch:
        """Finaliza um dispatch ativo."""
        item, dispatch = self._required_dispatch(command.tenant_id, command.run_id)
        if dispatch.dispatch_id != command.dispatch_id:
            raise Conflict("dispatch_id_conflict")
        if dispatch.state is DispatchState.TERMINAL:
            if dispatch.terminal_outcome is command.outcome:
                return dispatch
            raise Conflict("dispatch_outcome_conflict")
        if dispatch.lease_until <= command.finished_at:
            raise Conflict("dispatch_expired")
        updated = dispatch.model_copy(
            update={
                "state": DispatchState.TERMINAL,
                "terminal_outcome": command.outcome,
            }
        )
        self._transact((put_action(self._table_name, self._dispatch_item(updated), payload(item)),))
        return updated
    def _required_dispatch(self, tenant_id: str, run_id: str) -> tuple[Item, RunDispatch]:
        item = self._get_item(dispatch_key(tenant_id, run_id))
        if item is None:
            raise NotFound("dispatch_missing")
        return item, decode_model(item, RunDispatch)
    def get_active_run_dispatch(self, tenant_id: str, run_id: str) -> RunDispatch | None:
        """Retorna o dispatch ativo do run."""
        item = self._get_item(dispatch_key(tenant_id, run_id))
        if item is None:
            return None
        dispatch = decode_model(item, RunDispatch)
        active = (
            dispatch.state is not DispatchState.TERMINAL and dispatch.lease_until > self._clock()
        )
        return dispatch if active else None
