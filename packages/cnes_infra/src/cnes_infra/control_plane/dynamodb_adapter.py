"""DynamoDB control-plane adapter."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from cnes_domain.control_plane.entities import (
    AccessRequest,
    Agent,
    Job,
    ManifestRef,
    Membership,
    OutboxEvent,
    RawManifestRecord,
    Run,
    RunUnit,
    Tenant,
)
from cnes_domain.control_plane.enums import AgentState, JobState, RunState, RunUnitState
from cnes_domain.control_plane.errors import Conflict, NotFound
from cnes_domain.control_plane.transitions import (
    transition_job,
    transition_run,
    transition_run_unit,
)
from cnes_infra.control_plane.dynamodb_claims import DynamoDBClaims
from cnes_infra.control_plane.dynamodb_codec import (
    Action,
    CandidateQuery,
    Item,
    aggregate_replay,
    bounded_candidates,
    check_action,
    decode_model,
    encode_marker,
    encode_model,
    execute_transaction,
    payload,
    put_action,
    query_partition,
    raw_head_chain,
    raw_manifest_actions,
)
from cnes_infra.control_plane.dynamodb_keys import (
    dependency_marker_key,
    entity_key,
    item_key,
    key_component,
    raw_partition,
    run_entity_key,
    run_partition,
    timestamp,
    unit_key,
)
from cnes_infra.control_plane.dynamodb_publication import DynamoDBPublication

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from cnes_domain.control_plane.commands import (
        CancelJob,
        FinalizeRunCancellation,
        PutRunUnits,
        TransitionRun,
    )

_RECOVERABLE = {
    RunState.WAITING_INPUTS,
    RunState.PROCESSING,
    RunState.PUBLISHING,
    RunState.CANCEL_REQUESTED,
}
_NONTERMINAL_UNITS = {
    RunUnitState.PENDING,
    RunUnitState.LEASED,
    RunUnitState.FAILED_RETRYABLE,
}


class DynamoDBControlPlane(DynamoDBClaims, DynamoDBPublication):
    """Persiste o plano de controle em uma tabela DynamoDB."""

    def __init__(self, client: Any, table_name: str, clock: Callable[[], datetime]) -> None:
        self._client = client
        self._table_name = table_name
        self._clock = clock
    def _get_item(self, key: tuple[str, str]) -> Item | None:
        response = self._client.get_item(
            TableName=self._table_name,
            Key=item_key(*key),
            ConsistentRead=True,
        )
        return response.get("Item")
    def _get_model[T: BaseModel](self, key: tuple[str, str], model_type: type[T]) -> T | None:
        item = self._get_item(key)
        return decode_model(item, model_type) if item is not None else None
    def _query[T: BaseModel](
        self, index_name: str, partition: str, query: CandidateQuery[T]
    ) -> tuple[T, ...]:
        request = {
            "TableName": self._table_name,
            "IndexName": index_name,
            "KeyConditionExpression": f"{index_name}pk = :partition",
            "ExpressionAttributeValues": {":partition": {"S": partition}},
        }
        return bounded_candidates(self._client, request, query)
    def _transact(self, actions: tuple[Action, ...]) -> None:
        execute_transaction(self._client, actions)
    def _put_direct(self, item: Item) -> None:
        self._client.put_item(TableName=self._table_name, Item=item)
    def _job_item(self, job: Job) -> Item:
        values = (job.tenant_id, job.source_type, job.file_subtype, job.competencia)
        identity = "RAW#" + "#".join(key_component(value) for value in values)
        attributes = {
            "gsi2pk": identity,
            "gsi2sk": (
                f"JOB#{timestamp(job.created_at)}#{key_component(job.agent_id)}#"
                f"{key_component(job.job_id)}"
            ),
        }
        if job.state in {JobState.PENDING, JobState.LEASED, JobState.FAILED_RETRYABLE}:
            attributes |= {
                "gsi1pk": (
                    f"JOB_CLAIM#{key_component(job.tenant_id)}#{key_component(job.agent_id)}"
                ),
                "gsi1sk": f"{timestamp(job.created_at)}#{key_component(job.job_id)}",
            }
        key = entity_key(job.tenant_id, "JOB", job.job_id)
        return encode_model(job, "JOB", key, attributes)
    def _raw_item(self, record: RawManifestRecord) -> Item:
        values = (record.tenant_id, record.source_type, record.file_subtype, record.competencia)
        identity = "RAW#" + "#".join(key_component(value) for value in values)
        attributes = {
            "gsi2pk": identity,
            "gsi2sk": (
                f"RAW#{record.sequence:020d}#{timestamp(record.created_at)}#"
                f"{key_component(record.agent_id)}#{key_component(record.snapshot_id)}"
            ),
        }
        partition = raw_partition(
            record.tenant_id, record.source_type, record.file_subtype, record.competencia
        )
        key = partition, f"MANIFEST#{key_component(record.manifest_id)}"
        return encode_model(record, "RAWMANIFESTRECORD", key, attributes)
    def _raw_actions(self, record: RawManifestRecord) -> tuple[Action, ...]:
        return raw_manifest_actions(self._client, self._table_name, record, self._raw_item(record))
    def _run_item(self, run: Run) -> Item:
        attributes = {}
        if run.state in _RECOVERABLE:
            attributes = {
                "gsi4pk": "RUN_RECOVERABLE",
                "gsi4sk": (
                    f"{timestamp(run.created_at)}#{key_component(run.tenant_id)}#"
                    f"{key_component(run.run_id)}"
                ),
            }
        return encode_model(run, "RUN", run_entity_key(run.tenant_id, run.run_id), attributes)
    def _unit_item(self, unit: RunUnit) -> Item:
        attributes = {
            "gsi5pk": (f"RUN_ITEMS#{key_component(unit.tenant_id)}#{key_component(unit.run_id)}"),
            "gsi5sk": f"UNIT#{key_component(unit.unit_id)}",
        }
        return encode_model(
            unit,
            "RUNUNIT",
            unit_key(unit.tenant_id, unit.run_id, unit.unit_id),
            attributes,
        )
    def get_tenant(self, tenant_id: str) -> Tenant | None:
        """Retorna o tenant solicitado."""
        return self._get_model(entity_key(tenant_id, "TENANT", tenant_id), Tenant)
    def put_tenant(self, tenant: Tenant) -> None:
        """Persiste um tenant."""
        self._put_direct(
            encode_model(tenant, "TENANT", entity_key(tenant.tenant_id, "TENANT", tenant.tenant_id))
        )
    def get_membership(self, tenant_id: str, user_id: str) -> Membership | None:
        """Retorna a associação solicitada."""
        return self._get_model(entity_key(tenant_id, "MEMBERSHIP", user_id), Membership)
    def put_membership(self, membership: Membership) -> None:
        """Persiste uma associação."""
        key = entity_key(membership.tenant_id, "MEMBERSHIP", membership.user_id)
        self._put_direct(encode_model(membership, "MEMBERSHIP", key))
    def get_agent(self, tenant_id: str, agent_id: str) -> Agent | None:
        """Retorna o agente solicitado."""
        return self._get_model(entity_key(tenant_id, "AGENT", agent_id), Agent)
    def put_agent(self, agent: Agent) -> None:
        """Persiste um agente."""
        key = entity_key(agent.tenant_id, "AGENT", agent.agent_id)
        self._put_direct(encode_model(agent, "AGENT", key))
    def create_job(self, job: Job, event: OutboxEvent) -> Job:
        """Cria um job e seu evento atomicamente."""
        key = entity_key(job.tenant_id, "JOB", job.job_id)
        existing = self._get_model(key, Job)
        if existing is not None:
            if existing != job:
                raise Conflict("job_conflict")
            self._require_event_replay(job.tenant_id, event)
            return existing
        actions = (
            put_action(self._table_name, self._job_item(job), None),
            self._event_action(job.tenant_id, event),
        )
        try:
            self._transact(actions)
        except Conflict:
            winner = self._get_model(key, Job)
            if winner is None:
                raise
            if winner != job:
                raise Conflict("job_conflict") from None
            self._require_event_replay(job.tenant_id, event)
            return winner
        return job
    def get_job(self, tenant_id: str, job_id: str) -> Job | None:
        """Retorna o job solicitado."""
        return self._get_model(entity_key(tenant_id, "JOB", job_id), Job)
    def latest_succeeded_job(
        self, tenant_id: str, agent_id: str, source_type: str, file_subtype: str,
        competencia: str) -> Job | None:
        """Retorna o job concluído mais recente da identidade.
        Args: tenant_id, agent_id, source_type, file_subtype, competencia.
        Returns: Job encontrado, se houver.
        """
        partition = raw_partition(tenant_id, source_type, file_subtype, competencia)
        return self._get_model((partition, f"LATEST_JOB#{key_component(agent_id)}"), Job)
    def _latest_job_action(self, job: Job) -> Action:
        partition = raw_partition(job.tenant_id, job.source_type, job.file_subtype, job.competencia)
        key = partition, f"LATEST_JOB#{key_component(job.agent_id)}"
        current_item = self._get_item(key)
        marker = encode_model(job, "JOB", key)
        if current_item is None:
            return put_action(self._table_name, marker, None)
        current = decode_model(current_item, Job)
        if (current.created_at, current.job_id) >= (job.created_at, job.job_id):
            return check_action(self._table_name, current_item)
        return put_action(self._table_name, marker, payload(current_item))
    def list_raw_manifest_chain(
        self, tenant_id: str, source_type: str, file_subtype: str,
        competencia: str, limit: int = 31) -> tuple[ManifestRef, ...]:
        """Retorna a cadeia válida de manifestos raw.
        Args: tenant_id, source_type, file_subtype, competencia, limit.
        Returns: Manifestos ordenados da cadeia válida.
        """
        partition = raw_partition(tenant_id, source_type, file_subtype, competencia)
        return raw_head_chain(self._client, self._table_name, partition, limit)
    def list_claimable_jobs(self, tenant_id: str, agent_id: str, limit: int) -> tuple[Job, ...]:
        """Lista jobs elegíveis para claim."""
        agent = self.get_agent(tenant_id, agent_id)
        if agent is None or agent.state is not AgentState.ACTIVE:
            return ()
        partition = f"JOB_CLAIM#{key_component(tenant_id)}#{key_component(agent_id)}"
        eligible = self._query(
            "gsi1", partition,
            CandidateQuery(Job, lambda job: self._job_is_claimable(job, self._clock()), limit),
        )
        return tuple(sorted(eligible, key=lambda job: (job.created_at, job.job_id))[:limit])
    @staticmethod
    def _job_is_claimable(job: Job, now: datetime) -> bool:
        if job.state in {JobState.PENDING, JobState.FAILED_RETRYABLE}:
            return True
        return (
            job.state is JobState.LEASED and job.lease_until is not None and job.lease_until <= now
        )
    def _dependency_actions(self, run: Run, reserved_actions: int) -> tuple[Action, ...]:
        if run.state is not RunState.WAITING_INPUTS:
            return ()
        if len(run.dependencies) + reserved_actions > 100:
            raise Conflict("transaction_limit")
        base_key = run_entity_key(run.tenant_id, run.run_id)
        actions = []
        for dependency in run.dependencies:
            values = (run.tenant_id, dependency.source_type,
                      dependency.file_subtype, run.competencia)
            identity = "RUN_DEP#" + "#".join(key_component(value) for value in values)
            marker_key = dependency_marker_key(run.tenant_id, run.run_id, identity)
            attributes = {
                "gsi3pk": identity,
                "gsi3sk": f"{timestamp(run.created_at)}#{key_component(run.run_id)}",
            }
            marker = encode_marker("RUN_DEP", marker_key, base_key, attributes)
            actions.append(put_action(self._table_name, marker, None))
        return tuple(actions)
    def put_run(self, run: Run) -> None:
        """Persiste um run e seus índices de dependência."""
        if run.state is not RunState.WAITING_INPUTS:
            self._put_direct(self._run_item(run))
            return
        if self._waiting_run_replay(run):
            return
        actions = (put_action(self._table_name, self._run_item(run), None),
                   *self._dependency_actions(run, 1))
        try:
            self._transact(actions)
        except Conflict:
            if self._waiting_run_replay(run):
                return
            raise
    def _waiting_run_replay(self, run: Run) -> bool:
        actions = self._dependency_actions(run, 1)
        children = tuple(action["Put"]["Item"] for action in actions)
        child_key = dependency_marker_key(run.tenant_id, run.run_id, "")
        expected = self._run_item(run), child_key, children
        return aggregate_replay(self._client, self._table_name, expected)
    def get_run(self, tenant_id: str, run_id: str) -> Run | None:
        """Retorna o run solicitado."""
        return self._get_model(run_entity_key(tenant_id, run_id), Run)
    def list_waiting_runs_for_dependency(
        self, tenant_id: str, source_type: str, file_subtype: str,
        competencia: str, limit: int = 100) -> tuple[Run, ...]:
        """Lista runs aguardando uma dependência.
        Args: tenant_id, source_type, file_subtype, competencia, limit.
        Returns: Runs elegíveis ordenados.
        """
        values = (tenant_id, source_type, file_subtype, competencia)
        identity = "RUN_DEP#" + "#".join(key_component(value) for value in values)
        def valid(run: Run) -> bool:
            return (
                run.state is RunState.WAITING_INPUTS
                and run.tenant_id == tenant_id
                and run.competencia == competencia
                and any((dep.source_type, dep.file_subtype) == (source_type, file_subtype)
                        for dep in run.dependencies)
            )
        runs = self._query("gsi3", identity, CandidateQuery(Run, valid, limit))
        return tuple(sorted(runs, key=lambda run: (run.created_at, run.run_id))[:limit])
    def list_recoverable_runs(self, now: datetime, limit: int = 100) -> tuple[Run, ...]:
        """Lista runs recuperáveis no instante informado."""
        valid = self._query(
            "gsi4", "RUN_RECOVERABLE",
            CandidateQuery(
                Run, lambda run: run.state in _RECOVERABLE and run.created_at <= now, limit
            ),
        )
        ordered = sorted(valid, key=lambda run: (run.created_at, run.tenant_id, run.run_id))
        return tuple(ordered[:limit])
    def transition_run(self, command: TransitionRun, event: OutboxEvent) -> Run:
        """Transiciona um run e grava o evento atomicamente."""
        key = run_entity_key(command.tenant_id, command.run_id)
        item = self._get_item(key)
        if item is None:
            raise NotFound("run_missing")
        run = decode_model(item, Run)
        if run.state is not command.expected_state:
            raise Conflict("run_state_conflict")
        updated = transition_run(run, command.new_state).model_copy(
            update={"missing_sources": command.missing_sources}
        )
        actions = (
            put_action(self._table_name, self._run_item(updated), payload(item)),
            *self._dependency_actions(updated, 2),
            self._event_action(command.tenant_id, event),
        )
        self._transact(actions)
        return updated
    def put_run_units(self, command: PutRunUnits) -> tuple[RunUnit, ...]:
        """Persiste o grafo de unidades atomicamente."""
        units = tuple(sorted(command.units, key=lambda unit: unit.unit_id))
        if len(units) >= 100:
            raise Conflict("transaction_limit")
        run_item = self._get_item(run_entity_key(command.tenant_id, command.run_id))
        if run_item is None:
            raise NotFound("run_missing")
        run = decode_model(run_item, Run)
        if run.state is not command.expected_run_state:
            raise Conflict("run_state_conflict")
        existing = tuple(
            decode_model(item, RunUnit)
            for item in query_partition(
                self._client, self._table_name,
                run_partition(command.tenant_id, command.run_id), "UNIT#"
            )
        )
        if any(existing):
            if existing == units:
                return units
            raise Conflict("run_units_conflict")
        actions = (check_action(self._table_name, run_item),) + tuple(
            put_action(self._table_name, self._unit_item(unit), None) for unit in units
        )
        self._transact(actions)
        return units
    def list_run_units(self, tenant_id: str, run_id: str) -> tuple[RunUnit, ...]:
        """Lista as unidades do run."""
        partition = run_partition(tenant_id, run_id)
        items = query_partition(self._client, self._table_name, partition, "UNIT#")
        units = (decode_model(item, RunUnit) for item in items)
        return tuple(sorted(units, key=lambda unit: unit.unit_id))
    def cancel_job(self, command: CancelJob, event: OutboxEvent) -> Job:
        """Solicita o cancelamento de um job leased."""
        key = entity_key(command.tenant_id, "JOB", command.job_id)
        item = self._get_item(key)
        if item is None:
            raise NotFound("job_missing")
        job = decode_model(item, Job)
        if job.state is JobState.CANCEL_REQUESTED:
            self._require_event_replay(job.tenant_id, event)
            return job
        if job.state is not JobState.LEASED:
            raise Conflict("job_state_conflict")
        updated = transition_job(job, JobState.CANCEL_REQUESTED)
        self._transact(
            (
                put_action(self._table_name, self._job_item(updated), payload(item)),
                self._event_action(command.tenant_id, event),
            )
        )
        return updated
    def finalize_run_cancellation(
        self, command: FinalizeRunCancellation, event: OutboxEvent
    ) -> Run:
        """Finaliza o cancelamento do agregado do run."""
        run_key = run_entity_key(command.tenant_id, command.run_id)
        run_item = self._get_item(run_key)
        if run_item is None:
            raise NotFound("run_missing")
        run = decode_model(run_item, Run)
        if run.state is RunState.CANCELED:
            self._require_event_replay(run.tenant_id, event)
            return run
        if run.state is not command.expected_state:
            raise Conflict("run_state_conflict")
        items = query_partition(
            self._client,
            self._table_name,
            run_partition(command.tenant_id, command.run_id),
            "UNIT#",
        )
        units = tuple(decode_model(item, RunUnit) for item in items)
        cancellable = tuple(unit for unit in units if unit.state in _NONTERMINAL_UNITS)
        if len(cancellable) >= 99:
            raise Conflict("transaction_limit")
        updated_run = transition_run(run, RunState.CANCELED)
        actions = [put_action(self._table_name, self._run_item(updated_run), payload(run_item))]
        actions.extend(self._cancel_unit_action(unit, run) for unit in cancellable)
        actions.append(self._event_action(command.tenant_id, event))
        self._transact(tuple(actions))
        return updated_run
    def _cancel_unit_action(self, unit: RunUnit, run: Run) -> Action:
        canceled = transition_run_unit(unit, RunUnitState.CANCELED, run).model_copy(
            update={"lease_owner": None, "lease_until": None}
        )
        expected = payload(self._unit_item(unit))
        return put_action(self._table_name, self._unit_item(canceled), expected)
    def put_access_request(self, request: AccessRequest, event: OutboxEvent) -> None:
        """Cria uma solicitação de acesso atomicamente."""
        key = entity_key(request.tenant_id, "ACCESS", request.request_id)
        item = encode_model(request, "ACCESSREQUEST", key)
        existing = self._get_model(key, AccessRequest)
        if existing is not None:
            self._require_access_replay(request, event, existing)
            return
        try:
            self._transact(
                (
                    put_action(self._table_name, item, None),
                    self._event_action(request.tenant_id, event),
                )
            )
        except Conflict:
            winner = self._get_model(key, AccessRequest)
            if winner is None:
                raise
            self._require_access_replay(request, event, winner)
    def _require_access_replay(
        self, request: AccessRequest, event: OutboxEvent, existing: AccessRequest
    ) -> None:
        if existing != request:
            raise Conflict("access_request_conflict")
        self._require_event_replay(request.tenant_id, event)
    def get_access_request(self, tenant_id: str, request_id: str) -> AccessRequest | None:
        """Retorna a solicitação de acesso."""
        return self._get_model(entity_key(tenant_id, "ACCESS", request_id), AccessRequest)
    def decide_access_request(self, request: AccessRequest, event: OutboxEvent) -> AccessRequest:
        """Persiste a decisão de acesso atomicamente."""
        key = entity_key(request.tenant_id, "ACCESS", request.request_id)
        item = self._get_item(key)
        if item is None:
            raise NotFound("access_request_missing")
        existing = decode_model(item, AccessRequest)
        if existing == request:
            self._require_event_replay(request.tenant_id, event)
            return existing
        original_identity = (existing.tenant_id, existing.request_id, existing.user_id)
        requested_identity = (request.tenant_id, request.request_id, request.user_id)
        if existing.state.value != "PENDING" or original_identity != requested_identity:
            raise Conflict("access_request_conflict")
        updated = encode_model(request, "ACCESSREQUEST", key)
        self._transact(
            (
                put_action(self._table_name, updated, payload(item)),
                self._event_action(request.tenant_id, event),
            )
        )
        return request
