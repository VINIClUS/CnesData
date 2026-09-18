from datetime import timedelta
from decimal import Decimal
from typing import Any

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from cnes_domain.control_plane.commands import (
    BeginIdempotency,
    CancelJob,
    CompleteJob,
    FinalizeRunCancellation,
    PutRunUnits,
    ReserveRunDispatch,
    TransitionRun,
)
from cnes_domain.control_plane.entities import AccessRequest, Tenant
from cnes_domain.control_plane.enums import AccessRequestState, JobState, RunState
from cnes_domain.control_plane.errors import Conflict, NotFound
from cnes_domain.control_plane.errors import ControlPlaneErrorCode as ErrorCode
from cnes_domain.control_plane.queries import (
    RawIdentity,
    RawManifestChainQuery,
    WaitingRunsForDependencyQuery,
)
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_codec import encode_marker
from cnes_infra.control_plane.dynamodb_keys import (
    entity_key,
    key_component,
    outbox_key,
    run_entity_key,
)
from packages.cnes_infra.tests.contracts.clock import (
    _HASH_A,
    _NOW,
    _TENANT,
    MutableClock,
    _agent,
    _claim_job,
    _event,
    _job,
    _raw_record,
    _run,
    _store_record,
    _unit,
)
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import (
    _TABLE_NAME,
    ClientSpy,
    _create_table,
    _put_many_units,
)

_HASH_B = "b" * 64
_IDENTITY = RawIdentity(_TENANT, "CNES", "ST", "2026-07")
type _DynamoContext = tuple[Any, MutableClock, DynamoDBControlPlane]


class OneItemPageClient(ClientSpy):
    def query(self, **kwargs: Any) -> dict[str, Any]:
        kwargs.setdefault("Limit", 1)
        response = super().query(**kwargs)
        if kwargs.get("IndexName") == "gsi6":
            response["Items"] *= 2
        if kwargs.get("IndexName") == "gsi2" and (hidden := getattr(self, "hidden_gsi2sk", None)):
            response["Items"] = [item for item in response["Items"] if item["gsi2sk"] != hidden]
        return response


def _raise_transaction_conflict(_: list[dict[str, Any]]) -> None:
    raise ClientError(
        {
            "Error": {"Code": "TransactionCanceledException", "Message": "cancelled"},
            "CancellationReasons": [{"Code": "ConditionalCheckFailed"}],
        },
        "TransactWriteItems",
    )

def _lose_transaction_response(_: list[dict[str, Any]]) -> None:
    raise Conflict(ErrorCode.TRANSACTION_CONFLICT)

def _submit_raw_record(adapter: DynamoDBControlPlane, record: Any, clock: MutableClock) -> None:
    job = _job(f"job-{record.agent_id}-{record.snapshot_id}", record.agent_id).model_copy(
        update={"source_type": record.source_type, "file_subtype": record.file_subtype,
                "competencia": record.competencia}
    )
    adapter.put_agent(_agent(record.agent_id))
    adapter.create_job(job, _event(f"created-{job.job_id}"))
    claimed = adapter.claim_job(_claim_job(job.job_id, "raw-worker", clock))
    assert claimed is not None
    adapter.complete_job(
        CompleteJob(tenant_id=_TENANT, job_id=job.job_id, owner="raw-worker",
                    fencing_token=claimed.fencing_token, manifest=record),
        _event(f"completed-{job.job_id}"),
    )


@pytest.fixture
def ctx() -> Any:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        _create_table(client)
        clock = MutableClock(_NOW)
        yield client, clock, DynamoDBControlPlane(client, _TABLE_NAME, clock.now)

def test_deduplica_candidatos_e_rele_base_antes_do_claim(ctx: _DynamoContext) -> None:
    client, clock, adapter = ctx
    adapter.put_agent(_agent("agent-a"))
    adapter.create_job(_job("job-a"), _event("job-created"))
    base_key = entity_key(_TENANT, "JOB", "job-a")
    attributes = {
        "gsi1pk": f"JOB_CLAIM#{key_component(_TENANT)}#{key_component('agent-a')}",
        "gsi1sk": f"{_NOW.isoformat()}#PENDING#duplicate",
    }
    for suffix in ("a", "b", "missing"):
        marker = encode_marker(
            "STALE_JOB",
            entity_key(_TENANT, "STALE_JOB", suffix),
            entity_key(_TENANT, "JOB", suffix) if suffix == "missing" else base_key,
            attributes,
        )
        client.put_item(TableName=_TABLE_NAME, Item=marker)
    adapter._client = spy = ClientSpy(client)
    jobs = adapter.list_claimable_jobs(_TENANT, "agent-a", 10)
    assert tuple(job.job_id for job in jobs) == ("job-a",)
    consistent_reads = [call for call in spy.calls if call == "get_item"]
    assert len(consistent_reads) == 3
    for job_id in ("job-c", "job-b"):
        adapter.create_job(_job(job_id), _event(f"created-{job_id}"))
    paginated = OneItemPageClient(adapter._client)
    adapter._client = paginated
    assert tuple(job.job_id for job in adapter.list_claimable_jobs(_TENANT, "agent-a", 1)) == (
        "job-a",
    )
    assert (adapter.list_claimable_jobs(_TENANT, "agent-a", 0), paginated.calls.count("query")) == (
        (),
        4,
    )
    assert adapter.claim_job(_claim_job("job-a", "worker-a", clock)) is not None

def test_cadeia_raw_serializa_corrida(ctx: _DynamoContext) -> None:
    client, clock, adapter = ctx
    full = _raw_record("base", "agent-a", 1, clock.now() + timedelta(minutes=4))
    sibling_a = _raw_record("zeta", "agent-a", 2, clock.now() + timedelta(minutes=1))
    sibling_b = _raw_record("delta-b", "agent-a", 2, clock.now() + timedelta(minutes=2))
    sibling_c = _raw_record("delta-c", "agent-a", 2, clock.now())
    head = _raw_record("head", "agent-a", 3, clock.now() + timedelta(minutes=3))

    def linked(record: Any, **updates: Any) -> Any:
        return record.model_copy(update={"base_snapshot_id": "base", **updates})

    sibling_a = linked(sibling_a, manifest_sha256=_HASH_B)
    sibling_b = linked(sibling_b, manifest_sha256="c" * 64)
    sibling_c = linked(sibling_c, manifest_sha256="d" * 64)
    head = linked(head, previous_manifest_sha256="c" * 64)
    contender = DynamoDBControlPlane(client, _TABLE_NAME, clock.now)
    calls = 0

    def contend(_: list[dict[str, Any]]) -> None:
        nonlocal calls
        calls += 1
        if calls in (3, 4):
            record = (sibling_b, sibling_c)[calls - 3]
            _submit_raw_record(contender, record, clock)

    adapter._client = ClientSpy(client, before_transaction=contend)
    _submit_raw_record(adapter, full, clock)
    adapter._raw_actions(full)
    for record in (head, sibling_a):
        _store_record(adapter, record, clock)
    assert adapter.get_job(_TENANT, "job-agent-a-base").state is JobState.SUCCEEDED
    adapter._client = stale = OneItemPageClient(adapter._client)
    stale.hidden_gsi2sk = adapter._raw_item(head)["gsi2sk"]
    chain = adapter.query_raw_manifest_chain(RawManifestChainQuery(_IDENTITY, 3))
    assert tuple(ref.manifest_id for ref in chain) == (
        full.manifest_id,
        sibling_b.manifest_id,
        head.manifest_id,
    )
    gets = [request for name, request in stale.requests if name == "get_item"]
    assert (stale.query_requests, len(gets), gets[0].get("ConsistentRead")) == ([], 1, True)
    assert adapter.query_raw_manifest_chain(RawManifestChainQuery(_IDENTITY, 0)) == ()


def test_reparo_raw_rejeita_ancestry_ausente_e_excesso(ctx: _DynamoContext) -> None:
    client, clock, adapter = ctx
    _store_record(adapter, _raw_record("delta-pending", "agent-a", 2, clock.now()), clock)
    waiting = next(
        item
        for item in client.scan(TableName=_TABLE_NAME)["Items"]
        if item.get("entity", {}).get("S") == "RAWWAITING"
    )
    ancestry_key = {"pk": waiting["base_pk"], "sk": waiting["base_sk"]}
    client.delete_item(TableName=_TABLE_NAME, Key=ancestry_key)
    base = _raw_record("base-agent-a", "agent-a", 1, clock.now())
    with pytest.raises(Conflict, match="raw_ancestry_conflict"):
        adapter._raw_actions(base)
    client.put_item(
        TableName=_TABLE_NAME,
        Item={
            **waiting,
            "pk": waiting["base_pk"],
            "sk": waiting["base_sk"],
            "entity": {"S": "RAWANCESTRY"},
            "chain": {"S": "[]"},
        },
    )
    with pytest.raises(Conflict, match="raw_ancestry_conflict"):
        adapter._raw_actions(base)
    for index in range(95):
        clone = {**waiting, "sk": {"S": f"{waiting['sk']['S']}#{index:03d}"}}
        client.put_item(TableName=_TABLE_NAME, Item=clone)
    with pytest.raises(Conflict, match="transaction_limit"):
        adapter._raw_actions(base)

def test_candidato_gsi_obsoleto_nao_reivindica_job_ou_run_terminal(ctx: _DynamoContext) -> None:
    client, clock, adapter = ctx
    adapter.put_agent(_agent("agent-a"))
    job = _job("job-a").model_copy(update={"state": JobState.FAILED_FINAL})
    adapter._put_direct(adapter._job_item(job))
    base_key = entity_key(_TENANT, "JOB", "job-a")
    attrs = {
        "gsi1pk": f"JOB_CLAIM#{key_component(_TENANT)}#{key_component('agent-a')}",
        "gsi1sk": f"{_NOW.isoformat()}#PENDING#job-a",
    }
    marker = encode_marker("STALE_JOB", entity_key(_TENANT, "STALE_JOB", "job-a"), base_key, attrs)
    client.put_item(TableName=_TABLE_NAME, Item=marker)
    assert adapter.list_claimable_jobs(_TENANT, "agent-a", 10) == ()
    assert adapter.claim_job(_claim_job("job-a", "worker-a", clock)) is None
    adapter.put_run(_run("run-a", RunState.PUBLISHED))
    marker = encode_marker(
        "STALE_RUN",
        entity_key(_TENANT, "STALE_RUN", "run-a"),
        run_entity_key(_TENANT, "run-a"),
        {"gsi4pk": "RUN_RECOVERABLE", "gsi4sk": f"{_NOW.isoformat()}#{_TENANT}#run-a"},
    )
    client.put_item(TableName=_TABLE_NAME, Item=marker)
    assert adapter.list_recoverable_runs(clock.now(), 10) == ()
    tenant_z = _run("run-a").model_copy(update={"tenant_id": "z"})
    tenant_a = _run("run-z").model_copy(update={"tenant_id": "a"})
    adapter.put_run(tenant_z)
    adapter.put_run(tenant_a)
    assert adapter.list_recoverable_runs(clock.now(), 1) == (tenant_a,)

def test_expiracao_logica_substitui_item_ttl_ainda_presente(ctx: _DynamoContext) -> None:
    client, clock, adapter = ctx
    first = BeginIdempotency(
        tenant_id=_TENANT,
        scope="jobs",
        key="key-a",
        request_hash=_HASH_A,
        resource_id="resource-a",
        now=clock.now(),
        expires_at=clock.now() + timedelta(minutes=5),
    )
    adapter.begin_idempotency(first)
    clock.advance(timedelta(minutes=6))
    retained = client.scan(TableName=_TABLE_NAME)["Items"]
    assert any(item.get("expires_at") for item in retained)
    replacement = first.model_copy(
        update={
            "request_hash": _HASH_B,
            "resource_id": "resource-b",
            "now": clock.now(),
            "expires_at": clock.now() + timedelta(minutes=5),
        }
    )
    outcome = adapter.begin_idempotency(replacement)
    assert outcome.created
    assert outcome.record.resource_id == "resource-b"


def test_colisao_global_de_evento_rejeita_segundo_tenant_sem_mutacao(ctx: _DynamoContext) -> None:
    _, _, adapter = ctx
    adapter.create_job(_job("job-a"), _event("shared-event"))
    other_job, other_event = (
        _job("job-b", tenant_id="other"),
        _event("shared-event", tenant_id="other"),
    )
    with pytest.raises(Conflict, match="event_id_conflict"):
        adapter.create_job(other_job, other_event)
    assert adapter.get_job("other", "job-b") is None
    assert adapter.get_outbox_event("shared-event").tenant_id == _TENANT
    with pytest.raises(Conflict, match="event_tenant_conflict"):
        adapter.create_job(_job("job-c"), _event("fresh-other", tenant_id="other"))
    assert adapter.get_job(_TENANT, "job-c") is None
    assert adapter.get_outbox_event("fresh-other") is None


def test_chaves_compostas_isolam_componentes_com_delimitador(ctx: _DynamoContext) -> None:
    _, _, adapter = ctx
    identities = (("a", "b#RUN#c"), ("a#RUN#b", "c"))
    expected = []
    for tenant_id, run_id in identities:
        run = _run(run_id).model_copy(update={"tenant_id": tenant_id})
        unit = _unit("shared").model_copy(update={"tenant_id": tenant_id, "run_id": run_id})
        adapter.put_run(run)
        adapter.put_run_units(
            PutRunUnits(
                tenant_id=tenant_id,
                run_id=run_id,
                expected_run_state=RunState.PROCESSING,
                units=(unit,),
            )
        )
        adapter.reserve_run_dispatch(
            ReserveRunDispatch(
                tenant_id=tenant_id,
                run_id=run_id,
                wave_id="a" * 16,
                unit_ids=("shared",),
                now=_NOW,
                lease_seconds=30,
            )
        )
        expected.append(unit)
    for identity, unit in zip(identities, expected, strict=True):
        assert adapter.list_run_units(*identity) == (unit,)
    partitions = {
        item["gsi5pk"]["S"]
        for item in adapter._client.scan(TableName=_TABLE_NAME)["Items"]
        if "gsi5pk" in item
    }
    assert len(partitions) == 2


def test_claim_cas_perdedor_nao_desfaz_vencedor_persistido(ctx: _DynamoContext) -> None:
    client, clock, adapter = ctx
    adapter.put_agent(_agent("agent-a"))
    adapter.create_job(_job("job-a"), _event("job-created"))
    winners: list[Any] = []

    def win_before_stale_cas(_: list[dict[str, Any]]) -> None:
        winners.append(adapter.claim_job(_claim_job("job-a", "worker-b", clock)))

    loser = DynamoDBControlPlane(
        ClientSpy(client, before_transaction=win_before_stale_cas),
        _TABLE_NAME,
        clock.now,
    )
    assert loser.claim_job(_claim_job("job-a", "worker-a", clock)) is None
    assert winners[0] is not None
    assert adapter.get_job(_TENANT, "job-a") == winners[0]

def test_codec_nao_introduz_decimal_nos_itens(ctx: _DynamoContext) -> None:
    client, clock, adapter = ctx
    adapter.create_job(_job("job-a"), _event("job-created"))
    tenant = Tenant(tenant_id=_TENANT, municipality_name="Epitácio", created_at=clock.now())
    adapter.put_tenant(tenant)
    assert adapter.get_tenant(_TENANT) == tenant

    def contains_decimal(value: Any) -> bool:
        if isinstance(value, dict):
            return any(contains_decimal(item) for item in value.values())
        if isinstance(value, list):
            return any(contains_decimal(item) for item in value)
        return isinstance(value, Decimal)

    assert not contains_decimal(client.scan(TableName=_TABLE_NAME)["Items"])


def test_create_job_retorna_replay_e_rejeita_divergencia(ctx: _DynamoContext) -> None:
    _, _, adapter = ctx
    adapter._client = spy = ClientSpy(adapter._client)
    delivered = _event("already-delivered").model_copy(update={"delivered_at": _NOW})
    with pytest.raises(Conflict, match="event_delivery_conflict"):
        adapter.create_job(_job("delivered"), delivered)
    assert "transact_write_items" not in spy.calls
    job, event = _job("job-a"), _event("job-created")
    adapter._client = ClientSpy(spy, after_transaction=_lose_transaction_response)
    assert adapter.create_job(job, event) == job
    adapter._client = spy
    assert adapter.create_job(job, event) == job
    adapter._client = ClientSpy(spy.client, before_transaction=_raise_transaction_conflict)
    with pytest.raises(Conflict, match="transaction_conflict"):
        adapter.create_job(_job("no-winner"), _event("no-winner-created"))
    adapter._client = spy
    candidate = _job("divergent-winner")

    def divergent_wins(_: list[dict[str, Any]]) -> None:
        adapter._put_direct(adapter._job_item(candidate.model_copy(update={"agent_id": "agent-b"})))
        raise Conflict(ErrorCode.TRANSACTION_CONFLICT)

    adapter._client = ClientSpy(spy, before_transaction=divergent_wins)
    with pytest.raises(Conflict, match="job_conflict"):
        adapter.create_job(candidate, _event("divergent-winner-created"))
    adapter._client = spy
    with pytest.raises(Conflict, match="event_id_conflict"):
        adapter.create_job(job, _foreign_event(adapter))
    divergent = event.model_copy(update={"payload": {"event_id": "divergent"}})
    with pytest.raises(Conflict, match="event_id_conflict"):
        adapter.create_job(job, divergent)
    assert adapter.get_job(_TENANT, "job-a") == job
    with pytest.raises(Conflict, match="job_conflict"):
        adapter.create_job(job.model_copy(update={"agent_id": "agent-b"}), event)
    key = {
        name: {"S": value}
        for name, value in zip(("pk", "sk"), outbox_key("job-created"), strict=True)
    }
    adapter._client.delete_item(TableName=_TABLE_NAME, Key=key)
    with pytest.raises(Conflict, match="event_id_conflict"):
        adapter.create_job(job, event)


def test_transicao_e_unidades_rejeitam_run_ausente_ou_estado_obsoleto(ctx: _DynamoContext) -> None:
    _, _, adapter = ctx
    transition = TransitionRun(
        tenant_id=_TENANT,
        run_id="run-a",
        expected_state=RunState.PLANNED,
        new_state=RunState.WAITING_INPUTS,
    )
    with pytest.raises(NotFound, match="run_missing"):
        adapter.transition_run(transition, _event("missing-run"))
    with pytest.raises(NotFound, match="run_missing"):
        _put_many_units(adapter, 1)
    adapter.put_run(_run("run-a", RunState.PUBLISHING))
    with pytest.raises(Conflict, match="run_state_conflict"):
        adapter.transition_run(transition, _event("stale-run"))
    waiting = adapter.query_waiting_runs_for_dependency(
        WaitingRunsForDependencyQuery(_IDENTITY, 10)
    )
    assert (waiting, adapter.get_outbox_event("stale-run")) == ((), None)
    with pytest.raises(Conflict, match="run_state_conflict"):
        _put_many_units(adapter, 1)


def test_cancel_job_exige_lease_e_e_idempotente(ctx: _DynamoContext) -> None:
    _, clock, adapter = ctx
    command = CancelJob(tenant_id=_TENANT, job_id="job-a", requested_by="user-a")
    with pytest.raises(NotFound, match="job_missing"):
        adapter.cancel_job(command, _event("missing-cancel"))
    adapter.put_agent(_agent("agent-a"))
    adapter.create_job(_job("job-a"), _event("job-created"))
    with pytest.raises(Conflict, match="job_state_conflict"):
        adapter.cancel_job(command, _event("pending-cancel"))
    assert adapter.claim_job(_claim_job("job-a", "worker-a", clock)) is not None
    event = _event("job-cancel-requested")
    canceled = adapter.cancel_job(command, event)
    assert canceled.state is JobState.CANCEL_REQUESTED
    assert adapter.cancel_job(command, event) == canceled
    with pytest.raises(Conflict, match="event_id_conflict"):
        adapter.cancel_job(command, _foreign_event(adapter))
    assert adapter.get_job(_TENANT, "job-a") == canceled
    command = FinalizeRunCancellation(
        tenant_id=_TENANT,
        run_id="run-a",
        expected_state=RunState.CANCEL_REQUESTED,
        canceled_at=clock.now(),
    )
    with pytest.raises(NotFound, match="run_missing"):
        adapter.finalize_run_cancellation(command, _event("missing-run"))
    adapter.put_run(_run("run-a", RunState.CANCEL_REQUESTED))
    event = _event("run-canceled")
    canceled = adapter.finalize_run_cancellation(command, event)
    assert adapter.finalize_run_cancellation(command, event) == canceled
    with pytest.raises(Conflict, match="event_id_conflict"):
        adapter.finalize_run_cancellation(command, _foreign_event(adapter))


def _access_request(state: AccessRequestState = AccessRequestState.PENDING) -> AccessRequest:
    decided = state is not AccessRequestState.PENDING
    return AccessRequest(
        tenant_id=_TENANT,
        request_id="request-a",
        user_id="user-a",
        state=state,
        decided_by="admin-a" if decided else None,
        decided_at=_NOW if decided else None,
    )


def _foreign_event(adapter: DynamoDBControlPlane) -> Any:
    adapter.create_job(
        _job("foreign-job", tenant_id="other"), event := _event("foreign-event", tenant_id="other")
    )
    return event
