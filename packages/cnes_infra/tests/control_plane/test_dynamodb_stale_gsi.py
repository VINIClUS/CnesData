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
    RenewJobLease,
    ReserveRunDispatch,
    TransitionRun,
)
from cnes_domain.control_plane.entities import AccessRequest, Tenant
from cnes_domain.control_plane.enums import AccessRequestState, AgentState, JobState, RunState
from cnes_domain.control_plane.errors import Conflict, LeaseLost, NotFound
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_codec import encode_marker, execute_transaction
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
    _fail_job,
    _job,
    _raw_record,
    _run,
    _unit,
)
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import (
    _TABLE_NAME,
    ClientSpy,
    FailingTransactionClient,
    _create_table,
    _put_many_units,
    _store_record_matching_mode,
)

_HASH_B = "b" * 64
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
            "STALE_JOB", entity_key(_TENANT, "STALE_JOB", suffix),
            entity_key(_TENANT, "JOB", suffix) if suffix == "missing" else base_key,
            attributes)
        client.put_item(TableName=_TABLE_NAME, Item=marker)
    adapter._client = spy = ClientSpy(client)
    jobs = adapter.list_claimable_jobs(_TENANT, "agent-a", 10)
    assert tuple(job.job_id for job in jobs) == ("job-a",)
    consistent_reads = [call for call in spy.calls if call == "get_item"]
    assert len(consistent_reads) == 3
    assert adapter.claim_job(_claim_job("job-a", "worker-a", clock)) is not None
def test_query_limitada_para_apos_primeiro_candidato_valido(ctx: _DynamoContext) -> None:
    _, _, adapter = ctx
    adapter.put_agent(_agent("agent-a"))
    for job_id in ("job-c", "job-b", "job-a"):
        adapter.create_job(_job(job_id), _event(f"created-{job_id}"))
    paginated = OneItemPageClient(adapter._client)
    adapter._client = paginated
    jobs = adapter.list_claimable_jobs(_TENANT, "agent-a", 1)
    assert tuple(job.job_id for job in jobs) == ("job-a",)
    assert (adapter.list_claimable_jobs(_TENANT, "agent-a", 0),
            paginated.calls.count("query")) == ((), 1)
@pytest.mark.parametrize("base_minutes", [3, 4])
def test_cadeia_raw_prefere_descendente(base_minutes: int, ctx: _DynamoContext) -> None:
    _, clock, adapter = ctx
    full = _raw_record("base", "agent-a", 1, clock.now() + timedelta(minutes=base_minutes))
    sibling_a = _raw_record("delta-a", "agent-a", 2, clock.now() + timedelta(minutes=1))
    sibling_b = _raw_record("delta-b", "agent-a", 2, clock.now() + timedelta(minutes=2))
    head = _raw_record("head", "agent-a", 3, clock.now() + timedelta(minutes=3))
    def linked(record: Any, **updates: Any) -> Any:
        return record.model_copy(update={"base_snapshot_id": "base", **updates})
    sibling_a = linked(sibling_a, manifest_sha256=_HASH_B)
    sibling_b = linked(sibling_b, manifest_sha256="c" * 64)
    head = linked(head, previous_manifest_sha256="c" * 64)
    for record in (full, sibling_a, sibling_b, head):
        _store_record_matching_mode(adapter, record, clock)
    adapter._client = stale = OneItemPageClient(adapter._client)
    stale.hidden_gsi2sk = adapter._raw_item(head)["gsi2sk"]
    chain = adapter.list_raw_manifest_chain(_TENANT, "CNES", "ST", "2026-07", 3)
    assert tuple(ref.manifest_id for ref in chain) == (
        full.manifest_id, sibling_b.manifest_id, head.manifest_id)
def test_candidato_gsi_obsoleto_nao_reivindica_job_terminal(ctx: _DynamoContext) -> None:
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
def test_candidato_gsi_obsoleto_nao_recupera_run_terminal(ctx: _DynamoContext) -> None:
    client, clock, adapter = ctx
    run = _run("run-a", RunState.PUBLISHED)
    adapter.put_run(run)
    marker = encode_marker(
        "STALE_RUN",
        entity_key(_TENANT, "STALE_RUN", "run-a"),
        run_entity_key(_TENANT, "run-a"),
        {
            "gsi4pk": "RUN_RECOVERABLE",
            "gsi4sk": f"{_NOW.isoformat()}#{_TENANT}#run-a",
        },
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
        tenant_id=_TENANT, scope="jobs", key="key-a", request_hash=_HASH_A,
        resource_id="resource-a", now=clock.now(),
        expires_at=clock.now() + timedelta(minutes=5))
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
    other_job = _job("job-b", tenant_id="other")
    other_event = _event("shared-event", tenant_id="other")
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
        adapter.put_run_units(PutRunUnits(
            tenant_id=tenant_id, run_id=run_id,
            expected_run_state=RunState.PROCESSING, units=(unit,)))
        adapter.reserve_run_dispatch(ReserveRunDispatch(
            tenant_id=tenant_id, run_id=run_id, wave_id="a" * 16,
            unit_ids=("shared",), now=_NOW, lease_seconds=30))
        expected.append(unit)
    for identity, unit in zip(identities, expected, strict=True):
        assert adapter.list_run_units(*identity) == (unit,)
    partitions = {item["gsi5pk"]["S"] for item in adapter._client.scan(
        TableName=_TABLE_NAME)["Items"] if "gsi5pk" in item}
    assert len(partitions) == 2
def test_claim_cas_perdedor_nao_desfaz_vencedor_persistido(ctx: _DynamoContext) -> None:
    client, clock, adapter = ctx
    adapter.put_agent(_agent("agent-a"))
    adapter.create_job(_job("job-a"), _event("job-created"))
    loser = DynamoDBControlPlane(client, _TABLE_NAME, clock.now)
    transact = loser._transact
    winners = []
    def win_before_stale_cas(actions: Any) -> None:
        winners.append(adapter.claim_job(_claim_job("job-a", "worker-b", clock)))
        transact(actions)
    loser._transact = win_before_stale_cas
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
    job = _job("job-a")
    event = _event("job-created")
    adapter.create_job(job, event)
    assert adapter.create_job(job, event) == job
    with pytest.raises(Conflict, match="event_id_conflict"):
        adapter.create_job(job, _foreign_event(adapter))
    divergent = event.model_copy(update={"payload": {"event_id": "divergent"}})
    with pytest.raises(Conflict, match="event_id_conflict"):
        adapter.create_job(job, divergent)
    assert adapter.get_job(_TENANT, "job-a") == job
    with pytest.raises(Conflict, match="job_conflict"):
        adapter.create_job(job.model_copy(update={"agent_id": "agent-b"}), event)
    key = {name: {"S": value} for name, value in
           zip(("pk", "sk"), outbox_key("job-created"), strict=True)}
    adapter._client.delete_item(TableName=_TABLE_NAME, Key=key)
    with pytest.raises(Conflict, match="event_id_conflict"):
        adapter.create_job(job, event)
def test_transicao_e_unidades_rejeitam_run_ausente_ou_estado_obsoleto(ctx: _DynamoContext) -> None:
    _, _, adapter = ctx
    transition = TransitionRun(
        tenant_id=_TENANT, run_id="run-a", expected_state=RunState.PLANNED,
        new_state=RunState.WAITING_INPUTS,
    )
    with pytest.raises(NotFound, match="run_missing"):
        adapter.transition_run(transition, _event("missing-run"))
    with pytest.raises(NotFound, match="run_missing"):
        _put_many_units(adapter, 1)
    adapter.put_run(_run("run-a", RunState.PUBLISHING))
    with pytest.raises(Conflict, match="run_state_conflict"):
        adapter.transition_run(transition, _event("stale-run"))
    waiting = adapter.list_waiting_runs_for_dependency(_TENANT, "CNES", "ST", "2026-07", 10)
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
def test_finalizacao_ausente_e_replay_cancelado_sao_deterministas(ctx: _DynamoContext) -> None:
    _, clock, adapter = ctx
    command = FinalizeRunCancellation(
        tenant_id=_TENANT, run_id="run-a", expected_state=RunState.CANCEL_REQUESTED,
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
    assert adapter.get_run(_TENANT, "run-a") == canceled
def _access_request(state: AccessRequestState = AccessRequestState.PENDING) -> AccessRequest:
    decided = state is not AccessRequestState.PENDING
    return AccessRequest(
        tenant_id=_TENANT, request_id="request-a", user_id="user-a", state=state,
        decided_by="admin-a" if decided else None, decided_at=_NOW if decided else None,
    )
def _foreign_event(adapter: DynamoDBControlPlane) -> Any:
    event = _event("foreign-event", tenant_id="other")
    adapter.create_job(_job("foreign-job", tenant_id="other"), event)
    return event
def test_access_request_tem_criacao_decisao_e_replays_atomicos(ctx: _DynamoContext) -> None:
    _, _, adapter = ctx
    pending = _access_request()
    with pytest.raises(NotFound, match="access_request_missing"):
        adapter.decide_access_request(
            _access_request(AccessRequestState.APPROVED), _event("missing-access")
        )
    adapter.put_access_request(pending, _event("access-created"))
    adapter.put_access_request(pending, _event("access-created"))
    assert adapter.get_access_request(_TENANT, "request-a") == pending
    foreign = _foreign_event(adapter)
    with pytest.raises(Conflict, match="event_id_conflict"):
        adapter.put_access_request(pending, foreign)
    with pytest.raises(Conflict, match="access_request_conflict"):
        adapter.put_access_request(
            pending.model_copy(update={"user_id": "user-b"}), _event("divergent-access")
        )
    approved = _access_request(AccessRequestState.APPROVED)
    changed_user = approved.model_copy(update={"user_id": "user-b"})
    with pytest.raises(Conflict, match="access_request_conflict"):
        adapter.decide_access_request(changed_user, _event("changed-user"))
    assert adapter.get_access_request(_TENANT, "request-a") == pending
    assert adapter.get_outbox_event("changed-user") is None
    assert adapter.decide_access_request(approved, _event("access-approved")) == approved
    assert adapter.decide_access_request(approved, _event("access-approved")) == approved
    with pytest.raises(Conflict, match="event_id_conflict"):
        adapter.decide_access_request(approved, foreign)
    assert adapter.get_access_request(_TENANT, "request-a") == approved
    with pytest.raises(Conflict, match="access_request_conflict"):
        adapter.decide_access_request(
            _access_request(AccessRequestState.REJECTED), _event("access-rejected")
        )
def test_outbox_entrega_remove_pendencia_e_rejeita_redecisao(ctx: _DynamoContext) -> None:
    client, clock, adapter = ctx
    adapter.create_job(_job("job-a"), _event("job-created"))
    delivered_at = clock.now() + timedelta(seconds=1)
    adapter.mark_outbox_delivered("job-created", delivered_at)
    assert adapter.pending_outbox(10) == ()
    assert adapter.create_job(_job("job-a"), _event("job-created")) == _job("job-a")
    divergent = _event("job-created").model_copy(update={"payload": {"changed": True}})
    with pytest.raises(Conflict, match="event_id_conflict"):
        adapter.create_job(_job("job-a"), divergent)
    adapter.mark_outbox_delivered("job-created", delivered_at)
    with pytest.raises(Conflict, match="outbox_delivery_conflict"):
        adapter.mark_outbox_delivered("job-created", delivered_at + timedelta(seconds=1))
    with pytest.raises(NotFound, match="outbox_event_missing"):
        adapter.mark_outbox_delivered("missing", delivered_at)
    events = tuple(_event(f"event-{index}").model_copy(
        update={"created_at": clock.now() + timedelta(seconds=index)}
    ) for index in range(3)
    )
    for index, event in enumerate(events):
        adapter.create_job(_job(f"job-{index}"), event)
    markers = (("missing", "missing", 3), ("stale", "job-created", 2), ("newer", "event-1", 1))
    for marker_id, base_id, minutes in markers:
        marker = encode_marker(
            "STALE_OUTBOX", entity_key(_TENANT, "STALE_OUTBOX", marker_id),
            outbox_key(base_id), {"gsi6pk": "OUTBOX#PENDING", "gsi6sk":
                f"{(clock.now() - timedelta(minutes=minutes)).isoformat()}#{marker_id}"},
        )
        client.put_item(TableName=_TABLE_NAME, Item=marker)
    paginated = OneItemPageClient(client)
    adapter._client = paginated
    assert (adapter.pending_outbox(0), adapter.pending_outbox(1)) == ((), events[:1])
    paginated = OneItemPageClient(client)
    adapter._client = paginated
    assert adapter.pending_outbox(2) == events[:2]
    assert paginated.query_limits == [2, 2, 1]
def test_claims_rejeitam_ausencia_e_manifesto_de_outra_identidade(ctx: _DynamoContext) -> None:
    _, clock, adapter = ctx
    assert adapter.claim_job(_claim_job("missing", "worker-a", clock)) is None
    renewal = RenewJobLease(
        tenant_id=_TENANT, job_id="missing", owner="worker-a", fencing_token=1,
        now=clock.now(), lease_seconds=30,
    )
    with pytest.raises(NotFound, match="job_missing"):
        adapter.renew_job_lease(renewal)
    adapter.put_agent(_agent("agent-a"))
    adapter.create_job(_job("job-a"), _event("job-created"))
    with pytest.raises(LeaseLost, match="job_not_leased"):
        adapter.renew_job_lease(renewal.model_copy(update={"job_id": "job-a"}))
    claimed = adapter.claim_job(_claim_job("job-a", "worker-a", clock))
    contender = DynamoDBControlPlane(adapter._client, _TABLE_NAME, clock.now)
    transact = adapter._transact
    def revoke_then_renew(actions: Any) -> None:
        contender.put_agent(_agent("agent-a", AgentState.REVOKED))
        transact(actions)
    adapter._transact = revoke_then_renew
    with pytest.raises(Conflict, match="transaction_conflict"):
        adapter.renew_job_lease(renewal.model_copy(update={"job_id": "job-a"}))
    adapter._transact = transact
    assert adapter.get_job(_TENANT, "job-a") == claimed
    adapter.put_agent(_agent("agent-a"))
    adapter._transact = revoke_then_renew
    with pytest.raises(Conflict, match="transaction_conflict"):
        adapter.fail_job(_fail_job("worker-a", claimed.fencing_token, "failed"),
                         _event("failed-after-revoke"))
    adapter._transact = transact
    assert adapter.get_job(_TENANT, "job-a") == claimed
    assert adapter.get_outbox_event("failed-after-revoke") is None
    adapter.put_agent(_agent("agent-a"))
    manifests = (_raw_record("result", "agent-b", 1, clock.now()),
                 _raw_record("result", "agent-a", 2, clock.now()))
    for index, manifest in enumerate(manifests):
        complete = CompleteJob(tenant_id=_TENANT, job_id="job-a", owner="worker-a",
            fencing_token=claimed.fencing_token, manifest=manifest)
        with pytest.raises(Conflict, match="manifest_identity_conflict"):
            adapter.complete_job(complete, _event(f"invalid-manifest-{index}"))
@pytest.mark.parametrize("changes", [
    {"dataset_name": "silver"},
    {"run_manifest_key": f"reconciliation/{_TENANT}/2026-06/run-a/run-manifest.json"}])
def test_publicacao_valida_run_e_replay(changes: dict[str, str], ctx: _DynamoContext) -> None:
    _, clock, adapter = ctx
    from packages.cnes_infra.tests.contracts.control_plane_contract import _publish
    command = _publish("run-a", "published", None, False)
    with pytest.raises(NotFound, match="run_missing"):
        adapter.publish_dataset(command)
    adapter.put_run(_run("run-a", RunState.PUBLISHING))
    invalid = command.model_copy(update={"version": command.version.model_copy(update=changes)})
    with pytest.raises(Conflict, match="publication_run_conflict"):
        adapter.publish_dataset(invalid)
    named = command.model_copy(update={"pointer_name": "candidate"})
    pointer = adapter.publish_dataset(named)
    assert pointer.pointer_name == "candidate"
    assert adapter.publish_dataset(named) == pointer
    adapter.mark_outbox_delivered(named.event.event_id, clock.now())
    assert adapter.publish_dataset(named) == pointer
    changed = named.model_copy(update={"event": named.event.model_copy(
        update={"payload": {"changed": True}})})
    with pytest.raises(Conflict, match="publication_replay_conflict"):
        adapter.publish_dataset(changed)
def test_codec_propaga_erro_dynamodb_nao_condicional() -> None:
    class InvalidClient:
        @staticmethod
        def transact_write_items(**kwargs: Any) -> None:
            raise ClientError(
                {"Error": {"Code": "ValidationException", "Message": "invalid"}},
                "TransactWriteItems",
            )
    with pytest.raises(ClientError):
        execute_transaction(InvalidClient(), ())
@pytest.mark.parametrize("reason", [None, "ThrottlingError", "ProvisionedThroughputExceeded",
                                    "TransactionConflict"])
def test_codec_propaga_cancelamento_sem_falha_condicional(reason: str | None) -> None:
    class CanceledClient:
        @staticmethod
        def transact_write_items(**kwargs: Any) -> None:
            reasons = [] if reason is None else [{"Code": reason}]
            raise ClientError(
                {"Error": {"Code": "TransactionCanceledException"},
                 "CancellationReasons": reasons}, "TransactWriteItems",
            )
    with pytest.raises(ClientError):
        execute_transaction(CanceledClient(), ())
def test_idempotencia_recupera_resultado_de_corrida_confirmada(ctx: _DynamoContext) -> None:
    _, clock, adapter = ctx
    command = BeginIdempotency(
        tenant_id=_TENANT, scope="jobs", key="race", request_hash=_HASH_A,
        resource_id="resource-a", now=clock.now(),
        expires_at=clock.now() + timedelta(minutes=5),
    )
    transact = adapter._transact
    def wins_then_reports_conflict(actions: Any) -> None:
        transact(actions)
        raise Conflict("transaction_conflict")
    adapter._transact = wins_then_reports_conflict
    outcome = adapter.begin_idempotency(command)
    assert not outcome.created
    assert outcome.record.resource_id == "resource-a"
def test_idempotencia_propaga_conflito_sem_vencedor_visivel(ctx: _DynamoContext) -> None:
    _, clock, adapter = ctx
    adapter._client = FailingTransactionClient(adapter._client)
    command = BeginIdempotency(
        tenant_id=_TENANT, scope="jobs", key="lost-race", request_hash=_HASH_A,
        resource_id="resource-a",
        now=clock.now(),
        expires_at=clock.now() + timedelta(minutes=5),
    )
    with pytest.raises(Conflict, match="transaction_conflict"):
        adapter.begin_idempotency(command)
