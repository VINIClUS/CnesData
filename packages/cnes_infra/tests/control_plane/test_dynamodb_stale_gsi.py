from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from decimal import Decimal
from threading import Barrier
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
    RenewJobLease,
    TransitionRun,
)
from cnes_domain.control_plane.entities import AccessRequest, Tenant
from cnes_domain.control_plane.enums import AccessRequestState, JobState, RunState
from cnes_domain.control_plane.errors import Conflict, LeaseLost, NotFound
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_codec import encode_marker, execute_transaction
from cnes_infra.control_plane.dynamodb_keys import entity_key, run_entity_key
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
)
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import (
    _TABLE_NAME,
    ClientSpy,
    FailingTransactionClient,
    _create_table,
    _put_many_units,
)

_HASH_B = "b" * 64


class OneItemPageClient(ClientSpy):
    def query(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append("query")
        return self.client.query(**kwargs, Limit=1)


@pytest.fixture
def dynamodb_context() -> Any:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        _create_table(client)
        clock = MutableClock(_NOW)
        yield client, clock, DynamoDBControlPlane(client, _TABLE_NAME, clock.now)


def test_deduplica_candidatos_e_rele_base_antes_do_claim(
    dynamodb_context: tuple[Any, MutableClock, DynamoDBControlPlane],
) -> None:
    client, clock, adapter = dynamodb_context
    adapter.put_agent(_agent("agent-a"))
    adapter.create_job(_job("job-a"), _event("job-created"))
    base_key = entity_key(_TENANT, "JOB", "job-a")
    attributes = {
        "gsi1pk": f"JOB_CLAIM#{_TENANT}#agent-a",
        "gsi1sk": f"{_NOW.isoformat()}#PENDING#duplicate",
    }
    for suffix in ("a", "b"):
        marker = encode_marker(
            "STALE_JOB",
            entity_key(_TENANT, "STALE_JOB", suffix),
            base_key,
            attributes,
        )
        client.put_item(TableName=_TABLE_NAME, Item=marker)
    spy = ClientSpy(client)
    adapter._client = spy

    jobs = adapter.list_claimable_jobs(_TENANT, "agent-a", 10)

    assert tuple(job.job_id for job in jobs) == ("job-a",)
    consistent_reads = [call for call in spy.calls if call == "get_item"]
    assert len(consistent_reads) == 2
    assert adapter.claim_job(_claim_job("job-a", "worker-a", clock)) is not None


def test_query_percorre_todas_as_paginas_antes_de_ordenar_e_limitar(
    dynamodb_context: tuple[Any, MutableClock, DynamoDBControlPlane],
) -> None:
    _, _, adapter = dynamodb_context
    adapter.put_agent(_agent("agent-a"))
    for job_id in ("job-b", "job-a"):
        adapter.create_job(_job(job_id), _event(f"created-{job_id}"))
    paginated = OneItemPageClient(adapter._client)
    adapter._client = paginated

    jobs = adapter.list_claimable_jobs(_TENANT, "agent-a", 2)

    assert tuple(job.job_id for job in jobs) == ("job-a", "job-b")
    assert paginated.calls.count("query") == 2


def test_candidato_gsi_obsoleto_nao_reivindica_job_terminal(
    dynamodb_context: tuple[Any, MutableClock, DynamoDBControlPlane],
) -> None:
    client, clock, adapter = dynamodb_context
    adapter.put_agent(_agent("agent-a"))
    job = _job("job-a").model_copy(update={"state": JobState.FAILED_FINAL})
    adapter._put_direct(adapter._job_item(job))
    base_key = entity_key(_TENANT, "JOB", "job-a")
    marker = encode_marker(
        "STALE_JOB",
        entity_key(_TENANT, "STALE_JOB", "job-a"),
        base_key,
        {
            "gsi1pk": f"JOB_CLAIM#{_TENANT}#agent-a",
            "gsi1sk": f"{_NOW.isoformat()}#PENDING#job-a",
        },
    )
    client.put_item(TableName=_TABLE_NAME, Item=marker)

    assert adapter.list_claimable_jobs(_TENANT, "agent-a", 10) == ()
    assert adapter.claim_job(_claim_job("job-a", "worker-a", clock)) is None


def test_candidato_gsi_obsoleto_nao_recupera_run_terminal(
    dynamodb_context: tuple[Any, MutableClock, DynamoDBControlPlane],
) -> None:
    client, clock, adapter = dynamodb_context
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


def test_expiracao_logica_substitui_item_ttl_ainda_presente(
    dynamodb_context: tuple[Any, MutableClock, DynamoDBControlPlane],
) -> None:
    client, clock, adapter = dynamodb_context
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


def test_colisao_global_de_evento_rejeita_segundo_tenant_sem_mutacao(
    dynamodb_context: tuple[Any, MutableClock, DynamoDBControlPlane],
) -> None:
    _, _, adapter = dynamodb_context
    adapter.create_job(_job("job-a"), _event("shared-event"))
    other_job = _job("job-b", tenant_id="other")
    other_event = _event("shared-event", tenant_id="other")

    with pytest.raises(Conflict, match="event_id_conflict"):
        adapter.create_job(other_job, other_event)

    assert adapter.get_job("other", "job-b") is None
    assert adapter.get_outbox_event("shared-event").tenant_id == _TENANT


def test_claim_concorrente_produz_um_unico_vencedor(
    dynamodb_context: tuple[Any, MutableClock, DynamoDBControlPlane],
) -> None:
    client, clock, adapter = dynamodb_context
    adapter.put_agent(_agent("agent-a"))
    adapter.create_job(_job("job-a"), _event("job-created"))
    barrier = Barrier(2)

    def claim(owner: str) -> Any:
        contender = DynamoDBControlPlane(client, _TABLE_NAME, clock.now)
        command = _claim_job("job-a", owner, clock)
        barrier.wait()
        return contender.claim_job(command)

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = tuple(executor.map(claim, ("worker-a", "worker-b")))

    winners = tuple(outcome for outcome in outcomes if outcome is not None)
    assert len(winners) == 1
    assert winners[0].fencing_token == 1
    assert adapter.get_job(_TENANT, "job-a") == winners[0]


def test_codec_nao_introduz_decimal_nos_itens(
    dynamodb_context: tuple[Any, MutableClock, DynamoDBControlPlane],
) -> None:
    client, _, adapter = dynamodb_context
    adapter.create_job(_job("job-a"), _event("job-created"))

    def contains_decimal(value: Any) -> bool:
        if isinstance(value, dict):
            return any(contains_decimal(item) for item in value.values())
        if isinstance(value, list):
            return any(contains_decimal(item) for item in value)
        return isinstance(value, Decimal)

    assert not contains_decimal(client.scan(TableName=_TABLE_NAME)["Items"])


def test_persiste_tenant_com_isolamento(
    dynamodb_context: tuple[Any, MutableClock, DynamoDBControlPlane],
) -> None:
    _, clock, adapter = dynamodb_context
    tenant = Tenant(tenant_id=_TENANT, municipality_name="Epitácio", created_at=clock.now())

    adapter.put_tenant(tenant)

    assert adapter.get_tenant(_TENANT) == tenant
    assert adapter.get_tenant("other") is None


def test_create_job_retorna_replay_e_rejeita_divergencia(
    dynamodb_context: tuple[Any, MutableClock, DynamoDBControlPlane],
) -> None:
    _, _, adapter = dynamodb_context
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


def test_transicao_e_unidades_rejeitam_run_ausente_ou_estado_obsoleto(
    dynamodb_context: tuple[Any, MutableClock, DynamoDBControlPlane],
) -> None:
    _, _, adapter = dynamodb_context
    transition = TransitionRun(
        tenant_id=_TENANT,
        run_id="run-a",
        expected_state=RunState.PROCESSING,
        new_state=RunState.FAILED,
    )
    with pytest.raises(NotFound, match="run_missing"):
        adapter.transition_run(transition, _event("missing-run"))
    with pytest.raises(NotFound, match="run_missing"):
        _put_many_units(adapter, 1)
    adapter.put_run(_run("run-a", RunState.PUBLISHING))
    with pytest.raises(Conflict, match="run_state_conflict"):
        adapter.transition_run(transition, _event("stale-run"))
    with pytest.raises(Conflict, match="run_state_conflict"):
        _put_many_units(adapter, 1)


def test_cancel_job_exige_lease_e_e_idempotente(
    dynamodb_context: tuple[Any, MutableClock, DynamoDBControlPlane],
) -> None:
    _, clock, adapter = dynamodb_context
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


def test_finalizacao_ausente_e_replay_cancelado_sao_deterministas(
    dynamodb_context: tuple[Any, MutableClock, DynamoDBControlPlane],
) -> None:
    _, clock, adapter = dynamodb_context
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
    assert adapter.get_run(_TENANT, "run-a") == canceled


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
    event = _event("foreign-event", tenant_id="other")
    adapter.create_job(_job("foreign-job", tenant_id="other"), event)
    return event


def test_access_request_tem_criacao_decisao_e_replays_atomicos(
    dynamodb_context: tuple[Any, MutableClock, DynamoDBControlPlane],
) -> None:
    _, _, adapter = dynamodb_context
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

    assert adapter.decide_access_request(approved, _event("access-approved")) == approved
    assert adapter.decide_access_request(approved, _event("access-approved")) == approved
    with pytest.raises(Conflict, match="event_id_conflict"):
        adapter.decide_access_request(approved, foreign)
    assert adapter.get_access_request(_TENANT, "request-a") == approved
    with pytest.raises(Conflict, match="access_request_conflict"):
        adapter.decide_access_request(
            _access_request(AccessRequestState.REJECTED), _event("access-rejected")
        )


def test_outbox_entrega_remove_pendencia_e_rejeita_redecisao(
    dynamodb_context: tuple[Any, MutableClock, DynamoDBControlPlane],
) -> None:
    _, clock, adapter = dynamodb_context
    adapter.create_job(_job("job-a"), _event("job-created"))
    delivered_at = clock.now() + timedelta(seconds=1)

    adapter.mark_outbox_delivered("job-created", delivered_at)

    assert adapter.pending_outbox(10) == ()
    adapter.mark_outbox_delivered("job-created", delivered_at)
    with pytest.raises(Conflict, match="outbox_delivery_conflict"):
        adapter.mark_outbox_delivered("job-created", delivered_at + timedelta(seconds=1))
    with pytest.raises(NotFound, match="outbox_event_missing"):
        adapter.mark_outbox_delivered("missing", delivered_at)


def test_claims_rejeitam_ausencia_e_manifesto_de_outra_identidade(
    dynamodb_context: tuple[Any, MutableClock, DynamoDBControlPlane],
) -> None:
    _, clock, adapter = dynamodb_context
    assert adapter.claim_job(_claim_job("missing", "worker-a", clock)) is None
    renewal = RenewJobLease(
        tenant_id=_TENANT,
        job_id="missing",
        owner="worker-a",
        fencing_token=1,
        now=clock.now(),
        lease_seconds=30,
    )
    with pytest.raises(NotFound, match="job_missing"):
        adapter.renew_job_lease(renewal)
    adapter.put_agent(_agent("agent-a"))
    adapter.create_job(_job("job-a"), _event("job-created"))
    with pytest.raises(LeaseLost, match="job_not_leased"):
        adapter.renew_job_lease(renewal.model_copy(update={"job_id": "job-a"}))
    claimed = adapter.claim_job(_claim_job("job-a", "worker-a", clock))
    manifest = _raw_record("result", "agent-b", 1, clock.now())
    complete = CompleteJob(
        tenant_id=_TENANT,
        job_id="job-a",
        owner="worker-a",
        fencing_token=claimed.fencing_token,
        manifest=manifest,
    )
    with pytest.raises(Conflict, match="manifest_identity_conflict"):
        adapter.complete_job(complete, _event("invalid-manifest"))


def test_publicacao_rejeita_run_ausente(
    dynamodb_context: tuple[Any, MutableClock, DynamoDBControlPlane],
) -> None:
    _, _, adapter = dynamodb_context
    from packages.cnes_infra.tests.contracts.control_plane_contract import _publish

    with pytest.raises(NotFound, match="run_missing"):
        adapter.publish_dataset(_publish("run-a", "published", None, False))


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


def test_idempotencia_recupera_resultado_de_corrida_confirmada(
    dynamodb_context: tuple[Any, MutableClock, DynamoDBControlPlane],
) -> None:
    _, clock, adapter = dynamodb_context
    command = BeginIdempotency(
        tenant_id=_TENANT,
        scope="jobs",
        key="race",
        request_hash=_HASH_A,
        resource_id="resource-a",
        now=clock.now(),
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


def test_idempotencia_propaga_conflito_sem_vencedor_visivel(
    dynamodb_context: tuple[Any, MutableClock, DynamoDBControlPlane],
) -> None:
    _, clock, adapter = dynamodb_context
    adapter._client = FailingTransactionClient(adapter._client)
    command = BeginIdempotency(
        tenant_id=_TENANT,
        scope="jobs",
        key="lost-race",
        request_hash=_HASH_A,
        resource_id="resource-a",
        now=clock.now(),
        expires_at=clock.now() + timedelta(minutes=5),
    )

    with pytest.raises(Conflict, match="transaction_conflict"):
        adapter.begin_idempotency(command)
