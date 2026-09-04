from datetime import timedelta
from typing import Any

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from cnes_domain.control_plane.commands import (
    BeginIdempotency,
    CompleteJob,
    RenewJobLease,
)
from cnes_domain.control_plane.entities import AccessRequest
from cnes_domain.control_plane.enums import AccessRequestState, AgentState, RunState
from cnes_domain.control_plane.errors import Conflict, LeaseLost, NotFound
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_codec import encode_marker, execute_transaction
from cnes_infra.control_plane.dynamodb_keys import (
    entity_key,
    outbox_key,
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
)
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import (
    _TABLE_NAME,
    ClientSpy,
    _create_table,
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


def _raise_transaction_conflict(_: list[dict[str, Any]]) -> None:
    raise ClientError(
        {
            "Error": {"Code": "TransactionCanceledException", "Message": "cancelled"},
            "CancellationReasons": [{"Code": "ConditionalCheckFailed"}],
        },
        "TransactWriteItems",
    )


def _lose_transaction_response(_: list[dict[str, Any]]) -> None:
    raise Conflict("transaction_conflict")


@pytest.fixture
def ctx() -> Any:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        _create_table(client)
        clock = MutableClock(_NOW)
        yield client, clock, DynamoDBControlPlane(client, _TABLE_NAME, clock.now)


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


def test_access_request_tem_criacao_decisao_e_replays_atomicos(ctx: _DynamoContext) -> None:
    _, _, adapter = ctx
    pending = _access_request()
    with pytest.raises(NotFound, match="access_request_missing"):
        adapter.decide_access_request(
            _access_request(AccessRequestState.APPROVED), _event("missing-access")
        )
    event = _event("access-created")
    client = adapter._client
    adapter._client = ClientSpy(client, after_transaction=_lose_transaction_response)
    adapter.put_access_request(pending, event)
    adapter._client = client
    adapter.put_access_request(pending, event)
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
    events = tuple(
        _event(f"event-{index}").model_copy(
            update={"created_at": clock.now() + timedelta(seconds=index)}
        )
        for index in range(3)
    )
    for index, event in enumerate(events):
        adapter.create_job(_job(f"job-{index}"), event)
    markers = (("missing", "missing", 3), ("stale", "job-created", 2), ("newer", "event-1", 1))
    for marker_id, base_id, minutes in markers:
        marker = encode_marker(
            "STALE_OUTBOX",
            entity_key(_TENANT, "STALE_OUTBOX", marker_id),
            outbox_key(base_id),
            {
                "gsi6pk": "OUTBOX#PENDING",
                "gsi6sk": f"{(clock.now() - timedelta(minutes=minutes)).isoformat()}#{marker_id}",
            },
        )
        client.put_item(TableName=_TABLE_NAME, Item=marker)
    paginated = OneItemPageClient(client)
    adapter._client = paginated
    assert (adapter.pending_outbox(0), adapter.pending_outbox(1)) == ((), events[:1])
    paginated = OneItemPageClient(client)
    adapter._client = paginated
    assert adapter.pending_outbox(2) == events[:2]
    assert [request.get("Limit") for request in paginated.query_requests] == [2, 2, 1]


def test_claims_rejeitam_ausencia_e_manifesto_de_outra_identidade(ctx: _DynamoContext) -> None:
    _, clock, adapter = ctx
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
    client = adapter._client
    contender = DynamoDBControlPlane(client, _TABLE_NAME, clock.now)

    def revoke_then_renew(_: list[dict[str, Any]]) -> None:
        contender.put_agent(_agent("agent-a", AgentState.REVOKED))

    adapter._client = ClientSpy(client, before_transaction=revoke_then_renew)
    with pytest.raises(Conflict, match="transaction_conflict"):
        adapter.renew_job_lease(renewal.model_copy(update={"job_id": "job-a"}))
    adapter._client = client
    assert adapter.get_job(_TENANT, "job-a") == claimed
    adapter.put_agent(_agent("agent-a"))
    adapter._client = ClientSpy(client, before_transaction=revoke_then_renew)
    with pytest.raises(Conflict, match="transaction_conflict"):
        adapter.fail_job(
            _fail_job("worker-a", claimed.fencing_token, "failed"), _event("failed-after-revoke")
        )
    adapter._client = client
    assert adapter.get_job(_TENANT, "job-a") == claimed
    assert adapter.get_outbox_event("failed-after-revoke") is None
    adapter.put_agent(_agent("agent-a"))
    complete = CompleteJob(
        tenant_id=_TENANT,
        job_id="job-a",
        owner="worker-a",
        fencing_token=claimed.fencing_token,
        manifest=_raw_record("result", "agent-b", 1, clock.now()),
    )
    with pytest.raises(Conflict, match="manifest_identity_conflict"):
        adapter.complete_job(complete, _event("invalid-manifest"))


@pytest.mark.parametrize(
    "changes",
    [
        {"dataset_name": "silver"},
        {"run_manifest_key": f"reconciliation/{_TENANT}/2026-06/run-a/run-manifest.json"},
    ],
)
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
    client = adapter._client
    adapter._client = ClientSpy(client, after_transaction=_lose_transaction_response)
    pointer = adapter.publish_dataset(named)
    adapter._client = client
    assert pointer.pointer_name == "candidate"
    assert adapter.publish_dataset(named) == pointer
    adapter.mark_outbox_delivered(named.event.event_id, clock.now())
    assert adapter.publish_dataset(named) == pointer
    changed = named.model_copy(
        update={"event": named.event.model_copy(update={"payload": {"changed": True}})}
    )
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


@pytest.mark.parametrize(
    "reason", [None, "ThrottlingError", "ProvisionedThroughputExceeded", "TransactionConflict"]
)
def test_codec_propaga_cancelamento_sem_falha_condicional(reason: str | None) -> None:
    class CanceledClient:
        @staticmethod
        def transact_write_items(**kwargs: Any) -> None:
            reasons = [] if reason is None else [{"Code": reason}]
            raise ClientError(
                {"Error": {"Code": "TransactionCanceledException"}, "CancellationReasons": reasons},
                "TransactWriteItems",
            )

    with pytest.raises(ClientError):
        execute_transaction(CanceledClient(), ())


def test_idempotencia_recupera_resultado_de_corrida_confirmada(ctx: _DynamoContext) -> None:
    _, clock, adapter = ctx
    command = BeginIdempotency(
        tenant_id=_TENANT,
        scope="jobs",
        key="race",
        request_hash=_HASH_A,
        resource_id="resource-a",
        now=clock.now(),
        expires_at=clock.now() + timedelta(minutes=5),
    )
    client = adapter._client
    adapter._client = ClientSpy(client, after_transaction=_lose_transaction_response)
    outcome = adapter.begin_idempotency(command)
    assert not outcome.created
    assert outcome.record.resource_id == "resource-a"
    adapter._client = ClientSpy(client, before_transaction=_raise_transaction_conflict)
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
