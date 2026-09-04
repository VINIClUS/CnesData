from collections.abc import Iterator
from typing import Any

import boto3
import pytest
from moto import mock_aws

from cnes_domain.control_plane.commands import CompleteJob
from cnes_domain.control_plane.enums import JobState, RunState
from cnes_domain.control_plane.errors import Conflict
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from packages.cnes_infra.tests.contracts.clock import (
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
from packages.cnes_infra.tests.contracts.control_plane_contract import _publish
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import (
    _TABLE_NAME,
    ClientSpy,
    _create_table,
    _lose_transaction_response,
    _raise_transaction_canceled,
)

type _DynamoContext = tuple[DynamoDBControlPlane, MutableClock]


@pytest.fixture
def ctx() -> Iterator[_DynamoContext]:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        _create_table(client)
        clock = MutableClock(_NOW)
        yield DynamoDBControlPlane(client, _TABLE_NAME, clock.now), clock


def _leased_job(adapter: DynamoDBControlPlane, clock: MutableClock) -> Any:
    adapter.put_agent(_agent("agent-a"))
    adapter.create_job(_job("job-a"), _event("job-created"))
    claimed = adapter.claim_job(_claim_job("job-a", "worker-a", clock))
    assert claimed is not None
    return claimed


def test_complete_aceita_modo_de_manifesto_diferente_do_solicitado(
    ctx: _DynamoContext,
) -> None:
    adapter, clock = ctx
    adapter.put_agent(_agent("agent-a"))
    job = _job("job-a").model_copy(update={"requested_snapshot_mode": "DELTA"})
    adapter.create_job(job, _event("job-created"))
    claimed = adapter.claim_job(_claim_job("job-a", "worker-a", clock))
    assert claimed is not None
    manifest = _raw_record("snapshot-a", "agent-a", 1, clock.now())
    completed = adapter.complete_job(
        CompleteJob(
            tenant_id=_TENANT,
            job_id="job-a",
            owner="worker-a",
            fencing_token=claimed.fencing_token,
            manifest=manifest,
        ),
        _event("job-completed"),
    )
    assert completed.state is JobState.SUCCEEDED
    assert adapter.latest_succeeded_job(_TENANT, "agent-a", "CNES", "ST", "2026-07") == completed


def test_complete_tenta_tres_vezes_sem_mutar_em_contencao(
    ctx: _DynamoContext,
) -> None:
    adapter, clock = ctx
    claimed = _leased_job(adapter, clock)
    manifest = _raw_record("snapshot-a", "agent-a", 1, clock.now())
    adapter._client = failing = ClientSpy(
        adapter._client,
        before_transaction=_raise_transaction_canceled,
    )
    command = CompleteJob(
        tenant_id=_TENANT,
        job_id="job-a",
        owner="worker-a",
        fencing_token=claimed.fencing_token,
        manifest=manifest,
    )
    with pytest.raises(TimeoutError, match=r"^transaction_contention$"):
        adapter.complete_job(command, _event("job-completed"))
    assert len(failing.transactions) == 3
    assert adapter.get_job(_TENANT, "job-a") == claimed
    assert adapter.get_outbox_event("job-completed") is None
    assert adapter.list_raw_manifest_chain(_TENANT, "CNES", "ST", "2026-07", 1) == ()


def test_publicacao_aceita_replay_identico_apos_conflito_com_leitura_forte(
    ctx: _DynamoContext,
) -> None:
    adapter, _ = ctx
    command = _publish("run-a", "published", None, False)
    adapter.put_run(_run("run-a", RunState.PUBLISHING))
    adapter._client = spy = ClientSpy(
        adapter._client,
        after_transaction=_lose_transaction_response,
    )
    pointer = adapter.publish_dataset(command)
    get_requests = [request for name, request in spy.requests if name == "get_item"]
    assert pointer.version_id == "run-a"
    assert get_requests[-1]["ConsistentRead"] is True


def test_publicacao_rejeita_winner_divergente_apos_conflito(ctx: _DynamoContext) -> None:
    adapter, clock = ctx
    command = _publish("run-a", "published", None, False)
    winner = command.model_copy(update={"event": _event("winner-published", aggregate_id="run-a")})
    adapter.put_run(_run("run-a", RunState.PUBLISHING))
    contender = DynamoDBControlPlane(adapter._client, _TABLE_NAME, clock.now)

    def publish_winner_then_report_conflict(actions: list[dict[str, Any]]) -> None:
        contender.publish_dataset(winner)
        _lose_transaction_response(actions)

    adapter._client = ClientSpy(
        adapter._client,
        before_transaction=publish_winner_then_report_conflict,
    )
    with pytest.raises(Conflict, match="publication_replay_conflict"):
        adapter.publish_dataset(command)
    assert adapter.get_outbox_event(command.event.event_id) is None
