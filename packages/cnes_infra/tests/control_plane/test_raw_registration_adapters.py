from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import boto3
import pytest
from moto import mock_aws

from cnes_domain.control_plane.commands import CompleteJob, FailJob
from cnes_domain.control_plane.entities import RawResyncState
from cnes_domain.control_plane.enums import JobState
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.control_plane.queries import (
    AgentRawManifestChainQuery,
    LatestSucceededJobQuery,
    RawIdentity,
    RawResyncStateQuery,
)
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_codec import encode_model
from cnes_infra.control_plane.dynamodb_keys import (
    item_key,
    key_component,
    raw_manifest_lookup_key,
    raw_partition,
    raw_resync_key,
)
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from packages.cnes_infra.tests.contracts.clock import (
    _TENANT,
    MutableClock,
    _agent,
    _claim_job,
    _event,
    _fail_job,
    _job,
    _raw_record,
)
from packages.cnes_infra.tests.contracts.control_plane_raw_contract import (
    _accept_record,
    _case_delta_completion_cas,
    _case_delta_completion_rejects_marker,
    _case_raw_registration_state,
    _delta_completion,
)
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import _create_table

_TABLE_NAME = "cnesdata-control-plane"


@pytest.fixture
def clock() -> MutableClock:
    return MutableClock(datetime(2026, 7, 15, 12, tzinfo=UTC))


@pytest.fixture
def sqlite_control_plane(tmp_path, clock: MutableClock) -> SQLiteControlPlane:
    adapter = SQLiteControlPlane(tmp_path / "control-plane.sqlite3", clock.now)
    adapter.initialize()
    return adapter


@pytest.fixture
def dynamodb_adapter() -> Iterator[tuple[DynamoDBControlPlane, MutableClock]]:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        _create_table(client)
        clock = MutableClock(datetime(2026, 7, 15, 12, tzinfo=UTC))
        yield DynamoDBControlPlane(client, _TABLE_NAME, clock.now), clock


class _TransactionSpy:
    def __init__(self, client: Any) -> None:
        self.client = client
        self.transactions = []
        self.before_transaction = self._create_marker

    def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
        actions = kwargs["TransactItems"]
        self.transactions.append(actions)
        if self.before_transaction is not None:
            self.before_transaction(actions)
        return self.client.transact_write_items(**kwargs)

    def _create_marker(self, actions: list[dict[str, Any]]) -> None:
        self.before_transaction = None
        marker = next(
            action["Update"] for action in actions
            if "if_not_exists" in action.get("Update", {}).get("UpdateExpression", "")
        )
        self.client.update_item(**marker)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.client, name)


class _FullMarkerRaceSpy:
    def __init__(self, client: Any, marker_item: dict[str, Any]) -> None:
        self.client = client
        self.marker_item = marker_item
        self.transactions = 0

    def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
        self.transactions += 1
        if self.marker_item:
            item = self.marker_item
            self.marker_item = {}
            self.client.put_item(TableName=_TABLE_NAME, Item=item)
        return self.client.transact_write_items(**kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.client, name)


def test_sqlite_persiste_registro_raw_atomico(sqlite_control_plane, clock) -> None:
    _case_raw_registration_state(sqlite_control_plane, clock)


def test_dynamodb_persiste_registro_raw_atomico(dynamodb_adapter) -> None:
    adapter, clock = dynamodb_adapter
    _case_raw_registration_state(adapter, clock)


def test_sqlite_rejeita_fork_delta_concorrente(sqlite_control_plane, clock) -> None:
    _case_delta_completion_cas(sqlite_control_plane, clock)


def test_sqlite_aceite_antigo_nao_regride_cabeca(sqlite_control_plane, clock) -> None:
    sqlite_control_plane.put_agent(_agent("agent-a"))
    newest = _raw_record("newest", "agent-a", 1, clock.now())
    older = _raw_record("older", "agent-a", 1, clock.now())
    _accept_record(sqlite_control_plane, newest, clock, "job-z-newest")
    with sqlite_control_plane.write_transaction() as connection:
        connection.execute("DELETE FROM raw_agent_heads")
    _accept_record(sqlite_control_plane, older, clock, "job-a-older")
    oldest = _raw_record("oldest", "agent-a", 1, clock.now())
    _accept_record(sqlite_control_plane, oldest, clock, "job-0-oldest")
    query = AgentRawManifestChainQuery(
        RawIdentity(_TENANT, "CNES", "ST", "2026-07"), "agent-a"
    )
    chain = sqlite_control_plane.query_agent_raw_manifest_chain(query)
    assert tuple(item.manifest_id for item in chain) == (newest.manifest_id,)


def test_sqlite_aceita_primeiro_delta_de_banco_anterior_ao_ponteiro(
    sqlite_control_plane, clock
) -> None:
    sqlite_control_plane.put_agent(_agent("agent-a"))
    base = _raw_record("base-agent-a", "agent-a", 1, clock.now())
    _accept_record(sqlite_control_plane, base, clock, "job-z-base")
    with sqlite_control_plane.write_transaction() as connection:
        connection.execute("DELETE FROM raw_agent_heads")
    delta = _raw_record("delta-upgrade", "agent-a", 2, clock.now()).model_copy(
        update={"previous_manifest_sha256": base.manifest_sha256}
    )
    completion = _delta_completion(sqlite_control_plane, delta, clock, "job-a-delta")
    sqlite_control_plane.complete_job(completion, _event("delta-upgrade-accepted"))
    query = AgentRawManifestChainQuery(
        RawIdentity(_TENANT, "CNES", "ST", "2026-07"), "agent-a"
    )
    chain = sqlite_control_plane.query_agent_raw_manifest_chain(query)
    assert tuple(item.manifest_id for item in chain) == (base.manifest_id, delta.manifest_id)


def test_dynamodb_rejeita_fork_delta_concorrente(dynamodb_adapter) -> None:
    adapter, clock = dynamodb_adapter
    _case_delta_completion_cas(adapter, clock)


def test_sqlite_rejeita_delta_se_marcador_surgir(sqlite_control_plane, clock) -> None:
    _case_delta_completion_rejects_marker(sqlite_control_plane, clock)


def test_dynamodb_rejeita_delta_se_marcador_surgir(dynamodb_adapter) -> None:
    adapter, clock = dynamodb_adapter
    _case_delta_completion_rejects_marker(adapter, clock)


def test_sqlite_rejeicao_obsoleta_nao_bloqueia_full(sqlite_control_plane, clock) -> None:
    _case_rejection_loses_to_full(sqlite_control_plane, clock)


def test_dynamodb_rejeicao_obsoleta_nao_bloqueia_full(dynamodb_adapter) -> None:
    adapter, clock = dynamodb_adapter
    _case_rejection_loses_to_full(adapter, clock)


def _case_rejection_loses_to_full(adapter: Any, clock: MutableClock) -> None:
    adapter.put_agent(_agent("agent-a"))
    rejected = _job("job-rejected")
    adapter.create_job(rejected, _event("rejected-created"))
    claim = adapter.claim_job(_claim_job(rejected.job_id, "worker", clock))
    failure = _fail_job("worker", claim.fencing_token, "RAW_RESYNC_BASE_UNKNOWN").model_copy(
        update={
            "job_id": rejected.job_id,
            "retryable": False,
            "rejected_manifest_sha256": "b" * 64,
            "expected_resync_marker": False,
        }
    )
    full = _raw_record("concurrent-full", "agent-a", 1, clock.now())
    _accept_record(adapter, full, clock, "job-full")
    with pytest.raises(Conflict):
        adapter.fail_job(failure, _event("stale-resync"))
    assert adapter.get_job(_TENANT, rejected.job_id).state is JobState.LEASED
    identity = RawIdentity(_TENANT, "CNES", "ST", "2026-07")
    assert adapter.query_raw_resync_state(RawResyncStateQuery(identity, "agent-a")) is None


def _guarded_failure(
    job_id: str, fence: int, marker: bool, head: str | None = None
) -> FailJob:
    return FailJob(
        tenant_id=_TENANT,
        job_id=job_id,
        owner="worker",
        fencing_token=fence,
        error_code="RAW_RESYNC_BASE_UNKNOWN",
        retryable=False,
        rejected_manifest_sha256="b" * 64,
        expected_head_manifest_id=head,
        expected_resync_marker=marker,
    )


def test_sqlite_rejeicao_guardada_perde_para_full(sqlite_control_plane, clock) -> None:
    _case_guarded_rejection_loses_to_full(sqlite_control_plane, clock)


def test_dynamodb_rejeicao_guardada_perde_para_full(dynamodb_adapter) -> None:
    adapter, clock = dynamodb_adapter
    _case_guarded_rejection_loses_to_full(adapter, clock)


def _case_guarded_rejection_loses_to_full(adapter: Any, clock: MutableClock) -> None:
    adapter.put_agent(_agent("agent-a"))
    base = _raw_record("guard-base", "agent-a", 1, clock.now())
    _accept_record(adapter, base, clock, "job-base")
    first = _job("job-first-rejection")
    adapter.create_job(first, _event("first-rejection-created"))
    claim = adapter.claim_job(_claim_job(first.job_id, "worker", clock))
    adapter.fail_job(
        _guarded_failure(first.job_id, claim.fencing_token, False, base.manifest_id),
        _event("first-resync"),
    )
    second = _job("job-second-rejection")
    adapter.create_job(second, _event("second-rejection-created"))
    claim = adapter.claim_job(_claim_job(second.job_id, "worker", clock))
    stale = _guarded_failure(second.job_id, claim.fencing_token, True)
    full = _raw_record("guard-full", "agent-a", 1, clock.now())
    _accept_record(adapter, full, clock, "job-z-full")
    with pytest.raises(Conflict):
        adapter.fail_job(stale, _event("stale-marker-resync"))
    assert adapter.get_job(_TENANT, second.job_id).state is JobState.LEASED


def test_sqlite_full_de_recuperacao_antigo_vira_cabeca(sqlite_control_plane, clock) -> None:
    _case_recovery_full_becomes_head(sqlite_control_plane, clock)


def test_dynamodb_full_de_recuperacao_antigo_vira_cabeca(dynamodb_adapter) -> None:
    adapter, clock = dynamodb_adapter
    _case_recovery_full_becomes_head(adapter, clock)


def _case_recovery_full_becomes_head(adapter: Any, clock: MutableClock) -> None:
    adapter.put_agent(_agent("agent-a"))
    head = _raw_record("existing-head", "agent-a", 1, clock.now())
    _accept_record(adapter, head, clock, "job-z-head")
    rejected = _job("job-marker")
    adapter.create_job(rejected, _event("marker-created"))
    claim = adapter.claim_job(_claim_job(rejected.job_id, "worker", clock))
    adapter.fail_job(
        _guarded_failure(rejected.job_id, claim.fencing_token, False, head.manifest_id),
        _event("marker-required"),
    )
    recovery = _raw_record("recovery", "agent-a", 1, clock.now())
    _accept_record(adapter, recovery, clock, "job-a-recovery")
    identity = RawIdentity(_TENANT, "CNES", "ST", "2026-07")
    latest = adapter.query_latest_succeeded_job(LatestSucceededJobQuery(identity, "agent-a"))
    assert latest.result_manifest_id == recovery.manifest_id
    assert adapter.query_raw_resync_state(RawResyncStateQuery(identity, "agent-a")) is None


def test_dynamodb_retry_de_full_reavalia_marcador_criado_em_corrida(
    dynamodb_adapter,
) -> None:
    adapter, clock = dynamodb_adapter
    adapter.put_agent(_agent("agent-a"))
    head = _raw_record("race-head", "agent-a", 1, clock.now())
    _accept_record(adapter, head, clock, "job-z-head")
    recovery = _raw_record("race-recovery", "agent-a", 1, clock.now())
    job = _job("job-a-recovery")
    adapter.create_job(job, _event("recovery-created"))
    claim = adapter.claim_job(_claim_job(job.job_id, "worker", clock))
    command = CompleteJob(
        tenant_id=_TENANT, job_id=job.job_id, owner="worker",
        fencing_token=claim.fencing_token, manifest=recovery,
    )
    partition = raw_partition(_TENANT, "CNES", "ST", "2026-07")
    key = raw_resync_key(partition, "agent-a")
    state = RawResyncState(
        tenant_id=_TENANT, agent_id="agent-a", source_type="CNES",
        file_subtype="ST", competencia="2026-07", required_since=clock.now(),
    )
    adapter._client = spy = _FullMarkerRaceSpy(
        adapter._client, encode_model(state, "RAWRESYNCSTATE", key)
    )
    adapter.complete_job(command, _event("recovery-accepted"))
    identity = RawIdentity(_TENANT, "CNES", "ST", "2026-07")
    latest = adapter.query_latest_succeeded_job(LatestSucceededJobQuery(identity, "agent-a"))
    assert latest.result_manifest_id == recovery.manifest_id
    assert spy.transactions == 2


def test_dynamodb_rejeita_delta_se_head_sumir(dynamodb_adapter) -> None:
    adapter, clock = dynamodb_adapter
    adapter.put_agent(_agent("agent-a"))
    delta = _raw_record("delta-sem-head", "agent-a", 2, clock.now())
    completion = _delta_completion(adapter, delta, clock, "job-delta-sem-head")
    with pytest.raises(Conflict):
        adapter.complete_job(completion, _event("delta-sem-head-accepted"))
    assert adapter.get_job(_TENANT, completion.job_id).state is JobState.LEASED


def test_dynamodb_rejeita_resync_se_head_observado_sumir(dynamodb_adapter) -> None:
    adapter, clock = dynamodb_adapter
    adapter.put_agent(_agent("agent-a"))
    base = _raw_record("guard-missing", "agent-a", 1, clock.now())
    _accept_record(adapter, base, clock, "job-base")
    rejected = _job("job-rejected-missing")
    adapter.create_job(rejected, _event("rejected-missing-created"))
    claim = adapter.claim_job(_claim_job(rejected.job_id, "worker", clock))
    failure = _guarded_failure(
        rejected.job_id, claim.fencing_token, False, base.manifest_id
    )
    partition = raw_partition(_TENANT, "CNES", "ST", "2026-07")
    key = partition, f"LATEST_JOB#{key_component('agent-a')}"
    adapter._client.delete_item(TableName=_TABLE_NAME, Key=item_key(*key))
    with pytest.raises(Conflict):
        adapter.fail_job(failure, _event("missing-head-resync"))
    assert adapter.get_job(_TENANT, rejected.job_id).state is JobState.LEASED


def test_sqlite_rejeita_delta_se_head_sumir(sqlite_control_plane, clock) -> None:
    sqlite_control_plane.put_agent(_agent("agent-a"))
    delta = _raw_record("delta-sem-head", "agent-a", 2, clock.now())
    completion = _delta_completion(sqlite_control_plane, delta, clock, "job-delta-sem-head")
    with pytest.raises(Conflict):
        sqlite_control_plane.complete_job(completion, _event("delta-sem-head-accepted"))
    assert sqlite_control_plane.get_job(_TENANT, completion.job_id).state is JobState.LEASED


def test_rejeicao_dynamodb_preserva_marcador_criado_em_corrida(dynamodb_adapter) -> None:
    adapter, clock = dynamodb_adapter
    adapter.put_agent(_agent("agent-a"))
    adapter.create_job(_job("job-a"), _event("job-created"))
    claimed = adapter.claim_job(_claim_job("job-a", "worker-a", clock))
    command = _fail_job("worker-a", claimed.fencing_token, "RAW_RESYNC_BASE_UNKNOWN").model_copy(
        update={"retryable": False, "rejected_manifest_sha256": "b" * 64}
    )
    adapter._client = spy = _TransactionSpy(adapter._client)
    failed = adapter.fail_job(command, _event("resync-required"))
    identity = RawIdentity(_TENANT, "CNES", "ST", "2026-07")
    assert failed.state is JobState.FAILED_FINAL
    assert adapter.query_raw_resync_state(RawResyncStateQuery(identity, "agent-a")) is not None
    assert len(spy.transactions) == 1


def test_sqlite_omite_cadeia_se_lookup_da_cabeca_sumir(sqlite_control_plane, clock) -> None:
    base = _raw_record("full-base", "agent-a", 1, clock.now())
    record = _raw_record("full-corrupted", "agent-a", 1, clock.now())
    sqlite_control_plane.put_agent(_agent("agent-a"))
    _accept_record(sqlite_control_plane, base, clock, "job-a-base")
    _accept_record(sqlite_control_plane, record, clock, "job-z-corrupted")
    with sqlite_control_plane.write_transaction() as connection:
        connection.execute(
            "DELETE FROM raw_manifests WHERE tenant_id = ? AND manifest_id = ?",
            (_TENANT, record.manifest_id),
        )
    query = AgentRawManifestChainQuery(
        RawIdentity(_TENANT, "CNES", "ST", "2026-07"), "agent-a"
    )
    assert sqlite_control_plane.query_agent_raw_manifest_chain(query) == ()


def test_dynamodb_omite_cadeia_se_lookup_da_cabeca_sumir(dynamodb_adapter) -> None:
    adapter, clock = dynamodb_adapter
    record = _raw_record("full-corrupted", "agent-a", 1, clock.now())
    adapter.put_agent(_agent("agent-a"))
    _accept_record(adapter, record, clock, "job-corrupted")
    key = raw_manifest_lookup_key(_TENANT, record.manifest_id)
    adapter._client.delete_item(TableName=_TABLE_NAME, Key=item_key(*key))
    query = AgentRawManifestChainQuery(
        RawIdentity(_TENANT, "CNES", "ST", "2026-07"), "agent-a"
    )
    assert adapter.query_agent_raw_manifest_chain(query) == ()
