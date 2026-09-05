from collections.abc import Iterator
from datetime import timedelta
from inspect import currentframe
from pathlib import Path
from warnings import catch_warnings, simplefilter

import boto3
import pytest
from moto import mock_aws

from cnes_domain.control_plane.entities import RunDependency
from cnes_domain.control_plane.enums import RunState
from cnes_domain.control_plane.queries import (
    LatestSucceededJobQuery,
    RawIdentity,
    RawManifestChainQuery,
    WaitingRunsForDependencyQuery,
)
from cnes_domain.ports.control_plane import ControlPlanePort, TypedRawQueryPort
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_keys import item_key, run_entity_key
from packages.cnes_infra.tests.contracts.clock import (
    _NOW,
    _TENANT,
    MutableClock,
    _job,
    _raw_record,
    _run,
    _store_record,
)
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import (
    _TABLE_NAME,
    ClientSpy,
    _create_table,
)
from packages.cnes_infra.tests.control_plane.test_dynamodb_completion_retries import (
    _complete_raw_job,
)
from packages.cnes_infra.tests.control_plane.test_dynamodb_stale_gsi import OneItemPageClient

_IDENTITY = RawIdentity("354130", "CNES", "ST", "2026-07")
_VALUES = ("354130", "CNES", "ST", "2026-07")
_FIELDS = ("tenant_id", "source_type", "file_subtype", "competencia")
_METHODS = (
    ("latest_succeeded_job", "query_latest_succeeded_job"),
    ("list_raw_manifest_chain", "query_raw_manifest_chain"),
    ("list_waiting_runs_for_dependency", "query_waiting_runs_for_dependency"),
)
type _DynamoContext = tuple[DynamoDBControlPlane, MutableClock]


@pytest.fixture
def ctx() -> Iterator[_DynamoContext]:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        _create_table(client)
        clock = MutableClock(_NOW)
        store = DynamoDBControlPlane(client, _TABLE_NAME, clock.now)
        yield store, clock


@pytest.fixture
def adapter(ctx: _DynamoContext) -> DynamoDBControlPlane:
    store, clock = ctx
    base = _raw_record("base-agent-a", "agent-a", 1, clock.now())
    _complete_raw_job(store, clock, _job("job-a"), base)
    for run_id in ("waiting-b", "waiting-a"):
        store.put_run(_run(run_id, RunState.WAITING_INPUTS))
    return store


def _arguments(legacy, form):
    values, fields = _VALUES, _FIELDS
    if legacy == "latest_succeeded_job":
        values = (_VALUES[0], "agent-a", *_VALUES[1:])
        fields = (_FIELDS[0], "agent_id", *_FIELDS[1:])
    split = {"posicional": len(values), "nomeado": 0, "misto": 2}[form]
    return values[:split], dict(zip(fields[split:], values[split:], strict=True))


def _query(legacy):
    if legacy == "latest_succeeded_job":
        return LatestSucceededJobQuery(_IDENTITY, "agent-a")
    if legacy == "list_raw_manifest_chain":
        return RawManifestChainQuery(_IDENTITY)
    return WaitingRunsForDependencyQuery(_IDENTITY)


def test_dynamodb_cumpre_ambos_protocolos(adapter):
    assert isinstance(adapter, ControlPlanePort)
    assert isinstance(adapter, TypedRawQueryPort)


@pytest.mark.parametrize(("legacy", "typed"), _METHODS)
@pytest.mark.parametrize("form", ["posicional", "nomeado", "misto"])
def test_shim_preserva_resultado_e_avisa_no_chamador(adapter, legacy, typed, form):
    args, kwargs = _arguments(legacy, form)
    with catch_warnings(record=True) as warnings:
        simplefilter("always")
        line = currentframe().f_lineno + 1
        result = getattr(adapter, legacy)(*args, **kwargs)
    assert len(warnings) == 1
    assert warnings[0].category is DeprecationWarning
    assert str(warnings[0].message) == f"method={legacy} replacement={typed}"
    assert Path(warnings[0].filename) == Path(__file__)
    assert warnings[0].lineno == line
    with catch_warnings(record=True) as typed_warnings:
        simplefilter("always")
        assert result == getattr(adapter, typed)(_query(legacy))
    assert typed_warnings == []
    assert result is not None
    assert result != ()


@pytest.mark.parametrize("limit", [0, -1, -100])
@pytest.mark.parametrize("methods", _METHODS[1:])
def test_limite_nao_positivo_retorna_vazio_sem_consultar_dynamodb(adapter, limit, methods):
    legacy, typed = methods
    adapter._client = spy = ClientSpy(adapter._client)
    query = type(_query(legacy))(_IDENTITY, limit=limit)
    assert getattr(adapter, typed)(query) == ()
    with pytest.warns(DeprecationWarning, match=f"method={legacy} replacement={typed}"):
        assert getattr(adapter, legacy)(*_VALUES, limit=limit) == ()
    assert spy.calls == []


def test_consultas_preservam_ordem_isolamento_e_persistencia(adapter):
    reopened = DynamoDBControlPlane(adapter._client, _TABLE_NAME, adapter._clock)
    latest = reopened.query_latest_succeeded_job(LatestSucceededJobQuery(_IDENTITY, "agent-a"))
    assert latest.job_id == "job-a"
    waiting = reopened.query_waiting_runs_for_dependency(WaitingRunsForDependencyQuery(_IDENTITY))
    assert tuple(run.run_id for run in waiting) == ("waiting-a", "waiting-b")
    chain = reopened.query_raw_manifest_chain(RawManifestChainQuery(_IDENTITY))
    assert tuple(ref.manifest_id for ref in chain) == ("manifest-agent-a-base-agent-a",)
    for legacy, typed in _METHODS:
        foreign = RawIdentity("other", "CNES", "ST", "2026-07")
        query = type(_query(legacy))(foreign, "agent-a") if legacy == _METHODS[0][0] else (
            type(_query(legacy))(foreign)
        )
        result = getattr(reopened, typed)(query)
        assert result is None if legacy == _METHODS[0][0] else result == ()


def test_consultas_raw_usam_marcadores_com_leitura_forte(adapter):
    adapter._client = spy = ClientSpy(adapter._client)
    assert adapter.query_latest_succeeded_job(LatestSucceededJobQuery(_IDENTITY, "agent-a"))
    assert adapter.query_raw_manifest_chain(RawManifestChainQuery(_IDENTITY))
    assert spy.query_requests == []
    requests = [request for name, request in spy.requests if name == "get_item"]
    assert len(requests) == 2
    assert all(request["ConsistentRead"] is True for request in requests)
    assert [request["Key"] for request in requests] == [
        {
            "pk": {"S": "TENANT#333534313330#RAW#434e4553#5354#323032362d3037"},
            "sk": {"S": "LATEST_JOB#6167656e742d61"},
        },
        {
            "pk": {"S": "TENANT#333534313330#RAW#434e4553#5354#323032362d3037"},
            "sk": {"S": "HEAD#CURRENT"},
        },
    ]


@pytest.mark.parametrize("updates", [
    {"state": RunState.PROCESSING},
    {"tenant_id": "other"},
    {"competencia": "2026-08"},
    {"dependencies": (RunDependency(source_type="SIHD", file_subtype="AIH", required=True),)},
])
def test_rele_candidato_obsoleto_e_pagina_ate_encontrar_run_valido(adapter, updates):
    stale = _run("obsolete", RunState.WAITING_INPUTS)
    adapter.put_run(stale)
    adapter._client.update_item(
        TableName=_TABLE_NAME,
        Key=item_key(*run_entity_key("354130", "obsolete")),
        UpdateExpression="SET payload = :payload",
        ExpressionAttributeValues={
            ":payload": {"S": stale.model_copy(update=updates).model_dump_json()}
        },
    )
    adapter._client = spy = ClientSpy(adapter._client)
    result = adapter.query_waiting_runs_for_dependency(
        WaitingRunsForDependencyQuery(_IDENTITY, limit=1)
    )
    assert tuple(run.run_id for run in result) == ("waiting-a",)
    assert len(spy.query_requests) == 2
    assert all(request["IndexName"] == "gsi3" for request in spy.query_requests)
    assert all("ConsistentRead" not in request for request in spy.query_requests)
    gets = [request for name, request in spy.requests if name == "get_item"]
    assert len(gets) == 2
    assert all(request["ConsistentRead"] is True for request in gets)


def test_latest_succeeded_ignora_omissao_do_gsi_e_historico(ctx: _DynamoContext) -> None:
    adapter, clock = ctx
    adapter._client = spy = ClientSpy(adapter._client)
    old = _raw_record("base", "agent-a", 1, clock.now())
    new = _raw_record("zeta", "agent-a", 1, clock.now() + timedelta(seconds=1))
    for record in (old, new):
        _store_record(adapter, record, clock)
    requests = [next(iter(action.values())) for action in spy.transactions[-1]]
    items = [request["Item" if "Item" in request else "Key"] for request in requests]
    assert len(requests) == len({(item["pk"]["S"], item["sk"]["S"]) for item in items}) == 8
    head = next(
        request for request in requests if request.get("Item", {}).get("entity") == {"S": "RAWHEAD"}
    )
    assert head["ConditionExpression"] == "payload = :expected"
    completed = adapter.get_job(_TENANT, "job-agent-a-zeta")
    adapter._client = stale = OneItemPageClient(spy.client)
    stale.hidden_gsi2sk = adapter._job_item(completed)["gsi2sk"]
    result = adapter.query_latest_succeeded_job(LatestSucceededJobQuery(_IDENTITY, "agent-a"))
    assert (result, stale.query_requests) == (completed, [])
    adapter._client = spy.client
    records = tuple(
        _raw_record(f"full-{index:02}", f"agent-{index}", 1, clock.now() + timedelta(seconds=index))
        for index in range(11)
    )
    for record in records:
        _store_record(adapter, record, clock)
    chain = adapter.query_raw_manifest_chain(RawManifestChainQuery(_IDENTITY, 1))
    assert tuple(ref.manifest_id for ref in chain) == (records[-1].manifest_id,)
