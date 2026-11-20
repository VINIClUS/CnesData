"""Handler ABI2 close-drain sobre o shim de context.call e o vetor versionado (#473)."""

from __future__ import annotations

import hashlib
import json
import re
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from cnes_domain.control_plane.environment_gate import ClosePromotionFence, ReopenPromotionFence
from cnes_domain.orchestration.dispatch_admission import EnvironmentAdmission
from cnes_domain.ports.processing import ExecutionCallbacks, ExecutionPolicyConfig
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.executor.step_functions import StepFunctionsExecutionProbe, StepFunctionsExecutor
from data_processor.orchestration.coordinator import (
    CoordinatorDependencies,
    PipelineCoordinator,
    allow_execution,
    noop_execution_started,
)
from packages.cnes_infra.tests.control_plane import test_dynamodb_adapter as tables
from scripts.promotion import promote_api, promote_close
from scripts.promotion.environment_gate import DrainError
from tests.production.delivery.test_phase_b import NOW, Clock, Probe, _claim, _seed

if TYPE_CHECKING:
    from collections.abc import Iterator

SHA = "a" * 40
ACCOUNT = "123456789012"
MACHINE_ARN = f"arn:aws:states:us-east-2:{ACCOUNT}:stateMachine:cnesdata-prod"
EXECUTION = MACHINE_ARN.replace(":stateMachine:", ":execution:")
TASKS = f"arn:aws:ecs:us-east-2:{ACCOUNT}:task-definition/cnesdata-prod-processor-"
VECTOR = Path(__file__).with_name("close_drain_call_vector.json")
INFRA_CLOSE_DRAIN_ROW = {"inputs": "activation expirations", "stage": {"application-prepare"},
                         "observation": "close-drain"}
INFRA_CLOSE_DRAIN_FIELDS = (
    "version kind common fence close_confirmed_at drain_observed_at frozen_membership_sha256 "
    "members proofs permit input_expirations fixed_deadline decision"
)
INFRA_CLOSE_EXPIRATION_KINDS = frozenset({"envelope", "artifact", "platform", "cost",
                                          "credential", "activation", "plan"})
OWNER = {"repository": "VINIClUS/CnesData", "source_sha": SHA, "run_id": 123, "run_attempt": 1,
         "release_sha256": "5" * 64, "generation": 7, "session_sha256": "6" * 64,
         "owner_sha256": "7" * 64}
CALLER = {"repository": "VINIClUS/CnesData", "source_sha": SHA, "ref": "refs/heads/main",
          "workflow_path": ".github/workflows/promote-production.yml", "workflow_sha": SHA,
          "run_id": 123, "run_attempt": 1}
EXPIRY = {"kind": "credential", "object_sha256": "f" * 64, "expires_at": "2026-11-09T13:00:00Z"}
FENCE = {"pk": {"S": "ENV#prod#FENCE"}, "sk": {"S": "FENCE"}}
SEMAPHORE = {"pk": {"S": "ENV#prod#SEMAPHORE"}, "sk": {"S": "UNIT"}}
OTHER = {"kind": "artifact", "object_sha256": "0" * 64, "expires_at": "2026-11-09T14:00:00Z"}
SCENARIOS = ("empty", "reopened-cancel", "terminal")


def canonical(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
            + "\n").encode()


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _epoch(seconds: int) -> int:
    return int((NOW + timedelta(seconds=seconds)).timestamp())


PREPARE = {"close": None, "fixed_deadline": None, "input_deadline": _epoch(3600),
           "stage": "application-prepare", "admitted_at": _epoch(0), "deadline": _epoch(180),
           "decision": None, "preparation_deadline": _epoch(180), "sequence": 0,
           "checkpoint_sha256": "8" * 64}


def _activation(**changes: Any) -> bytes:
    payload = {
        "project": "cnesdata", "environment": "prod", "schema_version": 1,
        "repository": "VINIClUS/CnesData", "release_id": "r" + "1" * 31, "source_sha": SHA,
        "candidate_sha256": "b" * 64, "binding_sha256": "c" * 64, "canary_id": "c" * 32,
        "derivation": "cnesdata:prod:canary:v1",
        "revisions": {"unit": TASKS + "unit:2", "recovery": TASKS + "recovery:1",
                      "audit": TASKS + "audit:1"},
        "ecr_uri": f"{ACCOUNT}.dkr.ecr.us-east-2.amazonaws.com/cnesdata-prod@sha256:" + "d" * 64,
        "phase_a_evidence_digest": "e" * 64,
    }
    return canonical({key: value for key, value in (payload | changes).items()
                      if value is not None})


def _inputs(activation: bytes | None = None, expirations: bytes | None = None) -> dict[str, bytes]:
    return {"activation": _activation() if activation is None else activation,
            "expirations": canonical([EXPIRY]) if expirations is None else expirations}


def _request(inputs: dict[str, bytes], stage: dict[str, Any] = PREPARE,
             owner: dict[str, Any] = OWNER) -> dict[str, Any]:
    return {"abi_version": 2, "operation": "close-drain", "repository": "VINIClUS/CnesData",
            "source_sha": SHA, "owner": owner, "checkpoint_sha256": stage["checkpoint_sha256"],
            "activation_sha256": sha(inputs["activation"]), "stage": stage,
            "inputs": {name: {"sha256": sha(raw), "size": len(raw)}
                       for name, raw in sorted(inputs.items())}}


@dataclass
class Context:
    ddb: Any
    probe: Probe
    inputs: dict[str, bytes]
    calls: list[tuple[str, str, str, dict[str, Any]]] = field(default_factory=list)
    observed: list[tuple[str, bytes]] = field(default_factory=list)
    guards: int = 0

    def read(self, name: str) -> bytes:
        return self.inputs[name]

    def guard(self) -> None:
        self.guards += 1

    def observe(self, kind: str, raw: bytes) -> str:
        self.observed.append((kind, raw))
        return sha(raw)

    def evidence_snapshot(self) -> dict[str, Any]:
        return {"call_receipts_sha256": "e" * 64, "pending": [], "unknown": []}

    def call(self, role: str, service: str, operation: str, parameters: dict[str, Any]) -> Any:
        self.calls.append((role, service, operation, json.loads(json.dumps(parameters))))
        if service == "stepfunctions":
            return self._describe(operation, parameters)
        try:
            response = getattr(self.ddb, operation)(**parameters)
        except ClientError as error:
            raise RuntimeError("native SDK outcome unknown") from error
        return json.loads(json.dumps({k: v for k, v in response.items()
                                      if k != "ResponseMetadata"}))

    def _describe(self, operation: str, parameters: dict[str, Any]) -> dict[str, Any]:
        assert operation == "describe_execution"
        assert parameters["includedData"] == "METADATA_ONLY"
        arn = parameters["executionArn"]
        if not self.probe.terminal:
            raise ClientError({"Error": {"Code": "ExecutionDoesNotExist"}}, "DescribeExecution")
        return {"executionArn": arn, "stateMachineArn": MACHINE_ARN, "name": arn.rsplit(":", 1)[1],
                "status": "SUCCEEDED", "startDate": "2026-11-09T12:00:01Z"}


def _recovering_clock(adapter: DynamoDBControlPlane, probe: Probe) -> Clock:
    native = StepFunctionsExecutionProbe(cast("Any", probe), MACHINE_ARN)
    admission = EnvironmentAdmission(adapter, native, "prod", 300, adapter)
    executor = StepFunctionsExecutor(cast("Any", probe), MACHINE_ARN)
    dependencies = CoordinatorDependencies(adapter, executor, cast("Any", None),
                                           lambda: NOW + timedelta(seconds=1))
    execution = ExecutionPolicyConfig(1, 300, ExecutionCallbacks(allow_execution,
                                                                 noop_execution_started), admission)
    coordinator = PipelineCoordinator(dependencies, execution)
    return Clock(lambda: coordinator.recover())


@contextmanager
def _environment(name: str, monkeypatch: pytest.MonkeyPatch) -> Iterator[tuple[Any, Any, Probe]]:
    with mock_aws():
        raw = boto3.client("dynamodb", region_name="us-east-2")
        monkeypatch.setattr(tables, "_TABLE_NAME", promote_close.TABLE)
        tables._create_table(raw)
        adapter = DynamoDBControlPlane(raw, promote_close.TABLE, lambda: NOW)
        probe = Probe()
        if name.startswith("reopened"):
            older = NOW - timedelta(hours=1)
            assert adapter.close_promotion_fence(ClosePromotionFence("prod", "older", older))
            assert adapter.reopen_promotion_fence(ReopenPromotionFence("prod", "older", older))
        if name != "empty":
            dispatch, execution, permit = _seed(adapter)
            probe.request = execution
            if name == "terminal":
                _claim(adapter, dispatch, execution, permit)
                probe.terminal = True
        monkeypatch.setattr(promote_close, "DRAIN_CLOCK", _recovering_clock(adapter, probe))
        yield raw, adapter, probe


@pytest.fixture(autouse=True)
def _relogio(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(promote_api, "CLOCK", lambda: NOW)


def _run(context: Context, request: dict[str, Any] | None = None) -> dict[str, Any]:
    return promote_api.execute_shared_operation("close-drain", request or _request(context.inputs),
                                                context)


def test_tabela_do_close_drain_e_copia_literal_da_correcao_do_infra() -> None:
    """Correção infra-ansible#55 6906038586 (close-drain) e main ca3643e5 (campos, kinds)."""
    assert promote_api.INPUTS["close-drain"] == frozenset(INFRA_CLOSE_DRAIN_ROW["inputs"].split())
    assert promote_api.STAGES["close-drain"] == INFRA_CLOSE_DRAIN_ROW["stage"]
    assert promote_api.OBSERVATIONS["close-drain"] == INFRA_CLOSE_DRAIN_ROW["observation"]
    assert INFRA_CLOSE_EXPIRATION_KINDS - {"envelope"} == promote_close.EXPIRATION_KINDS
    assert promote_close.ROLE == "fence_operator"
    assert promote_close.TABLE == "cnesdata-prod-control-plane"


@pytest.mark.parametrize("name", SCENARIOS)
def test_close_drain_fecha_drena_e_observa_documento_canonico(
    name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _environment(name, monkeypatch) as (raw, adapter, probe):
        context = Context(raw, probe, _inputs())
        result = _run(context)
        state = adapter.observe_environment_gate("prod")
    [(kind, observation)] = context.observed
    record = json.loads(observation)
    assert kind == "close-drain"
    assert observation == canonical(record)
    assert set(record) == set(INFRA_CLOSE_DRAIN_FIELDS.split())
    activation = json.loads(_activation())
    assert record["common"] == {
        **{key: activation[key] for key in
           ("repository", "release_id", "source_sha", "candidate_sha256", "canary_id")},
        "activation_sha256": sha(_activation()), "promotion_id": OWNER["owner_sha256"],
        "promotion_generation": OWNER["generation"], "caller": CALLER,
    }
    assert record["fence"]["promotion_id"] == state.fence.promotion_id == OWNER["owner_sha256"]
    assert state.fence.state.value == "CLOSED"
    assert state.permit is None
    assert len(record["members"]) == (0 if name == "empty" else 1)
    assert EXPIRY in record["input_expirations"]
    assert [item["kind"] for item in record["input_expirations"]] == ["credential", "envelope"]
    assert {role for role, *_ in context.calls} == {"fence_operator"}
    assert context.guards == len(context.calls)
    assert result == {"abi_version": 2, "operation": "close-drain", "generation": 7,
                      "owner_sha256": OWNER["owner_sha256"], "unknown": [], "pending": [],
                      "checkpoint_sha256": PREPARE["checkpoint_sha256"], "call_receipts_sha256":
                      "e" * 64, "observations": [{"kind": "close-drain", "size": len(observation),
                                                  "sha256": sha(observation)}]}
    assert promote_close.owner_identity(_request(_inputs())) == {
        "caller": CALLER, "promotion_generation": 7, "promotion_id": OWNER["owner_sha256"]}


SNAPSHOT = "<AND over sorted semaphore attributes: #vN = :vN | attribute_not_exists(#vN)>"


def _item_shape(item: dict[str, Any]) -> dict[str, Any]:
    [(kind, body)] = item.items()
    if kind == "ConditionCheck" and body["ConditionExpression"] != "attribute_not_exists(pk)":
        return {kind: body | dict.fromkeys(("ConditionExpression", "ExpressionAttributeNames",
                                            "ExpressionAttributeValues"), SNAPSHOT)}
    if "ExpressionAttributeValues" in body:
        body = body | {"ExpressionAttributeValues": sorted(body["ExpressionAttributeValues"])}
    return {kind: body}


def _shape(call: tuple[str, str, str, dict[str, Any]]) -> dict[str, Any]:
    role, service, operation, parameters = call
    if operation == "describe_execution":
        assert parameters["executionArn"].startswith(EXECUTION + ":")
        parameters = parameters | {"executionArn": "<state-machine-execution>:<dispatch_id>"}
    elif operation == "update_item":
        parameters = parameters | {
            "ExpressionAttributeValues": sorted(parameters["ExpressionAttributeValues"])
        }
    elif operation == "transact_write_items":
        parameters = {"TransactItems": ["<close_transaction.update>",
                                        "<close_transaction.condition_check>"]}
    return {"role": role, "service": service, "operation": operation, "parameters": parameters}


def _vector_of(calls: list[tuple[str, str, str, dict[str, Any]]]) -> dict[str, list[Any]]:
    transactions = [call[3]["TransactItems"] for call in calls
                    if call[2] == "transact_write_items"]
    return {
        "calls": [_shape(call) for call in calls],
        "update": [_item_shape(items[0]) for items in transactions],
        "condition_check": [_item_shape(items[1]) for items in transactions],
    }


def test_vetor_do_shim_e_a_fixture_versionada(monkeypatch: pytest.MonkeyPatch) -> None:
    raw = VECTOR.read_bytes()
    vector = json.loads(raw)
    assert raw == canonical(vector)
    assert (vector["version"], vector["role"]) == (1, "fence_operator")
    expected = {"calls": vector["calls"], **vector["close_transaction"]}
    seen: dict[str, list[Any]] = {part: [] for part in expected}
    for name in SCENARIOS:
        with _environment(name, monkeypatch) as (client, _, probe):
            context = Context(client, probe, _inputs())
            _run(context)
        for part, shapes in _vector_of(context.calls).items():
            assert all(shape in expected[part] for shape in shapes), (name, part)
            seen[part].extend(shape for shape in shapes if shape not in seen[part])
    assert {part: sorted(map(canonical, shapes)) for part, shapes in seen.items()} == {
        part: sorted(map(canonical, shapes)) for part, shapes in expected.items()}


def test_shim_recusa_operacao_fora_das_quatro_antes_da_chamada() -> None:
    context = Context(None, Probe(), {})
    shim = promote_close.FenceShim(context, MACHINE_ARN)
    for service, operation in (("dynamodb", "put_item"), ("dynamodb", "query"),
                               ("dynamodb", "delete_item"), ("stepfunctions", "start_execution"),
                               ("stepfunctions", "get_item"), ("s3", "get_object")):
        with pytest.raises(ValueError, match="shim=operation"):
            getattr(shim.client(service), operation)(TableName=promote_close.TABLE)
    assert (context.calls, context.guards) == ([], 0)


@pytest.mark.parametrize(
    ("operation", "parameters"),
    [("get_item", {"TableName": "outra", "Key": FENCE, "ConsistentRead": True}),
     ("get_item", {"TableName": "cnesdata-prod-control-plane", "Key": FENCE}),
     ("get_item", {"TableName": "cnesdata-prod-control-plane", "ConsistentRead": True,
                   "Key": {"pk": {"S": "TENANT#x"}, "sk": {"S": "RUN#y"}}}),
     ("update_item", {"TableName": "cnesdata-prod-control-plane", "UpdateExpression": "SET a=:a",
                      "Key": SEMAPHORE}),
     ("update_item", {"TableName": "cnesdata-prod-control-plane", "Key": SEMAPHORE,
                      "UpdateExpression": "SET #confirmed = :confirmed", "ConditionExpression": "x",
                      "ExpressionAttributeNames": {}, "ExpressionAttributeValues": {}}),
     ("transact_write_items", {"TransactItems": [{"Put": {"TableName": "x", "Item": {}}}]}),
     ("transact_write_items", {"TransactItems": [
         {"Update": {"TableName": "cnesdata-prod-control-plane", "Key": FENCE}},
         {"ConditionCheck": {"TableName": "cnesdata-prod-control-plane", "Key": FENCE}}]})],
)
def test_shim_recusa_parametros_fora_do_vetor(operation: str, parameters: dict[str, Any]) -> None:
    context = Context(None, Probe(), {})
    client = promote_close.FenceShim(context, MACHINE_ARN).client("dynamodb")
    with pytest.raises(ValueError, match="shim=parameters"):
        getattr(client, operation)(**parameters)
    assert context.calls == []


def test_shim_acrescenta_metadata_only_e_recusa_outra_state_machine() -> None:
    context = Context(None, Probe(), {})
    states = promote_close.FenceShim(context, MACHINE_ARN).client("stepfunctions")
    for arn in (EXECUTION + "-x:" + "a" * 16, EXECUTION + ":" + "A" * 16):
        with pytest.raises(ValueError, match="shim=parameters"):
            states.describe_execution(executionArn=arn)
    assert context.calls == []
    with pytest.raises(ClientError):
        states.describe_execution(executionArn=EXECUTION + ":" + "a" * 16)
    [(role, service, operation, parameters)] = context.calls
    assert (role, service, operation) == ("fence_operator", "stepfunctions", "describe_execution")
    assert parameters["includedData"] == "METADATA_ONLY"


def test_gate_sela_a_escrita_no_confirm_do_close(monkeypatch: pytest.MonkeyPatch) -> None:
    promotion = OWNER["owner_sha256"]
    reopen = ReopenPromotionFence("prod", promotion, NOW)
    with _environment("empty", monkeypatch) as (raw, _, probe):
        context = Context(raw, probe, {})
        shim = promote_close.FenceShim(context, MACHINE_ARN)
        gate = promote_close._Gate(shim)
        assert gate.close_promotion_fence(ClosePromotionFence("prod", promotion, NOW))
        with pytest.raises(ValueError, match="shim=parameters"):
            gate.reopen_promotion_fence(reopen)
        gate.confirm_promotion_close("prod", promotion, NOW)
        with pytest.raises(ValueError, match="shim=sealed"):
            gate.reopen_promotion_fence(reopen)
        with pytest.raises(ValueError, match="shim=sealed"):
            shim.client("dynamodb").transact_write_items(TransactItems=[])
    writes = [call[2] for call in context.calls if call[2] != "get_item"]
    assert writes == ["transact_write_items", "update_item"]


@pytest.mark.parametrize(
    ("changes", "error"),
    [pytest.param({"owner": OWNER | {"repository": "outro/repo"}}, "request=source",
                  id="owner-de-outro-repositorio"),
     pytest.param({"owner": OWNER | {"source_sha": "b" * 40}}, "request=source",
                  id="owner-de-outro-source"),
     pytest.param({"stage": PREPARE | {"close": _epoch(0)}}, "request=stage",
                  id="close-em-application-prepare"),
     pytest.param({"stage": PREPARE | {"stage": "api", "close": _epoch(0)}}, "request=stage",
                  id="estagio-api"),
     pytest.param({"stage": PREPARE | {"stage": "static"}}, "request=stage", id="estagio-static"),
     pytest.param({"activation_sha256": "9" * 64}, "activation=sha256",
                  id="activation-diferente-do-request"),
     pytest.param({"late": True}, "close_drain=preparation_deadline", id="prazo-de-preparacao")],
)
def test_request_fora_do_contrato_e_recusado_antes_de_chamar(
    changes: dict[str, Any], error: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    if changes.get("late"):
        monkeypatch.setattr(promote_api, "CLOCK", lambda: NOW + timedelta(seconds=180))
    context = Context(None, Probe(), _inputs())
    request = _request(context.inputs, changes.get("stage", PREPARE), changes.get("owner", OWNER))
    request |= {key: changes[key] for key in changes.keys() & {"activation_sha256"}}
    with pytest.raises(ValueError, match=error):
        _run(context, request)
    assert context.calls == []


def test_membro_com_generation_da_promocao_e_recusado(monkeypatch: pytest.MonkeyPatch) -> None:
    with _environment("terminal", monkeypatch) as (raw, _, probe):
        context = Context(raw, probe, _inputs())
        with pytest.raises(DrainError, match="membership=promotion_generation"):
            _run(context, _request(context.inputs, owner=OWNER | {"generation": 1}))
    assert context.observed == []


@pytest.mark.parametrize(
    ("activation", "error"),
    [pytest.param(_activation(canary_id=None), "activation=fields", id="sem-canary_id"),
     pytest.param(_activation(extra="x"), "activation=fields", id="campo-extra"),
     pytest.param(_activation(release_id=7), "activation=fields", id="release_id-nao-str"),
     pytest.param(b"[]\n", "activation=fields", id="nao-objeto"),
     pytest.param(b"{", "activation=encoding", id="json-invalido"),
     pytest.param(_activation(revisions={"unit": TASKS + "unit:2", "audit": TASKS.replace(
         ACCOUNT, "210987654321") + "audit:1"}), "activation=account", id="accounts-mistos"),
     pytest.param(_activation(revisions={"unit": TASKS.replace("us-east-2", "us-east-1")
                                         + "unit:2"}), "activation=account", id="outra-regiao"),
     pytest.param(_activation(revisions=[TASKS + "unit:2"]), "activation=account",
                  id="revisions-lista"),
     pytest.param(_activation(revisions={}), "activation=account", id="revisions-vazio"),
     pytest.param(_activation(source_sha="b" * 40), "common=caller_binding", id="common-source"),
     pytest.param(_activation(repository="outro/repo"), "common=schema", id="common-repository"),
     pytest.param(_activation(canary_id="c" * 31), "common=canary_id", id="common-canary_id")],
)
def test_activation_sem_fonte_autenticada_e_recusada(activation: bytes, error: str) -> None:
    context = Context(None, Probe(), _inputs(activation=activation))
    with pytest.raises(DrainError if error.startswith("common=") else ValueError, match=error):
        _run(context)
    assert context.calls == []


@pytest.mark.parametrize(
    "expirations",
    [pytest.param(canonical([]), id="lista-vazia"),
     pytest.param(canonical({}), id="nao-lista"),
     pytest.param(canonical([EXPIRY | {"kind": "envelope"}]), id="envelope-do-caller"),
     pytest.param(canonical([EXPIRY | {"kind": "lease"}]), id="kind-fora"),
     pytest.param(canonical([EXPIRY | {"extra": 1}]), id="campo-extra"),
     pytest.param(canonical([EXPIRY | {"object_sha256": "F" * 64}]), id="hex-maiusculo"),
     pytest.param(canonical([EXPIRY | {"object_sha256": "f" * 63}]), id="hex-curto"),
     pytest.param(canonical([EXPIRY | {"expires_at": "2026-11-09T13:00:00.5Z"}]), id="fracao"),
     pytest.param(canonical([EXPIRY | {"expires_at": "2026-11-09T13:00:00+00:00"}]),
                  id="offset"),
     pytest.param(canonical([EXPIRY | {"expires_at": 1794830400}]), id="epoch-int"),
     pytest.param(canonical([EXPIRY | {"expires_at": "2026-13-09T13:00:00Z"}]), id="mes-13"),
     pytest.param(canonical([EXPIRY | {"expires_at": "2026-11-9T13:00:00Z"}]), id="sem-zero"),
     pytest.param(canonical([EXPIRY, OTHER]), id="fora-de-ordem"),
     pytest.param(canonical([EXPIRY, EXPIRY]), id="duplicado"),
     pytest.param(json.dumps([EXPIRY]).encode(), id="nao-canonico"),
     pytest.param(b"[", id="json-invalido"),
     pytest.param(canonical(["x"]), id="item-nao-objeto")],
)
def test_expirations_fora_do_contrato_sao_recusadas(expirations: bytes) -> None:
    context = Context(None, Probe(), _inputs(expirations=expirations))
    with pytest.raises(ValueError, match="expirations="):
        _run(context)
    assert context.calls == []


class Steps(Clock):
    def __init__(self, values: list[float]) -> None:
        super().__init__()
        self.values = values

    def monotonic(self) -> float:
        return self.values.pop(0) if len(self.values) > 1 else self.values[0]


def _instant(value: str) -> str:
    assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", value), value
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").strftime("%H:%M:%S")  # noqa: DTZ007


@pytest.mark.parametrize("end", [1.5, 2.0])
def test_timestamps_seguem_o_contract_instant(end: float, monkeypatch: pytest.MonkeyPatch) -> None:
    """Âncoras de prazo no piso do segundo, drain_observed_at no teto (correção 6906183797)."""
    monkeypatch.setattr(promote_api, "CLOCK", lambda: NOW + timedelta(seconds=0.7))
    items = [OTHER, EXPIRY]
    with _environment("empty", monkeypatch) as (raw, adapter, probe):
        monkeypatch.setattr(promote_close, "DRAIN_CLOCK", Steps([0.0, 0.237, end]))
        context = Context(raw, probe, _inputs(expirations=canonical(items)))
        _run(context)
        assert adapter.observe_environment_gate("prod").fence.confirmed_at == NOW
    record = json.loads(context.observed[0][1])
    assert [item for item in record["input_expirations"] if item["kind"] != "envelope"] == items
    stamps = [*(item["expires_at"] for item in record["input_expirations"]),
              record["fence"]["changed_at"], record["close_confirmed_at"],
              record["drain_observed_at"], record["fixed_deadline"]]
    assert [_instant(stamp) for stamp in stamps] == [
        "14:00:00", "13:00:00", "12:30:00", "12:00:00", "12:00:00", "12:00:02", "12:30:00"]


def test_teto_do_drain_igual_ao_prazo_fixo_recusa(monkeypatch: pytest.MonkeyPatch) -> None:
    expiry = EXPIRY | {"expires_at": "2026-11-09T12:00:10Z"}
    with _environment("empty", monkeypatch) as (raw, _, probe):
        monkeypatch.setattr(promote_close, "DRAIN_CLOCK", Steps([0.0, 0.0, 9.5]))
        context = Context(raw, probe, _inputs(expirations=canonical([expiry])))
        with pytest.raises(DrainError, match="drain=deadline_exceeded"):
            _run(context)
    assert context.observed == []
