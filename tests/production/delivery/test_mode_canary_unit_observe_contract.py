"""Fontes do unit-start: o produto gera a seção unit_observe e os construtores a reproduzem."""

from __future__ import annotations

import hashlib
import json
import tempfile
from datetime import timedelta
from io import BytesIO
from pathlib import Path
from typing import Any, cast

import boto3
import pytest
from moto import mock_aws

from cnes_contracts.manifests.outputs import OutputManifest
from cnes_contracts.manifests.raw import SourceType
from cnes_domain.control_plane.commands import ClaimRunUnit
from cnes_domain.control_plane.entities import RunUnit
from cnes_domain.control_plane.environment_gate import ClosePromotionFence
from cnes_infra.aws.mode_canary import canonical_text
from cnes_infra.aws.mode_canary_unit_observe import (
    describe_tasks_request,
    unit_cluster,
    unit_execution_request,
    unit_output_reads,
    unit_outputs,
    unit_task_arns,
)
from cnes_infra.aws.settings import _PRODUCTION_LEASE_SECONDS  # pyright: ignore[reportPrivateUsage]
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_codec import decode_model
from cnes_infra.control_plane.dynamodb_keys import item_key, unit_key
from cnes_infra.executor.step_functions_probe import StepFunctionsExecutionProbe
from cnes_infra.object_store.filesystem import FilesystemObjectStore
from data_processor.orchestration.unit_worker import UnitWorker, UnitWorkerDependencies
from packages.cnes_infra.tests.aws.mode_canary_native import (
    MACHINE,
    NativeCase,
    Recorder,
    native_start,
)
from scripts.promotion.mode_canary import execute_mode_canary
from tests.production.delivery import test_mode_canary_dispatch_contract as dispatch_contract
from tests.production.delivery.test_mode_canary import _Context, _record

ROOT = Path(__file__).resolve().parents[3]
VECTOR: dict[str, Any] = json.loads(
    Path(__file__).with_name("mode_canary_frames_vector.json").read_text(encoding="utf-8"))
SECTION: dict[str, Any] = VECTOR.get("unit_observe", {})
CASES: dict[str, dict[str, Any]] = {case["name"]: case for case in SECTION.get("cases", [])}
ASL = ROOT / "infra/opentofu/modules/processing/state_machine.asl.json"
ACCOUNT = "arn:aws:ecs:us-east-2:111122223333"
CLUSTER = f"{ACCOUNT}:cluster/cnesdata-prod"
REVISIONS = {mode: f"{ACCOUNT}:task-definition/cnesdata-prod-{mode}:17"
             for mode in ("unit", "recovery", "audit")}
TASK_IDS = ("0f" * 16, "ab" * 16)
FOREIGN = MACHINE.replace(":stateMachine:", ":execution:") + ":" + "f" * 16
EXPECTED = {
    "aceito-pascal": [], "aceito-camel": [], "sem-arn": ["unit_task_unbound"],
    "task-alheia": ["unit_task_unbound"], "task-missing": ["unit_task_missing"],
    "exit-nao-zero": ["unit_task_failed"],
    "execucao-nao-succeeded": ["unit_execution_not_succeeded", "unit_task_unbound"],
}


def rendered_definition() -> str:
    """Returns: Definição da máquina renderizada do template real com o cluster e a revisão."""
    text = ASL.read_text(encoding="utf-8").replace("${public_subnet_ids}", '["subnet-a"]')
    for name, value in (("cluster_arn", CLUSTER), ("task_definition_arn", REVISIONS["unit"]),
                        ("security_group_id", "sg-a")):
        text = text.replace("${" + name + "}", value)
    return json.dumps(json.loads(text), sort_keys=True, separators=(",", ":"))


class _Sfn:
    def __init__(self, response: dict[str, Any]) -> None:
        self.response = response
        self.calls: list[dict[str, Any]] = []

    def describe_execution(self, **request: Any) -> dict[str, Any]:
        self.calls.append(json.loads(json.dumps(request)))
        return self.response


class _Steps:
    def __init__(self, plane: DynamoDBControlPlane, recorder: Recorder) -> None:
        self._plane, self._recorder = plane, recorder

    def claim_run_unit(self, command: ClaimRunUnit) -> RunUnit | None:
        self._recorder.step = "claim-unit"
        return self._plane.claim_run_unit(command)

    def commit_run_unit(self, command: Any, event: Any) -> RunUnit:
        self._recorder.step = "commit-unit"
        return self._plane.commit_run_unit(command, event)


def _processor(unit: RunUnit, store: Any) -> tuple[OutputManifest, ...]:
    body = f"payload-{unit.unit_id}".encode()
    digest = hashlib.sha256(body).hexdigest()
    source = SourceType(unit.source_type)
    key = f"normalized/{unit.tenant_id}/{source.value}/{unit.partition}/{unit.run_id}/part.parquet"
    store.put(key, BytesIO(body), digest)
    return (OutputManifest(
        manifest_version=1, manifest_id=f"manifest-{unit.unit_id}", tenant_id=unit.tenant_id,
        layer="normalized", source_type=source, competencia=unit.partition, run_id=unit.run_id,
        unit_id=unit.unit_id, attempt=unit.attempt, schema_version="etl-v1", object_key=key,
        object_sha256=digest, row_count=1, created_at=dispatch_contract.CLOCKS["producao"][0]),)


def _commit_units(client: Any, result: dict[str, Any], root: Path) -> list[Any]:
    now = dispatch_contract.CLOCKS["producao"][0] + timedelta(seconds=30)
    recorder = Recorder(client)
    plane = DynamoDBControlPlane(cast("Any", recorder), dispatch_contract.TABLE, lambda: now)
    worker = UnitWorker(UnitWorkerDependencies(cast("Any", _Steps(plane, recorder)),
                                               FilesystemObjectStore(root), _processor,
                                               lambda: now))
    for unit_id in result["unit_ids"]:
        worker.execute(ClaimRunUnit(
            tenant_id=dispatch_contract.TENANT, run_id=dispatch_contract.RUN, unit_id=unit_id,
            dispatch_id=result["dispatch_id"], owner=result["execution_ref"], now=now,
            lease_seconds=_PRODUCTION_LEASE_SECONDS))
    return [kwargs for step, name, kwargs in recorder.calls
            if (step, name) == ("commit-unit", "get_item")
            and kwargs["Key"]["sk"]["S"].startswith("UNIT#")]


def _product() -> tuple[dict[str, Any], list[Any], list[Any]]:
    now = dispatch_contract.CLOCKS["producao"][0]
    with mock_aws(), tempfile.TemporaryDirectory() as root:
        client: Any = boto3.client("dynamodb", region_name="us-east-1")
        dispatch_contract.dyn._create_table(dispatch_contract._Named(client))
        dispatch_contract._seeded(client, now)
        plane = DynamoDBControlPlane(client, dispatch_contract.TABLE, lambda: now)
        assert plane.close_promotion_fence(ClosePromotionFence(
            "prod", dispatch_contract.OWNER, now - timedelta(minutes=1)))
        result = native_start(client, dispatch_contract.TABLE, NativeCase(
            dispatch_contract.TENANT, dispatch_contract.RUN, now, _PRODUCTION_LEASE_SECONDS,
            dispatch_contract.OWNER))["result"]
        reads = _commit_units(client, result, Path(root))
        items = [client.get_item(TableName=dispatch_contract.TABLE, Key=item_key(*unit_key(
            dispatch_contract.TENANT, dispatch_contract.RUN, unit_id)),
                                 ConsistentRead=True)["Item"] for unit_id in result["unit_ids"]]
    start = result["start_execution"]
    return {"tenant_id": dispatch_contract.TENANT, "run_id": dispatch_contract.RUN,
            "dispatch_id": result["dispatch_id"], "unit_ids": result["unit_ids"],
            "execution_ref": result["execution_ref"], "name": start["name"],
            "input": start["input"]}, reads, items


def _value(path: str, scope: dict[str, Any], dispatch: dict[str, Any], unit_id: str) -> str:
    context = {"$$.Map.Item.Value": unit_id, "$$.Execution.Id": dispatch["execution_ref"]}
    return context[path] if path in context else scope[path.removeprefix("$.")]


def _override(dispatch: dict[str, Any], unit_id: str) -> tuple[str, list[dict[str, str]]]:
    states = json.loads(rendered_definition())["States"]["RunUnits"]
    request = json.loads(dispatch["input"])
    item = {key.removesuffix(".$"): _value(path, request, dispatch, unit_id)
            for key, path in states["ItemSelector"].items()}
    [override] = states["ItemProcessor"]["States"]["RunProcessor"]["Parameters"]["Overrides"][
        "ContainerOverrides"]
    environment = [{"name": entry["Name"], "value": entry["Value"] if "Value" in entry
                    else _value(entry["Value.$"], item, dispatch, unit_id)}
                   for entry in override["Environment"]]
    return override["Name"], environment


def _arn(index: int) -> str:
    return f"{ACCOUNT}:task/cnesdata-prod/{TASK_IDS[index]}"


def _sync(dispatch: dict[str, Any], index: int) -> dict[str, Any]:
    name, environment = _override(dispatch, dispatch["unit_ids"][index])
    pascal = [{"Name": entry["name"], "Value": entry["value"]} for entry in environment]
    return {"TaskArn": _arn(index), "ClusterArn": CLUSTER, "LastStatus": "STOPPED",
            "TaskDefinitionArn": REVISIONS["unit"], "StopCode": "EssentialContainerExited",
            "Containers": [{"Name": name, "ExitCode": 0}],
            "Overrides": {"ContainerOverrides": [{"Name": name, "Environment": pascal}]}}


def _task(dispatch: dict[str, Any], index: int, **env: str) -> dict[str, Any]:
    name, environment = _override(dispatch, dispatch["unit_ids"][index])
    environment = [entry | ({"value": env[entry["name"]]} if entry["name"] in env else {})
                   for entry in environment]
    return {"taskArn": _arn(index), "clusterArn": CLUSTER, "lastStatus": "STOPPED",
            "taskDefinitionArn": REVISIONS["unit"], "containers": [{"name": name, "exitCode": 0}],
            "overrides": {"containerOverrides": [{"name": name, "environment": environment}]}}


def _execution(dispatch: dict[str, Any], output: Any, status: str = "SUCCEEDED") -> Any:
    response = {"executionArn": dispatch["execution_ref"], "stateMachineArn": MACHINE,
                "name": dispatch["name"], "status": status, "input": dispatch["input"]}
    return response | ({} if output is None else {"output": json.dumps(output)})


def _synthetic(dispatch: dict[str, Any]) -> dict[str, tuple[Any, Any]]:
    pascal = _execution(dispatch, [_sync(dispatch, index) for index in (0, 1)])
    tasks = [_task(dispatch, index) for index in (0, 1)]
    failed = [tasks[0] | {"containers": [tasks[0]["containers"][0] | {"exitCode": 1}]}, tasks[1]]
    return {
        "aceito-pascal": (pascal, {"tasks": tasks, "failures": []}),
        "aceito-camel": (_execution(dispatch, [{"taskArn": _arn(index), "lastStatus": "STOPPED"}
                                               for index in (0, 1)]),
                         {"tasks": tasks, "failures": []}),
        "sem-arn": (_execution(dispatch, [{"StopCode": "EssentialContainerExited"}] * 2), None),
        "task-alheia": (pascal, {"tasks": [_task(dispatch, 0, EXECUTION_OWNER=FOREIGN), tasks[1]],
                                 "failures": []}),
        "task-missing": (pascal, {"tasks": [tasks[1]],
                                  "failures": [{"arn": _arn(0), "reason": "MISSING"}]}),
        "exit-nao-zero": (pascal, {"tasks": failed, "failures": []}),
        "execucao-nao-succeeded": (_execution(dispatch, None, "FAILED"), None),
    }


def _readbacks() -> dict[str, Any]:
    unit = {"taskDefinitionArn": REVISIONS["unit"], "containerDefinitions": [{"name": "processor"}]}
    return {"state_machine": {"stateMachineArn": MACHINE, "definition": rendered_definition()},
            "task_definitions": {"unit": {"taskDefinition": unit}}}


def child_pending(observed: dict[str, Any], readbacks: dict[str, Any]) -> list[str]:
    """Returns: Pending do filho do unit-start sobre o observado e o ModeReadback do keeper."""
    record = _record("unit", machine_arn=MACHINE, revisions=REVISIONS,
                     canary_id=dispatch_contract.TENANT.removeprefix("prod-canary-"),
                     owner_sha256=dispatch_contract.OWNER, run_ids=[dispatch_contract.RUN])
    originals = {"record": canonical_text(record).encode(), "readbacks": readbacks,
                 "readbacks_after": readbacks, "previous": None, "frozen": False,
                 "seed_sha256": "5" * 64, "started_at": "2026-11-13T12:01:00Z"}
    result = execute_mode_canary("unit", originals, _Context({"unit": observed}))
    return json.loads(result)["pending"]


def keeper_observed(section: dict[str, Any], case: dict[str, Any]) -> dict[str, Any]:
    """Returns: Observado do dispatch_unit montado só pelos construtores sobre o caso."""
    execution = case["execution"]["response"]
    described = case["describe_tasks"]
    response = {"tasks": [], "failures": []} if described is None else described["response"]
    dispatch = section["dispatch"]
    outputs = unit_outputs(dispatch["tenant_id"], dispatch["run_id"], dispatch["unit_ids"],
                           section["unit_items"])
    return {"execution": execution, "tasks": response["tasks"],
            "failures": response["failures"], "outputs": outputs}


def _case(section: dict[str, Any], name: str, execution: Any, described: Any) -> dict[str, Any]:
    arns = unit_task_arns(execution, section["cluster_arn"])
    case = {"name": name, "execution": {"synthetic": True, "response": execution},
            "describe_tasks": None if described is None else {"synthetic": True,
                                                              "response": described},
            "task_arns": list(arns),
            "describe_request": describe_tasks_request(section["cluster_arn"], arns)
            if arns else None}
    return case | {"pending": child_pending(keeper_observed(section, case), section["readbacks"])}


def product_unit_observe() -> dict[str, Any]:
    """Returns: Seção `unit_observe`: caminho do produto, respostas sintéticas e pending."""
    dispatch, reads, items = _product()
    sfn = _Sfn(_execution(dispatch, [_sync(dispatch, index) for index in (0, 1)]))
    StepFunctionsExecutionProbe(cast("Any", sfn), MACHINE).probe(dispatch["dispatch_id"])
    section = {"table": dispatch_contract.TABLE, "machine_arn": MACHINE, "cluster_arn": CLUSTER,
               "revision": REVISIONS["unit"], "definition": rendered_definition(),
               "dispatch": dispatch, "execution_request": sfn.calls[0], "output_reads": reads,
               "unit_items": items, "readbacks": _readbacks()}
    section["outputs"] = unit_outputs(dispatch["tenant_id"], dispatch["run_id"],
                                      dispatch["unit_ids"], items)
    section["cases"] = [_case(section, name, execution, described)
                        for name, (execution, described) in _synthetic(dispatch).items()]
    return section


def test_caminho_do_produto_reproduz_a_secao_unit_observe() -> None:
    assert product_unit_observe() == SECTION


def test_construtores_reproduzem_o_request_da_execucao_e_as_leituras_do_commit() -> None:
    dispatch = SECTION["dispatch"]
    assert unit_execution_request(MACHINE, dispatch["dispatch_id"]) == SECTION[
        "execution_request"]
    assert list(unit_output_reads(SECTION["table"], dispatch["tenant_id"], dispatch["run_id"],
                                  dispatch["unit_ids"])) == SECTION["output_reads"]


def test_leituras_dos_outputs_sao_as_leituras_unit_do_reserve_ja_fixadas() -> None:
    reserve = dispatch_contract.CASES["producao"]["result"]["requests"]["reserve"]
    units = [kwargs for name, kwargs in reserve
             if name == "get_item" and kwargs["Key"]["sk"]["S"].startswith("UNIT#")]
    assert units == SECTION["output_reads"]
    assert SECTION["dispatch"]["dispatch_id"] == dispatch_contract.CASES["producao"]["result"][
        "dispatch_id"]


def test_projecao_dos_outputs_e_a_do_runuunit_decodificado_pelo_produto() -> None:
    dispatch = SECTION["dispatch"]
    projected = unit_outputs(dispatch["tenant_id"], dispatch["run_id"], dispatch["unit_ids"],
                             SECTION["unit_items"])
    decoded = [decode_model(item, RunUnit) for item in SECTION["unit_items"]]
    assert projected == SECTION["outputs"] == [
        {"unit_id": unit.unit_id, "attempt": unit.attempt, "dispatch_id": unit.dispatch_id,
         "state": unit.state.value, "output_manifests": [
             {"manifest_id": ref.manifest_id, "manifest_key": ref.manifest_key}
             for ref in unit.output_manifests]} for unit in decoded]


def test_cluster_da_maquina_renderizada_do_template_e_o_do_alvo() -> None:
    assert unit_cluster(SECTION["definition"]) == SECTION["cluster_arn"] == CLUSTER
    assert SECTION["definition"] == rendered_definition()


def test_readbacks_da_secao_sao_o_conjunto_unit_do_keeper_sem_schedules() -> None:
    readbacks = SECTION["readbacks"]
    assert set(readbacks) == {"state_machine", "task_definitions"}
    assert set(readbacks["task_definitions"]) == {"unit"}
    assert readbacks["state_machine"] == {"stateMachineArn": MACHINE,
                                          "definition": SECTION["definition"]}
    assert readbacks["task_definitions"]["unit"]["taskDefinition"]["taskDefinitionArn"] == (
        SECTION["revision"])


def test_respostas_sinteticas_rotuladas_sobre_os_campos_do_produto() -> None:
    assert set(CASES) == set(EXPECTED)
    dispatch = SECTION["dispatch"]
    for case in CASES.values():
        execution = case["execution"]
        assert execution["synthetic"] is True
        assert (execution["response"]["executionArn"], execution["response"]["input"]) == (
            dispatch["execution_ref"], dispatch["input"])
        assert case["describe_tasks"] is None or case["describe_tasks"]["synthetic"] is True


@pytest.mark.parametrize("name", list(EXPECTED))
def test_construtores_e_filho_reproduzem_o_caso(name: str) -> None:
    case = CASES[name]
    arns = unit_task_arns(case["execution"]["response"], SECTION["cluster_arn"])
    assert list(arns) == case["task_arns"]
    if arns:
        assert describe_tasks_request(SECTION["cluster_arn"], arns) == case["describe_request"]
    assert child_pending(keeper_observed(SECTION, case), SECTION["readbacks"]) == case[
        "pending"] == EXPECTED[name]
