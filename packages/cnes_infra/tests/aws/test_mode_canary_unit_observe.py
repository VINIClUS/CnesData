"""Fontes do unit-start: cluster da máquina, ARN das tasks, DescribeTasks e outputs do RUNUNIT."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pytest

from cnes_domain.orchestration.mode_canary import SyntheticContextRequired
from cnes_infra.aws.mode_canary_dispatch import UnitDispatch, UnitStart, step_reads
from cnes_infra.aws.mode_canary_unit_observe import (
    describe_tasks_request,
    unit_cluster,
    unit_execution_request,
    unit_output_reads,
    unit_outputs,
    unit_task_arns,
)
from cnes_infra.control_plane.dynamodb_keys import item_key, unit_key

ACCOUNT = "arn:aws:ecs:us-east-2:111122223333"
CLUSTER = f"{ACCOUNT}:cluster/cnesdata-prod"
TASK = f"{ACCOUNT}:task/cnesdata-prod/" + "0f" * 16
OTHER = f"{ACCOUNT}:task/cnesdata-prod/" + "ab" * 16
MACHINE = "arn:aws:states:us-east-2:111122223333:stateMachine:cnesdata-prod"
DISPATCH = "1a2b3c4d5e6f7a8b"
TENANT = "prod-canary-" + "c" * 32
RUN = "unit-aaaaaaaaaaaaaaaa"
TABLE = "cnesdata-prod-control-plane"
UNITS = ("u1", "u2")
MANY = tuple(f"{TASK[:-3]}{index:03x}" for index in range(101))


def _refused(reason: str) -> Any:
    return pytest.raises(SyntheticContextRequired, match=f"^reason={reason} field=tenant_id$")


def _machine(*parameters: Any) -> str:
    states = {f"S{index}": {"Parameters": value} for index, value in enumerate(parameters)}
    return json.dumps({"States": {"Map": {"ItemProcessor": {"States": states}}}})


def _execution(output: Any, status: str = "SUCCEEDED") -> dict[str, Any]:
    return {"status": status, "output": json.dumps(output)}


def _payload(unit_id: str, **changes: Any) -> dict[str, Any]:
    manifest = {"manifest_id": f"m-{unit_id}", "manifest_key": f"tmp/x/{unit_id}.json"}
    return {"tenant_id": TENANT, "run_id": RUN, "unit_id": unit_id, "stage": "NORMALIZE",
            "attempt": 1, "dispatch_id": DISPATCH, "state": "SUCCEEDED",
            "output_manifests": [manifest], "lease_owner": None} | changes


def _item(unit_id: str, payload: Any = None, entity: str = "RUNUNIT") -> dict[str, Any]:
    pk, sk = unit_key(TENANT, RUN, unit_id)
    text = payload if isinstance(payload, str) else json.dumps(payload or _payload(unit_id))
    return {"pk": {"S": pk}, "sk": {"S": sk}, "entity": {"S": entity}, "payload": {"S": text}}


def test_cluster_unico_da_definicao_da_maquina() -> None:
    assert unit_cluster(_machine({"Cluster": CLUSTER, "TaskDefinition": "td"})) == CLUSTER
    assert unit_cluster(_machine({"Cluster": CLUSTER}, [{"Cluster": CLUSTER}])) == CLUSTER


@pytest.mark.parametrize("definition", [
    _machine({"TaskDefinition": "td"}),
    _machine({"Cluster": CLUSTER}, [{"Cluster": f"{ACCOUNT}:cluster/outro"}]),
    _machine({"Cluster": 7}),
    _machine({"Cluster": "cnesdata-prod"}),
    "{nao-json",
], ids=["ausente", "dois", "nao-texto", "sem-cluster-no-arn", "json-invalido"])
def test_cluster_ausente_duplo_ou_invalido_e_none(definition: str) -> None:
    assert unit_cluster(definition) is None


def test_request_da_execucao_e_o_arn_exato_do_dispatch() -> None:
    assert unit_execution_request(MACHINE, DISPATCH) == {
        "executionArn": f"arn:aws:states:us-east-2:111122223333:execution:cnesdata-prod:{DISPATCH}"}


@pytest.mark.parametrize(("machine", "dispatch", "reason"), [
    (MACHINE, DISPATCH.upper(), "unit_dispatch_id"),
    (MACHINE, DISPATCH[:-1], "unit_dispatch_id"),
    (MACHINE, DISPATCH + "0", "unit_dispatch_id"),
    (f"{ACCOUNT}:cluster/cnesdata-prod", DISPATCH, "unit_machine"),
    (f"{MACHINE}:v1", DISPATCH, "unit_machine"),
])
def test_request_da_execucao_recusa_dispatch_ou_maquina_invalidos(
        machine: str, dispatch: str, reason: str) -> None:
    with _refused(reason):
        unit_execution_request(machine, dispatch)


@pytest.mark.parametrize("output", [
    [{"TaskArn": TASK, "ClusterArn": CLUSTER}],
    [{"taskArn": TASK}],
    [{"Tasks": [{"Detail": {"TaskArn": TASK}}]}],
], ids=["pascal", "camel", "aninhado-dois-niveis"])
def test_scan_acha_o_arn_da_task_em_qualquer_chave_e_nivel(output: Any) -> None:
    assert unit_task_arns(_execution(output), CLUSTER) == (TASK,)


@pytest.mark.parametrize("value", [
    f"{ACCOUNT}:task/outro-cluster/" + "0f" * 16,
    f"{TASK}/x",
    f"{TASK}\n",
    f"x{TASK}",
    f"{ACCOUNT}:task/cnesdata-prod/" + "0F" * 16,
    f"{ACCOUNT}:task/cnesdata-prod/" + "0f" * 15,
], ids=["cluster-alheio", "sufixo", "quebra-de-linha", "prefixo", "hex-maiusculo", "id-curto"])
def test_scan_ancorado_recusa_arn_fora_do_cluster_exato(value: str) -> None:
    assert unit_task_arns(_execution([{"TaskArn": value}]), CLUSTER) == ()


def test_scan_escapa_o_cluster_e_deduplica_em_ordem() -> None:
    dotted = f"{ACCOUNT}:cluster/cnes.prod"
    near = f"{ACCOUNT}:task/cnesXprod/" + "0f" * 16
    assert unit_task_arns(_execution([{"TaskArn": near}]), dotted) == ()
    output = [{"TaskArn": OTHER}, {"taskArn": TASK}, {"Nested": [TASK, {"k": OTHER}]}, 3, None]
    assert unit_task_arns(_execution(output), CLUSTER) == (TASK, OTHER)


@pytest.mark.parametrize("execution", [
    _execution([{"TaskArn": TASK}], "FAILED"),
    {"status": "SUCCEEDED", "output": "{nao-json"},
    {"status": "SUCCEEDED"},
    {"status": "SUCCEEDED", "output": None},
], ids=["nao-succeeded", "json-invalido", "sem-output", "output-nulo"])
def test_scan_so_le_output_de_execucao_succeeded(execution: dict[str, Any]) -> None:
    assert unit_task_arns(execution, CLUSTER) == ()


def test_scan_recusa_cluster_sem_arn_de_cluster() -> None:
    with _refused("unit_cluster"):
        unit_task_arns(_execution([{"TaskArn": TASK}]), "cnesdata-prod")


def test_describe_tasks_do_cluster_com_os_arns_do_scan() -> None:
    assert describe_tasks_request(CLUSTER, (TASK, OTHER)) == {
        "cluster": CLUSTER, "tasks": [TASK, OTHER]}


@pytest.mark.parametrize("tasks", [(), (TASK, TASK), MANY],
                         ids=["vazio", "duplicado", "acima-de-100"])
def test_describe_tasks_recusa_vazio_duplicado_ou_acima_do_limite(tasks: tuple[str, ...]) -> None:
    with _refused("unit_tasks"):
        describe_tasks_request(CLUSTER, tasks)


def test_leituras_dos_outputs_sao_as_leituras_unit_do_reserve() -> None:
    dispatch = UnitDispatch(TENANT, RUN, "w" * 16, DISPATCH, UNITS, 1)
    start = UnitStart(dispatch, TABLE, datetime(2026, 11, 13, 12, 5, tzinfo=UTC), 3600, "a" * 64)
    reads = unit_output_reads(TABLE, TENANT, RUN, UNITS)
    assert reads == step_reads(start, "reserve")[2:]
    assert reads[0] == {"TableName": TABLE, "Key": item_key(*unit_key(TENANT, RUN, "u1")),
                        "ConsistentRead": True}


def test_outputs_projetam_o_runuunit_fechado_na_ordem_dos_units() -> None:
    assert unit_outputs(TENANT, RUN, UNITS, [_item("u1"), _item("u2")]) == [
        {"unit_id": unit_id, "attempt": 1, "dispatch_id": DISPATCH, "state": "SUCCEEDED",
         "output_manifests": [{"manifest_id": f"m-{unit_id}",
                               "manifest_key": f"tmp/x/{unit_id}.json"}]}
        for unit_id in UNITS]


@pytest.mark.parametrize("items", [
    [_item("u1")],
    [_item("u2"), _item("u1")],
    [_item("u1"), _item("u2", entity="RUNDISPATCH")],
    [_item("u1"), _item("u2", "{nao-json")],
    [_item("u1"), _item("u2", "[1]")],
    [_item("u1"), _item("u2", {k: v for k, v in _payload("u2").items() if k != "state"})],
    [_item("u1"), _item("u2", _payload("u2", output_manifests={"m": 1}))],
    [_item("u1"), _item("u2", _payload("u2", output_manifests=[{"manifest_id": "m"}]))],
    [_item("u1"), _item("u2", _payload("u2", output_manifests=["m"]))],
    [_item("u1"), {"pk": {"S": "x"}}],
], ids=["contagem", "ordem", "entidade", "json-invalido", "nao-objeto", "sem-campo",
        "manifests-nao-lista", "manifest-sem-chave", "manifest-nao-objeto", "item-sem-chaves"])
def test_outputs_recusam_item_fora_do_dispatch_ou_do_formato(items: list[Any]) -> None:
    with _refused("unit_outputs"):
        unit_outputs(TENANT, RUN, UNITS, items)
