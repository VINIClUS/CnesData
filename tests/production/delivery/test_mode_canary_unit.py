"""Unit-start do canary de modo: execução, tasks e outputs amarrados ao dispatch do record."""
from __future__ import annotations

import json
from typing import Any

import pytest

from scripts.promotion.mode_canary import RESULT_FIELDS, execute_mode_canary
from scripts.promotion.mode_canary_contract import ContractError
from tests.production.delivery.test_mode_canary import (
    _ACCOUNT,
    _CLUSTER,
    _MACHINE,
    _OWNER,
    _REV,
    _TENANT,
    _Context,
    _originals,
    _readbacks,
    _route,
)

_RUN = f"unit-{_OWNER[:16]}"
_DISPATCH = "1a2b3c4d5e6f7a8b"
_WAVE = "61d919281eb236c7"
_UNITS = ("u1", "u2")
_EXECUTION = f"{_MACHINE.replace(':stateMachine:', ':execution:')}:{_DISPATCH}"
_PREFIX = f"{_ACCOUNT}:task/cnesdata-prod/"
_TASKS = {"u1": _PREFIX + "0f" * 16, "u2": _PREFIX + "ab" * 16, "bis": _PREFIX + "cd" * 16}
_BROKEN_INPUT = ["unit_execution_not_succeeded", "unit_output_invalid", "unit_task_unbound"]


def _input(**changes: Any) -> str:
    return json.dumps({"dispatch_id": _DISPATCH, "max_concurrency": 1, "run_id": _RUN,
                       "tenant_id": _TENANT, "unit_ids": list(_UNITS), "wave_id": _WAVE} | changes)


def _environment(unit_id: str, **changes: str) -> list[dict[str, str]]:
    values = {"TENANT_ID": _TENANT, "RUN_ID": _RUN, "WAVE_ID": _WAVE, "DISPATCH_ID": _DISPATCH,
              "UNIT_ID": unit_id, "EXECUTION_OWNER": _EXECUTION, "LEASE_SECONDS": "3600"} | changes
    return [{"name": name, "value": value} for name, value in values.items()]


def _overrides(unit_id: str, name: str = "processor", **changes: str) -> dict[str, Any]:
    return {"containerOverrides": [{"name": name, "environment": _environment(unit_id, **changes)}]}


def _task(unit_id: str, arn: str | None = None, **changes: Any) -> dict[str, Any]:
    return {"taskArn": arn or _TASKS[unit_id], "clusterArn": _CLUSTER, "lastStatus": "STOPPED",
            "taskDefinitionArn": _REV["unit"],
            "containers": [{"name": "processor", "exitCode": 0}],
            "overrides": _overrides(unit_id)} | changes


def _sync(unit_id: str, arn: str | None = None) -> dict[str, Any]:
    environment = [{"Name": item["name"], "Value": item["value"]}
                   for item in _environment(unit_id)]
    return {"TaskArn": arn or _TASKS[unit_id], "ClusterArn": _CLUSTER, "LastStatus": "STOPPED",
            "TaskDefinitionArn": _REV["unit"], "Containers": [{"Name": "processor", "ExitCode": 0}],
            "Overrides": {"ContainerOverrides": [{"Name": "processor",
                                                  "Environment": environment}]}}


def _output(unit_id: str, **changes: Any) -> dict[str, Any]:
    manifest = f"manifest-{unit_id}"
    key = f"tmp/{_TENANT}/{_RUN}/{unit_id}/1/manifests/{manifest}/manifest.json"
    return {"unit_id": unit_id, "attempt": 1, "dispatch_id": _DISPATCH, "state": "SUCCEEDED",
            "output_manifests": [{"manifest_id": manifest, "manifest_key": key}]} | changes


def _observed(tasks: list[Any] | None = None, output: Any = None,
              **changes: Any) -> dict[str, Any]:
    execution = {"executionArn": _EXECUTION, "name": _DISPATCH, "status": "SUCCEEDED",
                 "stateMachineArn": _MACHINE, "input": _input(),
                 "output": json.dumps([_sync(unit_id) for unit_id in _UNITS]
                                      if output is None else output)}
    return {"execution": execution,
            "tasks": [_task(unit_id) for unit_id in _UNITS] if tasks is None else tasks,
            "failures": [], "outputs": [_output(unit_id) for unit_id in _UNITS]} | changes


def _execution(**changes: Any) -> dict[str, Any]:
    observed = _observed()
    observed["execution"] |= changes
    return observed


def _first_task(**changes: Any) -> dict[str, Any]:
    return _observed(tasks=[_task("u1", **changes), _task("u2")])


def _first_output(**changes: Any) -> dict[str, Any]:
    return _observed(outputs=[_output("u1", **changes), _output("u2")])


def _key(attempt: int, manifest: str = "manifest-u1") -> list[dict[str, str]]:
    key = f"tmp/{_TENANT}/{_RUN}/u1/{attempt}/manifests/{manifest}/manifest.json"
    return [{"manifest_id": "manifest-u1", "manifest_key": key}]


def _result(observed: Any, **originals: Any) -> dict[str, Any]:
    context = _Context({"unit": observed})
    return json.loads(execute_mode_canary("unit", _originals("unit", **originals), context))


_TASK_CASES: dict[str, tuple[dict[str, Any], list[str]]] = {
    "camel-no-output": (_observed(output=[{"taskArn": _TASKS[u]} for u in _UNITS]), []),
    "sem-arn-no-output": (_observed(tasks=[], output=[{"StopCode": "EssentialContainerExited"}]),
                          ["unit_task_unbound"]),
    "owner-alheio": (_first_task(overrides=_overrides("u1", EXECUTION_OWNER=_EXECUTION + "0")),
                     ["unit_task_unbound"]),
    "dispatch-alheio": (_first_task(overrides=_overrides("u1", DISPATCH_ID="0" * 16)),
                        ["unit_task_unbound"]),
    "tenant-alheio": (_first_task(overrides=_overrides("u1", TENANT_ID="prod-canary-x")),
                      ["unit_task_unbound"]),
    "run-alheio": (_first_task(overrides=_overrides("u1", RUN_ID="unit-x")),
                   ["unit_task_unbound"]),
    "unit-fora-do-input": (_first_task(overrides=_overrides("u1", UNIT_ID="u9")),
                           ["unit_task_unbound"]),
    "env-so-em-outro-container": (_first_task(overrides=_overrides("u1", "sidecar")),
                                  ["unit_task_unbound"]),
    "dois-overrides-processor": (_first_task(overrides={"containerOverrides": [
        *_overrides("u1")["containerOverrides"], *_overrides("u1")["containerOverrides"]]}),
                                 ["unit_task_unbound"]),
    "env-com-nome-repetido": (_first_task(overrides={"containerOverrides": [
        {"name": "processor", "environment": [*_environment("u1"),
                                              {"name": "UNIT_ID", "value": "u2"}]}]}),
                              ["unit_task_unbound"]),
    "sem-overrides": (_first_task(overrides={}), ["unit_task_unbound"]),
    "duas-tasks-um-unit": (_observed(
        tasks=[_task("u1"), _task("u1", _TASKS["bis"]), _task("u2")],
        output=[_sync("u1"), _sync("u1", _TASKS["bis"]), _sync("u2")]), ["unit_task_unbound"]),
    "cluster-da-task-alheio": (_first_task(clusterArn=f"{_ACCOUNT}:cluster/outro"),
                               ["unit_task_unbound"]),
    "task-fora-do-scan": (_observed(output=[_sync("u2")]), ["unit_task_unbound"]),
    "missing-parcial": (_observed(tasks=[_task("u2")], failures=[
        {"arn": _TASKS["u1"], "reason": "MISSING"}]), ["unit_task_missing"]),
    "missing-total": (_observed(tasks=[], failures=[
        {"arn": _TASKS[u], "reason": "MISSING"} for u in _UNITS]), ["unit_task_missing"]),
    "falha-de-outro-motivo": (_observed(tasks=[_task("u2")], failures=[
        {"arn": _TASKS["u1"], "reason": "ACCESS_DENIED"}]), ["unit_task_unbound"]),
    "missing-fora-do-scan": (_observed(failures=[{"arn": _TASKS["bis"], "reason": "MISSING"}]),
                             ["unit_task_unbound"]),
    "running": (_first_task(lastStatus="RUNNING"), ["unit_task_not_stopped"]),
    "deprovisioning": (_first_task(lastStatus="DEPROVISIONING"), ["unit_task_not_stopped"]),
    "fora-da-revision": (_first_task(taskDefinitionArn=_REV["unit"].replace(":17", ":16")),
                         ["unit_task_not_stopped"]),
    "exit-1": (_first_task(containers=[{"name": "processor", "exitCode": 1}]),
               ["unit_task_failed"]),
    "exit-none": (_first_task(containers=[{"name": "processor", "exitCode": None}]),
                  ["unit_task_failed"]),
    "exit-ausente": (_first_task(containers=[{"name": "processor"}]), ["unit_task_failed"]),
    "exit-bool": (_first_task(containers=[{"name": "processor", "exitCode": False}]),
                  ["unit_task_failed"]),
    "sem-containers": (_first_task(containers=[]), ["unit_task_failed"]),
    "segundo-container-falhou": (_first_task(containers=[{"name": "processor", "exitCode": 0},
                                                         {"name": "sidecar", "exitCode": 137}]),
                                 ["unit_task_failed"]),
}

_EXECUTION_CASES: dict[str, tuple[dict[str, Any], list[str]]] = {
    "chave-extra-no-input": (_execution(input=_input(extra=1)), _BROKEN_INPUT),
    "chave-faltando-no-input": (_execution(input=_input().replace(f',"wave_id":"{_WAVE}"', "")),
                                _BROKEN_INPUT),
    "tenant-diverge": (_execution(input=_input(tenant_id="prod-canary-x")), _BROKEN_INPUT),
    "run-fora-do-record": (_execution(input=_input(run_id="unit-x")), _BROKEN_INPUT),
    "unit-ids-vazio": (_execution(input=_input(unit_ids=[])), _BROKEN_INPUT),
    "unit-ids-repetido": (_execution(input=_input(unit_ids=["u1", "u1"])), _BROKEN_INPUT),
    "unit-id-nao-texto": (_execution(input=_input(unit_ids=["u1", 2])), _BROKEN_INPUT),
    "input-invalido": (_execution(input="{x"), _BROKEN_INPUT),
    "input-nao-objeto": (_execution(input="[1]"), _BROKEN_INPUT),
    "input-ausente": (_execution(input=None), _BROKEN_INPUT),
    "nome-diverge": (_execution(name="0" * 16), _BROKEN_INPUT),
    "arn-da-execucao-diverge": (_execution(executionArn=_EXECUTION[:-1] + "0"), _BROKEN_INPUT),
    "dispatch-invalido": (_execution(input=_input(dispatch_id="XYZ"), name="XYZ",
                                     executionArn=_EXECUTION.replace(_DISPATCH, "XYZ")),
                          _BROKEN_INPUT),
    "execucao-failed": (_execution(status="FAILED"),
                        ["unit_execution_not_succeeded", "unit_task_unbound"]),
    "maquina-diverge": (_execution(stateMachineArn=_MACHINE + "-clone"),
                        ["unit_execution_not_succeeded"]),
}

_OUTPUT_CASES: dict[str, tuple[dict[str, Any], list[str]]] = {
    "outputs-vazio": (_observed(outputs=[]), ["unit_output_missing"]),
    "output-de-outro-dispatch": (_first_output(dispatch_id="0" * 16), ["unit_output_invalid"]),
    "output-nao-succeeded": (_first_output(state="RUNNING"), ["unit_output_invalid"]),
    "output-attempt-zero": (_first_output(attempt=0), ["unit_output_invalid"]),
    "output-attempt-bool": (_first_output(attempt=True), ["unit_output_invalid"]),
    "output-sem-manifest": (_first_output(output_manifests=[]), ["unit_output_invalid"]),
    "manifest-de-outro-attempt": (_first_output(output_manifests=_key(2)),
                                  ["unit_output_invalid"]),
    "manifest-de-outro-id": (_first_output(output_manifests=_key(1, "manifest-x")),
                             ["unit_output_invalid"]),
    "manifest-com-campo-extra": (_first_output(output_manifests=[
        _key(1)[0] | {"sha256": "6" * 64}]), ["unit_output_invalid"]),
    "outputs-de-um-unit": (_observed(outputs=[_output("u1")]), ["unit_output_invalid"]),
    "outputs-de-unit-repetido": (_observed(outputs=[_output("u1"), _output("u1")]),
                                 ["unit_output_invalid"]),
    "output-com-campo-extra": (_first_output(sha256="6" * 64), ["unit_output_invalid"]),
}


def test_unit_aceita_execucao_tasks_e_outputs_amarrados_ao_dispatch() -> None:
    context = _Context({"unit": _observed()})
    result = json.loads(execute_mode_canary("unit", _originals("unit"), context))
    assert context.units[0].run_ids == (_RUN,)
    assert (result["pending"], result["unknown"]) == ([], [])
    assert set(result) == set(RESULT_FIELDS)
    assert [len(result[name]) for name in ("execution_receipts", "task_receipts",
                                           "output_receipts")] == [1, 2, 2]


def test_rota_do_unit_le_o_cluster_unico_da_maquina() -> None:
    assert _route("unit").cluster_arn == _CLUSTER


def test_rota_do_unit_recusa_maquina_ou_revision_fora_do_record() -> None:
    readbacks = _readbacks()
    readbacks["state_machine"]["stateMachineArn"] += "-clone"
    with pytest.raises(ContractError):
        execute_mode_canary("unit", _originals("unit", readbacks=readbacks), _Context())
    other = _readbacks()
    other["state_machine"]["definition"] = other["state_machine"]["definition"].replace(
        ":17", ":16")
    blocked = _Context({"unit": _observed()})
    with pytest.raises(ContractError):
        execute_mode_canary("unit", _originals("unit", readbacks=other), blocked)
    assert blocked.units == []


@pytest.mark.parametrize("name", list(_TASK_CASES))
def test_unit_fica_pendente_com_a_task_divergente(name: str) -> None:
    observed, pending = _TASK_CASES[name]
    assert _result(observed)["pending"] == pending


@pytest.mark.parametrize("name", list(_EXECUTION_CASES))
def test_unit_fica_pendente_com_a_execucao_divergente(name: str) -> None:
    observed, pending = _EXECUTION_CASES[name]
    assert _result(observed)["pending"] == pending


@pytest.mark.parametrize("name", list(_OUTPUT_CASES))
def test_unit_fica_pendente_com_o_output_divergente(name: str) -> None:
    observed, pending = _OUTPUT_CASES[name]
    result = _result(observed)
    assert result["pending"] == pending
    assert len(result["output_receipts"]) == len(observed["outputs"])


def _cluster_diverge() -> dict[str, Any]:
    readbacks = _readbacks()
    readbacks["schedules"]["recovery"]["Target"]["Arn"] = f"{_ACCOUNT}:cluster/outro"
    return readbacks


def _schedule_absent() -> dict[str, Any]:
    readbacks = _readbacks()
    del readbacks["schedules"]["audit"]
    return readbacks


def _two_clusters() -> dict[str, Any]:
    readbacks = _readbacks()
    definition = json.loads(readbacks["state_machine"]["definition"])
    definition["States"]["Outro"] = {"Parameters": {"Cluster": f"{_ACCOUNT}:cluster/outro"}}
    readbacks["state_machine"]["definition"] = json.dumps(definition)
    return readbacks


@pytest.mark.parametrize("readbacks", [_cluster_diverge, _schedule_absent, _two_clusters],
                         ids=["cluster-do-schedule-diverge", "schedule-ausente", "dois-clusters"])
def test_unit_fica_pendente_sem_cluster_unico_igual_ao_dos_schedules(readbacks: Any) -> None:
    built = readbacks()
    result = _result(_observed(), readbacks=built, readbacks_after=built)
    assert result["pending"] == ["unit_task_unbound"]


@pytest.mark.parametrize("observed", [
    {k: v for k, v in _observed().items() if k != "failures"},
    _observed(extra=[]),
    [],
], ids=["sem-failures", "chave-extra", "nao-objeto"])
def test_unit_observado_fora_do_formato_vira_unknown(observed: Any) -> None:
    result = _result(observed)
    assert (result["pending"], result["unknown"]) == ([], ["unit_dispatch"])
    assert (result["execution_receipts"], result["task_receipts"]) == ([], [])
