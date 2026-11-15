"""Recriação de task def no provider aws 6.54: unknown folha a folha e vazio igual a null."""

from __future__ import annotations

import copy
import json
from typing import Any

import pytest

from scripts.promotion._canonical import object_hash
from tests.production.delivery._phase_a_fixtures import ACCOUNT, ECR, REGION

type Node = dict[str, Any]
TASK = 'module.processing.aws_ecs_task_definition.processor["{}"]'
MODES = ("unit", "recovery", "audit")
VOLUMES = ("tmp", "cache", "work")
OLD = ECR + "@sha256:" + "1" * 64
NEW = ECR + "@sha256:" + "2" * 64
CANARY_ROLE = "module.runtime_iam.aws_iam_role.canary_task[0]"
GRAPH = {
    "source_files": [],
    "configuration_sha256": "0" * 64,
    "unknown_policies": [],
    "known_policies": {},
}
UNKNOWN_COMPUTED = ("arn", "arn_without_revision", "id", "revision", "tags_all")


def _containers(image: str, **extra: Any) -> str:
    container = {"name": "processor", "image": image, "essential": True, "environment": []}
    return json.dumps([container | extra])


def _volume(name: str) -> Node:
    return {
        "configure_at_launch": False,
        "docker_volume_configuration": [],
        "efs_volume_configuration": [],
        "fsx_windows_file_server_volume_configuration": [],
        "host_path": "",
        "name": name,
    }


def _before(mode: str) -> Node:
    family = "cnesdata-prod-processor-" + mode
    arn = f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/{family}"
    return {
        "arn": arn + ":2",
        "arn_without_revision": arn,
        "container_definitions": _containers(OLD),
        "cpu": "256",
        "enable_fault_injection": False,
        "ephemeral_storage": [],
        "execution_role_arn": f"arn:aws:iam::{ACCOUNT}:role/cnesdata-prod-execution",
        "family": family,
        "id": family,
        "inference_accelerator": [],
        "ipc_mode": "",
        "memory": "512",
        "network_mode": "awsvpc",
        "pid_mode": "",
        "placement_constraints": [],
        "proxy_configuration": [],
        "region": REGION,
        "requires_compatibilities": ["FARGATE"],
        "revision": 2,
        "runtime_platform": [{"cpu_architecture": "X86_64", "operating_system_family": "LINUX"}],
        "skip_destroy": True,
        "tags": {},
        "tags_all": {},
        "task_role_arn": f"arn:aws:iam::{ACCOUNT}:role/{family}",
        "track_latest": False,
        "volume": [_volume(name) for name in VOLUMES],
    }


def _after(mode: str) -> Node:
    hidden = {*UNKNOWN_COMPUTED, "enable_fault_injection"}
    after = {key: value for key, value in _before(mode).items() if key not in hidden}
    after.update(container_definitions=_containers(NEW), ipc_mode=None, pid_mode=None, tags=None)
    for volume in after["volume"]:
        del volume["configure_at_launch"]
    return after


def _after_unknown() -> Node:
    nested = {
        "docker_volume_configuration": [],
        "efs_volume_configuration": [],
        "fsx_windows_file_server_volume_configuration": [],
    }
    return dict.fromkeys(UNKNOWN_COMPUTED, True) | {
        "enable_fault_injection": True,
        "ephemeral_storage": [],
        "inference_accelerator": [],
        "placement_constraints": [],
        "proxy_configuration": [],
        "requires_compatibilities": [False],
        "runtime_platform": [{}],
        "volume": [{"configure_at_launch": True} | nested for _ in VOLUMES],
    }


def _task(mode: str, actions: tuple[str, ...] = ("delete", "create")) -> Node:
    return {
        "address": TASK.format(mode),
        "mode": "managed",
        "type": "aws_ecs_task_definition",
        "name": "processor",
        "index": mode,
        "change": {
            "actions": list(actions),
            "before": _before(mode),
            "after": _after(mode),
            "after_unknown": _after_unknown(),
        },
    }


def _plan(actions: tuple[str, ...] = ("delete", "create")) -> Node:
    return {"resource_changes": [_task(mode, actions) for mode in MODES]}


def _change(plan: Node, mode: str = "unit") -> Node:
    return next(
        item["change"] for item in plan["resource_changes"] if item["address"] == TASK.format(mode)
    )


def _context(plan: Node) -> Node:
    from scripts.promotion.phase_a import COMPUTED

    resources = {
        item["address"]: {
            key: value for key, value in item["change"]["after"].items() if key not in COMPUTED
        }
        for item in plan["resource_changes"]
    }
    return {
        "resources": resources,
        "ecr_image": NEW,
        "approved_graph": GRAPH,
        "graph_proof": GRAPH | {"governance_sha256": object_hash(GRAPH)},
    }


def _verify(plan: Node) -> None:
    from scripts.promotion.phase_a import verify_delta

    verify_delta(plan, _context(plan))


def _denied(plan: Node, reason: str) -> None:
    with pytest.raises(ValueError, match=rf"^delta={reason}$"):
        _verify(plan)


def _known_launch_leaves(change: Node) -> None:
    change["after"]["enable_fault_injection"] = False
    for volume in change["after"]["volume"]:
        volume["configure_at_launch"] = False
    del change["after_unknown"]["enable_fault_injection"]
    for marker in change["after_unknown"]["volume"]:
        del marker["configure_at_launch"]


@pytest.mark.parametrize("actions", [("delete", "create"), ("create", "delete")])
def test_aceita_recriacao_das_tres_task_defs_no_formato_do_provider_6_54(actions):
    plan = _plan(actions)
    snapshot = copy.deepcopy(plan)
    _verify(plan)
    assert plan == snapshot


@pytest.mark.parametrize("actions", [("delete", "create"), ("update",)])
def test_marcadores_aninhados_sem_folha_true_nao_contam_como_unknown(actions):
    plan = _plan(actions)
    for mode in MODES:
        change = _change(plan, mode)
        _known_launch_leaves(change)
        change["after"].update(ipc_mode="", pid_mode="", tags={})
    _verify(plan)


def test_folhas_unknown_da_recriacao_recebem_o_valor_before():
    plan = _plan()
    for mode in MODES:
        _change(plan, mode)["after"].update(ipc_mode="", pid_mode="", tags={})
    _verify(plan)


@pytest.mark.parametrize("direction", ["vazio_para_null", "null_para_vazio"])
def test_vazio_e_null_equivalem_em_ipc_pid_e_tags(direction):
    plan = _plan()
    for mode in MODES:
        change = _change(plan, mode)
        _known_launch_leaves(change)
        if direction == "null_para_vazio":
            change["before"].update(ipc_mode=None, pid_mode=None, tags=None)
            change["after"].update(ipc_mode="", pid_mode="", tags={})
    _verify(plan)


def test_contrato_de_origem_e_o_after_do_plan_antes_da_substituicao():
    from scripts.promotion.phase_a import verify_delta

    plan = _plan()
    context = _context(plan)
    context["resources"][TASK.format("unit")].update(
        enable_fault_injection=False, ipc_mode="", pid_mode="", tags={}
    )
    with pytest.raises(ValueError, match=r"^delta=source_contract_mismatch$"):
        verify_delta(plan, context)


@pytest.mark.parametrize("mode", ["unit", "recovery"])
def test_recusa_enable_fault_injection_true_conhecido(mode):
    plan = _plan()
    change = _change(plan, mode)
    change["after"]["enable_fault_injection"] = True
    del change["after_unknown"]["enable_fault_injection"]
    _denied(plan, "task_functional_change")


def test_recusa_configure_at_launch_true_conhecido_em_volume_do_meio():
    plan = _plan()
    change = _change(plan)
    change["after"]["volume"][1]["configure_at_launch"] = True
    del change["after_unknown"]["volume"][1]["configure_at_launch"]
    _denied(plan, "task_functional_change")


def test_recusa_as_duas_folhas_true_conhecidas():
    plan = _plan()
    change = _change(plan, "audit")
    _known_launch_leaves(change)
    change["after"]["enable_fault_injection"] = True
    change["after"]["volume"][2]["configure_at_launch"] = True
    _denied(plan, "task_functional_change")


@pytest.mark.parametrize("field", ["cpu", "memory", "task_role_arn"])
def test_recusa_unknown_em_campo_funcional_da_task_def(field):
    plan = _plan()
    change = _change(plan)
    del change["after"][field]
    change["after_unknown"][field] = True
    _denied(plan, "unknown_functional")


def test_recusa_unknown_em_folha_de_volume_fora_da_allowlist():
    plan = _plan()
    change = _change(plan, "recovery")
    del change["after"]["volume"][2]["host_path"]
    change["after_unknown"]["volume"][2]["host_path"] = True
    _denied(plan, "unknown_functional")


def test_recusa_ipc_mode_host():
    plan = _plan()
    _change(plan)["after"]["ipc_mode"] = "host"
    _denied(plan, "task_functional_change")


def test_recusa_vazio_igual_null_fora_de_ipc_pid_e_tags():
    plan = _plan()
    change = _change(plan)
    change["before"]["network_mode"] = ""
    change["after"]["network_mode"] = None
    _denied(plan, "task_functional_change")


def test_recusa_entrada_de_environment_nova_no_container():
    plan = _plan()
    change = _change(plan)
    change["after"]["container_definitions"] = _containers(
        NEW, environment=[{"name": "EXTRA", "value": "1"}]
    )
    _denied(plan, "container_functional_change")


def test_recusa_folha_unknown_em_task_def_so_update():
    plan = _plan(("update",))
    _denied(plan, "unknown_functional")


@pytest.mark.parametrize(
    "unknown",
    [{"enable_fault_injection": True}, {"volume": [{"configure_at_launch": True}]}],
)
def test_recusa_folha_da_allowlist_fora_de_task_def(unknown):
    role = {"name": "cnesdata-prod-canary-task", "assume_role_policy": "{}"}
    plan = {
        "resource_changes": [
            {
                "address": CANARY_ROLE,
                "mode": "managed",
                "type": "aws_iam_role",
                "change": {
                    "actions": ["update"],
                    "before": role,
                    "after": role,
                    "after_unknown": unknown,
                },
            }
        ]
    }
    _denied(plan, "unknown_functional")


def test_recusa_recriacao_sem_skip_destroy_no_before():
    plan = _plan()
    _change(plan)["before"]["skip_destroy"] = False
    _denied(plan, "destructive_change")


def test_recusa_volume_a_mais_sem_estourar_indice():
    plan = _plan()
    _change(plan)["before"]["volume"].pop()
    _denied(plan, "task_functional_change")


def test_recusa_folha_unknown_que_tambem_traz_valor():
    plan = _plan()
    _change(plan)["after"]["volume"][1]["configure_at_launch"] = False
    _denied(plan, "unknown_leaf_value")


def test_substituicao_exige_recriacao_com_skip_destroy_no_before():
    from scripts.promotion._phase_a_graph import delta_after
    from scripts.promotion.phase_a import COMPUTED

    resource = _task("unit")
    resource["change"]["before"]["skip_destroy"] = False
    plan = {"resource_changes": [resource]}
    with pytest.raises(ValueError, match=r"^delta=unknown_functional$"):
        delta_after(resource, _context(plan), True, COMPUTED)
