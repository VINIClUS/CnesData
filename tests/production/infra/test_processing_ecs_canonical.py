"""O container renderizado já sai na forma que o ECS devolve, sem replace perpétuo das revisões.

O provider aws 6.54.0 compara `container_definitions` com o state, que guarda o readback do ECS.
Campos que o ECS preenche por default e o HCL não declara (`essential`, `portMappings`,
`systemControls`, `volumesFrom`, `linuxParameters.capabilities.add`) apareciam só no state e
forçavam replace das três revisões a cada plan (drift pós-apply da Phase A, #469). O fixture é o
readback real `describe-task-definition` da revisão 1, redigido: imagem, valores de ambiente e
grupo de log viram marcador, sem conta, ARN nem rede.
"""

import copy
import json
import re
from pathlib import Path

import pytest

from tests.production.infra._processing_native import NativePlans, Node
from tests.production.infra.test_processing_security import MODES, _witnesses

READBACK_PATH = Path(__file__).with_name("_ecs_readback_redacted.json")
REDACTED = "<redigido>"
# Default que o ECS ecoa e o provider não guarda no state: container sem cpu volta com cpu=0.
PROVIDER_DROPPED = {"cpu": 0}
ECS_DEFAULTS = ("essential", "portMappings", "systemControls", "volumesFrom")


def readback() -> dict[str, Node]:
    """Returns: containers do readback redigido por modo."""
    return json.loads(READBACK_PATH.read_text(encoding="utf-8"))


def canonical(container: Node) -> Node:
    """Returns: container comparável; o ECS reordena `environment`, então só os nomes contam."""
    value = {
        key: copy.deepcopy(item)
        for key, item in container.items()
        if key not in PROVIDER_DROPPED or PROVIDER_DROPPED[key] != item
    }
    value["image"] = REDACTED
    value["environment"] = sorted(item["name"] for item in value.get("environment", []))
    log = value["logConfiguration"]
    log["options"] = {**log["options"], "awslogs-group": REDACTED}
    return value


def differences(expected: Node, actual: Node) -> list[str]:
    """Returns: chaves de topo que divergem entre os dois containers canônicos."""
    left, right = canonical(expected), canonical(actual)
    return sorted(key for key in set(left) | set(right) if left.get(key) != right.get(key))


def test_readback_redigido_nao_carrega_conta_arn_nem_rede() -> None:
    text = READBACK_PATH.read_text(encoding="utf-8")
    assert re.search(r"[0-9]{12}", text) is None
    for marker in ("arn:", "sg-", "subnet-", "amazonaws.com", "cnesdata-prod-"):
        assert marker not in text, f"readback_not_redacted marker={marker}"
    assert set(readback()) == set(MODES)
    for container in readback().values():
        assert container["image"] == REDACTED
        assert {item["value"] for item in container["environment"]} == {REDACTED}
        assert container["logConfiguration"]["options"]["awslogs-group"] == REDACTED


@pytest.mark.parametrize("key", ECS_DEFAULTS)
def test_comparador_acusa_default_do_ecs_ausente(key: str) -> None:
    sample = readback()["recovery"]
    mutant = copy.deepcopy(sample)
    del mutant[key]
    assert differences(sample, mutant) == [key]


def test_comparador_acusa_capabilities_add_ausente_e_ignora_cpu_zero_e_ordem() -> None:
    sample = readback()["recovery"]
    mutant = copy.deepcopy(sample)
    del mutant["linuxParameters"]["capabilities"]["add"]
    assert differences(sample, mutant) == ["linuxParameters"]
    reordered = copy.deepcopy(sample)
    del reordered["cpu"]
    reordered["environment"] = list(reversed(reordered["environment"]))
    assert differences(sample, reordered) == []


@pytest.mark.parametrize("mode", MODES)
def test_container_renderizado_ja_esta_na_forma_canonica_do_ecs(
    processing_native: NativePlans, prod_root: Path, mode: str
) -> None:
    expected = readback()[mode]
    witnesses = list(_witnesses(processing_native, prod_root, mode))
    assert witnesses, "ecs_canonical witnesses=empty"
    for _, _, rendered in witnesses:
        assert differences(expected, rendered) == [], f"ecs_canonical mode={mode}"
