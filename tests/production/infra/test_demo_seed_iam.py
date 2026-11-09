"""Contrato IAM demo observado em planos nativos sem acesso à AWS."""

import json
import shutil
from copy import deepcopy
from fnmatch import fnmatchcase
from hashlib import sha256
from pathlib import Path

import pytest

from cnes_infra.control_plane.dynamodb_keys import tenant_partition
from tests.production.infra._processing_native import _native_result
from tests.production.infra._step_functions_iam_contracts import (
    Node,
    event_for,
    native_events,
    resource_at,
)
from tests.production.infra.test_canary_fence_iam import RUNTIME_BASE, _role_overrides

ACCOUNT = "123456789012"
PREFIX = "cnesdata-prod"
TABLE = f"arn:aws:dynamodb:us-east-2:{ACCOUNT}:table/{PREFIX}-control-plane"
PRINCIPAL = f"arn:aws:iam::{ACCOUNT}:role/synthetic/demo-operator"
PK = "TENANT#64656d6f"
DEMO = {"tenant_partition_key": PK, "trusted_principal_arn": PRINCIPAL}
ROLE = "module.subject.aws_iam_role.demo_seed[0]"
POLICY = "module.subject.aws_iam_role_policy.demo_seed[0]"
FIXTURE = Path(__file__).resolve().parents[2] / "fixtures" / "demo-seed-iam"
REQUESTS = json.loads((FIXTURE / "requests.json").read_text(encoding="utf-8"))
INVALID = {
    "global": DEMO | {"tenant_partition_key": "TENANT#_GLOBAL"},
    "vazio": DEMO | {"tenant_partition_key": "TENANT#"},
    "impar": DEMO | {"tenant_partition_key": "TENANT#123"},
    "maiusculo": DEMO | {"tenant_partition_key": "TENANT#AB"},
    "prefixo": DEMO | {"tenant_partition_key": PK + "#RUN#01"},
    "curinga": DEMO | {"tenant_partition_key": PK + "*"},
    "pk_nula": DEMO | {"tenant_partition_key": None},
    "principal_nulo": DEMO | {"trusted_principal_arn": None},
    "objeto_vazio": {},
    "parcial": {"tenant_partition_key": PK},
}
INVALID |= {
    name: DEMO | {"trusted_principal_arn": principal}
    for name, principal in {
        "outra_conta": "arn:aws:iam::999999999999:role/demo",
        "root": f"arn:aws:iam::{ACCOUNT}:root",
        "user": f"arn:aws:iam::{ACCOUNT}:user/demo",
        "sts": f"arn:aws:sts::{ACCOUNT}:assumed-role/demo/session",
        "oidc": f"arn:aws:iam::{ACCOUNT}:oidc-provider/token.actions.githubusercontent.com",
        "wildcard": f"arn:aws:iam::{ACCOUNT}:role/*",
        "interrogacao": f"arn:aws:iam::{ACCOUNT}:role/demo?",
        "unicode": f"arn:aws:iam::{ACCOUNT}:role/café/demo",
        "espaco": f"arn:aws:iam::{ACCOUNT}:role/demo operator",
        "nome_longo": f"arn:aws:iam::{ACCOUNT}:role/" + "a" * 65,
    }.items()
}


def _runs() -> dict[str, Node]:
    long_names = json.loads(json.dumps(RUNTIME_BASE).replace(PREFIX, "a" * 55))
    return {
        "padrao": {},
        "ativo": {"demo_seed": DEMO},
        "nulo": {"demo_seed": None},
        "nome_longo": long_names | {"demo_seed": DEMO, "canary_release": None},
        "sem_grants": {"demo_seed": DEMO, "runtime_grants": None, "canary_release": None},
    } | {f"invalido_{name}": {"demo_seed": value} for name, value in INVALID.items()}


@pytest.fixture(scope="module")
def demo_events(
    tmp_path_factory: pytest.TempPathFactory,
    prod_root: Path,
    tofu_context: tuple[str, dict[str, str]],
) -> list[Node]:
    root = tmp_path_factory.mktemp("demo-seed-iam")
    shutil.copytree(FIXTURE, root, dirs_exist_ok=True)
    main = root / "main.tf"
    source = str((prod_root / "../../modules/runtime-iam").resolve())
    main.write_text(main.read_text().replace("../../../infra/opentofu/modules/runtime-iam", source))
    shutil.copyfile(prod_root / ".terraform.lock.hcl", root / ".terraform.lock.hcl")
    overrides = (
        _role_overrides()
        + f"""override_resource {{
  target = {ROLE}
  values = {{ arn = "arn:aws:iam::{ACCOUNT}:role/{PREFIX}-demo-seed" }}
}}
"""
    )
    suite = root / "production.tftest.hcl"
    header = suite.read_text() + overrides
    for name, changes in _runs().items():
        inputs = RUNTIME_BASE | changes
        literals = "\n".join(f"    {key} = {json.dumps(value)}" for key, value in inputs.items())
        target = root / f"{name}.tftest.hcl"
        target.write_text(
            header + f'run "{name}" {{\n  command = plan\n  variables {{\n{literals}\n  }}\n}}\n'
        )
    suite.unlink()
    data = root / ".tofu"
    data.mkdir()
    tofu = tofu_context[0], tofu_context[1] | {"TF_DATA_DIR": str(data)}
    result = _native_result(tofu, root, ("init", "-backend=false", "-input=false"), "demo-init")
    assert result.returncode == 0, result.stderr
    result = _native_result(
        tofu,
        root,
        (
            "test",
            "-no-color",
            "-json",
            "-verbose",
            "-test-directory=.",
        ),
        "demo-plan",
    )
    events = native_events(result.stdout)
    assert events, f"native_events=missing stderr={result.stderr!r}"
    return events


def _plan(events: list[Node], label: str) -> Node:
    run = event_for(events, "test_run", label)["test_run"]
    errors = [e for e in events if e.get("type") == "diagnostic" and e.get("@testrun") == label]
    assert run["status"] == "pass", f"run={label} diagnostics={errors!r}"
    return event_for(events, "test_plan", label)["test_plan"]


def _values(plan: Node, address: str) -> Node:
    return resource_at(plan, address)["change"]["after"]


def _documents(events: list[Node]) -> tuple[Node, Node]:
    plan = _plan(events, "ativo")
    return (
        json.loads(_values(plan, POLICY)["policy"]),
        json.loads(_values(plan, ROLE)["assume_role_policy"]),
    )


def _condition_matches(condition: Node, context: Node) -> bool:
    for operator, entries in condition.items():
        for key, expected in entries.items():
            actual = context.get(key)
            values = expected if isinstance(expected, list) else [expected]
            if operator == "Null":
                if (actual is None or actual in ([], "")) != (expected == "true"):
                    return False
            elif operator == "ForAllValues:StringEquals":
                if not all(value in values for value in actual or []):
                    return False
            elif operator == "StringEquals":
                if actual not in values:
                    return False
            else:
                raise AssertionError(f"unsupported_condition={operator}")
    return True


def _allows(document: Node, request: Node) -> bool:
    for statement in document["Statement"]:
        actions = statement["Action"]
        actions = actions if isinstance(actions, list) else [actions]
        resources = statement.get("Resource", "*")
        resources = resources if isinstance(resources, list) else [resources]
        principal = statement.get("Principal", {}).get("AWS", "*")
        if (
            statement["Effect"] == "Allow"
            and any(fnmatchcase(request["action"], action) for action in actions)
            and any(fnmatchcase(request.get("resource", "*"), value) for value in resources)
            and fnmatchcase(request.get("principal", "*"), principal)
            and _condition_matches(statement.get("Condition", {}), request.get("context", {}))
        ):
            return True
    return False


def _assert_matrix(policy: Node, trust: Node) -> None:
    for request in REQUESTS:
        document = trust if request["action"].startswith("sts:") else policy
        assert _allows(document, request) == request["allowed"], f"request={request['name']}"


@pytest.mark.parametrize("label", ["padrao", "nulo"])
def test_desligado_nao_cria_recursos_nem_contrato(demo_events: list[Node], label: str) -> None:
    plan = _plan(demo_events, label)
    assert not [r for r in plan["resource_changes"] if ".demo_seed[" in r["address"]]
    assert (
        plan["planned_values"].get("outputs", {}).get("demo_seed_iam_contract", {}).get("value")
        is None
    )
    child = plan["configuration"]["root_module"]["module_calls"]["subject"]["module"]
    assert child["variables"]["demo_seed"]["default"] is None


def test_ativacao_adiciona_somente_role_e_policy_sem_alterar_canary(
    demo_events: list[Node],
) -> None:
    active, off = _plan(demo_events, "ativo"), _plan(demo_events, "padrao")
    before = {r["address"]: r["change"]["after"] for r in off["resource_changes"]}
    after = {r["address"]: r["change"]["after"] for r in active["resource_changes"]}
    assert after.keys() - before.keys() == {ROLE, POLICY}
    assert {key: after[key] for key in before} == before
    assert (
        active["planned_values"]["outputs"]["canary_contract"]
        == (off["planned_values"]["outputs"]["canary_contract"])
    )


def test_contrato_tem_nove_campos_e_hash_da_policy_real(demo_events: list[Node]) -> None:
    plan = _plan(demo_events, "ativo")
    role, policy = _values(plan, ROLE), _values(plan, POLICY)
    assert role["max_session_duration"] == 3600
    assert policy["role"] == role["name"]
    assert not role.get("inline_policy")
    assert not role.get("managed_policy_arns")
    contract = plan["planned_values"]["outputs"]["demo_seed_iam_contract"]["value"]
    assert contract == {
        "contract_version": "cnesdata.demo-seed-iam/v1",
        "role_arn": role["arn"],
        "policy_name": policy["name"],
        "policy_sha256": sha256(policy["policy"].encode()).hexdigest(),
        "table_arn": TABLE,
        "tenant_partition_key": PK,
        "role_session_name": "demo-seed",
        "max_session_duration_seconds": 3600,
        "allowed_actions": ["dynamodb:GetItem", "dynamodb:PutItem"],
    }
    assert tenant_partition("demo") == PK


def test_documentos_renderizados_respeitam_matriz_e_grants_exatos(demo_events: list[Node]) -> None:
    policy, trust = _documents(demo_events)
    _assert_matrix(policy, trust)
    assert policy == {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["dynamodb:GetItem", "dynamodb:PutItem"],
                "Resource": TABLE,
                "Condition": {
                    "ForAllValues:StringEquals": {"dynamodb:LeadingKeys": [PK]},
                    "Null": {"dynamodb:LeadingKeys": "false"},
                },
            }
        ],
    }
    assert trust == {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": PRINCIPAL},
                "Action": "sts:AssumeRole",
                "Condition": {"StringEquals": {"sts:RoleSessionName": "demo-seed"}},
            }
        ],
    }


def test_iam_nao_restringe_sk_ou_payload_dentro_da_pk_demo(demo_events: list[Node]) -> None:
    policy, _ = _documents(demo_events)
    assert _allows(
        policy,
        {
            "action": "dynamodb:PutItem",
            "resource": TABLE,
            "context": {"dynamodb:LeadingKeys": [PK]},
            "sort_key": "OTHER#arbitrary",
            "payload": {"entity_type": "arbitrary"},
        },
    )


@pytest.mark.parametrize("mutation", ["null", "action", "resource", "pk", "principal", "session"])
def test_matriz_detecta_alargamento_dos_documentos_renderizados(
    demo_events: list[Node],
    mutation: str,
) -> None:
    policy, trust = deepcopy(_documents(demo_events))
    statement = policy["Statement"][0]
    match mutation:
        case "null":
            del statement["Condition"]["Null"]
        case "action":
            statement["Action"] = ["dynamodb:*"]
        case "resource":
            statement["Resource"] = "*"
        case "pk":
            statement["Condition"]["ForAllValues:StringEquals"]["dynamodb:LeadingKeys"].append(
                "TENANT#6f74686572"
            )
        case "principal":
            trust["Statement"][0]["Principal"]["AWS"] = "*"
        case "session":
            del trust["Statement"][0]["Condition"]
    with pytest.raises(AssertionError, match="request="):
        _assert_matrix(policy, trust)


@pytest.mark.parametrize("label", ["sem_grants", "nome_longo"] + [f"invalido_{n}" for n in INVALID])
def test_inputs_invalidos_falham_no_child_demo(demo_events: list[Node], label: str) -> None:
    assert event_for(demo_events, "test_run", label)["test_run"]["status"] == "error"
    errors = [
        e["diagnostic"]
        for e in demo_events
        if e.get("type") == "diagnostic"
        and e.get("@testrun") == label
        and e["diagnostic"]["severity"] == "error"
    ]
    assert errors
    validation = [
        error
        for error in errors
        if error["summary"] in {"Invalid value for variable", "Invalid value for input variable"}
    ]
    assert validation
    assert any("demo_seed" in error["detail"] for error in validation)
    for error in errors:
        if label == "nome_longo" and error["summary"].startswith("expected length of name"):
            continue
        assert error in validation
        assert "demo_seed" in error["detail"]


def test_raiz_encaminha_input_e_contrato_sem_shared_outputs(tofu_artifacts: Node) -> None:
    root = tofu_artifacts["configuration"]["root_module"]
    assert root["module_calls"]["runtime_iam"]["expressions"]["demo_seed"] == {
        "references": ["var.demo_seed"]
    }
    assert root["outputs"]["demo_seed_iam_contract"]["expression"]["references"] == [
        "module.runtime_iam.demo_seed_iam_contract",
        "module.runtime_iam",
    ]
    assert root["variables"]["demo_seed"]["default"] is None
