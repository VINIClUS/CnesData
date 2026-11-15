"""Role OIDC da promoção ECR observada em planos nativos e avaliada sobre os documentos."""

import json
import shutil
from copy import deepcopy
from pathlib import Path

import pytest

from tests.production.infra._processing_native import _native_result
from tests.production.infra._root_policy_inventory import runtime_iam_entries
from tests.production.infra._static_promotion_iam_model import autoriza
from tests.production.infra._step_functions_iam_contracts import (
    Node,
    event_for,
    native_events,
    resource_at,
)
from tests.production.infra.test_canary_fence_iam import RUNTIME_BASE, _role_overrides

ACCOUNT = "123456789012"
PREFIX = "cnesdata-prod"
REPOSITORY = RUNTIME_BASE["repository_arn"]
ROLE = "module.subject.aws_iam_role.ecr_promotion[0]"
POLICY = "module.subject.aws_iam_role_policy.ecr_promotion[0]"
PROVIDER = f"arn:aws:iam::{ACCOUNT}:oidc-provider/token.actions.githubusercontent.com"
OIDC = "token.actions.githubusercontent.com"
WORKFLOW = ".github/workflows/phase-a-prepare.yml"
SUBJECT = "repo:VINIClUS/CnesData:environment:production"
WORKFLOW_REF = f"VINIClUS/CnesData/{WORKFLOW}@refs/heads/main"
ACTIVE = {
    "repository": "VINIClUS/CnesData",
    "environment": "production",
    "workflow_path": WORKFLOW,
    "ref": "refs/heads/main",
}
REPOSITORY_ACTIONS = (
    "BatchCheckLayerAvailability",
    "InitiateLayerUpload",
    "UploadLayerPart",
    "CompleteLayerUpload",
    "PutImage",
    "BatchGetImage",
    "DescribeImages",
    "DescribeRepositories",
    "GetDownloadUrlForLayer",
)
DENIED_ACTIONS = (
    "DeleteRepository",
    "BatchDeleteImage",
    "PutImageTagMutability",
    "SetRepositoryPolicy",
    "PutLifecyclePolicy",
    "CreateRepository",
    "PutImageScanningConfiguration",
)
INVALID = {
    "ref_develop": ACTIVE | {"ref": "refs/heads/develop"},
    "ref_curinga": ACTIVE | {"ref": "refs/heads/*"},
    "repositorio": ACTIVE | {"repository": "VINIClUS/outro"},
    "ambiente": ACTIVE | {"environment": "staging"},
    "workflow": ACTIVE | {"workflow_path": ".github/workflows/outro.yml"},
    "campo_extra": ACTIVE | {"extra": "x"},
    "campo_ausente": {key: value for key, value in ACTIVE.items() if key != "ref"},
    "campo_nulo": ACTIVE | {"ref": None},
    "objeto_vazio": {},
}
MAIN_TF = """terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "6.54.0"
    }
  }
}
provider "aws" {
  region = "us-east-2"
}
%(variables)s
module "subject" {
  source            = "%(source)s"
  name_prefix       = var.name_prefix
  account_id        = var.account_id
  repository_arn    = var.repository_arn
  log_group_arn     = var.log_group_arn
  state_machine_arn = var.state_machine_arn
  runtime_grants    = var.runtime_grants
  canary_release    = var.canary_release
  ecr_promotion     = var.ecr_promotion
}
"""
NAMES = (
    "name_prefix",
    "account_id",
    "repository_arn",
    "log_group_arn",
    "state_machine_arn",
    "runtime_grants",
    "canary_release",
)


def _runs() -> dict[str, Node]:
    other_region = REPOSITORY.replace("us-east-2", "us-east-1")
    other_repository = REPOSITORY.replace(f"{PREFIX}", "outro")
    return {
        "padrao": {},
        "nulo": {"ecr_promotion": None},
        "ativo": {"ecr_promotion": ACTIVE},
        "prefixo_estranho": {"ecr_promotion": ACTIVE, "name_prefix": "outro"},
        "repositorio_de_outra_regiao": {"ecr_promotion": ACTIVE, "repository_arn": other_region},
        "repositorio_de_outro_nome": {"ecr_promotion": ACTIVE, "repository_arn": other_repository},
    } | {f"invalido_{name}": {"ecr_promotion": value} for name, value in INVALID.items()}


@pytest.fixture(scope="module")
def ecr_events(
    tmp_path_factory: pytest.TempPathFactory,
    prod_root: Path,
    tofu_context: tuple[str, dict[str, str]],
) -> list[Node]:
    root = tmp_path_factory.mktemp("ecr-promotion-iam")
    variables = "".join(f'variable "{name}" {{\n  type = any\n}}\n' for name in NAMES)
    variables += 'variable "ecr_promotion" {\n  type    = any\n  default = null\n}\n'
    source = str((prod_root / "../../modules/runtime-iam").resolve())
    (root / "main.tf").write_text(MAIN_TF % {"variables": variables, "source": source})
    shutil.copyfile(prod_root / ".terraform.lock.hcl", root / ".terraform.lock.hcl")
    header = 'mock_provider "aws" {}\n' + _role_overrides()
    header += (
        f"override_resource {{\n  target = {ROLE}\n"
        f'  values = {{ arn = "arn:aws:iam::{ACCOUNT}:role/{PREFIX}-ecr-promotion" }}\n}}\n'
    )
    for name, changes in _runs().items():
        literals = "\n".join(
            f"    {key} = {json.dumps(value)}" for key, value in (RUNTIME_BASE | changes).items()
        )
        run = f'run "{name}" {{\n  command = plan\n  variables {{\n{literals}\n  }}\n}}\n'
        (root / f"{name}.tftest.hcl").write_text(header + run)
    data = root / ".tofu"
    data.mkdir()
    tofu = tofu_context[0], tofu_context[1] | {"TF_DATA_DIR": str(data)}
    done = _native_result(tofu, root, ("init", "-backend=false", "-input=false"), "ecr-init")
    assert done.returncode == 0, done.stderr
    done = _native_result(
        tofu, root, ("test", "-no-color", "-json", "-verbose", "-test-directory=."), "ecr-plan"
    )
    events = native_events(done.stdout)
    assert events, f"native_events=missing stderr={done.stderr!r}"
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
    return json.loads(_values(plan, POLICY)["policy"]), json.loads(
        _values(plan, ROLE)["assume_role_policy"]
    )


def _call(action: str, resource: str = REPOSITORY, **context: str) -> Node:
    return {"action": action, "resource": resource, "context": context}


def _assume(**claims: str) -> Node:
    base = {"aud": "sts.amazonaws.com", "sub": SUBJECT, "job_workflow_ref": WORKFLOW_REF}
    return {
        "action": "sts:AssumeRoleWithWebIdentity",
        "resource": "*",
        "context": {f"{OIDC}:{key}": value for key, value in (base | claims).items()},
    }


@pytest.mark.parametrize("label", ["padrao", "nulo"])
def test_desligado_nao_cria_recursos_da_role(ecr_events: list[Node], label: str) -> None:
    plan = _plan(ecr_events, label)
    assert not [r for r in plan["resource_changes"] if ".ecr_promotion[" in r["address"]]
    child = plan["configuration"]["root_module"]["module_calls"]["subject"]["module"]
    assert child["variables"]["ecr_promotion"]["default"] is None


def test_ativacao_adiciona_somente_role_e_policy(ecr_events: list[Node]) -> None:
    active, off = _plan(ecr_events, "ativo"), _plan(ecr_events, "padrao")
    before = {r["address"]: r["change"]["after"] for r in off["resource_changes"]}
    after = {r["address"]: r["change"]["after"] for r in active["resource_changes"]}
    assert after.keys() - before.keys() == {ROLE, POLICY}
    assert {key: after[key] for key in before} == before


def test_role_tem_nome_sessao_curta_e_nenhuma_policy_extra(ecr_events: list[Node]) -> None:
    plan = _plan(ecr_events, "ativo")
    role, policy = _values(plan, ROLE), _values(plan, POLICY)
    assert role["name"] == f"{PREFIX}-ecr-promotion"
    assert role["max_session_duration"] == 3600
    assert policy["name"] == f"{PREFIX}-ecr-promotion-runtime"
    assert policy["role"] == role["name"]
    assert not role.get("inline_policy")
    assert not role.get("managed_policy_arns")


def test_documentos_renderizados_sao_exatos(ecr_events: list[Node]) -> None:
    policy, trust = _documents(ecr_events)
    assert trust == {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Federated": PROVIDER},
                "Action": "sts:AssumeRoleWithWebIdentity",
                "Condition": {
                    "StringEquals": {
                        f"{OIDC}:aud": "sts.amazonaws.com",
                        f"{OIDC}:sub": SUBJECT,
                        f"{OIDC}:job_workflow_ref": WORKFLOW_REF,
                    }
                },
            }
        ],
    }
    assert policy == {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [f"ecr:{action}" for action in REPOSITORY_ACTIONS],
                "Resource": REPOSITORY,
            },
            {
                "Effect": "Allow",
                "Action": "ecr:GetAuthorizationToken",
                "Resource": "*",
                "Condition": {"StringEquals": {"aws:RequestedRegion": "us-east-2"}},
            },
        ],
    }


@pytest.mark.parametrize("action", REPOSITORY_ACTIONS)
def test_concede_cada_acao_no_repositorio_exato(ecr_events: list[Node], action: str) -> None:
    policy, _ = _documents(ecr_events)
    assert autoriza(policy, _call(f"ecr:{action}"))


def test_concede_token_de_autorizacao_so_em_us_east_2(ecr_events: list[Node]) -> None:
    policy, _ = _documents(ecr_events)
    action = "ecr:GetAuthorizationToken"
    assert autoriza(policy, _call(action, "*", **{"aws:RequestedRegion": "us-east-2"}))
    assert not autoriza(policy, _call(action, "*", **{"aws:RequestedRegion": "us-east-1"}))
    assert not autoriza(policy, _call(action, "*"))


@pytest.mark.parametrize(
    "resource",
    [
        REPOSITORY.replace("us-east-2", "us-east-1"),
        REPOSITORY.replace(PREFIX, "outro"),
        REPOSITORY + "-outro",
        REPOSITORY.replace(ACCOUNT, "999999999999"),
        "*",
    ],
    ids=["outra_regiao", "outro_repositorio", "sufixo", "outra_conta", "curinga"],
)
def test_nega_acoes_de_escrita_em_outro_repositorio_regiao_ou_conta(
    ecr_events: list[Node], resource: str
) -> None:
    policy, _ = _documents(ecr_events)
    assert not autoriza(policy, _call("ecr:PutImage", resource))
    assert not autoriza(policy, _call("ecr:InitiateLayerUpload", resource))


@pytest.mark.parametrize("action", DENIED_ACTIONS)
def test_nega_delete_mutabilidade_politica_e_ciclo_de_vida(
    ecr_events: list[Node], action: str
) -> None:
    policy, _ = _documents(ecr_events)
    assert not autoriza(policy, _call(f"ecr:{action}"))


@pytest.mark.parametrize(
    ("action", "resource"),
    [
        ("s3:GetObject", "arn:aws:s3:::cnesdata-prod-data/x"),
        ("s3:PutObject", "arn:aws:s3:::cnesdata-prod-data/x"),
        ("dynamodb:PutItem", f"arn:aws:dynamodb:us-east-2:{ACCOUNT}:table/cnesdata-prod-control"),
        ("kms:Decrypt", f"arn:aws:kms:us-east-2:{ACCOUNT}:key/x"),
        ("iam:PassRole", f"arn:aws:iam::{ACCOUNT}:role/cnesdata-prod-processor-unit"),
        ("ecr:*", REPOSITORY),
    ],
)
def test_nega_acesso_a_s3_dynamodb_kms_iam_e_curinga(
    ecr_events: list[Node], action: str, resource: str
) -> None:
    policy, _ = _documents(ecr_events)
    assert not autoriza(policy, _call(action, resource))


def test_confianca_aceita_so_a_combinacao_exata_de_aud_sub_e_workflow(
    ecr_events: list[Node],
) -> None:
    _, trust = _documents(ecr_events)
    assert autoriza(trust, _assume())


@pytest.mark.parametrize(
    "claims",
    [
        {"aud": "example.com"},
        {"sub": "repo:VINIClUS/outro:environment:production"},
        {"sub": "repo:VINIClUS/CnesData:environment:staging"},
        {"sub": "repo:VINIClUS/CnesData:ref:refs/heads/main"},
        {"sub": "repo:VINIClUS/CnesData:environment:production:extra"},
        {"job_workflow_ref": WORKFLOW_REF.replace("main", "develop")},
        {"job_workflow_ref": WORKFLOW_REF.replace("phase-a-prepare", "outro")},
        {"job_workflow_ref": WORKFLOW_REF.replace("CnesData", "outro")},
        {"job_workflow_ref": WORKFLOW_REF.replace("refs/heads/main", "refs/tags/main")},
    ],
    ids=[
        "aud",
        "outro_repositorio",
        "outro_ambiente",
        "sub_por_ref",
        "sub_com_sufixo",
        "ref_develop",
        "outro_workflow",
        "workflow_de_outro_repositorio",
        "ref_de_tag",
    ],
)
def test_confianca_nega_claim_divergente(ecr_events: list[Node], claims: dict[str, str]) -> None:
    _, trust = _documents(ecr_events)
    assert not autoriza(trust, _assume(**claims))


def test_confianca_nega_claim_ausente_e_assume_role_comum(ecr_events: list[Node]) -> None:
    _, trust = _documents(ecr_events)
    request = _assume()
    del request["context"][f"{OIDC}:job_workflow_ref"]
    assert not autoriza(trust, request)
    assert not autoriza(trust, _assume() | {"action": "sts:AssumeRole"})


@pytest.mark.parametrize("mutation", ["acao", "recurso", "curinga", "token_sem_regiao"])
def test_matriz_detecta_alargamento_da_policy(ecr_events: list[Node], mutation: str) -> None:
    policy, _ = deepcopy(_documents(ecr_events))
    first, second = policy["Statement"]
    match mutation:
        case "acao":
            first["Action"].append("ecr:DeleteRepository")
        case "recurso":
            first["Resource"] = "*"
        case "curinga":
            first["Action"] = "ecr:*"
        case "token_sem_regiao":
            del second["Condition"]
    allowed = (
        autoriza(policy, _call("ecr:DeleteRepository")),
        autoriza(policy, _call("ecr:PutImage", REPOSITORY.replace(PREFIX, "outro"))),
        autoriza(policy, _call("ecr:GetAuthorizationToken", "*", **{"aws:RequestedRegion": "x"})),
    )
    assert any(allowed)


@pytest.mark.parametrize("label", [f"invalido_{name}" for name in INVALID])
def test_valores_invalidos_da_variavel_falham_com_o_contrato(
    ecr_events: list[Node], label: str
) -> None:
    assert event_for(ecr_events, "test_run", label)["test_run"]["status"] == "error"
    errors = [
        e["diagnostic"]
        for e in ecr_events
        if e.get("type") == "diagnostic"
        and e.get("@testrun") == label
        and e["diagnostic"]["severity"] == "error"
    ]
    assert errors
    assert all("ecr_promotion=invalid_contract" in error["detail"] for error in errors)


@pytest.mark.parametrize(
    "label", ["prefixo_estranho", "repositorio_de_outra_regiao", "repositorio_de_outro_nome"]
)
def test_repositorio_e_prefixo_fora_do_contrato_recusam_a_ativacao(
    ecr_events: list[Node], label: str
) -> None:
    assert event_for(ecr_events, "test_run", label)["test_run"]["status"] == "error"
    details = [
        e["diagnostic"]["detail"]
        for e in ecr_events
        if e.get("type") == "diagnostic" and e.get("@testrun") == label
    ]
    assert any("ecr_promotion=invalid_contract" in detail for detail in details)


def test_raiz_encaminha_a_variavel_sem_novo_output(tofu_artifacts: Node) -> None:
    root = tofu_artifacts["configuration"]["root_module"]
    call = root["module_calls"]["runtime_iam"]["expressions"]
    assert call["ecr_promotion"] == {"references": ["var.ecr_promotion"]}
    assert root["variables"]["ecr_promotion"]["default"] is None
    assert not [name for name in root["outputs"] if "ecr_promotion" in name]


def _root_plan(ecr_promotion: Node | None) -> Node:
    api = f"arn:aws:iam::{ACCOUNT}:role/cnesdata-api"
    shared = {"value": {"roles_anywhere": {"api_role_arn": api}}}
    return {"variables": {"shared_outputs": shared, "ecr_promotion": {"value": ecr_promotion}}}


def test_inventario_da_raiz_inclui_a_policy_so_quando_a_variavel_esta_ativa() -> None:
    address = "module.runtime_iam.aws_iam_role_policy.ecr_promotion[0]"
    off = runtime_iam_entries(_root_plan(None))
    on = runtime_iam_entries(_root_plan(ACTIVE))
    assert address not in off
    assert on.pop(address) == (f"{PREFIX}-ecr-promotion-runtime", f"{PREFIX}-ecr-promotion")
    assert on == off
