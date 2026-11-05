"""Gate de aceite não-mutante da infraestrutura de produção (PROD-INFRA-012)."""

import ast
import json
import re
import subprocess
import tomllib
from pathlib import Path
from typing import Any

import pytest

from tests.production.infra._acceptance_gate import GATE
from tests.production.infra._athena_contracts import FIXTURES, REPO_ROOT
from tests.production.infra._athena_root_plan import (  # noqa: F401
    ACCOUNT,
    PREFIX,
    Node,
    fixture_scheduled_plan,
    resources,
)
from tests.production.infra._aws_mock_defaults import (
    BUCKET_REGIONAL_DOMAIN_NAME,
    MOCK_AWS_PROVIDERS,
)
from tests.production.infra._root_native_identities import TARGETS, _seed_values
from tests.production.infra.conftest import (
    PROD_ROOT,
    ROOT_FIXTURE_CONTRACT,
    VALID_SHARED_OUTPUTS,
    TofuContext,
    require_tool,
)
from tests.production.infra.test_opa_policy import (  # noqa: F401
    REJECTIONS,
    Conftest,
    fixture_conftest,
)

INFRA_TESTS = REPO_ROOT / "tests" / "production" / "infra"
TFTEST = REPO_ROOT / "infra" / "opentofu" / "tests" / "production.tftest.hcl"
TEST_DIRECTORY = "../../tests"
SKIPPERS = frozenset({"skip", "skipif", "xfail", "importorskip"})
ISSUE_FIXTURES = (
    "protegido_cognito_removido.json", "protegido_substituicao.json", "nat_gateway.json",
    "alb.json", "load_balancer.json", "bucket_publico.json", "ingress.json",
    "cloudfront_pago.json", "custo_produto_acima_de_8.json", "custo_agregado_acima_de_15.json",
    "regiao_fora_de_us_east_2.json", "regiao_em_modulo.json", "imagem_mutavel.json",
    "ignore_changes_em_roteamento.tf", "segredo_por_nome.json",
)
PROTECTED_EXTRA = ("aws_cognito_user_pool_client", "aws_cognito_user_pool_domain")
GUARD = re.compile(r'^\s*(prevent_destroy|deletion_protection\w*)\s*=\s*(true|"ACTIVE")')
RESOURCE = re.compile(r'^resource "(\w+)"')
STATE_PATH = re.compile(
    r"(^|/)\.terraform/|\.tfstate(\.|$)|(^|/)[^/]*\.tfplan$|(^|/)tfplan[^/]*$|\.(pem|p12|pfx)$"
)
STATE_CONTENT = re.compile(r'"lineage"\s*:')
PRIVATE_KEY = re.compile("-----BEGIN " + "[A-Z ]*PRIVATE KEY-----")
ACCESS_KEY = re.compile(r"\b(?:AK" + "IA|AS" + r"IA)[0-9A-Z]{16}\b")
ACCOUNT_ID = re.compile(r"arn:aws[a-z-]*:[a-z0-9-]+:[a-z0-9-]*:(\d{12}):|(\d{12})\.dkr\.ecr")
EXAMPLE_ACCOUNTS = frozenset({"123456789012", "000000000000", "999999999999"})
SURFACE = ("infra", "policies", "schemas", "tests/fixtures/plans", "tests/production",
           "docs/runbooks", ".github/workflows")
REQUIRED_RUNS = frozenset({
    "saidas_cognito_da_raiz", "contrato_de_ativacao", "orcamento_de_task_horas",
    "particao_do_freeze", "shared_outputs_com_conta_invalida"})


def _dotted(node: ast.AST) -> str:
    if isinstance(node, ast.Call):
        return _dotted(node.func)
    if isinstance(node, ast.Attribute):
        return f"{_dotted(node.value)}.{node.attr}"
    return node.id if isinstance(node, ast.Name) else ""


def _skips(node: ast.AST) -> bool:
    names = (_dotted(item) for item in ast.walk(node) if isinstance(item, ast.Call | ast.Attribute))
    return any(name.rsplit(".", 1)[-1] in SKIPPERS for name in names)


def _empty_parametrize(decorators: list[ast.expr]) -> bool:
    return any(
        isinstance(item, ast.Call) and _dotted(item).endswith(".parametrize")
        and len(item.args) > 1 and isinstance(item.args[1], ast.List | ast.Tuple)
        and not item.args[1].elts
        for item in decorators
    )


def _proves_something(function: ast.FunctionDef) -> bool:
    return any(
        isinstance(item, ast.Assert)
        or (isinstance(item, ast.Call) and re.search(r"assert|raises", _dotted(item)))
        for item in ast.walk(function)
    )


def _module_defects(path: Path, names: tuple[str, ...]) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    defined = [item for item in tree.body if isinstance(item, ast.FunctionDef)]
    defects = [
        f"module_skipped file={path.name}" for item in tree.body
        if isinstance(item, ast.Assign) and _dotted(item.targets[0]) == "pytestmark"
        and _skips(item.value)
    ]
    for name in names:
        found = [item for item in defined if item.name == name]
        if len(found) != 1:
            defects.append(f"test_not_unique file={path.name} name={name} count={len(found)}")
            continue
        if _skips(found[0]) or _empty_parametrize(found[0].decorator_list):
            defects.append(f"test_disabled file={path.name} name={name}")
        if not _proves_something(found[0]):
            defects.append(f"test_without_assertion file={path.name} name={name}")
    return defects


def _git_files(*patterns: str) -> list[str]:
    done = subprocess.run(  # noqa: S603
        [require_tool("git"), "ls-files", "--cached", "--others", "--exclude-standard", "-z",
         "--", *patterns],
        check=True, capture_output=True, cwd=REPO_ROOT, text=True,
    )
    return [item for item in done.stdout.split("\0") if item]


def test_gate_agrega_todas_as_tasks_de_infra(request: pytest.FixtureRequest) -> None:
    expected = {f"PROD-INFRA-{number:03d}" for number in range(1, 12)} | {"PROD-INFRA-441"}
    assert set(GATE) == expected, "gate_tasks_incomplete"
    defects: list[str] = []
    for task, files in GATE.items():
        assert files, f"task_without_tests task={task}"
        assert all(files.values()), f"test_list_empty task={task}"
        for name, tests in files.items():
            path = INFRA_TESTS / name
            assert path.is_file(), f"test_file_missing task={task} file={name}"
            defects += _module_defects(path, tests)
    assert not defects, defects
    collected = {
        (item.path.name, item.originalname)
        for item in request.session.items
        if isinstance(item, pytest.Function)
    }
    if {path.name for path in INFRA_TESTS.glob("test_*.py")} <= {file for file, _ in collected}:
        wanted = {
            (name, test)
            for files in GATE.values()
            for name, names in files.items()
            for test in names
        }
        assert wanted <= collected, f"gate_deselected missing={sorted(wanted - collected)}"


def test_gate_nao_pode_ser_silenciado_por_configuracao() -> None:
    options = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    pytest_options = options.get("tool", {}).get("pytest", {}).get("ini_options", {})
    flags = str(pytest_options.get("addopts", ""))
    assert not re.search(r"--ignore|--deselect|(^|\s)-[km]\b|--lf|--maxfail", flags), flags
    assert "norecursedirs" not in pytest_options
    for path in [REPO_ROOT / "conftest.py", *INFRA_TESTS.rglob("conftest.py")]:
        text = path.read_text(encoding="utf-8")
        assert not re.search(r"collect_ignore|pytest_collection_modifyitems", text), path.name
        assert "pytestmark" not in text, path.name


def test_gate_fixa_trust_do_scheduler_e_regras_sem_target(scheduled_plan: Node) -> None:
    roles = {
        item["address"]: json.loads(item["change"]["after"]["assume_role_policy"])
        for item in resources(scheduled_plan, "aws_iam_role")
        if item["address"].startswith("module.schedulers.aws_iam_role.scheduler[")
    }
    assert sorted(roles) == [
        'module.schedulers.aws_iam_role.scheduler["audit"]',
        'module.schedulers.aws_iam_role.scheduler["recovery"]',
    ]
    for address, document in roles.items():
        mode = address.split('"')[1]
        condition = document["Statement"][0]["Condition"]["StringEquals"]
        group = f"arn:aws:scheduler:us-east-2:{ACCOUNT}:schedule-group/{PREFIX}-{mode}"
        assert condition == {"aws:SourceAccount": ACCOUNT, "aws:SourceArn": group}, address
    rules = {item["address"] for item in resources(scheduled_plan, "aws_cloudwatch_event_rule")}
    assert len([name for name in rules if ".task_outcome[" in name]) == 3
    assert resources(scheduled_plan, "aws_cloudwatch_event_target") == []


def _plan(*entries: tuple[str, str, list[str], Node]) -> Node:
    return {
        "format_version": "1.2", "terraform_version": "1.13.2", "variables": {},
        "resource_changes": [
            {"address": f"{kind}.{name}", "mode": "managed", "type": kind, "name": name,
             "provider_name": "registry.opentofu.org/hashicorp/aws",
             "change": {"actions": actions, "before": None, "after": after, "after_unknown": {}}}
            for kind, name, actions, after in entries
        ],
    }


def _run_plan(conftest: Conftest, directory: Path, plan: Node, name: str = "plano.json") -> Any:
    target = directory / name
    target.write_text(json.dumps(plan), encoding="utf-8")
    code, report = conftest(target)
    failures = sorted(item["msg"] for entry in report for item in entry.get("failures") or [])
    return code, failures


def _guarded_types() -> set[str]:
    guarded: set[str] = set()
    for name in _git_files("infra/opentofu/modules/*.tf"):
        current = ""
        for line in (REPO_ROOT / name).read_text(encoding="utf-8").splitlines():
            if match := RESOURCE.match(line):
                current = match[1]
            elif GUARD.match(line):
                guarded.add(current)
    return guarded


def test_todo_tipo_protegido_nos_modulos_falha_delete_e_replacement(
    conftest: Conftest, tmp_path: Path
) -> None:
    guarded = _guarded_types()
    assert {"aws_s3_bucket", "aws_dynamodb_table", "aws_cognito_user_pool"} <= guarded
    variants = {
        "remocao": ["delete"],
        "troca": ["delete", "create"],
        "troca_invertida": ["create", "delete"],
    }
    protected = sorted(guarded | set(PROTECTED_EXTRA))
    entries = [
        (kind, label, actions, {}) for kind in protected for label, actions in variants.items()
    ]
    entries.append(("aws_sns_topic", "removivel", ["delete"], {}))
    code, failures = _run_plan(conftest, tmp_path, _plan(*entries))
    expected = sorted(
        f"protected_change type={kind} address={kind}.{label} actions={','.join(actions)}"
        for kind in protected for label, actions in variants.items()
    )
    assert (code, failures) == (1, expected)


@pytest.mark.parametrize("prefix", ["AK", "AS"])
def test_valores_com_padrao_de_segredo_falham_sem_literal_no_repositorio(
    conftest: Conftest, tmp_path: Path, prefix: str
) -> None:
    key = prefix + "IA" + "0123456789ABCDEF"
    pem = "-----BEGIN " + "RSA PRIVATE KEY" + "-----\nexemplo\n-----END " + "RSA PRIVATE KEY-----"
    plan = _plan(
        ("aws_lambda_function", "chave", ["create"], {"description": f"usa {key} aqui"}),
        ("aws_lambda_function", "pem", ["create"], {"description": pem}),
        ("aws_lambda_function", "limpo", ["create"], {"description": "AKIA curto"}),
    )
    code, failures = _run_plan(conftest, tmp_path, plan)
    assert (code, failures) == (1, [
        "secret_value reason=aws_access_key type=aws_lambda_function "
        "address=aws_lambda_function.chave",
        "secret_value reason=private_key type=aws_lambda_function address=aws_lambda_function.pem",
    ])


def test_fixtures_de_rejeicao_falham_a_politica(conftest: Conftest, scheduled_plan: Node,
                                                tmp_path: Path) -> None:
    rejected = sorted((FIXTURES / "rejected").glob("*.json"))
    assert {path.name for path in rejected} == set(REJECTIONS)
    assert all((FIXTURES / "rejected" / name).is_file() for name in ISSUE_FIXTURES)
    code, report = conftest(*rejected)
    assert code == 1
    grouped = {Path(entry["filename"]).name: sorted(
        item["msg"] for item in entry.get("failures") or []) for entry in report}
    assert grouped == {name: sorted(REJECTIONS[name]) for name in REJECTIONS}
    accepted = sorted((FIXTURES / "accepted").glob("*.json"))
    assert len(accepted) >= 3
    assert conftest(*accepted)[0] == 0
    assert _run_plan(conftest, tmp_path, scheduled_plan) == (0, [])


def test_ignore_changes_fora_da_allowlist_falha_e_os_tf_reais_passam(
    conftest: Conftest, tmp_path: Path
) -> None:
    code, report = conftest(FIXTURES / "rejected" / "ignore_changes_em_roteamento.tf")
    assert code == 1
    assert [item["msg"] for entry in report for item in entry["failures"]] == [
        "ignore_changes_forbidden type=aws_scheduler_schedule name=recovery"
    ]
    source = tmp_path / "frouxo.tf"
    source.write_text(
        'resource "aws_athena_workgroup" "tudo" {\n  lifecycle {\n'
        "    ignore_changes = all\n  }\n}\n"
        'resource "aws_athena_workgroup" "mais" {\n  lifecycle {\n'
        "    ignore_changes = [state, tags]\n  }\n}\n",
        encoding="utf-8",
    )
    _, report = conftest(source)
    assert sorted(item["msg"] for entry in report for item in entry["failures"]) == [
        "ignore_changes_forbidden type=aws_athena_workgroup name=mais",
        "ignore_changes_forbidden type=aws_athena_workgroup name=tudo",
    ]
    real = [str(REPO_ROOT / name) for name in _git_files("infra/opentofu/*.tf")]
    assert len(real) > 30
    code, report = conftest(*real)
    assert (code, [i for e in report for i in e.get("failures") or []]) == (0, [])


def test_nenhum_segredo_ou_state_versionado() -> None:
    tracked = _git_files()
    offenders = [name for name in tracked if STATE_PATH.search(name)]
    assert not offenders, f"state_or_key_tracked paths={offenders}"
    findings: list[str] = []
    for name in _git_files(*SURFACE):
        text = (REPO_ROOT / name).read_text(encoding="utf-8", errors="ignore")
        if PRIVATE_KEY.search(text) or ACCESS_KEY.search(text) or STATE_CONTENT.search(text):
            findings.append(f"secret_or_state_content path={name}")
        findings += [
            f"real_account_id path={name}" for match in ACCOUNT_ID.finditer(text)
            if (match[1] or match[2]) not in EXAMPLE_ACCOUNTS
        ]
    assert not findings, findings
    assert "infra/opentofu/env/prod/.terraform.lock.hcl" in tracked


def _hcl_value(raw: str) -> Any:
    if raw in {"true", "false"}:
        return raw == "true"
    return json.loads(raw) if re.fullmatch(r'".*"|\d+', raw) else raw


def _hcl_object(lines: list[str], start: int) -> tuple[Node, int]:
    node: Node = {}
    index = start
    while index < len(lines):
        line = lines[index].strip()
        index += 1
        key, _, value = (part.strip() for part in line.partition("="))
        if line == "}":
            return node, index
        if value == "{":
            node[key], index = _hcl_object(lines, index)
        elif line:
            node[key] = _hcl_value(value)
    return node, index


def _hcl_blocks(text: str) -> list[tuple[str, Node]]:
    lines, blocks, index = text.splitlines(), [], 0
    while index < len(lines):
        header = lines[index].strip()
        index += 1
        if header.endswith("{") and "=" not in header:
            body, index = _hcl_object(lines, index)
            blocks.append((header[:-1].strip(), body))
    return blocks


def test_tftest_usa_mocks_e_valores_iguais_as_constantes_do_harness() -> None:
    text = TFTEST.read_text(encoding="utf-8")
    assert MOCK_AWS_PROVIDERS in text
    assert text.count("mock_provider ") == 2
    assert re.findall(r"command\s*=\s*(\w+)", text) == ["plan"] * text.count('\nrun "')
    assert not re.search(
        r"^\s*(access_key|secret_key|profile|assume_role|token)\s*=", text, re.MULTILINE
    )
    head = text[text.index("override_resource"):text.index('\nrun "')]
    blocks = _hcl_blocks(head)
    overrides = {
        body["target"]: body["values"] for name, body in blocks if name == "override_resource"
    }
    expected: dict[str, Node] = {}
    for address, suffix, _ in TARGETS:
        kind = address.split(".")[2]
        name = f"{ROOT_FIXTURE_CONTRACT['name_prefix']}-{suffix}"
        values = _seed_values(kind, name, ROOT_FIXTURE_CONTRACT)
        if kind == "aws_s3_bucket":
            values = values | {"bucket_regional_domain_name": BUCKET_REGIONAL_DOMAIN_NAME}
        expected[address] = values
    assert overrides == expected
    assert [body for name, body in blocks if name == "variables"] == [
        {"shared_outputs": VALID_SHARED_OUTPUTS}
    ]


@pytest.fixture(name="tftest", scope="module")
def fixture_tftest(
    tofu_context: TofuContext, tmp_path_factory: pytest.TempPathFactory
) -> subprocess.CompletedProcess[str]:
    environment = dict(tofu_context.environment, TF_DATA_DIR=str(tmp_path_factory.mktemp("tftest")))

    def tofu(*arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(  # noqa: S603
            [tofu_context.executable, f"-chdir={PROD_ROOT}", *arguments],
            check=False, capture_output=True, env=environment, text=True,
        )

    init = tofu("init", "-backend=false", "-input=false", "-lockfile=readonly",
                f"-test-directory={TEST_DIRECTORY}")
    assert init.returncode == 0, f"tofu_init_failed stderr={init.stderr!r}"
    return tofu("test", "-no-color", "-json", f"-test-directory={TEST_DIRECTORY}")


def test_tofu_test_do_gate_passa_todas_as_runs_sem_skip(
    tftest: subprocess.CompletedProcess[str],
) -> None:
    assert tftest.returncode == 0, tftest.stdout[-3000:]
    events = [json.loads(line) for line in tftest.stdout.splitlines() if line.startswith("{")]
    statuses = {
        event["test_run"]["run"]: event["test_run"]["status"]
        for event in events
        if event["type"] == "test_run"
    }
    declared = set(re.findall(r'^run "(\w+)"', TFTEST.read_text(encoding="utf-8"), re.MULTILINE))
    assert REQUIRED_RUNS <= declared == set(statuses)
    assert set(statuses.values()) == {"pass"}
    summary = next(event["test_summary"] for event in events if event["type"] == "test_summary")
    assert (summary["passed"], summary["failed"], summary["errored"], summary["skipped"]) == (
        len(declared), 0, 0, 0,
    )


def test_formato_do_opentofu_esta_limpo(tofu_context: TofuContext) -> None:
    command = [tofu_context.executable, f"-chdir={REPO_ROOT / 'infra/opentofu'}", "fmt"]
    done = subprocess.run(  # noqa: S603
        [*command, "-check", "-recursive"],
        check=False, capture_output=True, env=tofu_context.environment, text=True,
    )
    assert done.returncode == 0, f"tofu_fmt_dirty files={done.stdout!r}"
