"""Contratos independentes do aceite de fonte e completude JUnit."""

from __future__ import annotations

import ast
import importlib.util
import sys
from dataclasses import fields, is_dataclass
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any
from xml.etree.ElementTree import ParseError

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable

_ROOT = Path(__file__).resolve().parents[2]
_API = "apps/central_api/src/central_api/"
_INFRA = "packages/cnes_infra/src/cnes_infra/"
ENV = "apps/web_dashboard/src/lib/env.ts"
CLIENT = "apps/web_dashboard/src/api/client.ts"
ACTIVATE = "apps/web_dashboard/src/api/hooks/useActivate.ts"
APP, OAUTH = _API + "app.py", _API + "routes/oauth.py"
SERVE, MIDDLEWARE = _API + "routes/serving.py", _API + "middleware.py"
EXECUTOR, SETTINGS = _INFRA + "executor/step_functions.py", _INFRA + "aws/settings.py"
CLIENTS, DYNAMO = _INFRA + "aws/runtime.py", _INFRA + "control_plane/dynamodb_publication.py"
ADMIT = "packages/cnes_domain/src/cnes_domain/orchestration/dispatch_admission.py"
NETWORK, SIGNED = "infra/opentofu/modules/network/main.tf", _API + "serving/aws_signed.py"
NEW = _INFRA + "control_plane/dynamodb_runtime377_probe.py"
BASE = 'const PRODUCTION_API_BASE_URL = "https://api.cnesdata.com.br/api/v1";'
GUARD = "    if (stripTrailingSlash(value.VITE_API_BASE_URL) !== PRODUCTION_API_BASE_URL) {"
RETURN = "    return JSONResponse(envelope.model_dump(), headers=headers)"
PATH = "        path = request.url.path\n        if path.startswith"
PROD = '_PRODUCTION_ASSIGN_PUBLIC_IP = "ENABLED"'
CHECK = '    if network.get("AssignPublicIp") != _PRODUCTION_ASSIGN_PUBLIC_IP:'
QUERY = "        response = self._client.query(**request)"
ACQUIRE = "        command = AcquireUnitPermit(self._environment, holder, now, self._lease_seconds)"
ATTEMPT = "        command = ConsumeExecutionAttempt("
ERROR_LOG = (
    '            logger.warning("serving_authorization_lookup_failed error=%s", '
    "type(error).__name__)"
)
_RULES = (
    ("relative_api_violations", "relative-production-api"),
    ("activation_origin_violations", "activation-at-origin"),
    ("url_log_violations", "url-in-log"),
    ("serving_redirect_violations", "serving-307"),
    ("production_public_ip_violations", "production-public-ip-disabled"),
    ("network_resource_violations", "nat-or-alb"),
    ("static_aws_key_violations", "static-aws-key"),
    ("adapter_scan_violations", "adapter-scan"),
    ("task_hours_start_violations", "task-hours-start"),
)
_GLOBS = (
    "apps/web_dashboard/src/api/**/*.ts",
    _API + "**/*.py",
    "infra/opentofu/env/prod/**/*.tf",
    "infra/opentofu/modules/**/*.tf",
    _INFRA + "control_plane/dynamodb*.py",
    _INFRA + "billing/dynamodb*.py",
    _INFRA + "auth/dynamodb*.py",
    _INFRA + "public_leads/dynamodb*.py",
    _INFRA + "aws/**/*.py",
    _API + "serving/**/*.py",
    "apps/data_processor/src/data_processor/aws_*.py",
)
_FILES = (
    ENV,
    CLIENT,
    ACTIVATE,
    APP,
    OAUTH,
    SERVE,
    EXECUTOR,
    ADMIT,
    _API + "auth/aws_oidc.py",
    _API + "raw_aws_runtime.py",
    _INFRA + "control_plane/dynamodb_execution_quota.py",
    "packages/cnes_domain/src/cnes_domain/control_plane/execution_quota.py",
    _API + "services/run_planning.py",
    "apps/data_processor/src/data_processor/orchestration/coordinator.py",
)
type Recipe = tuple[int, str, str, str]


def _log(expression: str) -> str:
    return "    import logging\n    logging.getLogger(__name__).info(" + expression + ")\n" + RETURN


def _hours(attribute: str, helper: bool = False) -> str:
    variable, anchor = ("permit", ATTEMPT) if helper else ("holder", ACQUIRE)
    return (
        f'        if getattr({variable}, "{attribute}", 0) >= 1:\n'
        '            raise RuntimeError("task_hours_exhausted")\n' + anchor
    )


def _response(status: str) -> str:
    return RETURN[:-1] + ", status_code=" + status + ")"


def _resource(kind: str) -> str:
    attributes = ' load_balancer_type = "application"\n subnets = ["subnet-test"]\n'
    if kind == "aws_nat_gateway":
        attributes = ' allocation_id = "eipalloc-test"\n subnet_id = "subnet-test"\n'
    if kind == "aws_elb":
        attributes = ' name = "test"\n subnets = ["subnet-test"]\n'
    return '\nresource "' + kind + '" "probe" {\n' + attributes + "}\n"


_MUTANTS = {
    "api_base_relativa": (0, ENV, BASE, 'const PRODUCTION_API_BASE_URL = "/api/v1";'),
    "api_fetch_relativo": (0, CLIENT, "fetch(url.href,", 'fetch("/api/v1/dashboard/auth/me",'),
    "api_guarda_removida": (0, ENV, GUARD, "    if (false) {"),
    "ativacao_fetch_na_origem": (
        1,
        ACTIVATE,
        'apiFetch<unknown>("/activate/confirm",',
        'fetch("/activate/confirm",',
    ),
    "ativacao_prefixo_removido": (
        1,
        APP,
        'app.include_router(oauth.activation_router, prefix="/api/v1")',
        "app.include_router(oauth.activation_router)",
    ),
    "ativacao_router_raiz": (
        1,
        OAUTH,
        '@activation_router.post("/activate/confirm", response_model=ActivateConfirmResponse)',
        '@router.post("/activate/confirm", response_model=ActivateConfirmResponse)',
    ),
    "log_url_argumento": (2, SERVE, RETURN, _log('"url=%s", grant.url')),
    "log_url_fstring": (2, SERVE, RETURN, _log('f"url={grant.url}"')),
    "log_url_request": (2, MIDDLEWARE, PATH, '        logger.info("url=%s", request.url)\n' + PATH),
    "log_url_format": (2, SERVE, RETURN, _log('"url={}".format(grant.url)')),
    "log_url_percentual": (2, SERVE, RETURN, _log('"url=%s" % grant.url')),
    "log_url_extra": (2, SERVE, RETURN, _log('"serving", extra={"url": grant.url}')),
    "serving_status_307": (3, SERVE, RETURN, _response("307")),
    "serving_redirect_padrao": (
        3,
        SERVE,
        RETURN,
        "    from starlette.responses import RedirectResponse\n"
        "    return RedirectResponse(envelope.url)",
    ),
    "serving_constante_307": (
        3,
        SERVE,
        RETURN,
        "    from starlette.status import HTTP_307_TEMPORARY_REDIRECT\n"
        + _response("HTTP_307_TEMPORARY_REDIRECT"),
    ),
    "ip_producao_desabilitado": (4, EXECUTOR, PROD, '_PRODUCTION_ASSIGN_PUBLIC_IP = "DISABLED"'),
    "ip_producao_usa_generico": (
        4,
        EXECUTOR,
        CHECK,
        '    if network.get("AssignPublicIp") != _ASSIGN_PUBLIC_IP:',
    ),
    "rede_nat": (5, NETWORK, "", _resource("aws_nat_gateway")),
    "rede_alb": (5, NETWORK, "", _resource("aws_lb")),
    "rede_alias_alb": (5, NETWORK, "", _resource("aws_alb")),
    "rede_elb_classico": (5, NETWORK, "", _resource("aws_elb")),
    "chave_campo_settings": (
        6,
        SETTINGS,
        "class AwsRuntimeSettings:\n    region: str",
        "class AwsRuntimeSettings:\n    aws_access_key_id: str\n    region: str",
    ),
    "chave_argumento_cliente": (
        6,
        CLIENTS,
        '            "dynamodb", region_name=settings.region,',
        '            "dynamodb", region_name=settings.region, aws_secret_access_key="test",',
    ),
    "chave_leitura_mapping": (
        6,
        SETTINGS,
        "        profile = _profile(values)",
        '        aws_access_key_id = values.get("AWS_ACCESS_KEY_ID")\n'
        "        profile = _profile(values)",
    ),
    "scan_chamada_direta": (7, DYNAMO, QUERY, "        response = self._client.scan(**request)"),
    "scan_paginador": (
        7,
        DYNAMO,
        QUERY,
        '        response = self._client.get_paginator("scan").paginate(**request)',
    ),
    "scan_adapter_novo": (7, NEW, QUERY, "        response = self._client.scan(**request)"),
    "scan_api_call": (
        7,
        DYNAMO,
        QUERY,
        '        response = self._client._make_api_call("Scan", request)',
    ),
    "scan_getattr": (
        7,
        DYNAMO,
        QUERY,
        '        response = getattr(self._client, "scan")(**request)',
    ),
    "horas_admit": (8, ADMIT, ACQUIRE, _hours("task_hours")),
    "horas_helper": (8, ADMIT, ATTEMPT, _hours("task_hours", True)),
    "horas_singular": (8, ADMIT, ACQUIRE, _hours("task_hour")),
    "horas_camel": (8, ADMIT, ACQUIRE, _hours("taskHours")),
    "horas_hifen": (8, ADMIT, ACQUIRE, _hours("task-hours")),
}
SAFE_API = "const PRODUCTION_API_BASE_URL = 'https://api.cnesdata.com.br/api/v1' ;"
SAFE_PATH = '        logger.info("path=%s", request.url.path)\n' + PATH
SAFE_SERVING = "    # RedirectResponse status_code=307\n" + RETURN
SAFE_NETWORK = '\n# resource "aws_nat_gateway" "ignored" {}\n# aws_lb aws_alb aws_elb\n'
SAFE_KEYS = "\n# aws_access_key_id aws_secret_access_key AWS_ACCESS_KEY_ID\n"
SAFE_SCAN = '\n# .scan() ScanIndexForward ScanAttributes\n"Scan"\n'
SAFE_METRIC = '        logger.info("task_hours=%s", getattr(holder, "task_hours", 0))\n' + ACQUIRE
_POSITIVES = {
    "api_generica_relativa": (0, ENV, '.default("/api/v1")', ".default('/api/v1')"),
    "api_formatacao_segura": (0, ENV, BASE, SAFE_API),
    "ativacao_versionada_e_ui": (1, ACTIVATE, "", "\n// UI /activate; origin /oauth/token\n"),
    "log_somente_path": (2, MIDDLEWARE, PATH, SAFE_PATH),
    "log_somente_tipo_erro": (2, SIGNED, ERROR_LOG, ERROR_LOG + "\n" + ERROR_LOG),
    "serving_envelope_200": (3, SERVE, RETURN, _response("200")),
    "serving_comentario_307": (3, SERVE, RETURN, SAFE_SERVING),
    "ip_generico_disabled": (4, EXECUTOR, PROD, "_PRODUCTION_ASSIGN_PUBLIC_IP = 'ENABLED'"),
    "rede_comentarios_nat_alb": (5, NETWORK, "", SAFE_NETWORK),
    "chaves_provider_chain": (6, CLIENTS, "", SAFE_KEYS),
    "chaves_raw_legado": (6, _API + "raw_aws_runtime.py", "", ""),
    "scan_adapter_novo_query": (7, NEW, QUERY, QUERY),
    "scan_nomes_e_comentarios": (7, DYNAMO, "", SAFE_SCAN),
    "horas_somente_metrica": (8, ADMIT, ACQUIRE, SAFE_METRIC),
}
_INVENTORY = [
    pytest.param((0, ENV), id="ausente_env"),
    pytest.param((1, APP), id="ausente_ativacao_app"),
    pytest.param((3, SERVE), id="ausente_serving"),
    pytest.param((4, EXECUTOR), id="ausente_executor_ip"),
    pytest.param((6, _API + "auth/aws_oidc.py"), id="ausente_aws_oidc"),
    pytest.param((8, ADMIT), id="ausente_admissao"),
    pytest.param((7, EXECUTOR), id="ausente_executor_scan"),
    pytest.param((2, _API + "**/*.py"), id="vazio_logs_api"),
    pytest.param((5, "infra/opentofu/env/prod/**/*.tf"), id="vazio_prod_tf"),
    pytest.param((5, "infra/opentofu/modules/**/*.tf"), id="vazio_modules_tf"),
    pytest.param((7, _INFRA + "control_plane/dynamodb*.py"), id="vazio_adapters_control"),
    pytest.param((7, _INFRA + "billing/dynamodb*.py"), id="vazio_adapters_billing"),
    pytest.param((7, _INFRA + "auth/dynamodb*.py"), id="vazio_adapters_auth"),
    pytest.param((7, _INFRA + "public_leads/dynamodb*.py"), id="vazio_adapters_leads"),
    pytest.param((7, _INFRA + "aws/**/*.py"), id="vazio_aws"),
    pytest.param((2, MIDDLEWARE + "!parse"), id="python_invalido"),
]
_SUITE = (
    '<testsuite tests="1" skipped="0" failures="0" errors="0"><testcase name="ok"/></testsuite>'
)


def _junit_child(tag: str) -> str:
    return _SUITE.replace('<testcase name="ok"/>', f"<testcase><{tag}/></testcase>")


_JUNIT = {
    "um_caso_verde": (_SUITE, True),
    "suites_irmas_verdes": ("<testsuites>" + _SUITE * 2 + "</testsuites>", True),
    "suites_aninhadas_verdes": (
        "<testsuites><testsuites>" + _SUITE + "</testsuites></testsuites>",
        True,
    ),
    "arquivo_ausente": (None, False),
    "arquivo_vazio": ("", False),
    "xml_invalido": ("<testsuite", False),
    "sem_testcase": ('<testsuite tests="0" skipped="0" failures="0" errors="0"/>', False),
    "contador_sem_testcase": ('<testsuite tests="1" skipped="0" failures="0" errors="0"/>', False),
    "caso_skipped": (_junit_child("skipped"), False),
    "caso_failure": (_junit_child("failure"), False),
    "caso_error": (_junit_child("error"), False),
    "skipped_na_segunda_suite": (
        "<testsuites>" + _SUITE + _SUITE.replace('skipped="0"', 'skipped="1"') + "</testsuites>",
        False,
    ),
}


def _copy(root: Path) -> dict[str, bytes]:
    paths = {_ROOT / name for name in _FILES}
    for pattern in _GLOBS:
        matches = set(_ROOT.glob(pattern))
        assert matches, f"source_glob_empty={pattern}"
        paths.update(p for p in matches if p.name != "generated.ts")
    originals = {}
    for path in sorted(paths):
        assert path.is_file(), f"source_missing={path}"
        name = path.relative_to(_ROOT).as_posix()
        originals[name] = path.read_bytes()
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(originals[name])
    return originals


def _hashes(root: Path) -> dict[str, str]:
    return {
        p.relative_to(root).as_posix(): sha256(p.read_bytes()).hexdigest()
        for p in root.rglob("*")
        if p.is_file()
    }


def _restore(root: Path, originals: dict[str, bytes]) -> None:
    for path in root.rglob("*"):
        if path.is_file() and path.relative_to(root).as_posix() not in originals:
            path.unlink()
    for name, content in originals.items():
        (root / name).write_bytes(content)
    assert _hashes(root) == {name: sha256(data).hexdigest() for name, data in originals.items()}


def _callable(name: str) -> Callable[[Path], Any]:
    path = _ROOT / "tests/production/test_runtime_acceptance.py"
    assert path.is_file(), "runtime_acceptance_gate_missing"
    spec = importlib.util.spec_from_file_location("runtime377_gate_contract", path)
    assert spec is not None
    assert spec.loader is not None
    gate = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = gate
    spec.loader.exec_module(gate)
    record = getattr(gate, "SourceViolation", None)
    assert isinstance(record, type)
    assert is_dataclass(record)
    assert tuple(field.name for field in fields(record)) == ("rule", "path", "line")
    assert vars(record)["__dataclass_params__"].frozen
    function = getattr(gate, name, None)
    assert callable(function), f"runtime_public_callable_missing={name}"
    return function


def _findings(result: Any, rule: str, path: str) -> None:
    assert isinstance(result, tuple)
    assert result
    keys = [(item.rule, item.path, item.line) for item in result]
    assert {(item.rule, item.path) for item in result} == {(rule, path)}
    assert keys == sorted(set(keys))
    for item in result:
        assert isinstance(item.line, int)
        assert item.line >= 1
        assert not Path(item.path).is_absolute()
        assert ".." not in Path(item.path).parts
        assert "\\" not in item.path


def _prepare(root: Path, recipe: Recipe) -> bytes:
    _, name, anchor, replacement = recipe
    text = (root / (DYNAMO if name == NEW else name)).read_text(encoding="utf-8")
    if anchor:
        assert text.count(anchor) == 1, f"mutation_anchor_count={text.count(anchor)} path={name}"
        changed = text.replace(anchor, replacement, 1)
    else:
        changed = text + replacement
    if name.endswith(".py"):
        ast.parse(changed, filename=name)
    return changed.encode()


def _exercise(root: Path, recipe: Recipe, forbidden: bool) -> None:
    originals = _copy(root)
    changed = _prepare(root, recipe)
    index, name, _, _ = recipe
    function = _callable(_RULES[index][0])
    assert function(root) == ()
    try:
        (root / name).write_bytes(changed)
        result = function(root)
        if forbidden:
            _findings(result, _RULES[index][1], name)
        else:
            assert result == ()
    finally:
        _restore(root, originals)
    assert function(root) == ()


@pytest.mark.parametrize(
    "index",
    range(9),
    ids=[
        "api",
        "ativacao",
        "log_url",
        "serving",
        "ip_publico",
        "rede",
        "chaves",
        "scan",
        "task_hours",
    ],
)
def test_aceita_fonte_real_restaurada(tmp_path: Path, index: int) -> None:
    originals = _copy(tmp_path)
    function = _callable(_RULES[index][0])
    assert function(tmp_path) == ()
    _restore(tmp_path, originals)


@pytest.mark.parametrize("recipe", _MUTANTS.values(), ids=list(_MUTANTS))
def test_rejeita_mutacao_real_e_restaura(tmp_path: Path, recipe: Recipe) -> None:
    _exercise(tmp_path, recipe, True)


@pytest.mark.parametrize("recipe", _POSITIVES.values(), ids=list(_POSITIVES))
def test_aceita_controle_positivo_e_restaura(tmp_path: Path, recipe: Recipe) -> None:
    _exercise(tmp_path, recipe, False)


@pytest.mark.parametrize("case", _INVENTORY)
def test_recusa_inventario_incompleto(tmp_path: Path, case: tuple[int, str]) -> None:
    originals = _copy(tmp_path)
    index, pattern = case
    invalid = pattern.endswith("!parse")
    targets = [tmp_path / MIDDLEWARE] if invalid else sorted(tmp_path.glob(pattern))
    assert targets
    assert all(path.is_file() for path in targets)
    function = _callable(_RULES[index][0])
    assert function(tmp_path) == ()
    try:
        for path in targets:
            if invalid:
                path.write_bytes(path.read_bytes() + b"\ndef broken(:\n")
            else:
                path.unlink()
        with pytest.raises((OSError, ValueError, SyntaxError)):
            function(tmp_path)
    finally:
        _restore(tmp_path, originals)
    assert function(tmp_path) == ()


@pytest.mark.parametrize(("xml", "valid"), _JUNIT.values(), ids=list(_JUNIT))
def test_junit_completo(tmp_path: Path, xml: str | None, valid: bool) -> None:
    report = tmp_path / "report.xml"
    if xml is not None:
        report.write_text(xml, encoding="utf-8")
    function = _callable("require_complete_junit")
    if valid:
        assert function(report) is None
    else:
        with pytest.raises((OSError, ValueError, ParseError)):
            function(report)


_ORACLE_IDS = (
    "registro_correto",
    "resultado_vazio",
    "regra_errada",
    "path_errado",
    "linha_zero",
    "registro_duplicado",
    "ordem_invertida",
)


@pytest.mark.parametrize("case", _ORACLE_IDS, ids=_ORACLE_IDS)
def test_oraculo_de_violacoes(case: str) -> None:
    correct = SimpleNamespace(rule="adapter-scan", path=DYNAMO, line=1)
    cases = {
        "registro_correto": (correct,),
        "resultado_vazio": (),
        "regra_errada": (SimpleNamespace(rule="wrong", path=DYNAMO, line=1),),
        "path_errado": (SimpleNamespace(rule="adapter-scan", path=ENV, line=1),),
        "linha_zero": (SimpleNamespace(rule="adapter-scan", path=DYNAMO, line=0),),
        "registro_duplicado": (correct, correct),
        "ordem_invertida": (SimpleNamespace(rule="adapter-scan", path=DYNAMO, line=2), correct),
    }
    if case == "registro_correto":
        _findings(cases[case], "adapter-scan", DYNAMO)
    else:
        with pytest.raises(AssertionError):
            _findings(cases[case], "adapter-scan", DYNAMO)
