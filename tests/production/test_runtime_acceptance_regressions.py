"""Regressões independentes de inventário, valores URL e JUnit ancestral."""

from __future__ import annotations

import ast
import importlib.util
import sys
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable

_ROOT = Path(__file__).resolve().parents[2]
_API = "apps/central_api/src/central_api/"
_INFRA = "packages/cnes_infra/src/cnes_infra/"
_WEB = "apps/web_dashboard/src/api/"
_SERVE, _MIDDLEWARE = _API + "routes/serving.py", _API + "middleware.py"
_EXECUTOR = _INFRA + "executor/step_functions.py"
_CLIENT, _ME, _LEADS = _WEB + "client.ts", _WEB + "hooks/useMe.ts", _WEB + "marketingLeads.ts"
_RAW, _GENERATED = _INFRA + "aws/raw_aws_runtime.py", _WEB + "generated.ts"
_CLONES = {_RAW: _INFRA + "aws/runtime.py", _GENERATED: _CLIENT}
_FAMILIES = {
    "control_plane_vazio": _INFRA + "control_plane/dynamodb*.py",
    "billing_vazio": _INFRA + "billing/dynamodb*.py",
    "auth_vazio": _INFRA + "auth/dynamodb*.py",
    "leads_vazio": _INFRA + "public_leads/dynamodb*.py",
    "executor_ausente": _EXECUTOR,
}
_GLOBS = (
    _WEB + "**/*.ts",
    _API + "**/*.py",
    *_FAMILIES.values(),
    _INFRA + "aws/**/*.py",
    "apps/data_processor/src/data_processor/aws_*.py",
)
_PUBLIC = {
    "url": ("url_log_violations", "url-in-log"),
    "api": ("relative_api_violations", "relative-production-api"),
    "key": ("static_aws_key_violations", "static-aws-key"),
}
_RETURN = "    return JSONResponse(envelope.model_dump(), headers=headers)"
_ME_CALL = 'apiFetch<Me>("/dashboard/auth/me",'
_CLIENT_CALL = '            "dynamodb", region_name=settings.region,'
_ASSIGN = "        self._client = client"
_PROVIDER = '        import boto3\n        self._client = boto3.client("dynamodb")'
_LEAD_ANCHOR = "        try:\n            response = self.client.get_item("
_LEAD_PROVIDER = (
    '        try:\n            import boto3\n            client = boto3.client("dynamodb")\n'
    "            response = client.get_item("
)
_STATE_ARN = "        self._state_machine_arn = _require_state_machine_arn(state_machine_arn)"
_EXECUTOR_ANCHOR = '        self._client = cast("_StepFunctionsClient", client)\n' + _STATE_ARN
_EXECUTOR_PROVIDER = _PROVIDER.replace('"dynamodb"', '"stepfunctions"') + "\n" + _STATE_ARN
type Recipe = tuple[str, str, str, str, str | None]


def _with_key(provider: str) -> str:
    assert provider.count('")') == 1
    return provider.replace('")', '", aws_access_key_id="test")', 1)


_MUTANTS = {
    "log_url_identificador": (
        "url",
        _SERVE,
        _RETURN,
        "    import logging\n    url = grant.url\n"
        '    logging.getLogger(__name__).info("url=%s", url)\n' + _RETURN,
        None,
    ),
    "api_fetch_hook": ("api", _ME, _ME_CALL, 'fetch("/api/v1/dashboard/auth/me",', None),
    "api_fetch_modulo": (
        "api",
        _LEADS,
        "apiFetch(LEADS_PATH,",
        'fetch("/api/v1/public/leads",',
        None,
    ),
    "chave_control_plane": (
        "key",
        _INFRA + "control_plane/dynamodb_adapter.py",
        _ASSIGN,
        _with_key(_PROVIDER),
        _PROVIDER,
    ),
    "chave_billing": (
        "key",
        _INFRA + "billing/dynamodb_quota.py",
        _ASSIGN,
        _with_key(_PROVIDER),
        _PROVIDER,
    ),
    "chave_auth": (
        "key",
        _INFRA + "auth/dynamodb_memberships.py",
        _ASSIGN,
        _with_key(_PROVIDER),
        _PROVIDER,
    ),
    "chave_public_leads": (
        "key",
        _INFRA + "public_leads/dynamodb.py",
        _LEAD_ANCHOR,
        _with_key(_LEAD_PROVIDER),
        _LEAD_PROVIDER,
    ),
    "chave_executor": (
        "key",
        _EXECUTOR,
        _EXECUTOR_ANCHOR,
        _with_key(_EXECUTOR_PROVIDER),
        _EXECUTOR_PROVIDER,
    ),
    "chave_basename_legado": (
        "key",
        _RAW,
        _CLIENT_CALL,
        _CLIENT_CALL + ' aws_access_key_id="test",',
        None,
    ),
}
_PATH_ANCHOR = "        path = request.url.path\n        if path.startswith"
_POSITIVES = {
    "log_identificador_path": (
        "url",
        _MIDDLEWARE,
        _PATH_ANCHOR,
        '        path = request.url.path\n        logger.info("path=%s", path)\n'
        "        if path.startswith",
        None,
    ),
    "api_fetch_absoluto_hook": (
        "api",
        _ME,
        _ME_CALL,
        'fetch("https://api.cnesdata.com.br/api/v1/dashboard/auth/me",',
        None,
    ),
    "api_generated_excluido": (
        "api",
        _GENERATED,
        "fetch(url.href,",
        'fetch("/api/v1/dashboard/auth/me",',
        None,
    ),
}
_LEAF = '<testsuite tests="1" skipped="0" failures="0" errors="0"><testcase/></testsuite>'
_PARENT = '<testsuite tests="2" skipped="0" failures="0" errors="0">'
_COUNTER_PARENT = _PARENT.replace('tests="2"', 'tests="1"')
_NEGATIVE_XML = {
    "contador_skipped": _COUNTER_PARENT.replace('skipped="0"', 'skipped="1"')
    + _LEAF
    + "</testsuite>",
    "contador_failures": _COUNTER_PARENT.replace('failures="0"', 'failures="1"')
    + _LEAF
    + "</testsuite>",
    "contador_errors": _COUNTER_PARENT.replace('errors="0"', 'errors="1"') + _LEAF + "</testsuite>",
    "contador_divergente": (
        '<testsuite tests="2" skipped="0" failures="0" errors="0">'
        '<testcase name="only"/></testsuite>'
    ),
    "raiz_contador_skipped": "<testsuites tests=\"1\" skipped=\"1\" failures=\"0\" errors=\"0\">"
    + _LEAF
    + "</testsuites>",
    "raiz_contador_failures": "<testsuites tests=\"1\" skipped=\"0\" failures=\"1\" errors=\"0\">"
    + _LEAF
    + "</testsuites>",
    "intermedio_contador_tests": (
        '<testsuites><testsuites tests="2" skipped="0" failures="0" errors="0">'
        + _LEAF
        + "</testsuites></testsuites>"
    ),
    "intermedio_contador_skipped": (
        '<testsuites><testsuites tests="1" skipped="1" failures="0" errors="0">'
        + _LEAF
        + "</testsuites></testsuites>"
    ),
    "intermedio_contador_failures": (
        '<testsuites><testsuites tests="1" skipped="0" failures="1" errors="0">'
        + _LEAF
        + "</testsuites></testsuites>"
    ),
    "caso_skipped": _PARENT + "<testcase><skipped/></testcase>" + _LEAF + "</testsuite>",
    "caso_failure": _PARENT + "<testcase><failure/></testcase>" + _LEAF + "</testsuite>",
    "caso_error": _PARENT + "<testcase><error/></testcase>" + _LEAF + "</testsuite>",
}
_POSITIVE_XML = {
    "pai_com_somente_filha": _PARENT.replace('tests="2"', 'tests="1"') + _LEAF + "</testsuite>",
    "pai_com_caso_e_filha": _PARENT + "<testcase/>" + _LEAF + "</testsuite>",
}


def _copy(root: Path) -> dict[str, bytes]:
    paths = {_ROOT / "apps/web_dashboard/src/lib/env.ts", _ROOT / _EXECUTOR}
    for pattern in _GLOBS:
        matches = set(_ROOT.glob(pattern))
        assert matches, f"copy_glob_empty={pattern}"
        paths.update(path for path in matches if path.name != "generated.ts")
    originals = {}
    for path in sorted(paths):
        assert path.is_file()
        name = path.relative_to(_ROOT).as_posix()
        originals[name] = path.read_bytes()
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(originals[name])
    return originals


def _hashes(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): sha256(path.read_bytes()).hexdigest()
        for path in root.rglob("*")
        if path.is_file()
    }


def _restore(root: Path, originals: dict[str, bytes]) -> str:
    for path in root.rglob("*"):
        if path.is_file() and path.relative_to(root).as_posix() not in originals:
            path.unlink()
    for name, data in originals.items():
        (root / name).write_bytes(data)
    expected = {name: sha256(data).hexdigest() for name, data in originals.items()}
    assert _hashes(root) == expected
    return sha256(repr(sorted(expected.items())).encode()).hexdigest()


def _load(name: str) -> Callable[[Path], Any]:
    path = _ROOT / "tests/production/test_runtime_acceptance.py"
    assert path.is_file(), "runtime_acceptance_gate_missing"
    spec = importlib.util.spec_from_file_location("runtime377_regression_gate", path)
    assert spec is not None
    assert spec.loader is not None
    gate = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = gate
    spec.loader.exec_module(gate)
    function = getattr(gate, name, None)
    assert callable(function), f"runtime_public_callable_missing={name}"
    return function


def _changed(text: str, anchor: str, replacement: str, name: str) -> bytes:
    assert text.count(anchor) == 1, f"anchor_count={text.count(anchor)} path={name}"
    changed = text.replace(anchor, replacement, 1)
    if name.endswith(".py"):
        ast.parse(changed, filename=name)
    return changed.encode()


def _findings(result: Any, rule: str, path: str) -> None:
    assert isinstance(result, tuple)
    assert result, f"source_violation_missing={rule} path={path}"
    keys = [(item.rule, item.path, item.line) for item in result]
    assert {(item.rule, item.path) for item in result} == {(rule, path)}
    assert keys == sorted(set(keys))
    for item in result:
        assert isinstance(item.line, int)
        assert item.line >= 1
        assert not Path(item.path).is_absolute()
        assert ".." not in Path(item.path).parts
        assert "\\" not in item.path


def _exercise(root: Path, case: tuple[Recipe, bool], properties: list[tuple[str, str]]) -> None:
    originals = _copy(root)
    recipe, forbidden = case
    kind, name, anchor, replacement, provider = recipe
    text = (root / _CLONES.get(name, name)).read_text(encoding="utf-8")
    changed = _changed(text, anchor, replacement, name)
    safe = _changed(text, anchor, provider, name) if provider is not None else None
    before = _restore(root, originals)
    properties.append(("copy_before", before))
    function = _load(_PUBLIC[kind][0])
    try:
        if name in _CLONES:
            (root / name).write_text(text, encoding="utf-8")
        assert function(root) == ()
        properties.append(("baseline", "passed"))
        if safe is not None:
            (root / name).write_bytes(safe)
            assert function(root) == ()
            properties.append(("paired_provider_chain", "passed"))
        (root / name).write_bytes(changed)
        result = function(root)
        properties.append(("changed_findings", str(len(result))))
        if forbidden:
            _findings(result, _PUBLIC[kind][1], name)
        else:
            assert result == ()
    finally:
        try:
            if name == _RAW:
                (root / name).write_text(text, encoding="utf-8")
                assert function(root) == ()
                properties.append(("restored_safe_clone", "passed"))
        finally:
            after = _restore(root, originals)
            assert after == before
            assert function(root) == ()
            properties.extend((("copy_after", after), ("restoration", "passed")))


@pytest.mark.parametrize("case", _MUTANTS.values(), ids=list(_MUTANTS))
def test_rejeita_regressao_de_fonte(
    tmp_path: Path, case: Recipe, request: pytest.FixtureRequest
) -> None:
    _exercise(tmp_path, (case, True), request.node.user_properties)


@pytest.mark.parametrize("case", _FAMILIES.values(), ids=list(_FAMILIES))
def test_recusa_inventario_de_chaves(
    tmp_path: Path, case: str, request: pytest.FixtureRequest
) -> None:
    originals = _copy(tmp_path)
    targets = sorted(tmp_path.glob(case))
    assert targets
    before = _restore(tmp_path, originals)
    request.node.user_properties.append(("copy_before", before))
    function = _load("static_aws_key_violations")
    assert function(tmp_path) == ()
    request.node.user_properties.append(("baseline", "passed"))
    try:
        for target in targets:
            target.unlink()
        with pytest.raises((OSError, ValueError)):
            function(tmp_path)
    finally:
        after = _restore(tmp_path, originals)
        assert after == before
        assert function(tmp_path) == ()
        request.node.user_properties.extend((("copy_after", after), ("restoration", "passed")))


@pytest.mark.parametrize("case", _POSITIVES.values(), ids=list(_POSITIVES))
def test_aceita_fonte_segura_de_regressao(
    tmp_path: Path, case: Recipe, request: pytest.FixtureRequest
) -> None:
    _exercise(tmp_path, (case, False), request.node.user_properties)


@pytest.mark.parametrize("xml", _NEGATIVE_XML.values(), ids=list(_NEGATIVE_XML))
def test_recusa_junit_negativo_em_pai(tmp_path: Path, xml: str) -> None:
    path = tmp_path / "junit.xml"
    path.write_text(xml, encoding="utf-8")
    function = _load("require_complete_junit")
    with pytest.raises(ValueError):
        function(path)


@pytest.mark.parametrize("xml", _POSITIVE_XML.values(), ids=list(_POSITIVE_XML))
def test_aceita_junit_pai_verde(tmp_path: Path, xml: str) -> None:
    path = tmp_path / "junit.xml"
    path.write_text(xml, encoding="utf-8")
    function = _load("require_complete_junit")
    assert function(path) is None
