"""Validadores bounded do aceite de runtime e completude dos relatórios JUnit."""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING
from xml.etree import ElementTree

if TYPE_CHECKING:
    from pathlib import Path


@dataclass(frozen=True, slots=True)
class SourceViolation:
    rule: str
    path: str
    line: int


_API = "apps/central_api/src/central_api/"
_INFRA = "packages/cnes_infra/src/cnes_infra/"
_RULES = {
    "relative": "relative-production-api",
    "activation": "activation-at-origin",
    "url": "url-in-log",
    "serving": "serving-307",
    "ip": "production-public-ip-disabled",
    "network": "nat-or-alb",
    "keys": "static-aws-key",
    "scan": "adapter-scan",
    "hours": "task-hours-start",
}
_KEY_NAMES = {
    "access_key_id",
    "secret_access_key",
    "aws_access_key_id",
    "aws_secret_access_key",
    "session_token",
    "aws_session_token",
}
_AWS_ENV_NAMES = {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"}
_LOG_LEVELS = {"debug", "error", "exception", "info", "warning", "critical"}


def _text(root: Path, relative: str, python: bool = False) -> str:
    path = root / relative
    if not path.is_file():
        raise OSError(f"source_missing={relative}")
    value = path.read_text(encoding="utf-8")
    if python:
        ast.parse(value, filename=relative)
    return value


def _files(root: Path, pattern: str, exclude: set[str] | None = None) -> list[Path]:
    paths = sorted(path for path in root.glob(pattern) if path.is_file())
    if exclude:
        paths = [path for path in paths if path.name not in exclude]
    if not paths:
        raise OSError(f"source_glob_empty={pattern}")
    return paths


def _ast(root: Path, path: Path) -> ast.Module:
    return ast.parse(
        _text(root, path.relative_to(root).as_posix(), python=True), filename=str(path)
    )


def _calls(tree: ast.AST, names: set[str]) -> list[ast.Call]:
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in names
    ]


def relative_api_violations(root: Path) -> tuple[SourceViolation, ...]:
    env = _text(root, "apps/web_dashboard/src/lib/env.ts")
    client = _text(root, "apps/web_dashboard/src/api/client.ts")
    _files(root, "apps/web_dashboard/src/api/**/*.ts", {"generated.ts"})
    findings: list[SourceViolation] = []
    base = re.search(r"PRODUCTION_API_BASE_URL\s*=\s*([\"'])(.*?)\1", env)
    if base is None or not base.group(2).startswith("https://"):
        line = env[: base.start() if base else 0].count("\n") + 1
        findings.append(
            SourceViolation(_RULES["relative"], "apps/web_dashboard/src/lib/env.ts", line)
        )
    guard = "stripTrailingSlash(value.VITE_API_BASE_URL) !== PRODUCTION_API_BASE_URL"
    if guard not in env:
        findings.append(SourceViolation(_RULES["relative"], "apps/web_dashboard/src/lib/env.ts", 1))
    findings.extend(
        SourceViolation(
            _RULES["relative"],
            "apps/web_dashboard/src/api/client.ts",
            client[: match.start()].count("\n") + 1,
        )
        for match in re.finditer(r"\bfetch\s*\(\s*([\"'])/api(?:/|\1)", client)
    )
    return tuple(sorted(set(findings), key=lambda item: (item.path, item.line)))


def activation_origin_violations(root: Path) -> tuple[SourceViolation, ...]:
    activate = _text(root, "apps/web_dashboard/src/api/hooks/useActivate.ts")
    app = _text(root, _API + "app.py", python=True)
    oauth = _text(root, _API + "routes/oauth.py", python=True)
    findings: list[SourceViolation] = []
    if not re.search(r"\bapiFetch\s*<[^>]+>\s*\(\s*[\"']/activate/confirm", activate):
        findings.append(
            SourceViolation(
                _RULES["activation"], "apps/web_dashboard/src/api/hooks/useActivate.ts", 1
            )
        )
    if not re.search(
        r"include_router\(\s*oauth\.activation_router\s*,\s*prefix\s*=\s*[\"']/api/v1", app
    ):
        findings.append(SourceViolation(_RULES["activation"], _API + "app.py", 1))
    if re.search(r"@router\.post\(\s*[\"']/activate/confirm", oauth):
        line = oauth[: oauth.index("@router.post")].count("\n") + 1
        findings.append(SourceViolation(_RULES["activation"], _API + "routes/oauth.py", line))
    return tuple(sorted(set(findings), key=lambda item: (item.path, item.line)))


def _url_value(node: ast.AST) -> bool:  # noqa: PLR0911
    if isinstance(node, ast.Attribute):
        if node.attr == "path":
            return False
        return node.attr == "url" or _url_value(node.value)
    if isinstance(node, ast.JoinedStr):
        return any(
            _url_value(value.value)
            for value in node.values
            if isinstance(value, ast.FormattedValue)
        )
    if isinstance(node, ast.Call):
        return any(_url_value(arg) for arg in (*node.args, *node.keywords))
    if isinstance(node, ast.keyword):
        return _url_value(node.value)
    if isinstance(node, ast.BinOp):
        return _url_value(node.left) or _url_value(node.right)
    if isinstance(node, ast.Dict):
        return any(_url_value(value) for value in node.values if value is not None)
    return False


def _is_logger(call: ast.Call) -> bool:
    if not isinstance(call.func, ast.Attribute):
        return False
    value = call.func.value
    return (isinstance(value, ast.Name) and value.id == "logger") or isinstance(value, ast.Call)


def url_log_violations(root: Path) -> tuple[SourceViolation, ...]:
    paths = _files(root, _API + "**/*.py")
    findings: list[SourceViolation] = []
    for path in paths:
        tree = _ast(root, path)
        for call in _calls(tree, _LOG_LEVELS):
            args = (
                call.args[1:] if call.args and isinstance(call.args[0], ast.Constant) else call.args
            )
            if _is_logger(call) and any(_url_value(node) for node in (*args, *call.keywords)):
                findings.append(
                    SourceViolation(_RULES["url"], path.relative_to(root).as_posix(), call.lineno)
                )
    return tuple(sorted(set(findings), key=lambda item: (item.path, item.line)))


def _status_307(call: ast.Call) -> bool:
    for keyword in call.keywords:
        if keyword.arg != "status_code":
            continue
        if isinstance(keyword.value, ast.Constant) and keyword.value.value == 307:
            return True
        if (
            isinstance(keyword.value, ast.Name)
            and keyword.value.id == "HTTP_307_TEMPORARY_REDIRECT"
        ):
            return True
    return False


def serving_redirect_violations(root: Path) -> tuple[SourceViolation, ...]:
    paths = [root / "apps/central_api/src/central_api/routes/serving.py"] + _files(
        root, _API + "serving/**/*.py"
    )
    findings: list[SourceViolation] = []
    for path in sorted(set(paths)):
        tree = _ast(root, path)
        for call in ast.walk(tree):
            if not isinstance(call, ast.Call) or not isinstance(call.func, ast.Name):
                continue
            if call.func.id == "RedirectResponse" or (
                call.func.id == "JSONResponse" and _status_307(call)
            ):
                findings.append(
                    SourceViolation(
                        _RULES["serving"], path.relative_to(root).as_posix(), call.lineno
                    )
                )
    return tuple(sorted(set(findings), key=lambda item: (item.path, item.line)))


def production_public_ip_violations(root: Path) -> tuple[SourceViolation, ...]:
    executor = _text(root, _INFRA + "executor/step_functions.py", python=True)
    paths = _files(root, "infra/opentofu/env/prod/**/*.tf")
    findings: list[SourceViolation] = []
    constant = re.search(r"_PRODUCTION_ASSIGN_PUBLIC_IP\s*=\s*[\"']([^\"']+)", executor)
    if constant is None or constant.group(1) != "ENABLED":
        findings.append(SourceViolation(_RULES["ip"], _INFRA + "executor/step_functions.py", 1))
    if 'network.get("AssignPublicIp") != _PRODUCTION_ASSIGN_PUBLIC_IP' not in executor:
        findings.append(SourceViolation(_RULES["ip"], _INFRA + "executor/step_functions.py", 1))
    for path in paths:
        for line, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            code = raw.split("#", 1)[0]
            if re.search(r"AssignPublicIp|assign_public_ip", code) and "DISABLED" in code:
                findings.append(
                    SourceViolation(_RULES["ip"], path.relative_to(root).as_posix(), line)
                )
    return tuple(sorted(set(findings), key=lambda item: (item.path, item.line)))


def network_resource_violations(root: Path) -> tuple[SourceViolation, ...]:
    paths = _files(root, "infra/opentofu/env/prod/**/*.tf") + _files(
        root, "infra/opentofu/modules/**/*.tf"
    )
    findings: list[SourceViolation] = []
    pattern = re.compile(r"^\s*resource\s+\"(aws_nat_gateway|aws_lb|aws_alb|aws_elb)\"(?:\s|\{)")
    for path in paths:
        for line, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if pattern.search(raw):
                findings.append(
                    SourceViolation(_RULES["network"], path.relative_to(root).as_posix(), line)
                )
    return tuple(sorted(set(findings), key=lambda item: (item.path, item.line)))


def _key_node(node: ast.AST) -> bool:
    if isinstance(node, ast.Name):
        return node.id in _KEY_NAMES or node.id in _AWS_ENV_NAMES
    if isinstance(node, ast.Attribute):
        return node.attr in _KEY_NAMES or _key_node(node.value)
    if isinstance(node, ast.keyword):
        return node.arg in _KEY_NAMES
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value in _AWS_ENV_NAMES
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "get"
    ):
        return bool(node.args) and _key_node(node.args[0])
    return False


def static_aws_key_violations(root: Path) -> tuple[SourceViolation, ...]:
    paths = _files(root, _INFRA + "aws/**/*.py")
    paths += [root / (_API + "auth/aws_oidc.py")] + _files(root, _API + "serving/**/*.py")
    paths += _files(root, "apps/data_processor/src/data_processor/aws_*.py")
    findings: list[SourceViolation] = []
    for path in sorted(set(paths)):
        if path.name == "raw_aws_runtime.py":
            continue
        tree = _ast(root, path)
        findings.extend(
            SourceViolation(
                _RULES["keys"],
                path.relative_to(root).as_posix(),
                getattr(node, "lineno", 1),
            )
            for node in ast.walk(tree)
            if _key_node(node) and not isinstance(node, (ast.Load,))
        )
    return tuple(sorted(set(findings), key=lambda item: (item.path, item.line)))


def adapter_scan_violations(root: Path) -> tuple[SourceViolation, ...]:
    patterns = (
        _INFRA + "control_plane/dynamodb*.py",
        _INFRA + "billing/dynamodb*.py",
        _INFRA + "auth/dynamodb*.py",
        _INFRA + "public_leads/dynamodb*.py",
        _INFRA + "aws/**/*.py",
    )
    paths = [path for pattern in patterns for path in _files(root, pattern)]
    paths.append(root / (_INFRA + "executor/step_functions.py"))
    findings: list[SourceViolation] = []
    for path in sorted(set(paths)):
        tree = _ast(root, path)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if isinstance(node.func, ast.Attribute) and node.func.attr == "scan":
                findings.append(
                    SourceViolation(_RULES["scan"], path.relative_to(root).as_posix(), node.lineno)
                )
            if (
                isinstance(node.func, ast.Attribute)
                and node.func.attr == "get_paginator"
                and node.args
                and isinstance(node.args[0], ast.Constant)
                and node.args[0].value == "scan"
            ):
                findings.append(
                    SourceViolation(_RULES["scan"], path.relative_to(root).as_posix(), node.lineno)
                )
            if (
                isinstance(node.func, ast.Attribute)
                and node.func.attr == "_make_api_call"
                and node.args
                and isinstance(node.args[0], ast.Constant)
                and node.args[0].value == "Scan"
            ):
                findings.append(
                    SourceViolation(_RULES["scan"], path.relative_to(root).as_posix(), node.lineno)
                )
            if (
                isinstance(node.func, ast.Name)
                and node.func.id == "getattr"
                and len(node.args) > 1
                and isinstance(node.args[1], ast.Constant)
                and node.args[1].value == "scan"
            ):
                findings.append(
                    SourceViolation(_RULES["scan"], path.relative_to(root).as_posix(), node.lineno)
                )
    return tuple(sorted(set(findings), key=lambda item: (item.path, item.line)))


def _contains_hours(node: ast.AST) -> bool:
    for child in ast.walk(node):
        if isinstance(child, ast.Name) and child.id in {"task_hours", "task_hour", "taskHours"}:
            return True
        if isinstance(child, ast.Attribute) and child.attr in {
            "task_hours",
            "task_hour",
            "taskHours",
        }:
            return True
        if isinstance(child, ast.Constant) and child.value in {
            "task_hours",
            "task_hour",
            "taskHours",
            "task-hours",
        }:
            return True
    return False


def task_hours_start_violations(root: Path) -> tuple[SourceViolation, ...]:
    relatives = (
        "packages/cnes_domain/src/cnes_domain/orchestration/dispatch_admission.py",
        "packages/cnes_domain/src/cnes_domain/control_plane/execution_quota.py",
        _INFRA + "control_plane/dynamodb_execution_quota.py",
        _API + "services/run_planning.py",
        "apps/data_processor/src/data_processor/orchestration/coordinator.py",
        _INFRA + "executor/step_functions.py",
    )
    findings: list[SourceViolation] = []
    for relative in relatives:
        tree = _ast(root, root / relative)
        findings.extend(
            SourceViolation(_RULES["hours"], relative, node.lineno)
            for node in ast.walk(tree)
            if isinstance(node, (ast.If, ast.IfExp, ast.While)) and _contains_hours(node.test)
        )
    return tuple(sorted(set(findings), key=lambda item: (item.path, item.line)))


def require_complete_junit(path: Path) -> None:
    root = ElementTree.parse(path).getroot()  # noqa: S314
    suites = [node for node in root.iter() if node.tag.rsplit("}", 1)[-1] == "testsuite"]
    if not suites:
        raise ValueError("junit_suites=missing")
    cases = 0
    for suite in suites:
        children = list(suite)
        nested = [child for child in children if child.tag.rsplit("}", 1)[-1] == "testsuite"]
        if nested:
            continue
        testcases = [child for child in children if child.tag.rsplit("}", 1)[-1] == "testcase"]
        if not testcases:
            raise ValueError("junit_testcases=missing")
        cases += len(testcases)
        for key in ("skipped", "failures", "errors"):
            if int(suite.attrib.get(key, "0")):
                raise ValueError(f"junit_{key}=nonzero")
        if any(
            child.tag.rsplit("}", 1)[-1] in {"skipped", "failure", "error"}
            for case in testcases
            for child in case
        ):
            raise ValueError("junit_case=nonzero")
    if not cases:
        raise ValueError("junit_testcases=missing")
