"""Escopo do Sonar para o leitor de credencial do API: caminho exato e testes no job obrigatório."""

from __future__ import annotations

import re
import shlex
from fnmatch import fnmatchcase
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
SONAR_PROPERTIES = REPO_ROOT / "sonar-project.properties"
RUNTIME_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "production-runtime.yml"
READER = "apps/central_api/docker/aws_credentials.py"
READER_SUBPROCESS_TESTS = (
    "tests/production/test_api_credential_reader.py",
    "tests/production/test_api_credential_process.py",
    "tests/production/test_api_entrypoint.py",
)
IGNORE_PREFIX = "sonar.issue.ignore.multicriteria"
PATH_TRAVERSAL_RULE = "pythonsecurity:S8707"
EXCLUSION_OPTIONS = ("--ignore", "--ignore-glob", "--deselect")
SHORT_OPTIONS = ("-k", "-m")
VALUE_OPTIONS = (*EXCLUSION_OPTIONS, *SHORT_OPTIONS)
NON_EXECUTING = frozenset(
    {"--co", "--collect-only", "--setup-only", "--setup-plan", "--fixtures", "--markers"},
)
EXCLUDED_MARKER = re.compile(r"\bnot\s+(\w+)")


def _properties() -> dict[str, str]:
    entries: dict[str, str] = {}
    pending = ""
    for raw in SONAR_PROPERTIES.read_text(encoding="utf-8").splitlines():
        line = pending + raw.strip()
        if not pending and (not line or line.startswith("#")):
            continue
        if line.endswith("\\"):
            pending = line[:-1]
            continue
        pending = ""
        key, value = line.split("=", 1)
        entries[key.strip()] = value.strip()
    return entries


def _list(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _matches(pattern: str, path: str) -> bool:
    forms = {pattern, pattern.replace("**/", "")}
    return any(fnmatchcase(path, form.replace("**", "*")) for form in forms)


def _excludes(flag: str, value: str, test: str) -> bool:
    target = value.split("::", 1)[0].rstrip("/")
    if flag == "--ignore-glob":
        return _matches(target, test)
    return test == target or test.startswith(f"{target}/")


def _arguments(command: list[str]) -> tuple[list[str], list[tuple[str, str]]]:
    positionals: list[str] = []
    options: list[tuple[str, str]] = []
    pending = ""
    for argument in command:
        flag, _, value = argument.partition("=")
        if pending:
            options.append((pending, argument))
            pending = ""
        elif argument in VALUE_OPTIONS:
            pending = argument
        elif flag in VALUE_OPTIONS:
            options.append((flag, value))
        elif argument[:2] in SHORT_OPTIONS:
            options.append((argument[:2], argument[2:]))
        else:
            positionals.append(argument)
    return positionals, options


def _runtime_python() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    workflow = yaml.safe_load(RUNTIME_WORKFLOW.read_text(encoding="utf-8"))
    job = workflow["jobs"]["runtime-python"]
    step = next(s for s in job["steps"] if s.get("id") == "python-contracts")
    return job, step, workflow["jobs"]["production-runtime-acceptance"]


def test_ignore_de_path_traversal_do_leitor_e_escopado_no_caminho_exato() -> None:
    entries = _properties()
    scoped = [
        identifier
        for identifier in _list(entries[IGNORE_PREFIX])
        if _matches(entries[f"{IGNORE_PREFIX}.{identifier}.ruleKey"], PATH_TRAVERSAL_RULE)
        and _matches(entries[f"{IGNORE_PREFIX}.{identifier}.resourceKey"], READER)
    ]

    assert [entries[f"{IGNORE_PREFIX}.{name}.resourceKey"] for name in scoped] == [READER]


def test_exclusao_de_cobertura_do_leitor_e_o_caminho_exato() -> None:
    exclusions = _list(_properties()["sonar.coverage.exclusions"])

    assert [pattern for pattern in exclusions if _matches(pattern, READER)] == [READER]


def test_leitor_continua_no_escopo_de_analise_do_sonar() -> None:
    entries = _properties()

    assert any(READER.startswith(f"{source}/") for source in _list(entries["sonar.sources"]))
    assert not [p for p in _list(entries["sonar.exclusions"]) if _matches(p, READER)]
    assert not [p for p in _list(entries["sonar.test.inclusions"]) if _matches(p, READER)]
    assert not [t for t in _list(entries["sonar.tests"]) if READER.startswith(f"{t}/")]


def test_job_obrigatorio_roda_os_testes_de_subprocesso_do_leitor() -> None:
    job, step, aggregate = _runtime_python()
    positionals, options = _arguments(shlex.split(step["run"]))
    gates = {key: v for key, v in (job | step).items() if key in {"if", "continue-on-error"}}

    assert "tests/production" in positionals
    assert NON_EXECUTING.isdisjoint(positionals)
    assert "runtime-python" in aggregate["needs"]
    assert gates == {}
    assert [flag for flag, _ in options if flag == "-k"] == []
    assert not [
        (flag, value, test) for flag, value in options for test in READER_SUBPROCESS_TESTS
        if flag in EXCLUSION_OPTIONS and _excludes(flag, value, test)
    ]


def test_testes_de_subprocesso_do_leitor_nao_carregam_marker_excluido_pelo_job() -> None:
    _, step, _ = _runtime_python()
    _, options = _arguments(shlex.split(step["run"]))
    expressions = [value for flag, value in options if flag == "-m"]
    excluded = {name for value in expressions for name in EXCLUDED_MARKER.findall(value)}
    sources = {
        test: (REPO_ROOT / test).read_text(encoding="utf-8") for test in READER_SUBPROCESS_TESTS
    }

    assert excluded
    assert not [
        (test, marker) for test, text in sources.items() for marker in excluded
        if f"mark.{marker}" in text
    ]
