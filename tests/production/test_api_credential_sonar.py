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
EXCLUSION_OPTIONS = ("--ignore=", "--ignore-glob=", "--deselect=")
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


def _excludes(option: str, test: str) -> bool:
    flag, target = option.split("=", 1)
    target = target.split("::", 1)[0].rstrip("/")
    if flag == "--ignore-glob":
        return _matches(target, test)
    return test == target or test.startswith(f"{target}/")


def _runtime_python() -> tuple[dict[str, Any], list[str], dict[str, Any]]:
    workflow = yaml.safe_load(RUNTIME_WORKFLOW.read_text(encoding="utf-8"))
    job = workflow["jobs"]["runtime-python"]
    command = shlex.split(next(s["run"] for s in job["steps"] if s.get("id") == "python-contracts"))
    return job, command, workflow["jobs"]["production-runtime-acceptance"]


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


def test_job_obrigatorio_roda_os_testes_de_subprocesso_do_leitor() -> None:
    job, command, aggregate = _runtime_python()
    options = [arg for arg in command if arg.startswith(EXCLUSION_OPTIONS)]

    assert "tests/production" in command
    assert "runtime-python" in aggregate["needs"]
    assert not job.get("continue-on-error")
    assert not [
        (option, test) for option in options for test in READER_SUBPROCESS_TESTS
        if _excludes(option, test)
    ]


def test_testes_de_subprocesso_do_leitor_nao_carregam_marker_excluido_pelo_job() -> None:
    _, command, _ = _runtime_python()
    excluded = set(EXCLUDED_MARKER.findall(command[command.index("-m") + 1]))
    sources = {
        test: (REPO_ROOT / test).read_text(encoding="utf-8") for test in READER_SUBPROCESS_TESTS
    }

    assert excluded
    assert not [
        (test, marker) for test, text in sources.items() for marker in excluded
        if f"mark.{marker}" in text
    ]
