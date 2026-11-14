"""Escopo do Sonar para o leitor de credencial do API: caminho exato e testes no job obrigatório."""

from __future__ import annotations

import shlex
from fnmatch import fnmatchcase
from pathlib import Path

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
    return fnmatchcase(path, pattern.replace("**", "*"))


def test_ignore_de_path_traversal_do_leitor_e_escopado_no_caminho_exato() -> None:
    entries = _properties()
    scoped = [
        identifier
        for identifier in _list(entries[IGNORE_PREFIX])
        if entries[f"{IGNORE_PREFIX}.{identifier}.ruleKey"] == PATH_TRAVERSAL_RULE
        and _matches(entries[f"{IGNORE_PREFIX}.{identifier}.resourceKey"], READER)
    ]

    assert [entries[f"{IGNORE_PREFIX}.{name}.resourceKey"] for name in scoped] == [READER]


def test_exclusao_de_cobertura_do_leitor_e_o_caminho_exato() -> None:
    exclusions = _list(_properties()["sonar.coverage.exclusions"])

    assert [pattern for pattern in exclusions if _matches(pattern, READER)] == [READER]


def test_job_obrigatorio_roda_os_testes_de_subprocesso_do_leitor() -> None:
    workflow = yaml.safe_load(RUNTIME_WORKFLOW.read_text(encoding="utf-8"))
    steps = workflow["jobs"]["runtime-python"]["steps"]
    command = shlex.split(next(s["run"] for s in steps if s.get("id") == "python-contracts"))
    ignored = [arg.split("=", 1)[1] for arg in command if arg.startswith("--ignore=")]
    aggregate = workflow["jobs"]["production-runtime-acceptance"]["needs"]

    assert "tests/production" in command
    assert "runtime-python" in aggregate
    assert all(
        not test.startswith(f"{prefix}/") for test in READER_SUBPROCESS_TESTS for prefix in ignored
    )
    assert all((REPO_ROOT / test).is_file() for test in READER_SUBPROCESS_TESTS)
