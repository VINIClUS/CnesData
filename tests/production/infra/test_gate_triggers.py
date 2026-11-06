"""O gatilho do infra-native cobre os arquivos que o gate lê fora de infra/."""

import ast
import re
from fnmatch import fnmatchcase
from pathlib import Path

import pytest
import yaml

from tests.production.infra._athena_contracts import REPO_ROOT

WORKFLOW = REPO_ROOT / ".github" / "workflows" / "infra-native.yml"
INFRA_TESTS = Path(__file__).parent
ROOT_CHAIN = re.compile(r"""(?i:repo_root|parents\[3\])((?:\s*/\s*["'][^"']+["'])+)""")
SEGMENT = re.compile(r"""["']([^"']+)["']""")
SOURCE_LITERAL = re.compile(r"""["']((?:apps|packages)/[\w./-]+)["']""")
WORKFLOW_SCRIPT = re.compile(r"(?<![\w/])tests/[\w./-]+\.py")
IMPORT_ROOTS = frozenset({"apps", "packages", "tests"})
# Varredura geral de workflows (runs-on, segredos): só outro.yml não dispara o gate, por decisão
# fixada em test_ci_opentofu_runner.py.
GENERAL_SCANS = frozenset({".github/workflows"})


def _trigger_paths() -> list[str]:
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    return workflow[True]["push"]["paths"]


def _module_file(module: str, name: str) -> str:
    base = module.replace(".", "/")
    candidates = (f"{base}/{name}.py", f"{base}.py", f"{base}/__init__.py")
    return next((item for item in candidates if (REPO_ROOT / item).is_file()), candidates[1])


def _imported_files(text: str) -> set[str]:
    found: set[str] = set()
    for node in ast.walk(ast.parse(text)):
        if not isinstance(node, ast.ImportFrom) or not node.module:
            continue
        if node.module.split(".")[0] in IMPORT_ROOTS:
            found.update(_module_file(node.module, alias.name) for alias in node.names)
    return found


def _read_paths(text: str) -> set[str]:
    chains = {"/".join(SEGMENT.findall(chain)) for chain in ROOT_CHAIN.findall(text)}
    return chains | set(SOURCE_LITERAL.findall(text))


def _workflow_scripts() -> set[str]:
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    steps = (step for job in workflow["jobs"].values() for step in job["steps"])
    runs = (step.get("run", "") for step in steps if step.get("id") != "changes")
    return {path for run in runs for path in WORKFLOW_SCRIPT.findall(run)}


def _gate_inputs() -> set[str]:
    found = _workflow_scripts()
    pending = sorted(path for path in INFRA_TESTS.rglob("*.py") if path != Path(__file__))
    while pending:
        text = pending.pop().read_text(encoding="utf-8")
        found.update(_read_paths(text))
        for path in _imported_files(text) - found:
            found.add(path)
            pending.append(REPO_ROOT / path)
    return found - GENERAL_SCANS


def _covered(path: str, patterns: list[str]) -> bool:
    return any(fnmatchcase(path, item) or fnmatchcase(f"{path}/x", item) for item in patterns)


def test_varredura_enxerga_as_fontes_conhecidas_do_gate() -> None:
    assert {
        "apps/data_processor/Dockerfile",
        "apps/data_processor/src",
        "packages/cnes_domain/src",
        "packages/cnes_infra/tests/fixtures/step_functions",
        "tests/integration/aws_capability/_fargate_mirror.py",
        "tests/integration/aws_capability/_matrix_policies.py",
        "packages/cnes_infra/tests/aws/production_values.py",
        "tests/production/test_runtime_acceptance.py",
        "tests/scripts/test_ci_opentofu_runner.py",
        "docs/development.md",
        ".github/CODEOWNERS",
        "pytest.ini",
    } <= _gate_inputs()


def test_gatilho_do_gate_cobre_as_fontes_lidas_fora_de_infra() -> None:
    patterns = _trigger_paths()
    assert sorted(path for path in _gate_inputs() if not _covered(path, patterns)) == []


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ('REPO_ROOT / "docs" / "x.md"', "docs/x.md"),
        ('repo_root / "docs/x.md"', "docs/x.md"),
        ('Path(__file__).resolve().parents[3] / "docs" / "x.md"', "docs/x.md"),
        ('FONTE = "apps/x/y.py"', "apps/x/y.py"),
        ('FONTE = "packages/x/y.py"', "packages/x/y.py"),
        ("from apps.x.y import z", "apps/x/y.py"),
        ("from packages.x.y import z", "packages/x/y.py"),
        ("from tests.x.y import z", "tests/x/y.py"),
        (
            "from tests.integration.aws_capability import _cleanup, _fargate_mirror",
            "tests/integration/aws_capability/_fargate_mirror.py",
        ),
    ],
)
def test_varredura_reconhece_cada_forma_de_leitura(source: str, expected: str) -> None:
    assert expected in _read_paths(source) | _imported_files(source)


def test_varredura_do_workflow_ignora_as_listas_de_filtro() -> None:
    scripts = _workflow_scripts()
    assert "tests/production/test_runtime_acceptance.py" in scripts
    assert "tests/scripts/test_ci_opentofu_runner.py" in scripts
    assert "tests/integration/aws_capability/_fargate_mirror.py" not in scripts
