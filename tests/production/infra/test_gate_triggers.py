"""O gatilho do infra-native cobre os arquivos que o gate lê fora de infra/."""

import re
from fnmatch import fnmatchcase
from pathlib import Path

import yaml

from tests.production.infra._athena_contracts import REPO_ROOT

WORKFLOW = REPO_ROOT / ".github" / "workflows" / "infra-native.yml"
INFRA_TESTS = Path(__file__).parent
ROOT_CHAIN = re.compile(r"""(?i:repo_root)((?:\s*/\s*["'][^"']+["'])+)""")
SEGMENT = re.compile(r"""["']([^"']+)["']""")
SOURCE_LITERAL = re.compile(r"""["']((?:apps|packages)/[\w./-]+)["']""")
HARNESS_IMPORT = re.compile(r"from (tests\.integration\.[\w.]+) import \(?\s*(\w+)")
# Varredura geral de workflows (runs-on, segredos): só outro.yml não dispara o gate, por decisão
# fixada em test_ci_opentofu_runner.py.
GENERAL_SCANS = frozenset({".github/workflows"})


def _trigger_paths() -> list[str]:
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    return workflow[True]["push"]["paths"]


def _harness_path(module: str, name: str) -> str:
    base = module.replace(".", "/")
    submodule = f"{base}/{name}.py"
    return submodule if (REPO_ROOT / submodule).is_file() else f"{base}.py"


def _gate_inputs() -> set[str]:
    found: set[str] = set()
    pending = sorted(INFRA_TESTS.rglob("*.py"))
    while pending:
        text = pending.pop().read_text(encoding="utf-8")
        found.update(SOURCE_LITERAL.findall(text))
        found.update("/".join(SEGMENT.findall(chain)) for chain in ROOT_CHAIN.findall(text))
        for module, name in HARNESS_IMPORT.findall(text):
            path = _harness_path(module, name)
            if path not in found:
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
        "docs/development.md",
        ".github/CODEOWNERS",
        "pytest.ini",
    } <= _gate_inputs()


def test_gatilho_do_gate_cobre_as_fontes_lidas_fora_de_infra() -> None:
    patterns = _trigger_paths()
    assert sorted(path for path in _gate_inputs() if not _covered(path, patterns)) == []
