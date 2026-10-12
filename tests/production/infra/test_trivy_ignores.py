import json
import re
import shutil
import subprocess
from collections.abc import Callable, Iterator
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

ToolLookup = Callable[[str], str | None]
RequireTool = Callable[[str, ToolLookup], str]

OPENTOFU_ROOT = Path(__file__).resolve().parents[3] / "infra" / "opentofu"
DATA_BUCKETS = OPENTOFU_ROOT / "modules" / "data-buckets"
IGNORE_DIRECTIVE = re.compile(r"(?:trivy|tfsec):ignore")
EXPIRING_IGNORE = re.compile(r"#trivy:ignore:AVD-AWS-\d{4}:exp:(?P<expiry>\d{4}-\d{2}-\d{2})")
REFERENCE_DAY = date(2026, 10, 12)


def _violation(line: str, today: date) -> str | None:
    match = EXPIRING_IGNORE.fullmatch(line.strip())
    if match is None:
        return "ignore_sem_expiracao"
    try:
        expiry = date.fromisoformat(match["expiry"])
    except ValueError:
        return "expiracao_invalida"
    return None if expiry > today else "ignore_vencido"


def _ignore_lines(root: Path = OPENTOFU_ROOT) -> Iterator[tuple[str, str]]:
    for path in sorted(root.rglob("*.tf")):
        relative = path.relative_to(root)
        if any(part.startswith(".") for part in relative.parts):
            continue
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if IGNORE_DIRECTIVE.search(line):
                yield f"{relative.as_posix()}:{number}", line


@pytest.mark.parametrize(
    ("line", "violation"),
    [
        ("#trivy:ignore:AVD-AWS-0132:exp:2026-10-13", None),
        ("  #trivy:ignore:AVD-AWS-0132:exp:2027-04-11", None),
        ("#trivy:ignore:AVD-AWS-0132[exp:2027-04-11]", "ignore_sem_expiracao"),
        ("#trivy:ignore:AVD-AWS-0132[exp:2020-01-01]", "ignore_sem_expiracao"),
        ("#trivy:ignore:AVD-AWS-0132", "ignore_sem_expiracao"),
        ("#tfsec:ignore:aws-s3-encryption-customer-key", "ignore_sem_expiracao"),
        ("#trivy:ignore:AVD-AWS-0132:exp:2026-10-12", "ignore_vencido"),
        ("#trivy:ignore:AVD-AWS-0132:exp:2020-01-01", "ignore_vencido"),
        ("#trivy:ignore:AVD-AWS-0132:exp:2027-02-30", "expiracao_invalida"),
    ],
    ids=[
        "amanha", "indentado", "colchetes_futuro", "colchetes_vencido", "sem_exp", "tfsec",
        "vence_hoje", "vencido", "data_inexistente",
    ],
)
def test_gate_rejeita_ignore_com_colchetes_sem_exp_ou_vencido(
    line: str, violation: str | None
) -> None:
    assert _violation(line, REFERENCE_DAY) == violation


def test_descoberta_de_ignores_percorre_subdiretorios_e_pula_ocultos(tmp_path: Path) -> None:
    root = tmp_path / ".worktrees" / "feat-x" / "opentofu"
    nested = root / "modules" / "x" / "main.tf"
    hidden = root / "env" / ".terraform" / "modules" / "y" / "main.tf"
    for path in (nested, hidden):
        path.parent.mkdir(parents=True)
        path.write_text('#trivy:ignore:AVD-AWS-0132[exp:2027-04-11]\n', encoding="utf-8")

    assert [where for where, _ in _ignore_lines(root)] == ["modules/x/main.tf:1"]


def test_ignores_inline_do_trivy_expiram_em_data_futura() -> None:
    today = datetime.now(UTC).date()
    violations = {where: _violation(line, today) for where, line in _ignore_lines()}

    assert any(where.startswith("modules/data-buckets/main.tf:") for where in violations)
    assert {where: reason for where, reason in violations.items() if reason} == {}


def test_modulo_data_buckets_dispensa_ignore_global_do_trivy(
    require_tool: RequireTool, tmp_path: Path
) -> None:
    trivy = require_tool("trivy", shutil.which)
    ignorefile = tmp_path / "trivyignore"
    ignorefile.write_text("", encoding="utf-8")
    result = subprocess.run(  # noqa: S603
        [
            trivy, "config", "--quiet", "--skip-check-update", "--format", "json",
            "--cache-dir", str(tmp_path / "cache"), "--ignorefile", str(ignorefile),
            "--severity", "HIGH,CRITICAL", "--exit-code", "1", str(DATA_BUCKETS),
        ],
        check=False, capture_output=True, cwd=tmp_path, text=True,
    )
    results = json.loads(result.stdout or "{}").get("Results") or []
    failures = [found["ID"] for item in results for found in item.get("Misconfigurations") or []]
    evaluated = sum((item.get("MisconfSummary") or {}).get("Successes", 0) for item in results)

    assert (result.returncode, failures) == (0, []), f"trivy_findings stderr={result.stderr!r}"
    assert evaluated > 0
