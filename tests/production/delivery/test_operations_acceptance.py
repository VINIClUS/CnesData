"""Gate final das operações de produção: matrizes, schema de evidência e runbooks."""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
RUNBOOKS = ROOT / "docs/runbooks"
PLAN = ROOT / ("docs/superpowers/plans/"
               "2026-08-31-cnesdata-production-delivery-and-operations-implementation-plan.md")
PROMOTION = RUNBOOKS / "production-promotion.md"
NEW = ("production-promotion.md", "recovery-drill.md", "roles-anywhere-incident.md",
       "cost-freeze.md", "quarterly-restore.md")
OVERLAPPING = ("runtime-acceptance.md", "infrastructure-plan.md", "aws-runtime-validation.md",
               "floci-and-aws-capability.md", "roles-anywhere-evidence.md",
               "object-lock-capability.md", "athena-operator.md")
REQUIRED_LINKS = {
    "production-promotion.md": (*OVERLAPPING[:6], "mode-canary.md", "rollback.md"),
    "recovery-drill.md": ("mode-canary.md", "rollback.md", "infrastructure-plan.md"),
    "roles-anywhere-incident.md": ("roles-anywhere-evidence.md", "infrastructure-plan.md"),
    "cost-freeze.md": ("athena-operator.md", "infrastructure-plan.md"),
    "quarterly-restore.md": ("rollback.md", "demo-seed.md", "aws-runtime-validation.md"),
}
COUNTS = {"PRE": 25, "SMK": 27}
PLAN_STEPS = {"PRE": (1, 20), "SMK": (2, 12)}
STAGES = ("#468", "#469", "#473", "#474", "#475", "#476")
FIELDS = ("Plano", "Origem", "Verificação", "Evidência", "Estágio")
DESVIOS = "infrastructure-plan.md#desvios-dos-comandos-da-issue"
_ROW = re.compile(r"^- \*\*((PRE|SMK)-\d{2})\*\* (\S.*)$")
_FIELD = re.compile(r"^  - (" + "|".join(FIELDS) + r"): (\S.*)$")
_LINK = re.compile(r"\]\(([^)#\s]+)(?:#[^)\s]*)?\)")
_REPO_PATH = re.compile(r"`((?:docs|scripts|tests|infra|packages|apps|release|\.github)/[^`\s:]+)")
_FENCE = re.compile(r"^```[a-z]*\n(.*?)^```", re.MULTILINE | re.DOTALL)
_MUTATING = (
    re.compile(r"\b(?:tofu|terraform)\b[^\n]*\b(?:apply|destroy|import|taint)\b"),
    re.compile(r"\bgh (?:workflow run|pr merge|release create|secret set|api -X)\b"),
    re.compile(r"\bgit push\b"),
    re.compile(r"\baws [a-z0-9-]+ (?:create|put|delete|update|disable|enable|attach|detach|"
               r"import|revoke|start|stop|terminate|run|invoke|cancel|deregister|register|"
               r"batch-delete|restore|tag|untag|reset|send)[a-z-]*"),
)
_SANDBOX_FREE = {"subprocess", "boto3", "botocore", "httpx", "requests", "socket", "urllib"}


def _runbook(name: str) -> str:
    path = RUNBOOKS / name
    assert path.is_file(), f"runbook_ausente={name}"
    return path.read_text()


def _rows(prefix: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    field = ""
    for line in _runbook(PROMOTION.name).splitlines():
        if row := _ROW.match(line):
            rows.append({"id": row.group(1), "título": row.group(3)})
        elif rows and (match := _FIELD.match(line)):
            field = match.group(1)
            rows[-1][field] = match.group(2)
        elif rows and field and line.startswith("    ") and line.strip():
            rows[-1][field] += " " + line.strip()
        elif not line.strip():
            field = ""
    return [row for row in rows if row["id"].startswith(prefix)]


def _plan_items(step: int) -> list[str]:
    text = PLAN.read_text()
    start = text.index(f"**Step {step}:", text.index("### Task 12:"))
    body = text[start:text.index("- [ ] **Step", start)].splitlines()[1:]
    paragraph = " ".join(line.strip() for line in body if line.strip())
    return [item.strip() for item in paragraph.rstrip(".").split(";")]


def _quoted(value: str) -> str:
    match = re.fullmatch(r"`([^`]+)`", value)
    assert match, f"plano_sem_citacao={value}"
    return match.group(1)


def _targets(text: str) -> list[Path]:
    links = [RUNBOOKS / target for target in _LINK.findall(text) if "://" not in target]
    return links + [ROOT / path.rstrip(".,;") for path in _REPO_PATH.findall(text)]


def _assert_complete(row: dict[str, str]) -> None:
    assert "Plano" in row or "Origem" in row, f"item_sem_origem={row['id']}"
    assert all(row.get(name) for name in FIELDS[2:]), f"item_incompleto={row['id']}"
    cited = _targets(row["Verificação"])
    assert cited, f"verificacao_sem_caminho={row['id']}"
    assert all(path.exists() for path in cited), f"caminho_inexistente={row['id']}"
    assert re.match(r"(Código|Produção da #\d+)", row["Estágio"]), f"estagio={row['id']}"


def _assert_matrix(prefix: str) -> list[dict[str, str]]:
    rows = _rows(prefix)
    step, size = PLAN_STEPS[prefix]
    planned = _plan_items(step)
    assert len(planned) == size
    expected = [f"{prefix}-{n:02d}" for n in range(1, COUNTS[prefix] + 1)]
    assert [row["id"] for row in rows] == expected
    assert sorted(_quoted(row["Plano"]) for row in rows if "Plano" in row) == sorted(planned)
    for row in rows:
        _assert_complete(row)
    return rows


def _origins(rows: list[dict[str, str]]) -> str:
    return " ".join(row.get("Origem", "") for row in rows)


def _documented_only() -> dict[str, bool]:
    text = _runbook("infrastructure-plan.md")
    table = text[text.index("### Só documentados"):text.index("[D5]:")]
    cells = [line.split("|")[1:3] for line in table.splitlines() if line.startswith("| ")]
    return {check.strip(): "[D5]" in where for check, where in cells[1:]}


def test_matriz_de_pre_producao_cobre_todos_os_itens_do_plano():
    rows = _assert_matrix("PRE")
    extra = [row for row in rows if "Plano" not in row]
    assert len(extra) == 5
    assert all("6715569897" in row["Origem"] for row in extra)
    assert all(row["Estágio"].startswith("Código") or "Produção da #" in row["Estágio"]
               for row in rows)


def test_matriz_de_producao_inclui_checks_do_d5():
    rows = _assert_matrix("SMK")
    origins = _origins(rows)
    documented = _documented_only()
    assert sum(documented.values()) == 3
    assert all(check in origins for check in documented)
    for check in (check for check, d5 in documented.items() if d5):
        own = [row for row in rows if check in row.get("Origem", "")]
        assert len(own) == 1, f"d5={check}"
        assert "D5" in own[0]["Origem"], f"d5={check}"
        assert own[0]["Estágio"].startswith("Produção da #476")
    for comment in ("6869274407", "6876585935", "6881970002"):
        assert comment in origins, f"criterio_ausente={comment}"
    for issue in STAGES:
        assert f"Estágio Produção da {issue}" in origins, f"estagio_ausente={issue}"


def _schema() -> dict[str, Any]:
    text = _runbook(PROMOTION.name)
    section = text[text.index("## Schema de evidência"):]
    block = _FENCE.search(section)
    assert block, "schema_ausente"
    return json.loads(block.group(1))


def _accepts(branch: dict[str, Any], record: dict[str, object]) -> bool:
    properties = branch["properties"]
    if set(record) != set(branch["required"]) or not set(record) <= set(properties):
        return False
    for name, value in record.items():
        rule = properties[name]
        if "const" in rule and value != rule["const"]:
            return False
        if "enum" in rule and value not in rule["enum"]:
            return False
        if "pattern" in rule and not re.fullmatch(rule["pattern"], str(value)):
            return False
    return True


def _record(matrix: str, ident: str, **override: object) -> dict[str, object]:
    record = {"schema_version": 1, "matrix": matrix, "item": ident, "develop_sha": "a" * 40,
              "status": "PASS", "observed_at": "2026-11-14T12:00:00Z",
              "evidence_sha256": "b" * 64, "redacted": True}
    if matrix == "production":
        record["release_id"] = f"candidate-{'c' * 40}-1-1"
    return {**record, **override}


def test_schema_de_evidencia_cobre_as_duas_matrizes():
    schema = _schema()
    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    branches = {branch["properties"]["matrix"]["const"]: branch for branch in schema["oneOf"]}
    assert set(branches) == {"pre-production", "production"}
    for matrix, prefix in (("pre-production", "PRE"), ("production", "SMK")):
        branch = branches[matrix]
        assert branch["additionalProperties"] is False
        assert set(branch["required"]) == set(branch["properties"])
        assert branch["properties"]["item"]["enum"] == [row["id"] for row in _rows(prefix)]
        assert branch["properties"]["redacted"] == {"type": "boolean", "const": True}
        assert branch["properties"]["status"]["enum"] == ["PASS", "FAIL", "BLOCKED"]
        item = f"{prefix}-01"
        assert _accepts(branch, _record(matrix, item))
        for bad in ({"redacted": False}, {"status": "OK"}, {"develop_sha": "a" * 39},
                    {"item": "PRE-99"}, {"observed_at": "2026-11-14 12:00"}, {"extra": 1}):
            assert not _accepts(branch, _record(matrix, item, **bad)), f"aceitou={bad}"
    assert "release_id" in branches["production"]["required"]
    assert "release_id" not in branches["pre-production"]["required"]
    release = _record("production", "SMK-01", release_id="v1")
    assert not _accepts(branches["production"], release)


def test_runbooks_novos_referenciam_os_existentes():
    for name, required in REQUIRED_LINKS.items():
        text = _runbook(name)
        linked = {target.split("#")[0] for target in _LINK.findall(text)}
        assert set(required) <= linked, f"referencia_ausente={name}"
        assert all(path.exists() for path in _targets(text)), f"caminho_inexistente={name}"
        assert "tofu -chdir=infra/opentofu test" not in text, f"comando_desatualizado={name}"
        assert "--policy policies" not in text, f"comando_desatualizado={name}"
    assert DESVIOS in _runbook(PROMOTION.name)
    assert "### Desvios dos comandos da issue" in _runbook("infrastructure-plan.md")
    for name in ("runtime-acceptance.md", "infrastructure-plan.md"):
        assert "production-promotion.md" in _LINK.findall(_runbook(name)), f"sem_referencia={name}"
    assert "athena-operator.md" in _runbook("cost-freeze.md")


def test_gate_nao_faz_deploy():
    for name in NEW:
        for block in _FENCE.findall(_runbook(name)):
            hits = [rule.pattern for rule in _MUTATING if rule.search(block)]
            assert not hits, f"comando_mutante={name}"
    tree = ast.parse(Path(__file__).read_text())
    imported = {alias.name.split(".")[0] for node in ast.walk(tree)
                if isinstance(node, ast.Import) for alias in node.names}
    imported |= {str(node.module).split(".")[0] for node in ast.walk(tree)
                 if isinstance(node, ast.ImportFrom)}
    assert not imported & _SANDBOX_FREE
    assert "não faz deploy" in _runbook(PROMOTION.name)
