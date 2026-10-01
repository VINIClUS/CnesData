"""Código de produção dos apps só cria Run pelo serviço de autorização faturada."""

import ast
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_APPS = _ROOT / "apps"
_RUN_WRITERS = frozenset({"put_run", "create_unmetered_run", "reserve_and_create_run"})
_ALLOWED = frozenset({
    Path("apps/central_api/src/central_api/services/run_authorization.py"),
})


def _sources() -> tuple[Path, ...]:
    return tuple(sorted(_APPS.glob("*/src/**/*.py")))


def _called_name(node: ast.Call) -> str | None:
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    if isinstance(node.func, ast.Name):
        return node.func.id
    return None


def _run_creations(source: str) -> list[str]:
    calls = (node for node in ast.walk(ast.parse(source)) if isinstance(node, ast.Call))
    names = (_called_name(call) for call in calls)
    return [name for name in names if name in _RUN_WRITERS or name == "Run"]


def _violations() -> list[str]:
    found = []
    for path in _sources():
        relative = path.relative_to(_ROOT)
        if relative in _ALLOWED:
            continue
        found.extend(f"{relative}:{name}" for name in _run_creations(path.read_text("utf-8")))
    return found


def test_apps_nao_criam_run_fora_do_servico_de_autorizacao() -> None:
    assert _sources()
    assert _violations() == []


def test_servico_de_autorizacao_permitido_ainda_cria_run() -> None:
    allowed = next(iter(_ALLOWED))
    assert _run_creations((_ROOT / allowed).read_text("utf-8")) == ["create_unmetered_run"]


@pytest.mark.parametrize(
    "source",
    [
        "plane.put_run(run)",
        "plane.create_unmetered_run(command)",
        "plane.reserve_and_create_run(command)",
        "Run(tenant_id='t')",
    ],
)
def test_detecta_criacao_direta_de_run(source: str) -> None:
    assert _run_creations(source)


def test_ignora_chamadas_nao_relacionadas() -> None:
    assert _run_creations("plane.get_run('t', 'r')\n(lambda: None)()\nTransitionRun()") == []
