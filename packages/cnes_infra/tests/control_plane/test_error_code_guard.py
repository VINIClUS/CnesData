import ast
from importlib.util import resolve_name
from pathlib import Path

import pytest

ERROR_NAMES = {"Conflict", "InvalidTransition", "LeaseLost", "FenceRejected", "NotFound"}
ERROR_MODULES = {"cnes_domain", "cnes_domain.control_plane.errors"}
ROOT = Path(__file__).resolve().parents[4]


def _package_name(path: Path | None) -> str:
    if path is None:
        return ""
    parts = []
    parent = path.parent
    while (parent / "__init__.py").is_file():
        parts.append(parent.name)
        parent = parent.parent
    return ".".join(reversed(parts))


def _import_module(node: ast.ImportFrom, package: str) -> str:
    if not node.level:
        return node.module or ""
    if not package:
        return ""
    try:
        return resolve_name("." * node.level + (node.module or ""), package)
    except ImportError:
        return ""


def _qualified_constructors(prefix: str) -> set[str]:
    return {f"{prefix}.{name}" for name in ERROR_NAMES}


def _from_import_constructors(node: ast.ImportFrom, package: str) -> set[str]:
    module = _import_module(node, package)
    if module in ERROR_MODULES:
        return {alias.asname or alias.name for alias in node.names if alias.name in ERROR_NAMES}
    if module == "cnes_domain.control_plane":
        return {
            name
            for alias in node.names
            if alias.name == "errors"
            for name in _qualified_constructors(alias.asname or alias.name)
        }
    return set()


def _import_constructors(node: ast.Import) -> set[str]:
    return {
        name
        for alias in node.names
        if alias.name in ERROR_MODULES
        for name in _qualified_constructors(alias.asname or alias.name)
    }


def _error_constructors(tree: ast.Module, path: Path | None = None) -> set[str]:
    package = _package_name(path)
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            names.update(_from_import_constructors(node, package))
        elif isinstance(node, ast.Import):
            names.update(_import_constructors(node))
    return names


def _literal_error_calls(source: str, path: Path | None = None) -> list[tuple[int, str]]:
    tree = ast.parse(source)
    constructors = _error_constructors(tree, path)
    violations = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or ast.unparse(node.func) not in constructors:
            continue
        arguments = node.args[:1] + [kw.value for kw in node.keywords if kw.arg == "code"]
        violations.extend(
            (node.lineno, argument.value)
            for argument in arguments
            if isinstance(argument, ast.Constant) and isinstance(argument.value, str)
        )
    return violations


@pytest.mark.parametrize(
    "source",
    [
        'from cnes_domain.control_plane.errors import Conflict\nConflict("new_code")',
        'from cnes_domain.control_plane.errors import Conflict as C\nC("new_code")',
        'from cnes_domain import NotFound\nNotFound("new_code")',
        'import cnes_domain.control_plane.errors as e\ne.LeaseLost("new_code")',
        'from cnes_domain.control_plane import errors as e\ne.FenceRejected("new_code")',
        "import cnes_domain.control_plane.errors\n"
        'cnes_domain.control_plane.errors.InvalidTransition(code="new_code")',
    ],
)
def test_guarda_detecta_literal_em_construtores_importados(source: str) -> None:
    assert _literal_error_calls(source) == [(2, "new_code")]


@pytest.mark.parametrize(
    ("module", "source"),
    [
        ("control_plane/check.py", 'from .errors import Conflict\nConflict("new_code")'),
        ("control_plane/__init__.py", 'from .errors import Conflict as C\nC("new_code")'),
        ("control_plane/check.py", 'from . import errors as e\ne.LeaseLost("new_code")'),
        ("control_plane/check.py", 'from .. import NotFound\nNotFound("new_code")'),
        ("ports/check.py", "from ..control_plane.errors import InvalidTransition\n"
         'InvalidTransition(code="new_code")'),
        ("ports/check.py", "from ..control_plane import errors as e\n"
         'e.FenceRejected("new_code")'),
    ],
)
def test_guarda_detecta_importacoes_relativas_no_contexto_do_pacote(module, source) -> None:
    path = ROOT / "packages/cnes_domain/src/cnes_domain" / module

    assert _literal_error_calls(source, path) == [(2, "new_code")]


def test_guarda_ignora_erros_relativos_de_outro_pacote() -> None:
    path = ROOT / "packages/cnes_infra/src/cnes_infra/control_plane/check.py"
    source = 'from .errors import Conflict\nConflict("external_code")'

    assert _literal_error_calls(source, path) == []


def test_guarda_aceita_enum_mensagem_dinamica_e_strings_de_outros_dominios() -> None:
    source = """
from cnes_domain.control_plane.errors import Conflict, ControlPlaneErrorCode
from elsewhere import NotFound
Conflict(ControlPlaneErrorCode.JOB_CONFLICT)
Conflict(f"transition={old}->{new}")
NotFound("external_code")
ValueError("validation_code")
message = "Conflict('only_text')"
"""

    assert _literal_error_calls(source) == []


def test_plano_de_controle_nao_introduz_codigos_literais_nos_construtores() -> None:
    roots = (
        ROOT / "packages/cnes_domain/src",
        ROOT / "packages/cnes_infra/src/cnes_infra/control_plane",
        ROOT / "packages/cnes_infra/tests/control_plane",
        ROOT / "packages/cnes_infra/tests/contracts",
    )
    excluded = {"audit_sink_contract.py", "object_store_contract.py"}
    violations = [
        (str(path.relative_to(ROOT)), line, code)
        for root in roots
        for path in root.rglob("*.py")
        if path.name not in excluded
        for line, code in _literal_error_calls(path.read_text(encoding="utf-8"), path)
    ]

    assert not violations, violations
