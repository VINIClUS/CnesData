"""O handler do close-drain carrega no child do application gate só com o runtime do infra."""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator

_ROOT = Path(__file__).resolve().parents[3]
# infra-ansible 8814a99b tools/cnesdata_delivery/runtime-requirements.txt (top-levels importáveis).
_RUNTIME = frozenset({"boto3", "botocore", "s3transfer", "jmespath", "dateutil", "six", "urllib3"})
_PACKAGES = {
    "cnes_domain": "packages/cnes_domain/src/cnes_domain",
    "cnes_infra": "packages/cnes_infra/src/cnes_infra",
}
_PRODUCT = frozenset({*_PACKAGES, "scripts", "release"})
_ENTRY = "scripts.promotion.promote_api"
# AuthorizedPackages copiado de infra-ansible 8814a99b tools/cnesdata_delivery/application_gate.py.
_CHILD = r"""
import importlib.machinery, importlib.util, os, sys
product, allowed = sys.argv[1], frozenset(sys.argv[2].split(","))
sys.path.insert(0, product)
class AuthorizedPackages:
    ROOTS = {"cnes_domain": "packages/cnes_domain/src/cnes_domain",
             "cnes_infra": "packages/cnes_infra/src/cnes_infra"}
    def find_spec(self, name, path=None, target=None):
        parts = name.split(".")
        if parts[0] not in self.ROOTS:
            return None
        base = os.path.join(product, self.ROOTS[parts[0]], *parts[1:])
        if os.path.isdir(base):
            spec = importlib.machinery.ModuleSpec(name, None, is_package=True)
            spec.submodule_search_locations = [base]
            return spec
        if os.path.isfile(base + ".py"):
            return importlib.util.spec_from_file_location(name, base + ".py")
        raise ModuleNotFoundError("adapter authorized path " + name, name=name)
class RuntimeOnly:
    def find_spec(self, name, path=None, target=None):
        top = name.partition(".")[0]
        if top in sys.stdlib_module_names or top in allowed:
            return None
        raise ModuleNotFoundError("adapter runtime " + name, name=name)
sys.meta_path.insert(0, AuthorizedPackages())
sys.meta_path.insert(0, RuntimeOnly())
spec = importlib.util.spec_from_file_location(
    "scripts.promotion.promote_api", product + "/scripts/promotion/promote_api.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
assert callable(module.execute_shared_operation)
assert "scripts.promotion.promote_close" in sys.modules
assert "scripts.promotion.promote_mode_effects" in sys.modules
assert "scripts.promotion.mode_canary_seed" in sys.modules
"""


def _location(name: str) -> Path:
    top, _, rest = name.partition(".")
    return _ROOT / _PACKAGES.get(top, top) / rest.replace(".", "/")


def _path(name: str) -> Path | None:
    target = _location(name)
    if target.with_suffix(".py").is_file():
        return target.with_suffix(".py")
    if name.partition(".")[0] not in _PACKAGES and (target / "__init__.py").is_file():
        return target / "__init__.py"
    return None


def _exists(name: str) -> bool:
    return _path(name) is not None or _location(name).is_dir()


def _type_checking(node: ast.AST) -> ast.If | None:
    if not isinstance(node, ast.If):
        return None
    test = node.test
    guarded = (isinstance(test, ast.Name) and test.id == "TYPE_CHECKING") or (
        isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"
    )
    return node if guarded else None


def _targets(name: str, path: Path) -> Iterator[tuple[int, str]]:
    package = name if path.name == "__init__.py" else name.rpartition(".")[0]
    pending: list[ast.AST] = [ast.parse(path.read_text(encoding="utf-8"))]
    while pending:
        node = pending.pop()
        if isinstance(node, ast.Import):
            yield from ((node.lineno, alias.name) for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            if node.level:
                anchor = package.rsplit(".", node.level - 1)[0] if node.level > 1 else package
                base = f"{anchor}.{base}" if base else anchor
            yield node.lineno, base
            yield from ((node.lineno, f"{base}.{alias.name}") for alias in node.names)
        elif (guarded := _type_checking(node)) is not None:
            pending.extend(guarded.orelse)
        else:
            pending.extend(ast.iter_child_nodes(node))


type Edges = dict[str, set[tuple[int, str]]]


def _visit(name: str, edges: Edges, foreign: dict[str, set[str]]) -> list[str]:
    path = _path(name)
    found: list[str] = []
    for line, target in () if path is None else _targets(name, path):
        top = target.partition(".")[0]
        if top in _PRODUCT:
            parts = target.split(".")
            modules = [".".join(parts[:size]) for size in range(1, len(parts) + 1)]
            reached = [module for module in modules if _exists(module)]
            edges[name].update((line, module) for module in reached)
            found.extend(reached)
        elif top not in sys.stdlib_module_names and top not in _RUNTIME:
            foreign[name].add(f"{name}:{line} {top}")
    return found


def _graph(entry: str = _ENTRY) -> tuple[Edges, dict[str, set[str]]]:
    edges: Edges = {}
    foreign: dict[str, set[str]] = {}
    pending = [entry]
    while pending:
        name = pending.pop()
        if name not in edges:
            edges[name], foreign[name] = set(), set()
            pending.extend(_visit(name, edges, foreign))
    return edges, foreign


def _closure(start: str, edges: Edges) -> set[str]:
    seen, pending = set[str](), [start]
    while pending:
        name = pending.pop()
        if name not in seen:
            seen.add(name)
            pending.extend(target for _, target in edges[name])
    return seen


def test_handler_carrega_no_child_so_com_o_runtime_do_infra(tmp_path):
    env = {
        "PATH": "/usr/bin:/bin",
        "HOME": str(tmp_path),
        "LANG": "C.UTF-8",
        "AWS_CONFIG_FILE": "/dev/null",
        "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
        "AWS_EC2_METADATA_DISABLED": "true",
    }
    allowed = ",".join(sorted(_RUNTIME | _PRODUCT))
    done = subprocess.run(  # noqa: S603 -- Executes the fixed child loader with this interpreter.
        [sys.executable, "-I", "-B", "-c", _CHILD, str(_ROOT), allowed],
        cwd=_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    failure = done.stderr.strip().splitlines()[-1:] if done.returncode else []
    assert failure == []


def _violations(entry: str) -> list[str]:
    edges, foreign = _graph(entry)
    violations: list[str] = []
    for name, items in sorted(edges.items()):
        if name.partition(".")[0] in _PACKAGES:
            continue
        violations.extend(sorted(foreign[name]))
        for line, target in sorted(items):
            if target.partition(".")[0] not in _PACKAGES:
                continue
            sites = sorted(site for module in _closure(target, edges) for site in foreign[module])
            if sites:
                violations.append(f"{name}:{line} -> {target}: {', '.join(sites)}")
    return violations


def test_fechamento_do_handler_com_imports_em_funcao_nao_sai_do_runtime():
    assert _violations(_ENTRY) == []


@pytest.mark.parametrize("entry", [
    "scripts.promotion.promote_mode_effects", "scripts.promotion.mode_canary_seed",
    "scripts.promotion.mode_canary",
])
def test_efeitos_de_modo_carregam_sem_pydantic_nem_cnes_contracts(entry: str):
    assert _path(entry) is not None
    assert _violations(entry) == []
