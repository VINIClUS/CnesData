"""O install do job dashboard cobre os imports de nível de módulo dos arquivos que ele roda."""

from __future__ import annotations

import ast
import re
import sys
import tomllib
from importlib.metadata import packages_distributions
from pathlib import Path
from shlex import split
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[3]
STEP = "Exercise compiled production client at HTTP boundary"
LOCAL = ("scripts", "tests")
SYNC_FLAGS = {"--locked", "--managed-python", "--python", "--package", "--only-dev"}


def _normal(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _install(command: str) -> tuple[list[str], bool]:
    args = split(command)
    assert args[:2] == ["uv", "sync"]
    assert {arg for arg in args if arg.startswith("--")} <= SYNC_FLAGS, command
    return [args[i + 1] for i, arg in enumerate(args) if arg == "--package"], "--only-dev" in args


def _closure(lock: dict[str, Any], packages: list[str], only_dev: bool) -> set[str]:
    index = {package["name"]: package for package in lock["package"]}
    root = next(package for package in lock["package"] if package["source"] == {"virtual": "."})
    pending = list(root["dev-dependencies"]["dev"]) if only_dev else []
    for name in packages:
        pending += [{"name": name}, *index[name].get("dev-dependencies", {}).get("dev", [])]
    seen: set[tuple[str, tuple[str, ...]]] = set()
    while pending:
        dependency = pending.pop()
        key = (dependency["name"], tuple(dependency.get("extra", ())))
        if key not in seen:
            seen.add(key)
            package = index[key[0]]
            pending += package.get("dependencies", [])
            for extra in key[1]:
                pending += package["optional-dependencies"][extra]
    return {name for name, _ in seen}


def _workspace(lock: dict[str, Any]) -> dict[str, str]:
    modules: dict[str, str] = {}
    for package in lock["package"]:
        editable = package["source"].get("editable")
        for init in (ROOT / editable).glob("src/*/__init__.py") if editable else ():
            modules[init.parent.name] = package["name"]
    return modules


def _runtime_imports(tree: ast.Module) -> list[ast.Import | ast.ImportFrom]:
    found: list[ast.Import | ast.ImportFrom] = []
    pending: list[ast.AST] = list(tree.body)
    while pending:
        node = pending.pop()
        if isinstance(node, ast.Import | ast.ImportFrom):
            found.append(node)
        elif isinstance(node, ast.If) and ast.unparse(node.test) != "TYPE_CHECKING":
            pending += [*node.body, *node.orelse]
        elif isinstance(node, ast.Try | ast.ExceptHandler):
            pending += list(ast.iter_child_nodes(node))
    return found


def _names(node: ast.Import | ast.ImportFrom) -> list[str]:
    if isinstance(node, ast.Import):
        return [alias.name for alias in node.names]
    assert node.level == 0, ast.unparse(node)
    assert node.module, ast.unparse(node)
    return [f"{node.module}.{alias.name}" for alias in node.names]


def _local_files(module: str) -> list[Path]:
    parts = module.split(".")
    bases = [ROOT.joinpath(*parts[:size]) for size in range(1, len(parts) + 1)]
    return [path for base in bases for path in (base / "__init__.py", base.with_suffix(".py"))
            if path.is_file()]


def _external_imports(files: list[Path]) -> set[str]:
    seen: set[Path] = set()
    external: set[str] = set()
    pending = list(files)
    while pending:
        path = pending.pop()
        if path in seen:
            continue
        seen.add(path)
        for name in (n for node in _runtime_imports(ast.parse(path.read_text()))
                     for n in _names(node)):
            top = name.partition(".")[0]
            if top in LOCAL:
                pending += _local_files(name)
            elif top not in sys.stdlib_module_names:
                external.add(top)
    return external


def _selected(run: str) -> list[Path]:
    files = [ROOT / path for path in re.findall(r"tests/[\w/]+\.py", run)]
    conftests = [directory / "conftest.py" for path in files for directory in path.parents
                 if (directory == ROOT or ROOT in directory.parents)
                 and (directory / "conftest.py").is_file()]
    return files + conftests


def test_install_do_job_dashboard_cobre_os_imports_dos_arquivos_que_ele_roda() -> None:
    workflow = yaml.safe_load((ROOT / ".github/workflows/verify-production.yml").read_text())
    steps = workflow["jobs"]["dashboard"]["steps"]
    lock = tomllib.loads((ROOT / "uv.lock").read_text())
    packages, only_dev = _install(next(s["run"] for s in steps
                                       if s.get("run", "").startswith("uv sync")))
    selected = _selected(next(s["run"] for s in steps if s.get("name") == STEP))
    assert ROOT / "tests/production/delivery/test_static_member_cap_real_build.py" in selected
    workspace, distributions = _workspace(lock), packages_distributions()
    imports = _external_imports(selected)
    providers = {module: {_normal(name) for name in ([workspace[module]] if module in workspace
                                                     else distributions.get(module, []))}
                 for module in imports}
    closure = _closure(lock, packages, only_dev)
    assert sorted(module for module in imports if not providers[module] & closure) == []
    root = next(p["name"] for p in lock["package"] if p["source"] == {"virtual": "."})
    assert set(packages) <= {root} | {workspace[m] for m in imports if m in workspace}
