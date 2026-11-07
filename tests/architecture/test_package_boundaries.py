"""Fronteiras de dependência das bibliotecas do monorepo."""

import ast
import tomllib
from graphlib import TopologicalSorter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ALLOWED = {
    "cnes_contracts": set(),
    "cnes_domain": {"cnes_contracts"},
    "cnes_observability": set(),
    "cnes_data": {"cnes_contracts", "cnes_domain", "cnes_observability"},
    "cnes_identity": {"cnes_contracts", "cnes_domain", "cnes_data"},
    "cnes_governance": {"cnes_domain", "cnes_data"},
    "cnes_control_plane": {"cnes_contracts", "cnes_domain", "cnes_data", "cnes_observability"},
    "cnes_runtime": {
        "cnes_contracts", "cnes_domain", "cnes_observability", "cnes_data",
        "cnes_identity", "cnes_governance", "cnes_control_plane",
    },
}


def test_workspace_contem_somente_os_oito_pacotes():
    actual = {p.parent.name for p in (ROOT / "packages").glob("*/pyproject.toml")}
    assert actual == set(ALLOWED)


def test_codigo_fica_diretamente_na_pasta_do_pacote():
    for package in ALLOWED:
        directory = ROOT / "packages" / package
        assert (directory / "__init__.py").is_file(), package
        assert not (directory / "src").exists(), package


def test_testes_de_apps_nao_sombreiam_namespace_compartilhado():
    for app in (ROOT / "apps").iterdir():
        if not (app / "__init__.py").exists():
            assert not (app / "tests" / "__init__.py").exists(), app.name


def _dependencies(source: Path) -> set[str]:
    names = set()
    for node in ast.walk(ast.parse(source.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
            names.add(node.module)
        elif isinstance(node, ast.Call) and node.args:
            function = getattr(node.func, "attr", getattr(node.func, "id", ""))
            argument = node.args[0]
            if function in {"import_module", "__import__"} and isinstance(argument, ast.Constant):
                names.add(str(argument.value))
    return {name.split(".")[0] for name in names}


def _package_dependencies(package: str) -> set[str]:
    directory = ROOT / "packages" / package
    dependencies = set()
    for source in directory.rglob("*.py"):
        if "tests" not in source.relative_to(directory).parts:
            dependencies.update(_dependencies(source))
    return dependencies - {package}


def test_bibliotecas_respeitam_fronteiras_inclusive_type_checking():
    apps = {p.parent.name for p in (ROOT / "apps").glob("*/src")}
    for package, allowed in ALLOWED.items():
        dependencies = _package_dependencies(package)
        assert not dependencies.intersection(apps | {"cnes_infra"}), package
        assert dependencies.intersection(ALLOWED) <= allowed, package


def test_grafo_de_dependencias_nao_contem_ciclos():
    assert set(TopologicalSorter(ALLOWED).static_order()) == set(ALLOWED)


def test_core_e_contracts_nao_importam_io_ou_telemetria():
    forbidden = {"sqlalchemy", "boto3", "botocore", "httpx", "requests", "dotenv",
                 "opentelemetry", "psycopg", "fdb", "os"}
    assert not _package_dependencies("cnes_domain").intersection(forbidden)
    assert not _package_dependencies("cnes_contracts").intersection(forbidden)


def test_guard_detecta_imports_dinamicos_e_sob_type_checking(tmp_path):
    source = tmp_path / "invalid.py"
    source.write_text(
        "if TYPE_CHECKING:\n    from central_api import app\n"
        "importlib.import_module('cnes_runtime.config')\n"
        "__import__('cnes_infra')\n", encoding="utf-8",
    )
    assert _dependencies(source) == {"central_api", "cnes_runtime", "cnes_infra"}


def test_dependencias_locais_declaradas_respeitam_o_grafo():
    for package, allowed in ALLOWED.items():
        manifest = ROOT / "packages" / package / "pyproject.toml"
        assert manifest.exists(), package
        project = tomllib.loads(manifest.read_text(encoding="utf-8"))["project"]
        dependencies = {name.split("[")[0].replace("-", "_")
                        for name in project.get("dependencies", [])}
        assert dependencies.intersection(ALLOWED) <= allowed, package
        assert _package_dependencies(package).intersection(ALLOWED) <= dependencies, package
        assert (manifest.parent / "CLAUDE.md").exists(), package


def test_sonar_classifica_testes_de_todas_capabilities_e_backup():
    properties = dict(
        line.split("=", 1) for line in (ROOT / "sonar-project.properties").read_text().splitlines()
        if line.startswith(("sonar.sources=", "sonar.tests="))
    )
    test_roots = [ROOT / value for value in properties["sonar.tests"].split(",")]
    source_roots = [ROOT / value for value in properties["sonar.sources"].split(",")]
    for package in ALLOWED:
        path = ROOT / "packages" / package / "tests"
        assert any(path.is_relative_to(root) for root in test_roots), package
    backup = ROOT / "ops" / "backup"
    assert any(backup.is_relative_to(root) for root in source_roots)
    assert any(backup.is_relative_to(root) for root in test_roots)
