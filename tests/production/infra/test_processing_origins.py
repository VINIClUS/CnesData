"""Namespace genuíno exige fontes vinculadas e recusa metadados estrangeiros."""

import importlib
import json
import sys
from collections.abc import Callable
from hashlib import sha256
from importlib.machinery import ModuleSpec, NamespaceLoader
from pathlib import Path
from re import escape
from types import ModuleType
from typing import Any
from unittest.mock import patch

import pytest

from tests.production.infra._fresh_process import run_fresh_process

type Node = dict[str, Any]
type Attempt = dict[str, str]
type PathCase = tuple[str, list[str], list[str], str]
type Restoration = tuple[object, str, object]

NAME = "cnes_contracts.manifests"
FOREIGN_PACKAGE = f"setup_error processing_foreign_package={NAME}"
FOREIGN_MODULE = f"setup_error processing_foreign_module={NAME} origin=None"
EXPECTED_CONTROLS = (
    "namespace_foreign_only",
    "namespace_mixed_paths",
    "namespace_wrong_directory_within_root",
    "namespace_empty_paths",
    "namespace_spec_path_disagreement",
    "ordinary_module_without_file",
    "namespace_spec_name_disagreement",
    "namespace_without_bound_sources",
    "ordinary_digest_mismatch",
    "ordinary_foreign_file",
)


def _observe(binding: Node) -> Node:
    from tests.production.infra.test_processing_runtime import _origins

    return _origins(binding)


def _spec(module: ModuleType) -> ModuleSpec:
    spec = module.__spec__
    assert isinstance(spec, ModuleSpec), "processing_origins_genuine_spec"
    return spec


def _namespace_directory(binding: Node) -> Path:
    return (Path(binding["roots"][1]) / "cnes_contracts/manifests").resolve()


def _manifest_files(binding: Node) -> dict[str, str]:
    directory = _namespace_directory(binding)
    paths = sorted(directory.glob("*.py"))
    assert {p.name for p in paths} == {"outputs.py", "processing.py", "raw.py", "validation.py"}
    assert not (directory / "__init__.py").exists(), "processing_origins_namespace_init"
    actual = {str(p.resolve()): sha256(p.read_bytes()).hexdigest() for p in paths}
    bound = {p: digest for p, digest in binding["files"].items() if Path(p).parent == directory}
    assert actual == bound, "processing_origins_namespace_source_binding"
    return actual


def _namespace_metadata(module: ModuleType, binding: Node) -> Node:
    spec = _spec(module)
    expected = [str(_namespace_directory(binding))]
    paths = [str(Path(p).resolve()) for p in getattr(module, "__path__", ())]
    assert module.__name__ == spec.name == module.__package__ == NAME
    assert module.__file__ is None
    assert spec.origin is None
    assert spec.has_location is False
    assert isinstance(spec.loader, NamespaceLoader), "processing_origins_genuine_loader"
    assert module.__loader__ is spec.loader, "processing_origins_loader_identity"
    assert spec.submodule_search_locations is not None, "processing_origins_namespace_paths"
    assert paths == expected
    assert [str(Path(p).resolve()) for p in spec.submodule_search_locations] == expected
    return {
        "name": module.__name__,
        "spec_name": spec.name,
        "loader": type(spec.loader).__module__ + "." + type(spec.loader).__name__,
        "file": module.__file__,
        "origin": spec.origin,
        "has_location": spec.has_location,
        "package": paths,
    }


def _positive_evidence(observed: Node, binding: Node) -> None:
    directory = _namespace_directory(binding)
    files = _manifest_files(binding)
    assert observed[NAME] == {
        "file": None,
        "sha256": None,
        "kind": "namespace",
        "package": [str(directory)],
        "bound_files": files,
    }
    package_file = str(directory.parent / "__init__.py")
    assert observed["cnes_contracts"] == {
        "file": package_file,
        "sha256": binding["files"][package_file],
        "package": [str(directory.parent)],
    }
    for file, digest in files.items():
        assert observed[NAME + "." + Path(file).stem] == {
            "file": file,
            "sha256": digest,
            "package": [],
        }


def _refused(
    operation: str, invoke: Callable[[], object], attempts: list[Attempt], marker: str
) -> None:
    attempts.append({"operation": operation, "expected": marker})
    with pytest.raises(pytest.fail.Exception, match="^" + escape(marker) + "$"):
        invoke()
    attempts[-1]["outcome"] = "strict_refusal"


def _path_probe(module: ModuleType, binding: Node, attempts: list[Attempt], case: PathCase) -> None:
    operation, paths, spec_paths, marker = case
    with (
        patch.object(module, "__path__", paths),
        patch.object(_spec(module), "submodule_search_locations", spec_paths),
    ):
        _refused(operation, lambda: _observe(binding), attempts, marker)


def _path_controls(
    module: ModuleType, binding: Node, attempts: list[Attempt], temporary: Path
) -> None:
    foreign = temporary / "foreign"
    foreign.mkdir()
    valid = [str(_namespace_directory(binding))]
    within_root = Path(binding["roots"][1])
    assert within_root.is_dir()
    assert within_root.resolve() != _namespace_directory(binding)
    cases = [
        ("namespace_foreign_only", [str(foreign)]),
        ("namespace_mixed_paths", valid + [str(foreign)]),
        ("namespace_wrong_directory_within_root", [str(within_root)]),
        ("namespace_empty_paths", []),
    ]
    for operation, paths in cases:
        _path_probe(module, binding, attempts, (operation, paths, paths, FOREIGN_PACKAGE))
    _path_probe(
        module,
        binding,
        attempts,
        ("namespace_spec_path_disagreement", valid, [str(foreign)], FOREIGN_MODULE),
    )


def _identity_controls(module: ModuleType, binding: Node, attempts: list[Attempt]) -> None:
    package = sys.modules["cnes_contracts"]
    with patch.object(package, "__file__", None):
        _refused(
            "ordinary_module_without_file",
            lambda: _observe(binding),
            attempts,
            "setup_error processing_foreign_module=cnes_contracts origin=None",
        )
    with patch.object(_spec(module), "name", NAME + ".foreign"):
        _refused(
            "namespace_spec_name_disagreement",
            lambda: _observe(binding),
            attempts,
            FOREIGN_MODULE,
        )


def _binding_controls(binding: Node, attempts: list[Attempt]) -> None:
    directory = _namespace_directory(binding)
    remaining = {p: digest for p, digest in binding["files"].items() if Path(p).parent != directory}
    _refused(
        "namespace_without_bound_sources",
        lambda: _observe(binding | {"files": remaining}),
        attempts,
        FOREIGN_PACKAGE,
    )
    changed = dict(binding["files"])
    changed[str(directory.parent / "__init__.py")] = "0" * 64
    _refused(
        "ordinary_digest_mismatch",
        lambda: _observe(binding | {"files": changed}),
        attempts,
        "setup_error processing_import_digest=cnes_contracts",
    )


def _foreign_file_control(binding: Node, attempts: list[Attempt], temporary: Path) -> None:
    foreign = temporary / "unbound-origin.txt"
    foreign.write_text("origin-control\n")
    with patch.object(sys.modules["cnes_contracts"], "__file__", str(foreign)):
        _refused(
            "ordinary_foreign_file",
            lambda: _observe(binding),
            attempts,
            f"setup_error processing_foreign_module=cnes_contracts origin={foreign}",
        )


def _metadata_originals(module: ModuleType) -> list[Restoration]:
    spec = _spec(module)
    names = ("__name__", "__package__", "__spec__", "__loader__", "__file__", "__path__")
    targets = [(module, name) for name in names]
    targets += [(spec, "name"), (spec, "submodule_search_locations")]
    targets += [(sys.modules["cnes_contracts"], "__file__")]
    return [(owner, name, getattr(owner, name)) for owner, name in targets]


def _assert_restored(originals: list[Restoration]) -> None:
    for owner, name, original in originals:
        assert getattr(owner, name) is original, f"processing_origins_restore={name}"


def _negative_contract(
    module: ModuleType, binding: Node, attempts: list[Attempt], temporary: Path
) -> None:
    _path_controls(module, binding, attempts, temporary)
    _identity_controls(module, binding, attempts)
    _binding_controls(binding, attempts)
    _foreign_file_control(binding, attempts, temporary)
    assert len(attempts) == 10
    assert [row["operation"] for row in attempts] == list(EXPECTED_CONTROLS)
    assert all(row["outcome"] == "strict_refusal" for row in attempts)


def _loaded(roots: tuple[str, ...]) -> list[str]:
    return [name for name in sys.modules if name.split(".")[0] in roots]


def _test_aceita_namespace_real_e_rejeita_origens_nao_vinculadas(
    tmp_path: Path, record_testsuite_property: Callable[[str, object], None]
) -> None:
    from tests.production.infra.test_processing_io_guards import (
        SDK_ROOTS,
        SUT_ROOTS,
        _socket_sentinel,
    )
    from tests.production.infra.test_processing_runtime import _bindings

    assert not _loaded(SDK_ROOTS + SUT_ROOTS), "processing_origins_preloaded"
    events: list[str] = []
    attempts: list[Attempt] = []
    metadata: Node = {}
    observed: Node = {}
    originals: list[Restoration] = []
    old_path = sys.path[:]
    _socket_sentinel(events)
    try:
        binding = _bindings()
        sys.path[:0] = binding["roots"]
        module = importlib.import_module(NAME)
        metadata = _namespace_metadata(module, binding)
        originals = _metadata_originals(module)
        observed = _observe(binding)
        _positive_evidence(observed, binding)
        _negative_contract(module, binding, attempts, tmp_path)
        assert _observe(binding) == observed, "processing_origins_metadata_restore"
        assert _namespace_metadata(module, binding) == metadata
        assert not _loaded(SDK_ROOTS), "processing_origins_sdk_import"
        assert not _loaded(("data_processor", "cnes_domain", "cnes_infra"))
    finally:
        sys.path[:] = old_path
        record_testsuite_property("processing_origins_metadata", json.dumps(metadata))
        record_testsuite_property("processing_origins_positive", json.dumps(observed))
        record_testsuite_property("processing_origins_controls", json.dumps(attempts))
        record_testsuite_property("processing_origins_socket_audit_events", json.dumps(events))
        _assert_restored(originals)
        assert not events, f"processing_origins_real_socket_events={events}"


def test_aceita_namespace_real_e_rejeita_origens_nao_vinculadas(
    tmp_path: Path, record_testsuite_property: Callable[[str, object], None]
) -> None:
    node = (
        "tests/production/infra/test_processing_origins.py::"
        "test_aceita_namespace_real_e_rejeita_origens_nao_vinculadas"
    )
    if run_fresh_process(node, tmp_path):
        return
    _test_aceita_namespace_real_e_rejeita_origens_nao_vinculadas(
        tmp_path, record_testsuite_property
    )
    record_testsuite_property("fresh_process_completed", node)
