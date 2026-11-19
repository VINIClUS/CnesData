"""Ambiente e argv nativos chegam aos seletores e settings públicos reais."""

import importlib
import json
import os
import sys
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import asdict
from hashlib import sha256
from importlib.machinery import ModuleSpec, NamespaceLoader
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from tests.production.infra._processing_io_guards import _io_guards
from tests.production.infra._processing_native import NativePlans, Node, Tofu, _managed_resources
from tests.production.infra.test_processing_security import COMMANDS, MODES, _record, _witnesses

COMMON = {
    "PROFILE": "aws",
    "AWS_PRODUCTION_POLICY": "true",
    "AWS_REGION": "us-east-2",
    "AWS_ENVIRONMENT": "prod",
    "BILLING_MODE": "disabled",
    "TMPDIR": "/tmp",  # noqa: S108
    "PYTHONDONTWRITEBYTECODE": "1",
}
INPUTS = {
    "control_plane_table": "module.control_plane.table_name",
    "data_bucket": "module.data_buckets.data_bucket_name",
    "audit_bucket": "module.data_buckets.audit_bucket_name",
    "audit_retention_days": "module.data_buckets.audit_retention_days",
    "public_subnet_ids": "module.network.public_subnet_ids",
    "security_group_id": "module.network.security_group_id",
    "log_group_name": "aws_cloudwatch_log_group.processor.name",
}
ENVELOPE = {
    "TENANT_ID": "runtime-tenant",
    "RUN_ID": "runtime-run",
    "WAVE_ID": "0123456789abcdef",
    "DISPATCH_ID": "fedcba9876543210",
    "UNIT_ID": "runtime-unit",
    "LEASE_SECONDS": "3600",
}
NAMESPACES = ("data_processor", "cnes_contracts", "cnes_domain", "cnes_infra")


def _producer(plan: Node, address: str) -> Node:
    found = [r for r in plan["resource_changes"] if r["address"] == address]
    assert len(found) == 1, f"processing_env_producer_count={address}"
    assert found[0]["mode"] == "managed", f"processing_env_producer_mode={address}"
    return found[0]["change"]["after"]


def _expected(plan: Node, mode: str) -> dict[str, str]:
    values = COMMON | {
        "AWS_CONTROL_PLANE_TABLE": _producer(
            plan, "module.control_plane.aws_dynamodb_table.control_plane"
        )["name"]
    }
    if mode == "audit":
        lock = _producer(plan, "module.data_buckets.aws_s3_bucket_object_lock_configuration.audit")
        retention = lock["rule"][0]["default_retention"][0]
        assert retention == {"days": 365, "mode": "COMPLIANCE", "years": None}, (
            "processing_env_audit_retention_producer"
        )
        return values | {
            "AWS_AUDIT_BUCKET": _producer(plan, "module.data_buckets.aws_s3_bucket.audit")[
                "bucket"
            ],
            "AWS_AUDIT_RETENTION_DAYS": str(retention["days"]),
        }
    subnets = [
        r["change"]["after"]["id"]
        for r in _managed_resources(plan, "aws_subnet")
        if r["address"].startswith("module.network.aws_subnet.public[")
    ]
    assert subnets, "processing_env_subnet_producers"
    assert len(subnets) == len(set(subnets)), "processing_env_subnet_duplicates"
    account = plan["variables"]["shared_outputs"]["value"]["account_id"]
    values |= {
        "AWS_DATA_BUCKET": _producer(plan, "module.data_buckets.aws_s3_bucket.data")["bucket"],
        "AWS_STATE_MACHINE_ARN": f"arn:aws:states:us-east-2:{account}:stateMachine:cnesdata-prod",
        "AWS_PROCESSOR_CONTAINER_NAME": "processor",
        "AWS_PROCESSOR_MAX_CONCURRENCY": "1",
        "AWS_PROCESSOR_LEASE_SECONDS": "3600",
        "AWS_PUBLIC_SUBNET_IDS": ",".join(sorted(subnets)),
        "AWS_SECURITY_GROUP_ID": _producer(plan, "module.network.aws_security_group.tasks")["id"],
    }
    return values | ({"AWS_PROCESSOR_RECOVERY_BATCH_SIZE": "100"} if mode == "recovery" else {})


def _environment(container: Node, expected: dict[str, str], marker: str) -> dict[str, str]:
    entries = container.get("environment", [])
    assert entries, marker
    assert all(set(e) == {"name", "value"} for e in entries), "processing_env_entry"
    names = [e["name"] for e in entries]
    assert len(names) == len(set(names)), "processing_env_duplicate"
    values = {e["name"]: e["value"] for e in entries}
    assert set(values) == set(expected), f"processing_env_keys={sorted(values)}"
    assert values == expected, f"processing_env_values expected={expected!r} actual={values!r}"
    assert not container.get("secrets"), "processing_env_secrets"
    return values


def _wiring(native: NativePlans) -> None:
    calls = native.configuration["root_module"]["module_calls"]
    expressions = calls["processing"]["expressions"]
    for name, reference in INPUTS.items():
        assert reference in expressions.get(name, {}).get("references", []), (
            f"processing_env_reference={name}"
        )
    state = expressions.get("state_machine_arn", {}).get("references", [])
    assert "var.shared_outputs.account_id" in state, "processing_env_state_account_reference"
    producers = {
        ("control_plane", "table_name"): "aws_dynamodb_table.control_plane.name",
        ("data_buckets", "data_bucket_name"): "aws_s3_bucket.data.bucket",
        ("data_buckets", "audit_bucket_name"): "aws_s3_bucket.audit.bucket",
        ("data_buckets", "audit_retention_days"): "local.audit_default_retention.days",
        ("network", "public_subnet_ids"): "aws_subnet.public",
        ("network", "security_group_id"): "aws_security_group.tasks.id",
    }
    for (module, output), reference in producers.items():
        observed = calls[module]["module"]["outputs"][output]["expression"].get("references", [])
        assert reference in observed, f"processing_env_output_reference={module}.{output}"


@pytest.mark.parametrize("mode", MODES)
def test_ambiente_e_argv_correspondem_aos_produtores_reais(
    processing_native: NativePlans, prod_root: Path, mode: str
) -> None:
    for plan, _, container in _witnesses(processing_native, prod_root, mode):
        _environment(container, _expected(plan, mode), "processing_env_missing")
        assert container.get("command", []) == COMMANDS[mode], "processing_command"
        _wiring(processing_native)


def _bindings() -> Node:
    path = os.environ.get("PROCESSING_SOURCE_BINDINGS")
    if not path:
        pytest.fail("setup_error processing_source_bindings=missing")
    binding = json.loads(Path(path).read_text())
    roots = binding["roots"]
    if len(roots) != 4 or len(set(roots)) != 4:
        pytest.fail("setup_error processing_source_roots=count")
    for name, root in zip(NAMESPACES, roots, strict=True):
        if not Path(root).is_absolute() or not (Path(root) / name).is_dir():
            pytest.fail(f"setup_error processing_source_root={root}")
    for file, digest in binding["files"].items():
        if sha256(Path(file).read_bytes()).hexdigest() != digest:
            pytest.fail(f"setup_error processing_source_digest={file}")
    return binding


def _namespace_spec(name: str, module: ModuleType) -> ModuleSpec:
    spec = module.__spec__
    if (
        not isinstance(spec, ModuleSpec)
        or not isinstance(spec.loader, NamespaceLoader)
        or module.__loader__ is not spec.loader
        or module.__name__ != name
        or spec.name != name
        or module.__package__ != name
        or spec.origin is not None
        or spec.has_location is not False
    ):
        pytest.fail(f"setup_error processing_foreign_module={name} origin=None")
    return spec


def _namespace_paths(name: str, module: ModuleType, spec: ModuleSpec) -> list[Any]:
    locations = spec.submodule_search_locations
    if locations is None:
        pytest.fail(f"setup_error processing_foreign_package={name}")
    try:
        paths = list(getattr(module, "__path__", ()))
        search = list(locations)
    except TypeError:
        pytest.fail(f"setup_error processing_foreign_package={name}")
    if paths != search:
        pytest.fail(f"setup_error processing_foreign_module={name} origin=None")
    return paths


def _namespace_origin(binding: Node, name: str, module: ModuleType, root: Path) -> Node:
    spec = _namespace_spec(name, module)
    paths = _namespace_paths(name, module, spec)
    directory = root.joinpath(*name.split(".")).resolve()
    try:
        invalid = (
            len(paths) != 1
            or not isinstance(paths[0], str)
            or not Path(paths[0]).is_absolute()
            or Path(paths[0]).resolve() != directory
            or not Path(paths[0]).is_dir()
            or (directory / "__init__.py").exists()
        )
    except (OSError, ValueError):
        invalid = True
    if invalid:
        pytest.fail(f"setup_error processing_foreign_package={name}")
    files = {
        file: digest
        for file, digest in binding["files"].items()
        if Path(file).suffix == ".py" and Path(file).resolve().is_relative_to(directory)
    }
    if not files:
        pytest.fail(f"setup_error processing_foreign_package={name}")
    return {
        "file": None,
        "sha256": None,
        "kind": "namespace",
        "package": [str(directory)],
        "bound_files": files,
    }


def _origins(binding: Node) -> Node:
    observed = {}
    for name, module in tuple(sys.modules.items()):
        if name.split(".")[0] not in NAMESPACES or module is None:
            continue
        file = getattr(module, "__file__", None)
        if file is None:
            root = Path(binding["roots"][NAMESPACES.index(name.split(".")[0])]).resolve()
            observed[name] = _namespace_origin(binding, name, module, root)
            continue
        if str(Path(file).resolve()) not in binding["files"]:
            pytest.fail(f"setup_error processing_foreign_module={name} origin={file}")
        digest = sha256(Path(file).read_bytes()).hexdigest()
        if digest != binding["files"][str(Path(file).resolve())]:
            pytest.fail(f"setup_error processing_import_digest={name}")
        root = Path(binding["roots"][NAMESPACES.index(name.split(".")[0])]).resolve()
        package = [str(Path(p).resolve()) for p in getattr(module, "__path__", [])]
        if not Path(file).resolve().is_relative_to(root) or any(
            not Path(p).is_relative_to(root) for p in package
        ):
            pytest.fail(f"setup_error processing_foreign_package={name}")
        observed[name] = {"file": str(Path(file).resolve()), "sha256": digest, "package": package}
    return observed


@contextmanager
def _pure_apis(context: Tofu, label: str) -> Iterator[Node]:
    binding, calls = _bindings(), []
    bootstrap: list[dict[str, object]] = []
    _origins(binding)
    old_path = sys.path[:]
    try:
        sys.path[:0] = binding["roots"]
        with ExitStack() as stack:
            _io_guards(stack, calls, bootstrap)
            selector = importlib.import_module("data_processor.aws_entrypoint")
            settings = importlib.import_module("cnes_infra.aws.processor_settings")
            api = {
                "select": selector.select_aws_invocation,
                "selector_error": selector.EntrypointConfigurationError,
                "settings_error": settings.AwsRuntimeConfigurationError,
            }
            api |= dict(
                zip(
                    MODES,
                    (
                        settings.AwsUnitSettings,
                        settings.AwsRecoverySettings,
                        settings.AwsAuditDispatchSettings,
                    ),
                    strict=True,
                )
            )
            yield api
            origins = _origins(binding)
            assert not calls, "setup_error processing_external_calls"
            _record(
                context,
                f"runtime-imports-{label}",
                [{"origins": origins, "external_calls": calls, "bootstrap_refusals": bootstrap}],
            )
    except ImportError as error:
        pytest.fail(f"setup_error processing_import={error}")
    finally:
        sys.path[:] = old_path


def _unit_envelope(values: dict[str, str]) -> dict[str, str]:
    owner = values["AWS_STATE_MACHINE_ARN"].replace(":stateMachine:", ":execution:") + ":runtime"
    return ENVELOPE | {"EXECUTION_OWNER": owner}


def _settings_witness(settings: Any, values: dict[str, str], mode: str) -> None:
    assert settings.region == values["AWS_REGION"], "processing_runtime_region"
    assert settings.control_plane_table == values["AWS_CONTROL_PLANE_TABLE"], "runtime_table"
    assert settings.production_policy is True, "processing_runtime_production"
    assert settings.dynamodb_endpoint_url is None, "processing_runtime_dynamodb_endpoint"
    assert settings.service_endpoint_url is None, "processing_runtime_service_endpoint"
    if mode == "audit":
        assert settings.audit_bucket == values["AWS_AUDIT_BUCKET"], "runtime_audit_bucket"
        assert settings.audit_retention_days == 365, "processing_runtime_retention"
        return
    assert settings.environment == "prod", "processing_runtime_environment"
    assert settings.data_bucket == values["AWS_DATA_BUCKET"], "processing_runtime_data_bucket"
    assert settings.state_machine_arn == values["AWS_STATE_MACHINE_ARN"], "runtime_state_machine"
    assert settings.processor_container_name == "processor", "processing_runtime_container"
    assert settings.processor_max_concurrency == 1, "processing_runtime_concurrency"
    assert settings.processor_lease_seconds == 3600, "processing_runtime_lease"
    assert settings.public_subnet_ids == tuple(values["AWS_PUBLIC_SUBNET_IDS"].split(",")), (
        "processing_runtime_subnets"
    )
    assert settings.security_group_id == values["AWS_SECURITY_GROUP_ID"], "runtime_security_group"


def _reject_setting(
    api: Node, values: dict[str, str], mode: str, defect: tuple[str, str, str]
) -> None:
    key, value, message = defect
    with pytest.raises(api["settings_error"], match=message):
        api[mode].from_mapping(values | {key: value})


def _unit_controls(api: Node, values: dict[str, str], mode: str) -> Node:
    lease_error = (
        "production_processor_lease_seconds=invalid"
        if mode == "unit"
        else ("fixed=AWS_PROCESSOR_LEASE_SECONDS expected=3600")
    )
    _reject_setting(api, values, mode, ("AWS_PROCESSOR_LEASE_SECONDS", "300", lease_error))
    _reject_setting(
        api,
        values,
        mode,
        ("AWS_PROCESSOR_MAX_CONCURRENCY", "2", "production_processor_max_concurrency=invalid"),
    )
    changed = api[mode].from_mapping(values | {"AWS_PROCESSOR_CONTAINER_NAME": "other"})
    with pytest.raises(AssertionError, match="processing_runtime_container"):
        _settings_witness(changed, values, mode)
    ids = values["AWS_PUBLIC_SUBNET_IDS"].split(",")
    _reject_setting(
        api,
        values,
        mode,
        ("AWS_PUBLIC_SUBNET_IDS", ",".join(ids + ids[:1]), "public_subnets_not_normalized"),
    )
    if mode == "recovery":
        _reject_setting(
            api,
            values,
            mode,
            (
                "AWS_PROCESSOR_RECOVERY_BATCH_SIZE",
                "99",
                "fixed=AWS_PROCESSOR_RECOVERY_BATCH_SIZE expected=100",
            ),
        )
    if len(ids) > 1:
        _reject_setting(
            api,
            values,
            mode,
            ("AWS_PUBLIC_SUBNET_IDS", ",".join(reversed(ids)), "public_subnets_not_normalized"),
        )
    return {"unsorted_csv": "observed" if len(ids) > 1 else "held_requires_two_real_subnets"}


def _settings_controls(api: Node, values: dict[str, str], mode: str) -> Node:
    missing = values.copy()
    del missing["AWS_CONTROL_PLANE_TABLE"]
    with pytest.raises(api["settings_error"], match="missing=AWS_CONTROL_PLANE_TABLE"):
        api[mode].from_mapping(missing)
    _reject_setting(api, values, mode, ("AWS_REGION", "us-west-2", "production_region=invalid"))
    if mode != "audit":
        return _unit_controls(api, values, mode)
    _reject_setting(
        api, values, mode, ("AWS_AUDIT_RETENTION_DAYS", "364", "production_audit_retention=invalid")
    )
    return {"unsorted_csv": "not_applicable"}


def _leak_controls(api: Node, values: dict[str, str], mode: str, envelope: Node) -> None:
    for key in envelope:
        message = (
            "unit_envelope=forbidden"
            if mode == "audit"
            else ("unit_mode=argv_forbidden" if key == "UNIT_ID" else "unit_envelope=partial")
        )
        for leaked in ("", envelope[key]):
            with pytest.raises(api["selector_error"], match=message):
                api["select"](values | {key: leaked}, COMMANDS[mode])


def _selector_controls(api: Node, values: dict[str, str], mode: str) -> None:
    witness = values | (_unit_envelope(values) if mode == "unit" else {})
    bad_command = "unit_mode=argv_forbidden" if mode == "unit" else "command=recover_once_required"
    with pytest.raises(api["selector_error"], match=bad_command):
        api["select"](witness, ["invalid-mode"])
    envelope = _unit_envelope(values) if mode == "unit" else ENVELOPE | {"EXECUTION_OWNER": "owner"}
    if mode != "unit":
        _leak_controls(api, values, mode, envelope)
        return
    for key in envelope:
        missing = witness.copy()
        del missing[key]
        message = "unit_envelope=partial" if key == "UNIT_ID" else f"missing={key}"
        with pytest.raises(api["selector_error"], match=message):
            api["select"](missing, [])
    accepted = api["select"](witness | {"LEASE_SECONDS": "300"}, [])
    assert accepted.envelope.lease_seconds == 300, "processing_generic_envelope_lease"


def _run_runtime_mode(
    processing_native: NativePlans, prod_root: Path, tofu_context: Tofu, mode: str
) -> None:
    observations = []
    for plan, task, container in _witnesses(processing_native, prod_root, mode):
        values = _environment(container, _expected(plan, mode), "processing_runtime_env_missing")
        assert container.get("command", []) == COMMANDS[mode], "processing_runtime_command"
        _wiring(processing_native)
        with _pure_apis(tofu_context, f"{mode}-{task['change']['after']['revision']}") as api:
            witness = values | (_unit_envelope(values) if mode == "unit" else {})
            invocation = api["select"](witness, container.get("command", []))
            expected_mode = {
                "unit": "run-unit",
                "recovery": "recover-once",
                "audit": "dispatch-outbox-once",
            }[mode]
            assert invocation.mode.value == expected_mode, "processing_runtime_mode"
            if mode == "unit":
                envelope = _unit_envelope(values)
                expected: Node = {key.lower(): value for key, value in envelope.items()}
                expected["lease_seconds"] = 3600
                assert asdict(invocation.envelope) == expected, "processing_runtime_envelope"
            else:
                assert invocation.envelope is None, "processing_runtime_envelope_forbidden"
            settings = api[mode].from_mapping(values)
            assert type(settings) is api[mode], "processing_runtime_settings_type"
            _settings_witness(settings, values, mode)
            _selector_controls(api, values, mode)
            controls = _settings_controls(api, values, mode)
            observations.append({"mode": mode, "controls": controls, "settings": asdict(settings)})
    _record(tofu_context, f"runtime-{mode}", observations)


def _validate_parent_native(
    processing_native: NativePlans, prod_root: Path, mode: str
) -> None:
    witnesses = list(_witnesses(processing_native, prod_root, mode))
    assert len(witnesses) == 2, f"processing_runtime_witnesses={len(witnesses)}"
    for plan, _, container in witnesses:
        _environment(container, _expected(plan, mode), "processing_runtime_env_missing")
        assert container.get("command", []) == COMMANDS[mode], "processing_runtime_command"
        _wiring(processing_native)


def _spawn_runtime_mode(
    processing_native: NativePlans, prod_root: Path, tofu_context: Tofu, mode: str
) -> None:
    import multiprocessing

    completion = Path(tofu_context[1]["TF_DATA_DIR"]) / f"processing-runtime-{mode}.json"
    assert not completion.exists(), f"processing_runtime_completion_preexisting={mode}"
    context = multiprocessing.get_context("spawn")
    child = context.Process(
        target=_run_runtime_mode,
        args=(processing_native, prod_root, tofu_context, mode),
    )
    child.start()
    timed_out = False
    try:
        child.join(60)
        timed_out = child.is_alive()
    finally:
        if child.is_alive():
            child.terminate()
            child.join(5)
        if child.is_alive():
            child.kill()
            child.join(5)
        assert not child.is_alive(), f"processing_runtime_child_alive={mode}"
        exitcode = child.exitcode
        child.close()
    assert not timed_out, f"processing_runtime_child_timeout={mode}"
    assert exitcode == 0, f"processing_runtime_child_exit={mode}:{exitcode}"
    assert completion.is_file(), f"processing_runtime_completion_missing={mode}"
    observations = json.loads(completion.read_text(encoding="utf-8"))
    assert isinstance(observations, list)
    assert len(observations) == 2, f"processing_runtime_observations={mode}"
    assert [item.get("mode") for item in observations] == [mode, mode]


@pytest.mark.parametrize("mode", MODES)
def test_ambiente_nativo_alimenta_seletor_e_settings_publicos_sem_io(
    processing_native: NativePlans, prod_root: Path, tofu_context: Tofu, mode: str
) -> None:
    _validate_parent_native(processing_native, prod_root, mode)
    _spawn_runtime_mode(processing_native, prod_root, tofu_context, mode)
