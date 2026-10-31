"""Contratos avaliados de isolamento, volumes e logs dos três modos ECS."""

import json
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.production.infra._processing_native import (
    A_RUN,
    B_RUN,
    MODULE_SOURCE,
    NativePlans,
    Node,
    Tofu,
    _configuration_address,
    _configuration_resources,
    _managed_resources,
    _module_resources,
)

MODES = ("unit", "recovery", "audit")
COMMANDS = {"unit": [], "recovery": ["recover-once"], "audit": ["dispatch-outbox-once"]}
MOUNT_PATHS = {"/tmp", "/logs", "/data"}  # noqa: S108
STORAGE_DEFAULT = (
    "https://docs.aws.amazon.com/AmazonECS/latest/developerguide/fargate-task-storage.html"
)
STORAGE_ARCHIVE_SHA256 = "d4a3a5c7399ebabfcab411dd1965e262c400af341ee3a4bdc397857737aa5956"


def _witnesses(native: NativePlans, root: Path, mode: str) -> Iterator[tuple[Node, Node, Node]]:
    for name in (A_RUN, B_RUN):
        plan = native.plans[name]
        tasks = [
            r
            for r in _module_resources(plan, native.configuration, root, MODULE_SOURCE)
            if r["type"] == "aws_ecs_task_definition"
        ]
        assert len(tasks) == 3, f"processing_task_count={len(tasks)}"
        assert {r.get("index") for r in tasks} == set(MODES), "processing_task_modes"
        selected = [r for r in tasks if r["index"] == mode]
        assert len(selected) == 1, f"processing_task_mode_count={mode}"
        task = selected[0]
        containers = json.loads(task["change"]["after"]["container_definitions"])
        assert len(containers) == 1, "processing_container_count"
        assert containers[0].get("name") == "processor", "processing_container_name"
        yield plan, task, containers[0]


def _record(context: Tofu, name: str, observations: list[Node]) -> None:
    (Path(context[1]["TF_DATA_DIR"]) / f"processing-{name}.json").write_text(
        json.dumps(observations, indent=2) + "\n", encoding="utf-8"
    )


@pytest.mark.parametrize("mode", MODES)
def test_task_exige_fargate_linux_e_orcamento_fixo(
    processing_native: NativePlans, prod_root: Path, mode: str
) -> None:
    for _, task, _ in _witnesses(processing_native, prod_root, mode):
        values = task["change"]["after"]
        assert values["requires_compatibilities"] == ["FARGATE"], "processing_fargate"
        assert values["network_mode"] == "awsvpc", "processing_network_mode"
        assert values["cpu"] == "256", "processing_cpu"
        assert values["memory"] == "512", "processing_memory"
        assert values["runtime_platform"] == [
            {"cpu_architecture": "X86_64", "operating_system_family": "LINUX"}
        ], "processing_platform"


@pytest.mark.parametrize("mode", MODES)
def test_container_exige_raiz_somente_leitura_usuario_app_e_sem_privilegios(
    processing_native: NativePlans, prod_root: Path, tofu_context: Tofu, mode: str
) -> None:
    observations = []
    for _, task, container in _witnesses(processing_native, prod_root, mode):
        assert container.get("readonlyRootFilesystem") is True, "processing_readonly"
        assert container.get("user") == "app", "processing_user"
        privileged = container.get("privileged")
        observations.append(
            {
                "address": task["address"],
                "privileged_present": "privileged" in container,
                "privileged_raw": container.get("privileged"),
                "privileged_effective": privileged,
                "omitted_default": "unestablished_requires_pinned_authority",
            }
        )
        _record(tofu_context, f"security-{mode}", observations)
        if "privileged" not in container:
            pytest.fail("setup_error processing_privileged_default_unestablished")
        assert privileged is False, "processing_privileged"
        linux = container.get("linuxParameters", {})
        assert linux.get("capabilities", {}).get("drop") == ["ALL"], "processing_drop_all"
        assert linux.get("capabilities", {}).get("add", []) == [], "processing_capabilities_add"
        assert not any(linux.get(k) for k in ("devices", "tmpfs", "initProcessEnabled")), (
            "processing_linux_workaround"
        )
        assert not linux.get("sharedMemorySize"), "processing_shared_memory"
        assert not container.get("volumesFrom"), "processing_volumes_from"


@pytest.mark.parametrize("mode", MODES)
def test_mounts_declaram_tres_volumes_efemeros_distintos_gravaveis(
    processing_native: NativePlans, prod_root: Path, mode: str
) -> None:
    for _, task, container in _witnesses(processing_native, prod_root, mode):
        mounts = container.get("mountPoints", [])
        assert len(mounts) == 3, "processing_mounts"
        assert {m.get("containerPath") for m in mounts} == MOUNT_PATHS, "processing_mount_paths"
        sources = [m.get("sourceVolume") for m in mounts]
        assert all(isinstance(s, str) and s for s in sources), "processing_mount_source"
        assert len(set(sources)) == 3, "processing_mount_source_alias"
        assert all(m.get("readOnly") is False for m in mounts), "processing_mount_readonly"
        volumes = task["change"]["after"].get("volume", [])
        assert len(volumes) == 3, "processing_volume_count"
        assert {v.get("name") for v in volumes} == set(sources), "processing_volume_reference"
        assert all(v.get("host_path") in (None, "") for v in volumes), "processing_volume_host"
        assert all(
            not v.get(k)
            for v in volumes
            for k in (
                "efs_volume_configuration",
                "docker_volume_configuration",
                "fsx_windows_file_server_volume_configuration",
                "host",
            )
        ), "processing_volume_external"
        assert all(not v.get("configure_at_launch") for v in volumes), "processing_volume_ebs"


def _log_group(plan: Node) -> Node:
    groups = [
        r
        for r in _managed_resources(plan, "aws_cloudwatch_log_group")
        if r["address"] == "aws_cloudwatch_log_group.processor"
    ]
    assert len(groups) == 1, "processing_log_group_producer_count"
    group = groups[0]["change"]["after"]
    assert group["name"] == "/ecs/cnesdata-prod-processor", "processing_log_group_name"
    assert group["retention_in_days"] == 7, "processing_log_retention"
    return group


@pytest.mark.parametrize("mode", MODES)
def test_logs_usam_grupo_proprio_regiao_e_prefixo_do_modo(
    processing_native: NativePlans, prod_root: Path, mode: str
) -> None:
    for plan, _, container in _witnesses(processing_native, prod_root, mode):
        logging = container.get("logConfiguration")
        assert isinstance(logging, dict), "processing_logs"
        assert logging.get("logDriver") == "awslogs", "processing_log_driver"
        assert logging.get("options") == {
            "awslogs-group": _log_group(plan)["name"],
            "awslogs-region": "us-east-2",
            "awslogs-stream-prefix": mode,
        }, "processing_log_options"
        assert not logging.get("secretOptions"), "processing_log_secrets"


@pytest.mark.parametrize("mode", MODES)
def test_storage_preserva_omissao_e_vinte_gib_incluidos(
    processing_native: NativePlans, prod_root: Path, tofu_context: Tofu, mode: str
) -> None:
    configured = dict(_configuration_resources(processing_native.configuration["root_module"]))
    observations = []
    for _, task, _ in _witnesses(processing_native, prod_root, mode):
        values = task["change"]["after"]
        resource = configured[_configuration_address(task["address"])]
        raw = values.get("ephemeral_storage", [])
        unknown = task["change"].get("after_unknown", {}).get("ephemeral_storage", [])
        assert raw == [], "processing_ephemeral_storage_budget"
        assert unknown in (False, []), "processing_ephemeral_storage_unknown"
        assert not resource.get("expressions", {}).get("ephemeral_storage"), (
            "processing_ephemeral_storage_configured"
        )
        observations.append(
            {
                "address": task["address"],
                "raw_present": "ephemeral_storage" in values,
                "raw_value": raw,
                "after_unknown": unknown,
                "configured_expression": resource.get("expressions", {}).get("ephemeral_storage"),
                "included_gib": 20,
                "documentation": STORAGE_DEFAULT,
                "documentation_archive_sha256": STORAGE_ARCHIVE_SHA256,
                "platform_requirement": "Fargate Linux >=1.4.0",
            }
        )
    _record(tofu_context, f"storage-{mode}", observations)
