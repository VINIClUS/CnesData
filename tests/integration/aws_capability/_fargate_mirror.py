"""Payload de register-task-definition espelhado de modules/processing/ecs.tf."""

# ruff: noqa: S108
from collections.abc import Mapping
from dataclasses import dataclass

REGION = "us-east-2"
MOUNTS = ("/tmp", "/logs", "/data")


@dataclass(frozen=True)
class TaskSettings:
    family: str
    image: str
    execution_role_arn: str
    task_role_arn: str
    log_group: str
    control_plane_table: str
    data_bucket: str
    state_machine_arn: str
    subnet_ids: tuple[str, ...]
    security_group_id: str
    tags: Mapping[str, str]


def _environment(settings: TaskSettings) -> list[dict[str, str]]:
    values = {
        "PROFILE": "aws",
        "AWS_PRODUCTION_POLICY": "true",
        "AWS_REGION": REGION,
        "AWS_ENVIRONMENT": "prod",
        "AWS_CONTROL_PLANE_TABLE": settings.control_plane_table,
        "BILLING_MODE": "disabled",
        "TMPDIR": "/tmp",
        "PYTHONDONTWRITEBYTECODE": "1",
        "AWS_DATA_BUCKET": settings.data_bucket,
        "AWS_STATE_MACHINE_ARN": settings.state_machine_arn,
        "AWS_PROCESSOR_CONTAINER_NAME": "processor",
        "AWS_PROCESSOR_MAX_CONCURRENCY": "1",
        "AWS_PROCESSOR_LEASE_SECONDS": "3600",
        "AWS_PUBLIC_SUBNET_IDS": ",".join(sorted(set(settings.subnet_ids))),
        "AWS_SECURITY_GROUP_ID": settings.security_group_id,
        "AWS_PROCESSOR_RECOVERY_BATCH_SIZE": "100",
    }
    return [{"name": name, "value": value} for name, value in sorted(values.items())]


def _container(
    settings: TaskSettings, entry_point: tuple[str, ...] | None
) -> dict[str, object]:
    container: dict[str, object] = {
        "name": "processor",
        "image": settings.image,
        "command": ["recover-once"],
        "readonlyRootFilesystem": True,
        "user": "app",
        "privileged": False,
        "linuxParameters": {"capabilities": {"drop": ["ALL"]}},
        "environment": _environment(settings),
        "mountPoints": [
            {"containerPath": path, "sourceVolume": path.lstrip("/"), "readOnly": False}
            for path in MOUNTS
        ],
        "logConfiguration": {
            "logDriver": "awslogs",
            "options": {
                "awslogs-group": settings.log_group,
                "awslogs-region": REGION,
                "awslogs-stream-prefix": "recovery",
            },
        },
    }
    if entry_point:
        container["entryPoint"] = list(entry_point)
    return container


def task_definition_payload(
    settings: TaskSettings, entry_point: tuple[str, ...] | None = None
) -> dict[str, object]:
    """Returns: payload do modo recovery; entry_point só para o probe de escrita."""
    return {
        "family": settings.family,
        "requiresCompatibilities": ["FARGATE"],
        "networkMode": "awsvpc",
        "cpu": "256",
        "memory": "512",
        "executionRoleArn": settings.execution_role_arn,
        "taskRoleArn": settings.task_role_arn,
        "runtimePlatform": {"cpuArchitecture": "X86_64", "operatingSystemFamily": "LINUX"},
        "volumes": [{"name": path.lstrip("/")} for path in MOUNTS],
        "containerDefinitions": [_container(settings, entry_point)],
        "tags": [{"key": key, "value": value} for key, value in settings.tags.items()],
    }
