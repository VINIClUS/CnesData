"""Payload de register-task-definition espelhado de modules/processing/ecs.tf."""

from collections.abc import Mapping
from dataclasses import dataclass

from tests.integration.aws_capability.contracts import ServiceFailure


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


def task_definition_payload(
    settings: TaskSettings, entry_point: tuple[str, ...] | None = None
) -> dict[str, object]:
    """Returns: payload do modo recovery; entry_point só para o probe de escrita."""
    raise ServiceFailure("service=not_implemented")
