"""Leituras frescas de pertença e posse de inline policies IAM."""

import re
from collections.abc import Mapping

from tests.integration.aws_capability._resource_ids import parse_iam_child
from tests.integration.aws_capability._safety import owned
from tests.integration.aws_capability._service_commands import call_command, require_data
from tests.integration.aws_capability._service_contracts import CommandPort
from tests.integration.aws_capability._service_iam import _remote_no_entity, inspect_role
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    Ownership,
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)


def lookup_iam_child(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> Ownership | None:
    """Returns:
    Posse fresca após presença exata ou ausência remota da policy.
    Raises:
    ServiceFailure: identidade, posse ou leitura inválida.
    """
    root, policy_name = parse_iam_child(resource, spec)
    owner = inspect_role(commands, spec, root, context)
    if not owned(root, owner, spec):
        raise ServiceFailure("service=not_owned")
    reply = call_command(
        commands,
        Request(
            "iam", "get-role-policy", {"RoleName": root.resource_id, "PolicyName": policy_name}
        ),
        context,
    )
    if not reply.ok:
        if _remote_no_entity(reply):
            return None
        raise ServiceFailure("service=describe_failed")
    _require_policy(require_data(reply), root.resource_id, policy_name)
    return owner


def inspect_iam_child(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> Ownership:
    """Returns:
    Posse fresca após comprovar presença da inline policy.
    Raises:
    ServiceFailure: filho ausente ou leitura inválida.
    """
    owner = lookup_iam_child(commands, spec, resource, context)
    if owner is None:
        raise ServiceFailure("service=not_found")
    return owner


def _require_policy(data: Mapping[str, object], role: str, policy: str) -> None:
    document = data.get("PolicyDocument")
    if (
        data.get("RoleName") != role
        or data.get("PolicyName") != policy
        or not isinstance(document, str)
        or not 1 <= len(document) <= 131072
        or re.fullmatch(r"[\u0009\u000A\u000D\u0020-\u00FF]+", document) is None
    ):
        raise ServiceFailure("service=describe_invalid")
