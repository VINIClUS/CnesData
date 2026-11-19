"""Criação atômica do pool raiz Cognito."""

from collections.abc import Mapping

from tests.integration.aws_capability._safety import run_name, tags
from tests.integration.aws_capability._service_commands import call_command, require_data
from tests.integration.aws_capability._service_contracts import CommandPort
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    Creation,
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)


def create_pool(
    commands: CommandPort, spec: RunSpec, creation: Creation, context: RequestContext
) -> Resource:
    if (
        context.lane != "work"
        or creation.spec != spec
        or creation.name != run_name(spec)
        or creation.tags != tags(spec)
    ):
        raise ServiceFailure("service=cognito_creation_invalid")
    request = Request("cognito-idp", "create-user-pool", _payload(spec))
    data = require_data(call_command(commands, request, context))
    if "UserPool" not in data:
        raise ServiceFailure("service=not_implemented")
    pool = data.get("UserPool")
    if not isinstance(pool, Mapping):
        raise ServiceFailure("service=cognito_create_invalid")
    pool_id, name = pool.get("Id"), pool.get("Name")
    arn = pool.get("Arn")
    expected = f"arn:aws:cognito-idp:{spec.region}:{spec.account}:userpool/{pool_id}"
    if (
        not isinstance(pool_id, str)
        or not pool_id
        or not isinstance(name, str)
        or name != run_name(spec)
        or arn != expected
        or not pool_id.startswith(spec.region + "_")
    ):
        raise ServiceFailure("service=cognito_create_invalid")
    return Resource("cognito", pool_id, name)


def _payload(spec: RunSpec) -> dict[str, object]:
    return {
        "PoolName": run_name(spec),
        "UserPoolTier": "ESSENTIALS",
        "DeletionProtection": "ACTIVE",
        "MfaConfiguration": "OFF",
        "UsernameAttributes": ["email"],
        "UsernameConfiguration": {"CaseSensitive": False},
        "AdminCreateUserConfig": {"AllowAdminCreateUserOnly": True},
        "AccountRecoverySetting": {
            "RecoveryMechanisms": [{"Name": "verified_email", "Priority": 1}]
        },
        "Policies": {
            "PasswordPolicy": {
                "MinimumLength": 14,
                "RequireLowercase": True,
                "RequireUppercase": True,
                "RequireNumbers": True,
                "RequireSymbols": True,
            }
        },
        "UserPoolTags": tags(spec),
    }
