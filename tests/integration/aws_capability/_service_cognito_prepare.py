"""Readback de prontidão do pool Cognito."""

from tests.integration.aws_capability._safety import run_name
from tests.integration.aws_capability._service_cognito_settings import (
    read_snapshot,
    validate_snapshot,
)
from tests.integration.aws_capability._service_contracts import CommandPort
from tests.integration.aws_capability.contracts import (
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)


def prepare_pool(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> None:
    if (
        context.lane != "work"
        or spec.probe not in {"cognito-login", "cognito-pkce-negatives", "cognito-sdk"}
        or resource.service != "cognito"
        or resource.name != run_name(spec)
    ):
        raise ServiceFailure("probe=cognito_prepare_invalid")
    pool, tags = read_snapshot(commands, spec, resource, context)
    validate_snapshot(pool, tags, spec, "ACTIVE")
    raise ServiceFailure("probe=not_implemented")
