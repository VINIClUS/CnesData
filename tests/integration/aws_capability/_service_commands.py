"""Conversão segura das respostas da porta de comandos."""

from collections.abc import Mapping

from tests.integration.aws_capability._service_contracts import ApiError, CommandPort, Reply
from tests.integration.aws_capability.cli import Cli, Request
from tests.integration.aws_capability.contracts import (
    BudgetExhausted,
    RequestContext,
    ServiceFailure,
)


def call_cli(
    region: str,
    environment: Mapping[str, str],
    timeout: float,
    request: Request,
    context: RequestContext,
) -> Reply:
    service_region = "us-east-1" if request.service in {"sts", "iam"} else region
    return Cli(service_region, timeout, environment, context).call(request)


def require_data(reply: Reply) -> Mapping[str, object]:
    if not reply.ok or not isinstance(reply.data, Mapping):
        raise ServiceFailure("service=request_failed")
    return reply.data


def require_error(reply: Reply) -> ApiError:
    if reply.ok or reply.error is None:
        raise ServiceFailure("service=request_failed")
    return reply.error


def call_command(commands: CommandPort, request: Request, context: RequestContext) -> Reply:
    try:
        reply = commands.call(request, context)
    except BudgetExhausted:
        raise
    except Exception:
        raise ServiceFailure("service=request_failed") from None
    if not isinstance(reply, Reply):
        raise ServiceFailure("service=request_failed")
    return reply


def is_remote_not_found(reply: Reply) -> bool:
    return (
        not reply.ok
        and reply.error is not None
        and reply.error.origin == "service"
        and reply.error.code == "ResourceNotFoundException"
    )
