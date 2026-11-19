"""Espera limitada pela ausência remota de um recurso recém-removido."""

from tests.integration.aws_capability._service_commands import call_command
from tests.integration.aws_capability._service_contracts import ApiError, ServicePorts
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import RequestContext, ServiceFailure

POLL_ATTEMPTS = 5
POLL_SECONDS = 1.0


def await_absent(
    ports: ServicePorts, probe: Request, absent_code: str, context: RequestContext
) -> None:
    """Raises: ServiceFailure se a leitura falhar ou o recurso seguir visível."""
    for attempt in range(POLL_ATTEMPTS):
        reply = call_command(ports.commands, probe, context)
        if not reply.ok:
            if _is_absent(reply.error, absent_code):
                return
            raise ServiceFailure("service=describe_failed")
        if attempt < POLL_ATTEMPTS - 1:
            ports.pause(POLL_SECONDS)
    raise ServiceFailure("service=delete_pending")


def _is_absent(error: ApiError | None, code: str) -> bool:
    return error is not None and error.origin == "service" and error.code == code
