"""Tentativas de probe: só status, código e booleanos viram observação."""

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime

from tests.integration.aws_capability._service_commands import call_command
from tests.integration.aws_capability._service_contracts import CommandPort, ServicePorts
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import RequestContext, ServiceFailure

CONTROL_ATTEMPTS = 12
CONTROL_SECONDS = 2.0


@dataclass(frozen=True)
class Attempt:
    ok: bool
    code: str
    data: Mapping[str, object] = field(default_factory=dict, repr=False)


def attempt(commands: CommandPort, request: Request, context: RequestContext) -> Attempt:
    """Returns: resultado remoto da chamada.

    Raises: ServiceFailure se a falha não vier do serviço remoto.
    """
    reply = call_command(commands, request, context)
    if reply.ok:
        return Attempt(True, "ok", reply.data)
    if reply.error is None or reply.error.origin != "service":
        raise ServiceFailure("probe=transport_failed")
    code = reply.error.code
    return Attempt(False, code if re.fullmatch(r"[A-Za-z0-9]{1,64}", code) else "Other")


def must(result: Attempt, step: str) -> Attempt:
    if not result.ok:
        raise ServiceFailure(f"probe=step_failed step={step}")
    return result


def version_of(result: Attempt) -> str:
    version = result.data.get("VersionId")
    if not isinstance(version, str) or not version:
        raise ServiceFailure("probe=step_failed step=version")
    return version


def stamp(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def retry_denied(
    action: Callable[[], Attempt], ports: ServicePorts, context: RequestContext, step: str
) -> tuple[Attempt, float]:
    """Returns: primeira tentativa bem-sucedida e os segundos esperados.

    Raises: ServiceFailure se o controle positivo não ficar verde dentro do limite.
    """
    start = ports.now()
    for index in range(CONTROL_ATTEMPTS):
        result = action()
        if result.ok:
            return result, round(ports.now() - start, 1)
        out_of_time = context.budget.remaining_seconds(context.lane) <= CONTROL_SECONDS + 1
        if result.code != "AccessDenied" or out_of_time or index == CONTROL_ATTEMPTS - 1:
            break
        ports.pause(CONTROL_SECONDS)
    raise ServiceFailure(f"probe=control_failed step={step}")
