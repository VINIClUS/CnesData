"""Sessão do role de teste: credenciais só em memória, nunca persistidas."""

from collections.abc import Mapping

from tests.integration.aws_capability._probe_attempt import attempt, retry_denied
from tests.integration.aws_capability._safety import run_name
from tests.integration.aws_capability._service_contracts import CommandPort, ServicePorts
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import RequestContext, RunSpec, ServiceFailure

SESSION_SECONDS = 900
_KEYS = {
    "AWS_ACCESS_KEY_ID": "AccessKeyId",
    "AWS_SECRET_ACCESS_KEY": "SecretAccessKey",
    "AWS_SESSION_TOKEN": "SessionToken",
}


def assume_role_session(
    ports: ServicePorts, spec: RunSpec, context: RequestContext
) -> tuple[CommandPort, float]:
    """Returns: porta de comandos do role e segundos até o trust propagar.

    Raises: ServiceFailure sem detalhes de credencial.
    """
    request = assume_request(spec, run_name(spec))
    result, seconds = retry_denied(
        lambda: attempt(ports.commands, request, context), ports, context, "assume"
    )
    return open_session(ports, result.data), seconds


def assume_request(
    spec: RunSpec, name: str, override: Mapping[str, object] | None = None
) -> Request:
    """Returns: AssumeRole base de 900 s; override troca ou acrescenta chaves."""
    payload: dict[str, object] = {
        "RoleArn": f"arn:aws:iam::{spec.account}:role/cnesdata-cap/{name}",
        "RoleSessionName": f"cap-{spec.run_id}",
        "DurationSeconds": SESSION_SECONDS,
    }
    payload.update(override or {})
    return Request("sts", "assume-role", payload)


def open_session(ports: ServicePorts, data: Mapping[str, object]) -> CommandPort:
    """Raises: ServiceFailure sem porta de sessão ou com credenciais inválidas."""
    if ports.session is None:
        raise ServiceFailure("probe=session_unavailable")
    return ports.session(_credentials(data))


def _credentials(data: Mapping[str, object]) -> dict[str, str]:
    raw = data.get("Credentials")
    if not isinstance(raw, Mapping):
        raise ServiceFailure("probe=session_invalid")
    credentials: dict[str, str] = {}
    for key, field in _KEYS.items():
        value = raw.get(field)
        if not isinstance(value, str) or not value:
            raise ServiceFailure("probe=session_invalid")
        credentials[key] = value
    return credentials
