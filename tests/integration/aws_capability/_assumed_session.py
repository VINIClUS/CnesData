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
    if ports.session is None:
        raise ServiceFailure("probe=session_unavailable")
    name = run_name(spec)
    request = Request(
        "sts",
        "assume-role",
        {
            "RoleArn": f"arn:aws:iam::{spec.account}:role/cnesdata-cap/{name}",
            "RoleSessionName": f"cap-{spec.run_id}",
            "DurationSeconds": SESSION_SECONDS,
        },
    )
    result, seconds = retry_denied(
        lambda: attempt(ports.commands, request, context), ports, context, "assume"
    )
    return ports.session(_credentials(result.data)), seconds


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
