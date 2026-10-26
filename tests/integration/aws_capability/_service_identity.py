"""Validação da identidade STS."""

import re

from tests.integration.aws_capability._service_commands import call_command, require_data
from tests.integration.aws_capability._service_contracts import CommandPort
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import RequestContext, RunSpec, ServiceFailure

_ARN = re.compile(r"arn:aws:(sts|iam)::([0-9]{12}):[^:]+/.+")


def read_identity(commands: CommandPort, spec: RunSpec, context: RequestContext) -> str:
    reply = call_command(commands, Request("sts", "get-caller-identity", {}), context)
    data = require_data(reply)
    account = data.get("Account")
    arn = data.get("Arn")
    if not isinstance(account, str) or not _valid_identity(account, arn, spec.account):
        raise ServiceFailure("service=identity_invalid")
    return account


def _valid_identity(account: object, arn: object, expected: str) -> bool:
    if (
        not isinstance(account, str)
        or account != expected
        or not re.fullmatch(r"[0-9]{12}", account)
    ):
        return False
    if not isinstance(arn, str):
        return False
    match = _ARN.fullmatch(arn)
    return match is not None and match.group(2) == expected
