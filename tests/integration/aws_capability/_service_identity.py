"""Validação da identidade STS."""

import re

from tests.integration.aws_capability._operator_inputs import OperatorInputs
from tests.integration.aws_capability._safety import SafetyFailure
from tests.integration.aws_capability._service_commands import call_command, require_data
from tests.integration.aws_capability._service_contracts import CommandPort
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import RequestContext, RunSpec, ServiceFailure

_ARN = re.compile(r"arn:aws:(sts|iam)::([0-9]{12}):[^:]+/.+")
_ROOT_ARN = re.compile(r"arn:aws:iam::([0-9]{12}):root")


def read_identity(
    commands: CommandPort,
    spec: RunSpec,
    context: RequestContext,
    operator: OperatorInputs | None = None,
) -> str:
    """Returns: conta; com operator exige sessão assumida do role do operador, nunca root."""
    reply = call_command(commands, Request("sts", "get-caller-identity", {}), context)
    data = require_data(reply)
    account = data.get("Account")
    arn = data.get("Arn")
    if not isinstance(account, str) or not _valid_identity(account, arn, spec.account):
        raise ServiceFailure("service=identity_invalid")
    if operator is not None and not _is_operator_session(arn, spec, operator):
        raise SafetyFailure("operator_mismatch")
    return account


def _is_operator_session(arn: object, spec: RunSpec, operator: OperatorInputs) -> bool:
    expected = (
        rf"arn:aws:sts::{spec.account}:assumed-role/{re.escape(operator.role_name)}/[\w+=,.@-]+"
    )
    return isinstance(arn, str) and re.fullmatch(expected, arn) is not None


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
    if match is not None:
        return match.group(2) == expected
    root_match = _ROOT_ARN.fullmatch(arn)
    return root_match is not None and root_match.group(1) == expected
