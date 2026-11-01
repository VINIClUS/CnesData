"""Entradas CAP_* do operador, validadas contra a conta autorizada."""

import re
from collections.abc import Mapping
from dataclasses import dataclass

from tests.integration.aws_capability._safety import SafetyFailure
from tests.integration.aws_capability.contracts import RunSpec

ROLE_KEY = "CAP_OPERATOR_ROLE_ARN"
BOUNDARY_KEY = "CAP_PERMISSIONS_BOUNDARY_ARN"
_ROLE = r"arn:aws:iam::{account}:role/cnesdata-cap/(cnesdata-cap-operator-[0-9a-f]{{8}})"
_BOUNDARY = r"arn:aws:iam::{account}:policy/cnesdata-cap/cnesdata-cap-boundary-[0-9a-f]{{8}}"


@dataclass(frozen=True)
class OperatorInputs:
    role_arn: str
    boundary_arn: str

    @property
    def role_name(self) -> str:
        return self.role_arn.rsplit("/", 1)[1]


def read_operator_inputs(spec: RunSpec, environment: Mapping[str, str]) -> OperatorInputs:
    """Returns: entradas validadas do operador.

    Raises: SafetyFailure para ARN ausente, de outra conta ou fora do formato fechado.
    """
    role = _valid(_ROLE, spec, environment.get(ROLE_KEY))
    if role is None:
        raise SafetyFailure("operator_role_invalid")
    boundary = _valid(_BOUNDARY, spec, environment.get(BOUNDARY_KEY))
    if boundary is None:
        raise SafetyFailure("permissions_boundary_invalid")
    return OperatorInputs(role, boundary)


def _valid(pattern: str, spec: RunSpec, value: str | None) -> str | None:
    expression = pattern.format(account=re.escape(spec.account))
    if isinstance(value, str) and re.fullmatch(expression, value) is not None:
        return value
    return None
