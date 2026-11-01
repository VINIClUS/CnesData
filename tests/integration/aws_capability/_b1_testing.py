"""Constantes e entradas compartilhadas pelos testes S3 e IAM do PR-B1."""

from collections.abc import Mapping

ACCOUNT = "111111111111"
OPERATOR_ARN = f"arn:aws:iam::{ACCOUNT}:role/cnesdata-cap/cnesdata-cap-operator-0a1b2c3d"
BOUNDARY_ARN = f"arn:aws:iam::{ACCOUNT}:policy/cnesdata-cap/cnesdata-cap-boundary-0a1b2c3d"
OPERATOR_KEY = "CAP_OPERATOR_ROLE_ARN"
BOUNDARY_KEY = "CAP_PERMISSIONS_BOUNDARY_ARN"


def operator_environment() -> Mapping[str, str]:
    return {OPERATOR_KEY: OPERATOR_ARN, BOUNDARY_KEY: BOUNDARY_ARN}
