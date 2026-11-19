"""Espelhos do ensaio iam-s3: statements do data bucket iguais às policies renderizadas."""

import json

import pytest

from tests.integration.aws_capability import _matrix_policies as policies
from tests.production.infra._step_functions_iam_contracts import Node, resource_at
from tests.production.infra.test_s3_missing_object_invariant import (
    DATA_ADDRESS,
    POLICY_ADDRESSES,
)
from tests.production.infra.test_step_functions_iam import ROOT_RUN, NativePlans

CAP = "arn:aws:s3:::cnesdata-cap-a1b2c3d4"
DATA = "arn:aws:s3:::fixture-data"
MODES = ("unit", "recovery", "api")


def _in_bucket(value: str, data_arn: str) -> bool:
    return value == data_arn or value.startswith(f"{data_arn}/")


def _touches(statement: Node, data_arn: str) -> bool:
    resource = statement["Resource"]
    listed = [resource] if isinstance(resource, str) else resource
    return any(_in_bucket(item, data_arn) for item in listed)


def _swap(value: object, data_arn: str) -> object:
    if isinstance(value, str):
        return CAP + value[len(data_arn) :] if _in_bucket(value, data_arn) else value
    if isinstance(value, list):
        return [_swap(item, data_arn) for item in value]
    if isinstance(value, dict):
        return {key: _swap(item, data_arn) for key, item in value.items()}
    return value


def data_statements(policy: str, data_arn: str) -> list[object]:
    """Args: Policy JSON renderizada e ARN do data bucket.
    Returns: Statements do data bucket, em ordem, com o ARN trocado pelo do ensaio.
    """
    statements = json.loads(policy)["Statement"]
    return [_swap(item, data_arn) for item in statements if _touches(item, data_arn)]


@pytest.mark.parametrize("mode", MODES)
def test_espelho_do_ensaio_reproduz_as_statements_do_data_bucket(
    processing_native: NativePlans, mode: str
) -> None:
    plan = processing_native.plans[ROOT_RUN]
    data_arn = resource_at(plan, DATA_ADDRESS)["change"]["after"]["arn"]
    policy = resource_at(plan, POLICY_ADDRESSES[mode])["change"]["after"]["policy"]
    rendered = data_statements(policy, data_arn)
    assert rendered, f"mirror role={mode} data_statements=empty"
    assert rendered == getattr(policies, f"{mode}_s3")(CAP)


def test_filtro_ignora_outro_bucket_e_bucket_irmao_e_preserva_a_ordem() -> None:
    document = json.dumps({"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": "s3:GetObject", "Resource": f"{DATA}/b/*"},
        {"Effect": "Allow", "Action": "s3:GetObject", "Resource": f"{DATA}2/a/*"},
        {"Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "arn:aws:dynamodb:x"},
        {"Effect": "Allow", "Action": "s3:ListBucket", "Resource": [DATA, f"{DATA}2"],
         "Condition": {"StringLike": {"s3:prefix": [f"{DATA}/literal"]}}},
    ]})
    assert data_statements(document, DATA) == [
        {"Effect": "Allow", "Action": "s3:GetObject", "Resource": f"{CAP}/b/*"},
        {"Effect": "Allow", "Action": "s3:ListBucket", "Resource": [CAP, f"{DATA}2"],
         "Condition": {"StringLike": {"s3:prefix": [f"{CAP}/literal"]}}},
    ]
