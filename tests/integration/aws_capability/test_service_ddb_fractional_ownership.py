"""Ownership DynamoDB preserva a precisão do deadline no cleanup real."""

from dataclasses import replace
from pathlib import Path

import pytest

from tests.integration.aws_capability._service_read_assertions import assert_process
from tests.integration.aws_capability._service_read_testing import ReadLab, make_read_lab, respond
from tests.integration.aws_capability.contracts import (
    Page,
    RequestContext,
    Resource,
    ServiceFailure,
)
from tests.integration.aws_capability.services import AwsServices

ROOT = Resource("dynamodb", "cnesdata-cap-a1b2c3d4", "cnesdata-cap-a1b2c3d4")
TABLE_ARN = "arn:aws:dynamodb:us-east-2:111111111111:table/cnesdata-cap-a1b2c3d4"
DESCRIBE = ("dynamodb", "describe-table", "us-east-2", {"TableName": "cnesdata-cap-a1b2c3d4"})
TAGS = ("dynamodb", "list-tags-of-resource", "us-east-2", {"ResourceArn": TABLE_ARN})


@pytest.fixture
def read_lab(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> ReadLab:
    lab = make_read_lab(monkeypatch, tmp_path)
    spec = replace(lab.services.spec, deadline=1060.25)
    services = AwsServices(spec, lab.services.ports)
    lab.context.budget.deadline = 999
    return replace(lab, services=services, context=RequestContext(lab.context.budget, "cleanup"))


def _dependents(lab: ReadLab, expires_at: str) -> Page | ServiceFailure:
    respond(
        lab,
        {
            "Table": {
                "TableName": "cnesdata-cap-a1b2c3d4",
                "TableArn": TABLE_ARN,
                "TableStatus": "ACTIVE",
            }
        },
    )
    respond(
        lab,
        {
            "Tags": [
                {"Key": "Project", "Value": "CnesData"},
                {"Key": "Purpose", "Value": "aws-capability"},
                {"Key": "Run", "Value": "a1b2c3d4"},
                {"Key": "ExpiresAt", "Value": expires_at},
            ]
        },
    )
    try:
        return lab.services.dependents(ROOT, None, lab.context)
    except ServiceFailure as error:
        return error


def _assert_cleanup_requests(lab: ReadLab) -> None:
    assert_process(lab, [DESCRIBE, TAGS])
    assert lab.context.budget.used == 2
    assert lab.context.budget.work_used == 0
    assert lab.context.budget.cleanup_deadline == 1030
    assert lab.context.budget.deadline == 999


def test_deadline_fracionario_com_tag_literal_correta_retorna_folha(read_lab: ReadLab) -> None:
    outcome = _dependents(read_lab, "1060.25")
    _assert_cleanup_requests(read_lab)
    assert outcome == Page()


def test_tag_truncada_nao_comprova_ownership_de_deadline_fracionario(read_lab: ReadLab) -> None:
    outcome = _dependents(read_lab, "1060")
    _assert_cleanup_requests(read_lab)
    assert isinstance(outcome, ServiceFailure)
    assert outcome.args == ("service=not_owned",)
