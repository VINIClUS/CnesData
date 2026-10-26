"""Identidade STS coerente pela facade e transporte CLI públicos."""

from pathlib import Path

import pytest

from tests.integration.aws_capability._service_read_assertions import (
    assert_process,
    assert_rejected,
)
from tests.integration.aws_capability._service_read_testing import (
    ReadLab,
    fixture_data,
    make_read_lab,
    respond,
    respond_error,
)
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import ServiceFailure


@pytest.fixture
def read_lab(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> ReadLab:
    return make_read_lab(monkeypatch, tmp_path)


@pytest.mark.parametrize("principal", ["sts", "iam", "iam_root"])
def test_identity_confere_account_e_principal_em_memoria(read_lab: ReadLab, principal: str) -> None:
    respond(read_lab, fixture_data("identity")[principal])
    assert read_lab.services.identity(read_lab.context) == "111111111111"
    assert_process(read_lab, [("sts", "get-caller-identity", "us-east-1", {})])
    assert list(read_lab.directory.iterdir()) == []


@pytest.mark.parametrize(
    "response",
    [
        "wrong_account",
        "arn_other_account",
        "wrong_service",
        "arn_with_region",
        "wrong_partition",
        "missing_arn",
        "bad_account_type",
        "missing_account",
        "empty",
    ],
)
def test_identity_recusa_identidade_incoerente(read_lab: ReadLab, response: str) -> None:
    respond(read_lab, fixture_data("identity")[response])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.identity(read_lab.context)
    assert_rejected(error.value)
    assert_process(read_lab, [("sts", "get-caller-identity", "us-east-1", {})])


@pytest.mark.parametrize(
    "failure", ["remote_denied", "local_not_found", "bad_json", "empty_stdout"]
)
def test_identity_nao_confunde_erro_com_account(read_lab: ReadLab, failure: str) -> None:
    respond_error(read_lab, failure)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.identity(read_lab.context)
    assert_rejected(error.value)
    assert_process(read_lab, [("sts", "get-caller-identity", "us-east-1", {})])


def test_awscommands_compoe_cli_real_e_debita_uma_vez(read_lab: ReadLab) -> None:
    respond(read_lab, {"TableNames": []})
    reply = read_lab.services.ports.commands.call(
        Request("dynamodb", "list-tables", {"Limit": 100}), read_lab.context
    )
    assert reply.ok is True
    assert reply.data == {"TableNames": []}
    assert reply.error is None
    assert_process(read_lab, [("dynamodb", "list-tables", "us-east-2", {"Limit": 100})])
