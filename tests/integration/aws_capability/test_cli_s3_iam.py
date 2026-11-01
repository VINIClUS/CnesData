"""Transporte CLI: recusa local de COMPLIANCE e respostas vazias de escrita S3 e IAM."""

import json
import subprocess
from pathlib import Path

import pytest

from tests.integration.aws_capability._service_testing import CliLab, make_cli_lab
from tests.integration.aws_capability.cli import Request


@pytest.fixture
def cli_lab(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> CliLab:
    lab = make_cli_lab(tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(subprocess, "run", lab.process.run)
    return lab


def put_object(**extra: object) -> Request:
    return Request("s3api", "put-object", {"Bucket": "b", "Key": "k", **extra})


@pytest.mark.parametrize(
    "request_",
    [
        put_object(ObjectLockMode="COMPLIANCE"),
        put_object(ObjectLockMode="compliance"),
        Request(
            "s3api",
            "put-object-retention",
            {"Bucket": "b", "Retention": {"Mode": "Compliance", "RetainUntilDate": "x"}},
        ),
        Request(
            "iam",
            "put-role-policy",
            {"RoleName": "r", "PolicyDocument": json.dumps({"Note": "COMPLIANCE"})},
        ),
        Request("s3api", "put-object-lock-configuration", {"Bucket": "b"}),
        Request("s3api", "put-object-lock-configuration", {"Bucket": "b", "Rule": {}}),
    ],
)
def test_recusa_compliance_e_configuracao_de_lock_antes_de_gastar_orcamento(
    cli_lab: CliLab, request_: Request
) -> None:
    reply = cli_lab.call(request_)
    assert reply.ok is False
    assert reply.error is not None
    assert (reply.error.origin, reply.error.code) == ("local", "invalid_body")
    assert cli_lab.process.calls == []
    assert cli_lab.cli.context.budget.used == 0


def test_leitura_da_configuracao_de_lock_continua_permitida(cli_lab: CliLab) -> None:
    cli_lab.respond((0, json.dumps({"ObjectLockConfiguration": {}}), ""))
    reply = cli_lab.call(Request("s3api", "get-object-lock-configuration", {"Bucket": "b"}))
    assert reply.ok is True
    assert cli_lab.cli.context.budget.used == 1


@pytest.mark.parametrize(
    ("service", "operation"),
    [
        ("s3api", "put-bucket-tagging"),
        ("s3api", "put-bucket-versioning"),
        ("s3api", "put-object-legal-hold"),
        ("s3api", "put-object-retention"),
        ("iam", "put-role-policy"),
    ],
)
@pytest.mark.parametrize("stdout", ["", "\n"])
def test_escrita_s3_e_iam_sem_documento_e_sucesso_vazio(
    cli_lab: CliLab, service: str, operation: str, stdout: str
) -> None:
    cli_lab.respond((0, stdout, ""))
    reply = cli_lab.call(Request(service, operation, {"Bucket": "b"}))
    assert reply.ok is True
    assert dict(reply.data) == {}


@pytest.mark.parametrize(
    ("service", "operation"),
    [("s3api", "head-object"), ("s3api", "create-bucket"), ("iam", "create-role")],
)
def test_leitura_ou_criacao_sem_documento_continua_invalida(
    cli_lab: CliLab, service: str, operation: str
) -> None:
    cli_lab.respond((0, "", ""))
    reply = cli_lab.call(Request(service, operation, {"Bucket": "b"}))
    assert reply.ok is False


@pytest.mark.parametrize("operation", ["put-item", "update-item"])
@pytest.mark.parametrize("returns", [{}, {"ReturnValues": "NONE"}])
def test_escrita_dynamodb_sem_return_values_e_sucesso_vazio(
    cli_lab: CliLab, operation: str, returns: dict
) -> None:
    cli_lab.respond((0, "", ""))
    reply = cli_lab.call(Request("dynamodb", operation, {"TableName": "t", **returns}))
    assert reply.ok is True
    assert dict(reply.data) == {}


@pytest.mark.parametrize("operation", ["put-item", "update-item"])
@pytest.mark.parametrize("returns", ["ALL_OLD", "ALL_NEW", "UPDATED_NEW"])
def test_escrita_dynamodb_com_return_values_sem_documento_continua_invalida(
    cli_lab: CliLab, operation: str, returns: str
) -> None:
    cli_lab.respond((0, "", ""))
    payload = {"TableName": "t", "ReturnValues": returns}
    reply = cli_lab.call(Request("dynamodb", operation, payload))
    assert reply.ok is False


@pytest.mark.parametrize(
    "operation", ["query", "scan", "batch-write-item", "update-table", "create-table"]
)
def test_outras_operacoes_dynamodb_sem_documento_continuam_invalidas(
    cli_lab: CliLab, operation: str
) -> None:
    cli_lab.respond((0, "", ""))
    reply = cli_lab.call(Request("dynamodb", operation, {"TableName": "t"}))
    assert reply.ok is False
