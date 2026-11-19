"""Operações Cognito do login que imprimem vazio no sucesso real."""

import subprocess

import pytest

from tests.integration.aws_capability._service_contracts import Reply
from tests.integration.aws_capability.cli import Cli, Request
from tests.integration.aws_capability.contracts import Limits, RequestBudget, RequestContext


def _cli(monkeypatch: pytest.MonkeyPatch) -> Cli:
    def run(argv: list[str], **options: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(subprocess, "run", run)
    budget = RequestBudget(Limits(), 2000.0, lambda: 1000.0)
    return Cli("us-east-2", 5.0, {}, RequestContext(budget, "work"))


@pytest.mark.parametrize("operation", ["admin-set-user-password", "admin-delete-user"])
def test_sucesso_vazio_admite_admin_de_usuario(
    monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    reply = _cli(monkeypatch).call(Request("cognito-idp", operation, {"UserPoolId": "p"}))
    assert reply == Reply(True, {}, None)


@pytest.mark.parametrize("operation", ["admin-create-user", "initiate-auth", "describe-user-pool"])
def test_stdout_vazio_recusa_leituras_e_criacoes(
    monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    reply = _cli(monkeypatch).call(Request("cognito-idp", operation, {"UserPoolId": "p"}))
    assert reply.ok is False
