"""Transporte CLI: só as operações que realmente respondem vazio contam como sucesso vazio."""

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


EMPTY_OK = [
    Request("logs", "create-log-group", {"logGroupName": "n", "tags": {"a": "b"}}),
    Request("logs", "put-retention-policy", {"logGroupName": "n", "retentionInDays": 1}),
    Request("logs", "delete-log-group", {"logGroupName": "n"}),
]
EMPTY_FAILS = [
    Request("ecr", "create-repository", {"repositoryName": "n"}),
    Request("ecr", "get-authorization-token", {}),
    Request("ecr", "describe-images", {"repositoryName": "n"}),
    Request("ecs", "create-cluster", {"clusterName": "n"}),
    Request("ecs", "register-task-definition", {"family": "n"}),
    Request("ecs", "run-task", {"cluster": "n", "taskDefinition": "t"}),
    Request("ecs", "describe-tasks", {"cluster": "n", "tasks": ["t"]}),
    Request("ecs", "stop-task", {"cluster": "n", "task": "t"}),
    Request("ecs", "list-task-definitions", {"familyPrefix": "n"}),
    Request("ec2", "create-security-group", {"GroupName": "n"}),
    Request("ec2", "revoke-security-group-egress", {"GroupId": "g"}),
    Request("ec2", "authorize-security-group-egress", {"GroupId": "g"}),
    Request("logs", "get-log-events", {"logGroupName": "n", "logStreamName": "s"}),
    Request("logs", "describe-log-groups", {}),
]


@pytest.mark.parametrize("request_", EMPTY_OK, ids=lambda r: f"{r.service}.{r.operation}")
def test_logs_que_respondem_vazio_sao_sucesso_vazio(cli_lab: CliLab, request_: Request) -> None:
    cli_lab.respond((0, "", ""))
    reply = cli_lab.call(request_)
    assert reply.ok is True
    assert dict(reply.data) == {}


@pytest.mark.parametrize("request_", EMPTY_FAILS, ids=lambda r: f"{r.service}.{r.operation}")
def test_operacao_que_deveria_devolver_corpo_e_falha_quando_vem_vazia(
    cli_lab: CliLab, request_: Request
) -> None:
    cli_lab.respond((0, "", ""))
    reply = cli_lab.call(request_)
    assert reply.ok is False
