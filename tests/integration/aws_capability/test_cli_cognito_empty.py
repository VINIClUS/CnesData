"""Somente UpdateUserPool aceita sucesso vazio adicional no transporte CLI."""

import pytest

from tests.integration.aws_capability._service_cognito_root_testing import (
    RootLab,
    assert_private_channels,
    assert_trace,
    load_requests,
    queue_trace,
    root_lab,
)
from tests.integration.aws_capability._service_contracts import Reply
from tests.integration.aws_capability.cli import Cli, Request
from tests.integration.aws_capability.services import AwsCommands

__all__ = ["root_lab"]


@pytest.mark.parametrize(
    "name",
    ["ok_empty", "ok_whitespace", "ok_json_empty"],
    ids=["vazio", "whitespace", "json_vazio"],
)
def test_vazio_somente_update_pool(root_lab: RootLab, name: str) -> None:
    expected = load_requests()["U"]
    queue_trace(root_lab, [["U", name]])
    commands = root_lab.services.ports.commands
    assert isinstance(commands, AwsCommands)
    cli = Cli(expected["region"], 7, commands.environment, root_lab.context)
    reply = cli.call(Request(expected["service"], expected["operation"], expected["stdin"]))
    assert_private_channels(root_lab, reply)
    assert_trace(root_lab, [["U", name]], ["work"])
    assert reply == Reply(True, {}, None)


@pytest.mark.parametrize(
    "name",
    ["C", "D", "UP_CLIENT", "UP_DDB"],
    ids=["create_pool", "describe_pool", "update_client", "update_table"],
)
def test_vazio_generico_falha(root_lab: RootLab, name: str) -> None:
    expected = load_requests()[name]
    queue_trace(root_lab, [[name, "ok_empty"]])
    commands = root_lab.services.ports.commands
    assert isinstance(commands, AwsCommands)
    cli = Cli(expected["region"], 7, commands.environment, root_lab.context)
    reply = cli.call(Request(expected["service"], expected["operation"], expected["stdin"]))
    assert_private_channels(root_lab, reply)
    assert_trace(root_lab, [[name, "ok_empty"]], ["work"])
    assert reply.ok is False
    assert reply.data == {}
    assert reply.error is not None
    assert reply.error.origin == "local"
    assert reply.error.code == "operation_failed"


@pytest.mark.parametrize(
    ("name", "origin", "code"),
    [
        ("error_denied", "service", "NotAuthorizedException"),
        ("error_local", "local", "ParameterError"),
        ("error_timeout", "local", "operation_failed"),
    ],
    ids=["remoto254", "local252", "timeout"],
)
def test_erro_preserva_origem(root_lab: RootLab, name: str, origin: str, code: str) -> None:
    expected = load_requests()["U"]
    queue_trace(root_lab, [["U", name]])
    commands = root_lab.services.ports.commands
    assert isinstance(commands, AwsCommands)
    cli = Cli(expected["region"], 7, commands.environment, root_lab.context)
    reply = cli.call(Request(expected["service"], expected["operation"], expected["stdin"]))
    assert_private_channels(root_lab, reply)
    assert_trace(root_lab, [["U", name]], ["work"])
    assert reply.ok is False
    assert reply.data == {}
    assert reply.error is not None
    assert (reply.error.origin, reply.error.code) == (origin, code)
