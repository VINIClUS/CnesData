"""Respostas com credenciais assumidas nunca aparecem em repr ou str."""

from pathlib import Path
from typing import TYPE_CHECKING, cast

from tests.integration.aws_capability._probe_attempt import Attempt
from tests.integration.aws_capability._s3_actor import Actor
from tests.integration.aws_capability._service_contracts import (
    CommandPort,
    Reply,
    ServicePorts,
    no_pause,
)
from tests.integration.aws_capability._trust_probe import _Trust
from tests.integration.aws_capability.contracts import RunSpec
from tests.integration.aws_capability.services import AwsCommands

if TYPE_CHECKING:
    from tests.integration.aws_capability.contracts import RequestContext

CANARIES = ("ACCESS_KEY_CANARY", "SECRET_KEY_CANARY", "SESSION_TOKEN_CANARY")
CREDENTIALS = {
    "Credentials": {
        "AccessKeyId": CANARIES[0],
        "SecretAccessKey": CANARIES[1],
        "SessionToken": CANARIES[2],
    }
}


class Leaky:
    def __repr__(self) -> str:
        return " ".join(CANARIES)


def assert_sem_canaries(value: object) -> None:
    text = repr(value) + str(value)
    assert not any(canary in text for canary in CANARIES)


def test_attempt_nao_expoe_credenciais_assumidas_em_repr_nem_str() -> None:
    assert_sem_canaries(Attempt(True, "ok", CREDENTIALS))


def test_reply_nao_expoe_credenciais_assumidas_em_repr_nem_str() -> None:
    assert_sem_canaries(Reply(True, CREDENTIALS, None))


def test_attempt_com_mensagem_e_credenciais_nao_expoe_nenhuma_em_repr() -> None:
    assert_sem_canaries(Attempt(False, "ValidationError", CREDENTIALS, " ".join(CANARIES)))


def test_comandos_de_sessao_nao_expoem_o_ambiente_com_credenciais() -> None:
    environment = {
        "AWS_ACCESS_KEY_ID": CANARIES[0],
        "AWS_SECRET_ACCESS_KEY": CANARIES[1],
        "AWS_SESSION_TOKEN": CANARIES[2],
    }
    assert_sem_canaries(AwsCommands("us-east-2", environment, 7))


def test_probe_de_trust_nao_expoe_porta_de_sessao_em_repr_nem_str() -> None:
    commands = cast("CommandPort", Leaky())
    ports = ServicePorts(commands, lambda: 0.0, no_pause, lambda _: cast("CommandPort", Leaky()))
    spec = RunSpec("123456789012", "us-east-2", "a1b2c3d4", 1.0, Path("."), probe="iam-trust")
    context = cast("RequestContext", None)
    assert_sem_canaries(_Trust(ports, spec, context, "arn:aws:iam::1:role/x"))


def test_actor_nao_expoe_a_porta_de_comandos_da_sessao_em_repr_nem_str() -> None:
    commands = cast("CommandPort", Leaky())
    context = cast("RequestContext", None)
    assert_sem_canaries(Actor(commands, "bucket", "123456789012", context))
