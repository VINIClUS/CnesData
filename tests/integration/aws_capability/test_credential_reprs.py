"""Respostas com credenciais assumidas nunca aparecem em repr ou str."""

from typing import TYPE_CHECKING, cast

from tests.integration.aws_capability._probe_attempt import Attempt
from tests.integration.aws_capability._s3_actor import Actor
from tests.integration.aws_capability._service_contracts import CommandPort, Reply

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


def test_actor_nao_expoe_a_porta_de_comandos_da_sessao_em_repr_nem_str() -> None:
    commands = cast("CommandPort", Leaky())
    context = cast("RequestContext", None)
    assert_sem_canaries(Actor(commands, "bucket", "123456789012", context))
