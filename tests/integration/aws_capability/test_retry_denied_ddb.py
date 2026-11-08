"""retry_denied no DynamoDB: AccessDeniedException repete e nenhum outro código do serviço."""

from pathlib import Path

import pytest

from tests.integration.aws_capability._probe_attempt import (
    PROPAGATION_DENIALS,
    Attempt,
    retry_denied,
)
from tests.integration.aws_capability._probe_testing import make_probe_lab
from tests.integration.aws_capability.contracts import ServiceFailure


def sequence(*codes: str):
    replies = iter(codes)
    calls: list[str] = []

    def action() -> Attempt:
        code = next(replies)
        calls.append(code)
        return Attempt(code == "ok", code)

    return action, calls


def test_conjunto_de_negacoes_de_propagacao_e_fechado() -> None:
    assert {"AccessDenied", "AccessDeniedException", "403"} == PROPAGATION_DENIALS


def test_controle_dynamodb_repete_access_denied_exception_ate_ficar_verde(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "ddb-throughput")
    action, calls = sequence("AccessDeniedException", "AccessDeniedException", "ok")
    result, seconds = retry_denied(action, lab.services.ports, lab.context, "get")
    assert result.ok
    assert calls == ["AccessDeniedException", "AccessDeniedException", "ok"]
    assert seconds > 0
    assert len(lab.pauses) == 2


@pytest.mark.parametrize(
    "code",
    [
        "ValidationException",
        "ResourceNotFoundException",
        "ThrottlingException",
        "ProvisionedThroughputExceededException",
        "ConditionalCheckFailedException",
        "TransactionCanceledException",
        "UnrecognizedClientException",
    ],
)
def test_controle_dynamodb_nao_repete_outros_codigos_do_servico(
    tmp_path: Path, code: str
) -> None:
    lab = make_probe_lab(tmp_path, "ddb-throughput")
    action, calls = sequence(code, "ok")
    with pytest.raises(ServiceFailure) as error:
        retry_denied(action, lab.services.ports, lab.context, "get")
    assert error.value.args == ("probe=control_failed step=get",)
    assert calls == [code]
    assert lab.pauses == []
