"""retry_denied: negações de propagação (AccessDenied e 403 de HEAD) repetem; o resto não."""

from pathlib import Path

import pytest

from tests.integration.aws_capability._probe_attempt import Attempt, retry_denied
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


@pytest.mark.parametrize("denial", ["403", "AccessDenied"])
def test_repete_negacao_de_propagacao_ate_ficar_verde(tmp_path: Path, denial: str) -> None:
    lab = make_probe_lab(tmp_path, "s3-retention")
    action, calls = sequence(denial, denial, "ok")
    result, seconds = retry_denied(action, lab.services.ports, lab.context, "p0")
    assert result.ok
    assert calls == [denial, denial, "ok"]
    assert seconds > 0
    assert len(lab.pauses) == 2


@pytest.mark.parametrize("code", ["404", "NoSuchKey", "Throttling", "NotFound", "Other"])
def test_nao_repete_codigo_que_nao_e_negacao(tmp_path: Path, code: str) -> None:
    lab = make_probe_lab(tmp_path, "s3-retention")
    action, calls = sequence(code, "ok")
    with pytest.raises(ServiceFailure) as error:
        retry_denied(action, lab.services.ports, lab.context, "p0")
    assert error.value.args == ("probe=control_failed step=p0",)
    assert calls == [code]
    assert lab.pauses == []


def test_403_permanente_falha_dentro_do_limite_de_tentativas(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "s3-retention")
    action, calls = sequence(*["403"] * 50)
    with pytest.raises(ServiceFailure) as error:
        retry_denied(action, lab.services.ports, lab.context, "p0")
    assert error.value.args == ("probe=control_failed step=p0",)
    assert len(calls) == 12
