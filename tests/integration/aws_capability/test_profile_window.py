"""Janela máxima de trabalho por perfil: o teto de 180 s vira o padrão, não a regra."""

from dataclasses import replace

import pytest

from tests.integration.aws_capability._profiles import PROFILES, Profile
from tests.integration.aws_capability._safety import SafetyFailure, validate
from tests.integration.aws_capability._testing import Lab
from tests.integration.aws_capability.contracts import Limits
from tests.integration.aws_capability.test_profiles import profile_ports

LIMITS = Limits(192, 8, 2, 150, 96)


def with_window(monkeypatch: pytest.MonkeyPatch, window: float) -> None:
    profile = replace(PROFILES["ddb-transactions"], window=window)
    monkeypatch.setitem(PROFILES, "ddb-transactions", profile)


def test_janela_padrao_do_perfil_e_180_segundos() -> None:
    assert Profile(("dynamodb",), Limits()).window == 180.0


@pytest.mark.parametrize(
    "case",
    [
        (120.0, 120, True),
        (120.0, 121, False),
        (200.0, 190, True),
        (200.0, 200, True),
        (200.0, 201, False),
        (180.0, 180, True),
        (180.0, 181, False),
    ],
)
def test_validacao_usa_a_janela_do_perfil_e_nao_o_teto_global(
    lab: Lab, monkeypatch: pytest.MonkeyPatch, case: tuple[float, int, bool]
) -> None:
    window, seconds, accepted = case
    with_window(monkeypatch, window)
    spec = replace(lab.spec, probe="ddb-transactions", deadline=1000 + seconds)
    ports = profile_ports(replace(lab, spec=spec), LIMITS)
    if accepted:
        validate(spec, ports)
        return
    with pytest.raises(SafetyFailure) as error:
        validate(spec, ports)
    assert error.value.reason == "invalid_deadline"


def test_recuperacao_ignora_a_janela_do_perfil(lab: Lab, monkeypatch: pytest.MonkeyPatch) -> None:
    with_window(monkeypatch, 30.0)
    spec = replace(lab.spec, probe="ddb-transactions", deadline=1000 + 150)
    validate(spec, profile_ports(replace(lab, spec=spec), LIMITS), recovery=True)
    with pytest.raises(SafetyFailure):
        validate(spec, profile_ports(replace(lab, spec=spec), LIMITS))
