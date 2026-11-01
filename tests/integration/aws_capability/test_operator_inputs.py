"""Entradas CAP_* do operador: validação fechada e leitura só em perfis com IAM."""

from dataclasses import replace

import pytest

from tests.integration.aws_capability import _production as production
from tests.integration.aws_capability._b1_testing import (
    ACCOUNT,
    BOUNDARY_ARN,
    BOUNDARY_KEY,
    OPERATOR_ARN,
    OPERATOR_KEY,
    operator_environment,
)
from tests.integration.aws_capability._operator_inputs import OperatorInputs
from tests.integration.aws_capability._production_testing import environment
from tests.integration.aws_capability._profiles import PROFILES
from tests.integration.aws_capability._safety import SafetyFailure
from tests.integration.aws_capability._testing import Lab
from tests.integration.aws_capability.contracts import Ports
from tests.integration.aws_capability.services import AwsCommands, AwsServices

IAM_PROFILES = sorted(name for name, profile in PROFILES.items() if "iam" in profile.services)
OTHER_PROFILES = sorted(name for name, profile in PROFILES.items() if "iam" not in profile.services)


def build(lab: Lab, probe: str, values: dict[str, str]) -> Ports:
    spec = replace(lab.spec, probe=probe)
    return production.build_ports(spec, values, lab.processes, lambda: lab.time)


def operator_of(ports: Ports) -> OperatorInputs | None:
    assert isinstance(ports.services, AwsServices)
    return ports.services.ports.operator


@pytest.mark.parametrize("probe", IAM_PROFILES)
def test_le_e_valida_entradas_do_operador_em_perfis_com_iam(lab: Lab, probe: str) -> None:
    ports = build(lab, probe, {**environment(), **operator_environment()})
    assert operator_of(ports) == OperatorInputs(OPERATOR_ARN, BOUNDARY_ARN)


@pytest.mark.parametrize("probe", OTHER_PROFILES)
def test_nao_le_entradas_do_operador_em_perfis_sem_iam(lab: Lab, probe: str) -> None:
    broken = {OPERATOR_KEY: "lixo", BOUNDARY_KEY: ""}
    ports = build(lab, probe, {**environment(), **broken})
    assert operator_of(ports) is None


@pytest.mark.parametrize(
    "role",
    [
        None,
        "",
        f"arn:aws:iam::{ACCOUNT}:role/cnesdata-cap-operator-0a1b2c3d",
        f"arn:aws:iam::{ACCOUNT}:role/other/cnesdata-cap-operator-0a1b2c3d",
        "arn:aws:iam::222222222222:role/cnesdata-cap/cnesdata-cap-operator-0a1b2c3d",
        f"arn:aws:iam::{ACCOUNT}:role/cnesdata-cap/cnesdata-cap-operator-0A1B2C3D",
        f"arn:aws:iam::{ACCOUNT}:role/cnesdata-cap/cnesdata-cap-operator-0a1b2c3",
        f"arn:aws:iam::{ACCOUNT}:role/cnesdata-cap/cnesdata-cap-operator-0a1b2c3d\n",
        f"arn:aws:iam::{ACCOUNT}:role/cnesdata-cap/cnesdata-cap-operator-0a1b2c3d/extra",
        f"arn:aws:iam::{ACCOUNT}:user/cnesdata-cap/cnesdata-cap-operator-0a1b2c3d",
        f"arn:aws:iam::{ACCOUNT}:role/cnesdata-cap/cnesdata-cap-0a1b2c3d",
    ],
)
def test_recusa_role_do_operador_invalida(lab: Lab, role: str | None) -> None:
    values = {**environment(), **operator_environment()}
    if role is None:
        del values[OPERATOR_KEY]
    else:
        values[OPERATOR_KEY] = role
    with pytest.raises(SafetyFailure) as error:
        build(lab, "s3-retention", values)
    assert error.value.reason == "operator_role_invalid"


@pytest.mark.parametrize(
    "boundary",
    [
        None,
        "",
        f"arn:aws:iam::{ACCOUNT}:policy/cnesdata-cap-boundary-0a1b2c3d",
        f"arn:aws:iam::{ACCOUNT}:policy/cnesdata-cap/cnesdata-cap-boundary-0a1b2c3d\n",
        "arn:aws:iam::222222222222:policy/cnesdata-cap/cnesdata-cap-boundary-0a1b2c3d",
        f"arn:aws:iam::{ACCOUNT}:role/cnesdata-cap/cnesdata-cap-boundary-0a1b2c3d",
        f"arn:aws:iam::{ACCOUNT}:policy/cnesdata-cap/cnesdata-cap-boundary-zzzzzzzz",
        "arn:aws:iam::aws:policy/AdministratorAccess",
    ],
)
def test_recusa_boundary_invalido(lab: Lab, boundary: str | None) -> None:
    values = {**environment(), **operator_environment()}
    if boundary is None:
        del values[BOUNDARY_KEY]
    else:
        values[BOUNDARY_KEY] = boundary
    with pytest.raises(SafetyFailure) as error:
        build(lab, "s3-retention", values)
    assert error.value.reason == "permissions_boundary_invalid"


def test_entradas_do_operador_chegam_explicitas_sem_leitura_global_de_ambiente(
    lab: Lab, monkeypatch: pytest.MonkeyPatch
) -> None:
    ports = build(lab, "s3-retention", {**environment(), **operator_environment()})
    monkeypatch.delenv(OPERATOR_KEY, raising=False)
    monkeypatch.delenv(BOUNDARY_KEY, raising=False)
    assert operator_of(ports) == OperatorInputs(OPERATOR_ARN, BOUNDARY_ARN)


def test_pausa_de_producao_e_o_sleep_real(lab: Lab) -> None:
    import time

    ports = build(lab, "s3-hold", dict(environment()))
    assert isinstance(ports.services, AwsServices)
    assert ports.services.ports.pause is time.sleep


def test_fabrica_de_sessao_troca_apenas_as_tres_credenciais(lab: Lab) -> None:
    values = {**environment(), **operator_environment()}
    ports = build(lab, "s3-retention", values)
    assert isinstance(ports.services, AwsServices)
    factory = ports.services.ports.session
    assert factory is not None
    session = factory(
        {
            "AWS_ACCESS_KEY_ID": "SESSION_KEY_CANARY",
            "AWS_SECRET_ACCESS_KEY": "SESSION_SECRET_CANARY",
            "AWS_SESSION_TOKEN": "SESSION_TOKEN_CANARY",
        }
    )
    assert isinstance(session, AwsCommands)
    assert (session.region, session.timeout) == ("us-east-2", production.COMMAND_TIMEOUT)
    assert session.environment == {
        **values,
        "AWS_ACCESS_KEY_ID": "SESSION_KEY_CANARY",
        "AWS_SECRET_ACCESS_KEY": "SESSION_SECRET_CANARY",
        "AWS_SESSION_TOKEN": "SESSION_TOKEN_CANARY",
    }
    assert "CANARY" not in repr(session) + repr(ports.services.ports)
    assert values["AWS_ACCESS_KEY_ID"] == environment()["AWS_ACCESS_KEY_ID"]
