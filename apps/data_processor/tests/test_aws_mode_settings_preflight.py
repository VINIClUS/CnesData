"""Configuração mínima dos modos AWS validada antes da fronteira de sessão."""

import os
import sys
from collections.abc import Iterator
from typing import NoReturn
from unittest.mock import patch

import pytest

UNIT_ENV = {
    "PYTHON_DOTENV_DISABLED": "1",
    "DB_URL": "postgresql+psycopg://cnesdata:cnesdata_test@localhost:5433/cnesdata_test",
    "PROFILE": "aws",
    "AWS_REGION": "us-east-2",
    "AWS_CONTROL_PLANE_TABLE": "cnesdata-control-plane",
    "AWS_DATA_BUCKET": "cnesdata-data",
    "AWS_STATE_MACHINE_ARN": (
        "arn:aws:states:us-east-2:000000000000:stateMachine:cnesdata-processor"
    ),
    "AWS_PROCESSOR_CONTAINER_NAME": "processor",
    "AWS_PROCESSOR_MAX_CONCURRENCY": "1",
    "AWS_PROCESSOR_LEASE_SECONDS": "3600",
    "AWS_PRODUCTION_POLICY": "true",
    "AWS_ENVIRONMENT": "prod",
    "AWS_PUBLIC_SUBNET_IDS": "subnet-00000000000000001,subnet-00000000000000002",
    "AWS_SECURITY_GROUP_ID": "sg-00000000000000001",
    "BILLING_MODE": "disabled",
    "BILLING_ENFORCEMENT_MODE": "off",
    "TENANT_ID": "354130",
    "RUN_ID": "run-01",
    "WAVE_ID": "1111111111111111",
    "DISPATCH_ID": "2222222222222222",
    "UNIT_ID": "unit-01",
    "EXECUTION_OWNER": (
        "arn:aws:states:us-east-2:000000000000:execution:cnesdata-processor:execution-01"
    ),
    "LEASE_SECONDS": "3600",
}
RECOVERY_ENV = {
    "PYTHON_DOTENV_DISABLED": "1",
    "DB_URL": "postgresql+psycopg://cnesdata:cnesdata_test@localhost:5433/cnesdata_test",
    "PROFILE": "aws",
    "AWS_REGION": "us-east-2",
    "AWS_CONTROL_PLANE_TABLE": "cnesdata-control-plane",
    "AWS_DATA_BUCKET": "cnesdata-data",
    "AWS_STATE_MACHINE_ARN": (
        "arn:aws:states:us-east-2:000000000000:stateMachine:cnesdata-processor"
    ),
    "AWS_PROCESSOR_CONTAINER_NAME": "processor",
    "AWS_PROCESSOR_MAX_CONCURRENCY": "1",
    "AWS_PRODUCTION_POLICY": "true",
    "AWS_ENVIRONMENT": "prod",
    "AWS_PUBLIC_SUBNET_IDS": "subnet-00000000000000001,subnet-00000000000000002",
    "AWS_SECURITY_GROUP_ID": "sg-00000000000000001",
    "BILLING_MODE": "disabled",
    "BILLING_ENFORCEMENT_MODE": "off",
}


@pytest.fixture
def external_trace() -> Iterator[list[str]]:
    trace: list[str] = []

    def session_spy() -> NoReturn:
        trace.append("Session")
        raise RuntimeError("external_session=deliberate_failure")

    with patch.dict(os.environ, UNIT_ENV, clear=True):
        from data_processor import main as processor_main

        with (
            patch.object(processor_main, "Session", session_spy),
            patch.object(processor_main, "configure_json_stdout"),
            patch.object(processor_main, "init_telemetry"),
            patch("data_processor.deadline.threading.Timer", autospec=True),
        ):
            yield trace


async def _invoke(mode: str, changes: dict[str, str | None]) -> int:
    from data_processor.main import main

    values = dict(UNIT_ENV if mode == "run-unit" else RECOVERY_ENV)
    for name, value in changes.items():
        if value is None:
            values.pop(name, None)
        else:
            values[name] = value
    args = [] if mode == "run-unit" else ["recover-once"]
    with (
        patch.dict(os.environ, values, clear=True),
        patch.object(sys, "argv", ["data-processor", *args]),
    ):
        return await main()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["run-unit", "recover-once"])
async def test_configuracao_minima_sem_api_ou_audit_alcanca_session(
    external_trace: list[str], mode: str,
) -> None:
    result = await _invoke(mode, {})

    assert (result, external_trace) == (1, ["Session"])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        pytest.param({"AWS_PROCESSOR_RECOVERY_BATCH_SIZE": "100"}, id="batch-explicito"),
        pytest.param({"AWS_PROCESSOR_LEASE_SECONDS": "3600"}, id="lease-explicito"),
        pytest.param(
            {"AWS_PROCESSOR_RECOVERY_BATCH_SIZE": "100", "AWS_PROCESSOR_LEASE_SECONDS": "3600"},
            id="batch-e-lease-explicitos",
        ),
    ],
)
async def test_recover_aceita_batch_100_e_lease_3600_explicitos(
    external_trace: list[str], changes: dict[str, str | None],
) -> None:
    result = await _invoke("recover-once", changes)

    assert (result, external_trace) == (1, ["Session"])


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["run-unit", "recover-once"])
async def test_api_e_audit_malformados_sao_irrelevantes_ao_worker(
    external_trace: list[str], mode: str,
) -> None:
    result = await _invoke(mode, {
        "AUTH_MODE": "invalid",
        "OIDC_ISSUER": "invalid",
        "OIDC_AUDIENCE": "",
        "OIDC_DOMAIN": "invalid",
        "AWS_AUDIT_BUCKET": "",
        "AWS_AUDIT_RETENTION_DAYS": "invalid",
        "AWS_SERVING_URL_TTL_SECONDS": "invalid",
    })

    assert (result, external_trace) == (1, ["Session"])


@pytest.mark.asyncio
@pytest.mark.parametrize("batch", ["99", "", "invalid"])
async def test_unidade_nao_consume_knob_de_batch_da_recuperacao(
    external_trace: list[str], batch: str,
) -> None:
    result = await _invoke("run-unit", {"AWS_PROCESSOR_RECOVERY_BATCH_SIZE": batch})

    assert (result, external_trace) == (1, ["Session"])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("name", "value"),
    [
        pytest.param("AWS_PROCESSOR_RECOVERY_BATCH_SIZE", "99", id="batch-menor"),
        pytest.param("AWS_PROCESSOR_RECOVERY_BATCH_SIZE", "101", id="batch-maior"),
        pytest.param("AWS_PROCESSOR_RECOVERY_BATCH_SIZE", "", id="batch-vazio"),
        pytest.param("AWS_PROCESSOR_RECOVERY_BATCH_SIZE", "invalid", id="batch-nao-inteiro"),
        pytest.param("AWS_PROCESSOR_LEASE_SECONDS", "3599", id="lease-menor"),
        pytest.param("AWS_PROCESSOR_LEASE_SECONDS", "3601", id="lease-maior"),
        pytest.param("AWS_PROCESSOR_LEASE_SECONDS", "", id="lease-vazio"),
        pytest.param("AWS_PROCESSOR_LEASE_SECONDS", "invalid", id="lease-nao-inteiro"),
    ],
)
async def test_recover_rejeita_batch_ou_lease_divergente_antes_de_session(
    external_trace: list[str], name: str, value: str,
) -> None:
    result = await _invoke("recover-once", {name: value})

    assert (result, external_trace) == (2, [])


@pytest.mark.asyncio
@pytest.mark.parametrize("lease", ["300", "", "invalid"])
async def test_unidade_de_producao_exige_configuracao_de_lease_3600(
    external_trace: list[str], lease: str,
) -> None:
    result = await _invoke("run-unit", {"AWS_PROCESSOR_LEASE_SECONDS": lease})

    assert (result, external_trace) == (2, [])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mode", "changes"),
    [
        pytest.param("run-unit", {"AWS_CONTROL_PLANE_TABLE": None}, id="unit-tabela-ausente"),
        pytest.param("recover-once", {"AWS_CONTROL_PLANE_TABLE": " "}, id="recover-tabela-branca"),
        pytest.param("recover-once", {"AWS_DATA_BUCKET": None}, id="recover-data-ausente"),
        pytest.param("run-unit", {"AWS_DATA_BUCKET": " "}, id="unit-data-branco"),
        pytest.param("run-unit", {"AWS_STATE_MACHINE_ARN": None}, id="unit-sf-ausente"),
        pytest.param("recover-once", {"AWS_STATE_MACHINE_ARN": " "}, id="recover-sf-branco"),
        pytest.param(
            "run-unit",
            {"AWS_STATE_MACHINE_ARN": "arn:aws:ecs:us-east-2:000000000000:task/processor"},
            id="unit-arn-servico-ecs",
        ),
        pytest.param(
            "recover-once", {"AWS_PROCESSOR_CONTAINER_NAME": None}, id="recover-container-ausente",
        ),
        pytest.param("run-unit", {"AWS_PROCESSOR_CONTAINER_NAME": " "}, id="unit-container-branco"),
        pytest.param("run-unit", {"AWS_REGION": None}, id="unit-regiao-ausente"),
        pytest.param("recover-once", {"AWS_REGION": " "}, id="recover-regiao-branca"),
    ],
)
async def test_recurso_obrigatorio_ausente_ou_branco_falha_antes_de_session(
    external_trace: list[str], mode: str, changes: dict[str, str | None],
) -> None:
    result = await _invoke(mode, changes)

    assert (result, external_trace) == (2, [])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mode", "changes"),
    [
        pytest.param("run-unit", {"AWS_PRODUCTION_POLICY": "invalid"}, id="unit-policy-invalida"),
        pytest.param("recover-once", {"AWS_PRODUCTION_POLICY": ""}, id="recover-policy-vazia"),
        pytest.param("recover-once", {"AWS_REGION": "us-east-1"}, id="recover-regiao-proibida"),
        pytest.param("run-unit", {"AWS_PROCESSOR_MAX_CONCURRENCY": "2"}, id="unit-concorrencia-2"),
        pytest.param(
            "recover-once", {"AWS_PROCESSOR_MAX_CONCURRENCY": "0"}, id="recover-concorrencia-zero",
        ),
        pytest.param("run-unit", {"AWS_ENVIRONMENT": None}, id="unit-ambiente-ausente"),
        pytest.param("recover-once", {"AWS_ENVIRONMENT": "Prod"}, id="recover-ambiente-invalido"),
        pytest.param("run-unit", {"AWS_PUBLIC_SUBNET_IDS": None}, id="unit-subnet-ausente"),
        pytest.param(
            "recover-once", {"AWS_PUBLIC_SUBNET_IDS": "subnet-invalid"},
            id="recover-subnet-invalida",
        ),
        pytest.param(
            "run-unit",
            {"AWS_PUBLIC_SUBNET_IDS": "subnet-00000000000000002,subnet-00000000000000001"},
            id="unit-subnets-fora-de-ordem",
        ),
        pytest.param("recover-once", {"AWS_SECURITY_GROUP_ID": None}, id="recover-sg-ausente"),
        pytest.param("run-unit", {"AWS_SECURITY_GROUP_ID": "sg-invalid"}, id="unit-sg-invalido"),
    ],
)
async def test_politica_de_producao_invalida_falha_antes_de_session(
    external_trace: list[str], mode: str, changes: dict[str, str | None],
) -> None:
    result = await _invoke(mode, changes)

    assert (result, external_trace) == (2, [])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("values", "args"),
    [
        pytest.param(
            {
                "PYTHON_DOTENV_DISABLED": "1",
                "DB_URL": (
                    "postgresql+psycopg://cnesdata:cnesdata_test@localhost:5433/cnesdata_test"
                ),
                "PROFILE": "aws",
                "AWS_REGION": "us-east-2",
                "AWS_CONTROL_PLANE_TABLE": "cnesdata-control-plane",
                "AWS_DATA_BUCKET": "cnesdata-data",
                "AWS_STATE_MACHINE_ARN": (
                    "arn:aws:states:us-east-2:000000000000:stateMachine:cnesdata-processor"
                ),
                "AWS_PROCESSOR_CONTAINER_NAME": "processor",
                "AWS_PROCESSOR_MAX_CONCURRENCY": "8",
                "AWS_PROCESSOR_LEASE_SECONDS": "300",
                "AWS_PRODUCTION_POLICY": "false",
                "BILLING_MODE": "disabled",
                "BILLING_ENFORCEMENT_MODE": "off",
                "TENANT_ID": "354130",
                "RUN_ID": "run-01",
                "WAVE_ID": "1111111111111111",
                "DISPATCH_ID": "2222222222222222",
                "UNIT_ID": "unit-01",
                "EXECUTION_OWNER": (
                    "arn:aws:states:us-east-2:000000000000:"
                    "execution:cnesdata-processor:execution-01"
                ),
                "LEASE_SECONDS": "300",
            },
            [],
            id="unit-generico",
        ),
        pytest.param(
            {
                "PYTHON_DOTENV_DISABLED": "1",
                "DB_URL": (
                    "postgresql+psycopg://cnesdata:cnesdata_test@localhost:5433/cnesdata_test"
                ),
                "PROFILE": "aws",
                "AWS_REGION": "us-east-2",
                "AWS_CONTROL_PLANE_TABLE": "cnesdata-control-plane",
                "AWS_DATA_BUCKET": "cnesdata-data",
                "AWS_STATE_MACHINE_ARN": (
                    "arn:aws:states:us-east-2:000000000000:stateMachine:cnesdata-processor"
                ),
                "AWS_PROCESSOR_CONTAINER_NAME": "processor",
                "AWS_PROCESSOR_MAX_CONCURRENCY": "8",
                "AWS_PRODUCTION_POLICY": "false",
                "BILLING_MODE": "disabled",
                "BILLING_ENFORCEMENT_MODE": "off",
            },
            ["recover-once"],
            id="recover-generico",
        ),
    ],
)
async def test_aws_generico_sem_config_api_ou_audit_alcanca_session(
    external_trace: list[str], values: dict[str, str], args: list[str],
) -> None:
    from data_processor.main import main

    with (
        patch.dict(os.environ, values, clear=True),
        patch.object(sys, "argv", ["data-processor", *args]),
    ):
        result = await main()

    assert (result, external_trace) == (1, ["Session"])
