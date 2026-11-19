"""Validação pública da invocação AWS antes de abrir a sessão externa."""

import os
import sys
from collections.abc import Iterator
from typing import NoReturn
from unittest.mock import patch

import pytest

PRODUCTION_ENV = {
    "PYTHON_DOTENV_DISABLED": "1",
    "DB_URL": "postgresql+psycopg://cnesdata:cnesdata_test@localhost:5433/cnesdata_test",
    "PROFILE": "aws",
    "AUTH_MODE": "oidc",
    "AWS_REGION": "us-east-2",
    "AWS_CONTROL_PLANE_TABLE": "cnesdata-control-plane",
    "AWS_DATA_BUCKET": "cnesdata-data",
    "AWS_AUDIT_BUCKET": "cnesdata-audit",
    "AWS_STATE_MACHINE_ARN": (
        "arn:aws:states:us-east-2:000000000000:stateMachine:cnesdata-processor"
    ),
    "AWS_PROCESSOR_CONTAINER_NAME": "processor",
    "AWS_PROCESSOR_MAX_CONCURRENCY": "1",
    "AWS_PROCESSOR_LEASE_SECONDS": "3600",
    "AWS_PROCESSOR_RECOVERY_BATCH_SIZE": "100",
    "AWS_SERVING_URL_TTL_SECONDS": "300",
    "AWS_AUDIT_RETENTION_DAYS": "365",
    "AWS_PRODUCTION_POLICY": "true",
    "AWS_ENVIRONMENT": "prod",
    "AWS_PUBLIC_SUBNET_IDS": "subnet-00000000000000001,subnet-00000000000000002",
    "AWS_SECURITY_GROUP_ID": "sg-00000000000000001",
    "OIDC_ISSUER": "https://cognito-idp.us-east-2.amazonaws.com/us-east-2_Test123",
    "OIDC_AUDIENCE": "https://api.cnesdata.com.br",
    "OIDC_DOMAIN": "https://cnesdata-test.auth.us-east-2.amazoncognito.com",
    "BILLING_MODE": "disabled",
    "BILLING_ENFORCEMENT_MODE": "off",
}
UNIT_ENV = {
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


@pytest.fixture
def external_trace() -> Iterator[list[str]]:
    trace: list[str] = []

    def session_spy() -> NoReturn:
        trace.append("Session")
        raise RuntimeError("external_session=deliberate_failure")

    with patch.dict(os.environ, PRODUCTION_ENV, clear=True):
        from data_processor import main as processor_main

        with (
            patch.object(processor_main, "Session", session_spy),
            patch.object(processor_main, "configure_json_stdout"),
            patch.object(processor_main, "init_telemetry"),
            patch("data_processor.deadline.threading.Timer", autospec=True),
        ):
            yield trace


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("name", "value"),
    [
        pytest.param("TENANT_ID", "   ", id="tenant-em-branco"),
        pytest.param("RUN_ID", "", id="run-vazio"),
        pytest.param("UNIT_ID", "", id="unidade-vazia"),
        pytest.param("WAVE_ID", "111111111111111A", id="wave-hex-maiusculo"),
        pytest.param("DISPATCH_ID", "2222", id="dispatch-hex-curto"),
        pytest.param(
            "EXECUTION_OWNER", "arn:aws:ecs:us-east-2:000000000000:task/x", id="owner-ecs",
        ),
        pytest.param("LEASE_SECONDS", "29", id="lease-abaixo-minimo"),
        pytest.param("LEASE_SECONDS", "3601", id="lease-acima-maximo"),
        pytest.param("LEASE_SECONDS", "trezentos", id="lease-nao-inteiro"),
    ],
)
async def test_rejeita_envelope_malformado_antes_de_session(
    external_trace: list[str], name: str, value: str,
) -> None:
    from data_processor.main import main

    with (
        patch.dict(os.environ, UNIT_ENV | {name: value}),
        patch.object(sys, "argv", ["data-processor"]),
    ):
        result = await main()

    assert (result, external_trace) == (2, [])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "partial",
    [
        pytest.param({"TENANT_ID": "354130"}, id="somente-tenant"),
        pytest.param({"UNIT_ID": "unit-01"}, id="somente-unidade"),
        pytest.param(
            {
                "TENANT_ID": "354130", "RUN_ID": "run-01", "WAVE_ID": "1111111111111111",
                "DISPATCH_ID": "2222222222222222", "UNIT_ID": "unit-01", "LEASE_SECONDS": "3600",
            },
            id="owner-ausente",
        ),
    ],
)
async def test_rejeita_envelope_parcial_antes_de_session(
    external_trace: list[str], partial: dict[str, str],
) -> None:
    from data_processor.main import main

    with (
        patch.dict(os.environ, partial),
        patch.object(sys, "argv", ["data-processor"]),
    ):
        result = await main()

    assert (result, external_trace) == (2, [])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name",
    [
        "TENANT_ID", "RUN_ID", "WAVE_ID", "DISPATCH_ID", "UNIT_ID", "EXECUTION_OWNER",
        "LEASE_SECONDS",
    ],
)
async def test_recover_rejeita_variavel_de_unidade_presente_vazia_antes_de_session(
    external_trace: list[str], name: str,
) -> None:
    from data_processor.main import main

    with (
        patch.dict(os.environ, {name: ""}),
        patch.object(sys, "argv", ["data-processor", "recover-once"]),
    ):
        result = await main()

    assert (result, external_trace) == (2, [])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("values", "args"),
    [
        pytest.param({}, [], id="comando-ausente"),
        pytest.param({}, ["unknown-mode"], id="comando-desconhecido"),
        pytest.param({}, ["recover-once", "--extra"], id="recover-argumento-extra"),
        pytest.param({}, ["recover-once", "recover-once"], id="recover-repetido"),
        pytest.param(UNIT_ENV, ["recover-once"], id="unidade-com-recover"),
        pytest.param(UNIT_ENV, ["--extra"], id="unidade-com-argumento-extra"),
    ],
)
async def test_rejeita_argv_invalido_antes_de_session(
    external_trace: list[str], values: dict[str, str], args: list[str],
) -> None:
    from data_processor.main import main

    with (
        patch.dict(os.environ, values),
        patch.object(sys, "argv", ["data-processor", *args]),
    ):
        result = await main()

    assert (result, external_trace) == (2, [])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("values", "args"),
    [
        pytest.param(UNIT_ENV, [], id="unidade-valida"),
        pytest.param({}, ["recover-once"], id="recover-valido"),
    ],
)
async def test_invocacao_valida_alcanca_session_e_classifica_falha_externa_como_1(
    external_trace: list[str], values: dict[str, str], args: list[str],
) -> None:
    from data_processor.main import main

    with (
        patch.dict(os.environ, values),
        patch.object(sys, "argv", ["data-processor", *args]),
    ):
        result = await main()

    assert (result, external_trace) == (1, ["Session"])
