"""Fronteira ECS: envelope de sete variáveis e modo recover-once."""
from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from unittest.mock import Mock

import pytest

from cnes_domain.ports.processing import RunUnitMessage
from data_processor.aws_entrypoint import (
    EcsUnitEnvelope,
    EntrypointConfigurationError,
    run_aws_entrypoint,
)
from data_processor.composition import AwsProcessorServices, ProcessorRuntimeComponents
from data_processor.recovery import ProcessorRecovery

NOW = datetime(2026, 9, 27, 12, tzinfo=UTC)
EXECUTION_ARN = "arn:aws:states:us-east-1:000000000000:execution:cnesdata-test:2222222222222222"
ENVELOPE = {
    "TENANT_ID": "354130",
    "RUN_ID": "run-01",
    "WAVE_ID": "1111111111111111",
    "DISPATCH_ID": "2222222222222222",
    "UNIT_ID": "unit-01",
    "EXECUTION_OWNER": EXECUTION_ARN,
    "LEASE_SECONDS": "300",
}
EXPECTED_MESSAGE = RunUnitMessage(
    tenant_id="354130", run_id="run-01", wave_id="1111111111111111",
    dispatch_id="2222222222222222", unit_id="unit-01", owner=EXECUTION_ARN,
    now=NOW, lease_seconds=300,
)


@pytest.fixture(autouse=True)
def _frozen_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("data_processor.aws_entrypoint._utc_now", lambda: NOW)


@pytest.fixture
def runtime() -> ProcessorRuntimeComponents:
    return ProcessorRuntimeComponents(
        control_plane=Mock(), object_store=Mock(), executor=Mock(), publisher=Mock(),
        source_registry=Mock(), stage_processor=Mock(), coordinator=Mock(),
        unit_worker=Mock(), unit_handler=Mock(),
        services=AwsProcessorServices(
            recovery=Mock(spec=ProcessorRecovery), recovery_batch_size=100,
        ),
    )


def test_entrypoint_ecs_converte_env_em_claim_canonico(
    runtime: ProcessorRuntimeComponents,
) -> None:
    assert run_aws_entrypoint(runtime, ENVELOPE, ()) == 0

    runtime.unit_handler.handle.assert_called_once_with(EXPECTED_MESSAGE)
    runtime.services.recovery.run_once.assert_not_called()


def test_envelope_gera_mensagem_exata_no_instante_informado() -> None:
    assert EcsUnitEnvelope.from_mapping(ENVELOPE).message(NOW) == EXPECTED_MESSAGE


def test_entrypoint_sem_unit_executa_recovery_once(runtime: ProcessorRuntimeComponents) -> None:
    assert run_aws_entrypoint(runtime, {}, ("recover-once",)) == 0

    runtime.services.recovery.run_once.assert_called_once_with(limit=100)
    runtime.unit_handler.handle.assert_not_called()


def test_entrypoint_propaga_falha_do_handler(runtime: ProcessorRuntimeComponents) -> None:
    runtime.unit_handler.handle.side_effect = RuntimeError("lease=lost")

    with pytest.raises(RuntimeError, match="lease=lost"):
        run_aws_entrypoint(runtime, ENVELOPE, ())


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"WAVE_ID": "111111111111111A"}, "invalid=WAVE_ID"),
        ({"WAVE_ID": "1111"}, "invalid=WAVE_ID"),
        ({"DISPATCH_ID": "2222222222222222ff"}, "invalid=DISPATCH_ID"),
        ({"EXECUTION_OWNER": ""}, "missing=EXECUTION_OWNER"),
        ({"EXECUTION_OWNER": "arn:aws:ecs:us-east-1:1:task/x"}, "invalid=EXECUTION_OWNER"),
        ({"LEASE_SECONDS": "29"}, "invalid=LEASE_SECONDS"),
        ({"LEASE_SECONDS": "3601"}, "invalid=LEASE_SECONDS"),
        ({"LEASE_SECONDS": "trezentos"}, "invalid=LEASE_SECONDS"),
        ({"TENANT_ID": " "}, "missing=TENANT_ID"),
        ({"RUN_ID": ""}, "missing=RUN_ID"),
    ],
    ids=[
        "wave-maiusculo", "wave-curto", "dispatch-longo", "owner-ausente", "owner-fora-sfn",
        "lease-baixo", "lease-alto", "lease-nao-inteiro", "tenant-em-branco", "run-ausente",
    ],
)
def test_envelope_invalido_falha_sem_chamar_handler(
    runtime: ProcessorRuntimeComponents, overrides: dict[str, str], message: str,
) -> None:
    with pytest.raises(EntrypointConfigurationError, match=message):
        run_aws_entrypoint(runtime, ENVELOPE | overrides, ())

    runtime.unit_handler.handle.assert_not_called()


@pytest.mark.parametrize(
    "name", ["TENANT_ID", "RUN_ID", "WAVE_ID", "DISPATCH_ID", "EXECUTION_OWNER", "LEASE_SECONDS"],
)
def test_envelope_parcial_sem_unit_falha(
    runtime: ProcessorRuntimeComponents, name: str,
) -> None:
    with pytest.raises(EntrypointConfigurationError, match="unit_envelope=partial"):
        run_aws_entrypoint(runtime, {name: ENVELOPE[name]}, ("recover-once",))

    runtime.services.recovery.run_once.assert_not_called()


def test_modo_unit_rejeita_argumentos(runtime: ProcessorRuntimeComponents) -> None:
    with pytest.raises(EntrypointConfigurationError, match="unit_mode=argv_forbidden"):
        run_aws_entrypoint(runtime, ENVELOPE, ("recover-once",))

    runtime.unit_handler.handle.assert_not_called()


@pytest.mark.parametrize(
    "argv", [(), ("recover",), ("recover-once", "--verbose")], ids=["vazio", "outro", "extra"],
)
def test_modo_recovery_exige_somente_recover_once(
    runtime: ProcessorRuntimeComponents, argv: tuple[str, ...],
) -> None:
    with pytest.raises(EntrypointConfigurationError, match="command=recover_once_required"):
        run_aws_entrypoint(runtime, {}, argv)

    runtime.services.recovery.run_once.assert_not_called()


def test_runtime_local_e_rejeitado_na_fronteira_aws(
    runtime: ProcessorRuntimeComponents,
) -> None:
    local = replace(runtime, services=None)

    with pytest.raises(EntrypointConfigurationError, match="aws_services=missing"):
        run_aws_entrypoint(local, ENVELOPE, ())

    runtime.unit_handler.handle.assert_not_called()
