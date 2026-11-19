"""Fronteira ECS: envelope de sete variáveis e modo recover-once."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import Mock

import pytest

from cnes_domain.ports.processing import RunUnitMessage
from data_processor.aws_composition import AwsRecoveryRuntime, AwsUnitRuntime
from data_processor.aws_entrypoint import (
    AwsInvocation,
    AwsMode,
    EcsUnitEnvelope,
    EntrypointConfigurationError,
    run_aws_entrypoint,
    select_aws_invocation,
)
from data_processor.composition import LocalProcessorRuntime
from data_processor.recovery import ProcessorRecovery

if TYPE_CHECKING:
    from data_processor.aws_composition import AwsModeRuntime

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
def runtime() -> AwsUnitRuntime:
    return AwsUnitRuntime(unit_handler=Mock())


@pytest.fixture
def recovery_runtime() -> AwsRecoveryRuntime:
    return AwsRecoveryRuntime(recovery=Mock(spec=ProcessorRecovery))


def test_entrypoint_ecs_converte_env_em_claim_canonico(
    runtime: Any,
) -> None:
    assert run_aws_entrypoint(select_aws_invocation(ENVELOPE, ()), runtime) == 0

    runtime.unit_handler.handle.assert_called_once_with(EXPECTED_MESSAGE)


def test_envelope_gera_mensagem_exata_no_instante_informado() -> None:
    assert EcsUnitEnvelope.from_mapping(ENVELOPE).message(NOW) == EXPECTED_MESSAGE


def test_entrypoint_sem_unit_executa_recovery_once(recovery_runtime: Any) -> None:
    assert run_aws_entrypoint(
        select_aws_invocation({}, ("recover-once",)), recovery_runtime,
    ) == 0

    recovery_runtime.recovery.run_once.assert_called_once_with(limit=100)


def test_entrypoint_propaga_falha_do_handler(runtime: Any) -> None:
    runtime.unit_handler.handle.side_effect = RuntimeError("lease=lost")

    with pytest.raises(RuntimeError, match="lease=lost"):
        run_aws_entrypoint(select_aws_invocation(ENVELOPE, ()), runtime)


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
    runtime: Any, overrides: dict[str, str], message: str,
) -> None:
    with pytest.raises(EntrypointConfigurationError, match=message):
        select_aws_invocation(ENVELOPE | overrides, ())

    runtime.unit_handler.handle.assert_not_called()


@pytest.mark.parametrize(
    "name", ["TENANT_ID", "RUN_ID", "WAVE_ID", "DISPATCH_ID", "EXECUTION_OWNER", "LEASE_SECONDS"],
)
def test_envelope_parcial_sem_unit_falha(
    recovery_runtime: Any, name: str,
) -> None:
    with pytest.raises(EntrypointConfigurationError, match="unit_envelope=partial"):
        select_aws_invocation({name: ENVELOPE[name]}, ("recover-once",))

    recovery_runtime.recovery.run_once.assert_not_called()


def test_modo_unit_rejeita_argumentos(runtime: Any) -> None:
    with pytest.raises(EntrypointConfigurationError, match="unit_mode=argv_forbidden"):
        select_aws_invocation(ENVELOPE, ("recover-once",))

    runtime.unit_handler.handle.assert_not_called()


@pytest.mark.parametrize(
    "argv", [(), ("recover",), ("recover-once", "--verbose")], ids=["vazio", "outro", "extra"],
)
def test_modo_recovery_exige_somente_recover_once(
    recovery_runtime: Any, argv: tuple[str, ...],
) -> None:
    with pytest.raises(EntrypointConfigurationError, match="command=recover_once_required"):
        select_aws_invocation({}, argv)

    recovery_runtime.recovery.run_once.assert_not_called()


def test_runtime_local_e_rejeitado_na_fronteira_aws() -> None:
    local = LocalProcessorRuntime(**{
        name: Mock(name=name) for name in (
            "control_plane", "object_store", "audit_sink", "executor", "publisher",
            "source_registry", "stage_processor", "coordinator", "unit_worker", "unit_handler",
        )
    })

    with pytest.raises(EntrypointConfigurationError):
        run_aws_entrypoint(select_aws_invocation(ENVELOPE, ()), cast("AwsModeRuntime", local))

    cast("Any", local.unit_handler).handle.assert_not_called()


@pytest.mark.parametrize("mode", [AwsMode.RUN_UNIT, AwsMode.RECOVER_ONCE])
def test_modo_rejeita_runtime_incompativel_sem_executar(
    mode: AwsMode, runtime: Any, recovery_runtime: Any,
) -> None:
    invocation = select_aws_invocation(ENVELOPE, ()) if mode is AwsMode.RUN_UNIT else (
        select_aws_invocation({}, ("recover-once",))
    )
    incompatible = recovery_runtime if mode is AwsMode.RUN_UNIT else runtime

    with pytest.raises(EntrypointConfigurationError):
        run_aws_entrypoint(invocation, incompatible)

    runtime.unit_handler.handle.assert_not_called()
    recovery_runtime.recovery.run_once.assert_not_called()


def test_modo_unit_sem_envelope_falha_sem_executar(runtime: Any) -> None:
    with pytest.raises(EntrypointConfigurationError):
        run_aws_entrypoint(AwsInvocation(AwsMode.RUN_UNIT, None), runtime)

    runtime.unit_handler.handle.assert_not_called()


_LOCATOR = (
    '{"context_kind":"promotion","owner_sha256":"' + "a" * 64 + '","phase":"recovery",'
    '"record_sha256":"' + "b" * 64 + '","run_ids":["recovery-a-0123456789abcdef"],'
    '"tenant_id":"prod-canary-0123456789abcdef0123456789abcdef"}'
)
_OBSERVE = (
    '{"event_ids":["e1"],"owner_sha256":"' + "a" * 64 + '","record_sha256":"' + "b" * 64 + '"}'
)


def test_recover_once_aceita_locator_do_binding_sem_autorizar_por_si() -> None:
    invocation = select_aws_invocation({"PROD_CANARY_BINDING": _LOCATOR}, ["recover-once"])

    assert invocation.mode is AwsMode.RECOVER_ONCE
    binding = cast("Any", invocation.mode_canary)
    assert binding.run_ids == ("recovery-a-0123456789abcdef",)
    assert binding.phase == "recovery"
    assert select_aws_invocation({}, ["recover-once"]).mode_canary is None


@pytest.mark.parametrize(
    ("values", "argv", "message"),
    [
        ({"PROD_CANARY_BINDING": "{"}, ["recover-once"], "invalid=PROD_CANARY_BINDING"),
        ({"PROD_CANARY_BINDING": _LOCATOR.replace('"recovery"', '"unit"')}, ["recover-once"],
         "invalid=PROD_CANARY_BINDING"),
        ({"PROD_CANARY_BINDING": _LOCATOR, **ENVELOPE}, [], "mode_canary=forbidden"),
        ({"PROD_CANARY_BINDING": _LOCATOR}, ["dispatch-outbox-once"], "mode_canary=forbidden"),
        ({"PROD_CANARY_AUDIT_OBSERVE": _OBSERVE}, ["recover-once"], "audit_observe=forbidden"),
        ({"PROD_CANARY_AUDIT_OBSERVE": _OBSERVE, **ENVELOPE}, [], "audit_observe=forbidden"),
        ({"PROD_CANARY_AUDIT_OBSERVE": "[]"}, ["dispatch-outbox-once"],
         "invalid=PROD_CANARY_AUDIT_OBSERVE"),
    ],
)
def test_overrides_canary_fora_do_modo_ou_invalidos_falham_fechado(
    values: dict[str, str], argv: list[str], message: str,
) -> None:
    with pytest.raises(EntrypointConfigurationError, match=message):
        select_aws_invocation(values, argv)


def test_audit_aceita_observacao_sem_mudar_o_modo() -> None:
    invocation = select_aws_invocation(
        {"PROD_CANARY_AUDIT_OBSERVE": _OBSERVE}, ["dispatch-outbox-once"],
    )

    assert invocation.mode is AwsMode.DISPATCH_OUTBOX_ONCE
    assert cast("Any", invocation.audit_observe).event_ids == ("e1",)
    assert invocation.mode_canary is None
