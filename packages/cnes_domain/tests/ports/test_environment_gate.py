"""Testes do gate de ambiente: transições puras de fence/semáforo e portas de admissão."""

from collections.abc import Callable
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta, timezone
from inspect import signature
from typing import Any, cast

import pytest

from cnes_domain.control_plane.environment_gate import (
    AcquireUnitPermit,
    AcquisitionStatus,
    BindUnitPermit,
    ClosePromotionFence,
    EnvironmentGateState,
    FenceState,
    PermitAcquisition,
    PermitHolder,
    PromotionFence,
    ReleaseUnitPermit,
    RenewUnitPermit,
    ReopenPromotionFence,
    UnitPermit,
    bind_permit,
    close_fence,
    decide_acquisition,
    issue_permit,
    open_fence,
    release_permit,
    renew_permit,
    reopen_fence,
    require_environment,
)
from cnes_domain.ports.environment_gate import (
    UNGATED_ADMISSION,
    DeferralReason,
    DispatchAdmissionPort,
    DispatchDeferred,
    EnvironmentGatePort,
    ExecutionObservation,
    ExecutionPresence,
    ExecutionProbePort,
    UnitAdmission,
)

NOW = datetime(2026, 10, 11, 12, tzinfo=UTC)
LATER = NOW + timedelta(minutes=20)
ENV = "prod"
PROMOTION = "release-2026-10-11"
OTHER_PROMOTION = "release-2026-10-12"
HOLDER = PermitHolder("354130", "run-01", "0123456789abcdef")
OTHER = PermitHolder("354130", "run-02", "fedcba9876543210")
NAIVE = datetime(2026, 10, 11, 12)  # noqa: DTZ001
OFFSET = NOW.astimezone(timezone(timedelta(hours=-3)))
PORT_SIGNATURES = {
    EnvironmentGatePort: {
        "observe_environment_gate": ((("environment", "str"),), "EnvironmentGateState"),
        "acquire_unit_permit": ((("command", "AcquireUnitPermit"),), "PermitAcquisition"),
        "bind_unit_permit": ((("command", "BindUnitPermit"),), "bool"),
        "renew_unit_permit": ((("command", "RenewUnitPermit"),), "bool"),
        "release_unit_permit": ((("command", "ReleaseUnitPermit"),), "bool"),
        "close_promotion_fence": ((("command", "ClosePromotionFence"),), "bool"),
        "reopen_promotion_fence": ((("command", "ReopenPromotionFence"),), "bool"),
    },
    ExecutionProbePort: {
        "probe": ((("dispatch_id", "str"),), "ExecutionObservation"),
    },
    DispatchAdmissionPort: {
        "admit": (
            (("holder", "PermitHolder"), ("now", "datetime")),
            "UnitAdmission | DispatchDeferred",
        ),
        "confirm": (
            (("admission", "UnitAdmission"), ("execution_ref", "str"), ("now", "datetime")),
            "None",
        ),
        "settle": ((("holder", "PermitHolder"), ("now", "datetime")), "None"),
    },
}


def _acquire(
    holder: PermitHolder = HOLDER, *, now: datetime = NOW, canary: str | None = None,
) -> AcquireUnitPermit:
    return AcquireUnitPermit(ENV, holder, now, 300, canary)


def _state(
    fence: PromotionFence | None = None, permit: UnitPermit | None = None,
) -> EnvironmentGateState:
    return EnvironmentGateState(ENV, fence or open_fence(ENV), permit)


def _closed(promotion: str = PROMOTION) -> PromotionFence:
    fence = close_fence(open_fence(ENV), ClosePromotionFence(ENV, promotion, NOW))
    assert fence is not None
    return fence


def _reopened() -> PromotionFence:
    fence = reopen_fence(_closed(), ReopenPromotionFence(ENV, PROMOTION, LATER))
    assert fence is not None
    return fence


def _bound(permit: UnitPermit, execution_ref: str = "exec-1") -> UnitPermit:
    bound = bind_permit(permit, BindUnitPermit(ENV, permit.token, execution_ref, NOW))
    assert bound is not None
    return bound


def test_fechar_fence_primeiro_bloqueia_acquire() -> None:
    fence = _closed()
    live = issue_permit(_acquire(OTHER), 4)

    assert fence == PromotionFence(ENV, FenceState.CLOSED, PROMOTION, 1, NOW)
    assert decide_acquisition(_state(fence), _acquire()) is AcquisitionStatus.FENCE_CLOSED
    assert decide_acquisition(_state(fence, live), _acquire()) is AcquisitionStatus.FENCE_CLOSED
    assert decide_acquisition(_state(), _acquire()) is AcquisitionStatus.ACQUIRED


def test_acquire_bind_renew_release() -> None:
    command = _acquire()
    renewal = NOW + timedelta(minutes=4)

    assert decide_acquisition(_state(), command) is AcquisitionStatus.ACQUIRED
    permit = issue_permit(command, 1)
    assert permit == UnitPermit(ENV, 1, HOLDER, None, NOW, NOW + timedelta(seconds=300), None)
    bound = _bound(permit)
    assert bound == replace(permit, execution_ref="exec-1")
    assert bind_permit(bound, BindUnitPermit(ENV, 1, "exec-1", LATER)) == bound
    renewed = renew_permit(bound, RenewUnitPermit(ENV, 1, renewal, 600))
    assert renewed == replace(bound, lease_until=renewal + timedelta(seconds=600))
    assert release_permit(renewed, ReleaseUnitPermit(ENV, 1, LATER)) is True
    assert decide_acquisition(_state(permit=renewed), command) is AcquisitionStatus.REENTERED
    assert decide_acquisition(_state(permit=renewed), _acquire(OTHER)) is AcquisitionStatus.HELD
    assert decide_acquisition(_state(), _acquire(OTHER)) is AcquisitionStatus.ACQUIRED


def test_so_canary_com_bind_passa_com_fence_fechado() -> None:
    fenced = _state(_closed())
    canary = _acquire(OTHER, canary=PROMOTION)

    assert decide_acquisition(fenced, canary) is AcquisitionStatus.ACQUIRED
    assert decide_acquisition(fenced, _acquire(OTHER, canary=OTHER_PROMOTION)) is (
        AcquisitionStatus.FENCE_CLOSED
    )
    assert decide_acquisition(fenced, _acquire(OTHER)) is AcquisitionStatus.FENCE_CLOSED
    permit = _bound(issue_permit(canary, 7), "exec-canary")
    assert (permit.canary_promotion_id, permit.execution_ref) == (PROMOTION, "exec-canary")
    assert decide_acquisition(replace(fenced, permit=permit), _acquire()) is (
        AcquisitionStatus.FENCE_CLOSED
    )
    assert decide_acquisition(replace(fenced, permit=permit), canary) is (
        AcquisitionStatus.REENTERED
    )
    assert decide_acquisition(_state(), _acquire(canary=PROMOTION)) is AcquisitionStatus.ACQUIRED


def test_token_divergente_e_rejeitado() -> None:
    permit = issue_permit(_acquire(), 2)
    bound = _bound(permit)
    successor = issue_permit(_acquire(OTHER), 3)

    assert bind_permit(permit, BindUnitPermit(ENV, 1, "exec-1", NOW)) is None
    assert bind_permit(bound, BindUnitPermit(ENV, 2, "exec-2", NOW)) is None
    assert bind_permit(None, BindUnitPermit(ENV, 2, "exec-1", NOW)) is None
    assert renew_permit(permit, RenewUnitPermit(ENV, 3, NOW, 300)) is None
    assert renew_permit(None, RenewUnitPermit(ENV, 2, NOW, 300)) is None
    assert release_permit(permit, ReleaseUnitPermit(ENV, 1, NOW)) is False
    assert release_permit(None, ReleaseUnitPermit(ENV, 2, NOW)) is False
    assert release_permit(successor, ReleaseUnitPermit(ENV, 2, NOW)) is False
    assert bind_permit(successor, BindUnitPermit(ENV, 2, "exec-1", NOW)) is None


def test_ttl_nao_e_autoridade() -> None:
    permit = issue_permit(_acquire(), 1)
    expired = NOW + timedelta(days=2)
    stale = _state(permit=permit)

    assert permit.lease_until < expired
    assert decide_acquisition(stale, _acquire(OTHER, now=expired)) is AcquisitionStatus.HELD
    assert decide_acquisition(stale, _acquire(now=expired)) is AcquisitionStatus.REENTERED
    assert bind_permit(permit, BindUnitPermit(ENV, 1, "exec-1", expired)) is not None
    renewed = renew_permit(permit, RenewUnitPermit(ENV, 1, expired, 60))
    assert renewed is not None
    assert renewed.lease_until == expired + timedelta(seconds=60)
    assert release_permit(permit, ReleaseUnitPermit(ENV, 1, expired)) is True


def test_reabrir_so_com_a_promocao_correspondente() -> None:
    fence = _closed()

    assert reopen_fence(fence, ReopenPromotionFence(ENV, OTHER_PROMOTION, LATER)) is None
    assert reopen_fence(open_fence(ENV), ReopenPromotionFence(ENV, PROMOTION, LATER)) is None
    reopened = reopen_fence(fence, ReopenPromotionFence(ENV, PROMOTION, LATER))
    assert reopened == PromotionFence(ENV, FenceState.OPEN, PROMOTION, 2, LATER)
    assert reopen_fence(_reopened(), ReopenPromotionFence(ENV, PROMOTION, NOW)) == reopened
    assert decide_acquisition(_state(reopened), _acquire()) is AcquisitionStatus.ACQUIRED


def test_fechar_fence_so_e_idempotente_para_a_mesma_promocao() -> None:
    fence = _closed()

    assert close_fence(fence, ClosePromotionFence(ENV, PROMOTION, LATER)) == fence
    assert close_fence(fence, ClosePromotionFence(ENV, OTHER_PROMOTION, LATER)) is None
    assert close_fence(_reopened(), ClosePromotionFence(ENV, OTHER_PROMOTION, LATER)) == (
        PromotionFence(ENV, FenceState.CLOSED, OTHER_PROMOTION, 3, LATER)
    )


def test_close_atrasado_apos_reabertura_da_mesma_promocao_e_rejeitado() -> None:
    assert close_fence(_reopened(), ClosePromotionFence(ENV, PROMOTION, LATER)) is None


@pytest.mark.parametrize(
    "value", ["", "Prod", "1prod", "prod_1", "prod#fence", "prod ", "a" * 33, None, 7],
)
def test_ambiente_fora_do_padrao_e_rejeitado(value: object) -> None:
    with pytest.raises(ValueError, match="reason=invalid_environment"):
        require_environment(value)


@pytest.mark.parametrize("value", ["prod", "default", "a", "a" + "-" * 31, "dev-2"])
def test_ambiente_no_padrao_e_aceito(value: str) -> None:
    assert require_environment(value) == value


@pytest.mark.parametrize("instant", [NAIVE, OFFSET])
@pytest.mark.parametrize(
    "build",
    [
        lambda at: AcquireUnitPermit(ENV, HOLDER, at, 300),
        lambda at: BindUnitPermit(ENV, 1, "exec-1", at),
        lambda at: RenewUnitPermit(ENV, 1, at, 300),
        lambda at: ReleaseUnitPermit(ENV, 1, at),
        lambda at: ClosePromotionFence(ENV, PROMOTION, at),
        lambda at: ReopenPromotionFence(ENV, PROMOTION, at),
        lambda at: PromotionFence(ENV, FenceState.CLOSED, PROMOTION, 1, at),
        lambda at: UnitPermit(ENV, 1, HOLDER, None, at, NOW, None),
        lambda at: UnitPermit(ENV, 1, HOLDER, None, NOW, at, None),
    ],
)
def test_instantes_sem_utc_sao_rejeitados(
    build: Callable[[datetime], object], instant: datetime,
) -> None:
    with pytest.raises(ValueError, match="reason=datetime_not_utc"):
        build(instant)


@pytest.mark.parametrize(
    ("build", "reason"),
    [
        (lambda: PermitHolder(" ", "run-01", HOLDER.dispatch_id), "blank_value field=tenant_id"),
        (lambda: PermitHolder("354130", "", HOLDER.dispatch_id), "blank_value field=run_id"),
        (lambda: PermitHolder("354130", "run-01", "ABCDEF0123456789"), "invalid_hex16"),
        (lambda: UnitPermit(ENV, 0, HOLDER, None, NOW, NOW, None), "out_of_range field=token"),
        (lambda: UnitPermit(ENV, True, HOLDER, None, NOW, NOW, None), "out_of_range field=token"),
        (lambda: UnitPermit(ENV, 1, HOLDER, " ", NOW, NOW, None),
         "blank_value field=execution_ref"),
        (lambda: UnitPermit(ENV, 1, HOLDER, None, NOW, NOW, ""),
         "blank_value field=canary_promotion_id"),
        (lambda: PromotionFence(ENV, FenceState.OPEN, None, -1, None),
         "out_of_range field=version"),
        (lambda: PromotionFence(ENV, FenceState.CLOSED, None, 1, NOW), "closed_fence"),
        (lambda: PromotionFence("Prod", FenceState.OPEN, None, 0, None), "invalid_environment"),
        (lambda: AcquireUnitPermit(ENV, HOLDER, NOW, 0), "out_of_range field=lease_seconds"),
        (lambda: AcquireUnitPermit(ENV, HOLDER, NOW, 300, " "),
         "blank_value field=canary_promotion_id"),
        (lambda: BindUnitPermit(ENV, 1, "", NOW), "blank_value field=execution_ref"),
        (lambda: RenewUnitPermit(ENV, 0, NOW, 300), "out_of_range field=token"),
        (lambda: RenewUnitPermit(ENV, 1, NOW, 0), "out_of_range field=lease_seconds"),
        (lambda: ReleaseUnitPermit("prod#x", 1, NOW), "invalid_environment"),
        (lambda: ClosePromotionFence(ENV, " ", NOW), "blank_value field=promotion_id"),
        (lambda: ReopenPromotionFence(ENV, "", NOW), "blank_value field=promotion_id"),
        (lambda: EnvironmentGateState(ENV, open_fence("dev"), None), "environment_mismatch"),
        (lambda: _state(permit=replace(issue_permit(_acquire(), 1), environment="dev")),
         "environment_mismatch"),
        (lambda: PermitAcquisition(AcquisitionStatus.HELD, None), "acquisition_requires_permit"),
    ],
)
def test_entidades_e_comandos_validam_invariantes(
    build: Callable[[], object], reason: str,
) -> None:
    with pytest.raises(ValueError, match=f"reason={reason}"):
        build()


def test_fence_fechado_dispensa_permit_no_resultado() -> None:
    assert PermitAcquisition(AcquisitionStatus.FENCE_CLOSED, None).permit is None


@pytest.mark.parametrize(
    "apply",
    [
        lambda: decide_acquisition(_state(), replace(_acquire(), environment="dev")),
        lambda: close_fence(open_fence("dev"), ClosePromotionFence(ENV, PROMOTION, NOW)),
        lambda: reopen_fence(_closed(), ReopenPromotionFence("dev", PROMOTION, NOW)),
        lambda: bind_permit(issue_permit(_acquire(), 1), BindUnitPermit("dev", 1, "x", NOW)),
        lambda: renew_permit(issue_permit(_acquire(), 1), RenewUnitPermit("dev", 1, NOW, 1)),
        lambda: release_permit(issue_permit(_acquire(), 1), ReleaseUnitPermit("dev", 1, NOW)),
    ],
)
def test_transicao_com_comando_de_outro_ambiente_e_rejeitada(apply: Callable[[], object]) -> None:
    with pytest.raises(ValueError, match="reason=environment_mismatch"):
        apply()


def test_entidades_e_comandos_sao_imutaveis() -> None:
    permit = issue_permit(_acquire(), 1)

    with pytest.raises(FrozenInstanceError):
        cast("Any", permit).token = 2
    with pytest.raises(FrozenInstanceError):
        cast("Any", _acquire()).now = LATER
    with pytest.raises(FrozenInstanceError):
        cast("Any", open_fence(ENV)).state = FenceState.CLOSED


def _parameters(method: Callable[..., Any]) -> tuple[tuple[str, object], ...]:
    parameters = tuple(signature(method).parameters.values())[1:]
    return tuple((parameter.name, parameter.annotation) for parameter in parameters)


def _structural(protocol: type, missing: str | None = None) -> object:
    methods = {
        name: (lambda self, *args, **kwargs: None)
        for name in cast("Any", protocol).__protocol_attrs__
        if name != missing
    }
    return type("AdapterEstrutural", (), methods)()


@pytest.mark.parametrize(("protocol", "expected"), PORT_SIGNATURES.items())
def test_portas_do_gate_expoem_assinaturas_congeladas(
    protocol: type, expected: dict[str, tuple[tuple[tuple[str, str], ...], str]],
) -> None:
    assert set(cast("Any", protocol).__protocol_attrs__) == set(expected)
    for name, (parameters, returns) in expected.items():
        method = getattr(protocol, name)
        assert _parameters(method) == parameters
        assert method.__annotations__["return"] == returns
        assert method(object(), *(None for _ in parameters)) is None


@pytest.mark.parametrize("protocol", list(PORT_SIGNATURES))
def test_portas_do_gate_sao_estruturais(protocol: type) -> None:
    assert isinstance(_structural(protocol), protocol)
    for name in cast("Any", protocol).__protocol_attrs__:
        assert not isinstance(_structural(protocol, name), protocol)


def test_admissao_sem_gate_admite_sem_persistir_nada() -> None:
    admission = UNGATED_ADMISSION.admit(HOLDER, NOW)

    assert isinstance(UNGATED_ADMISSION, DispatchAdmissionPort)
    assert admission == UnitAdmission(None, None)
    assert UNGATED_ADMISSION.confirm(UnitAdmission(None, None), "exec-1", NOW) is None
    assert UNGATED_ADMISSION.settle(HOLDER, NOW) is None


def test_adiamento_e_resultado_imutavel_e_nao_excecao() -> None:
    deferred = DispatchDeferred(DeferralReason.PERMIT_HELD)

    assert {reason.value for reason in DeferralReason} == {"fence_closed", "permit_held"}
    assert not isinstance(deferred, BaseException)
    assert deferred == DispatchDeferred(DeferralReason.PERMIT_HELD)
    with pytest.raises(FrozenInstanceError):
        cast("Any", deferred).reason = DeferralReason.FENCE_CLOSED


@pytest.mark.parametrize(
    ("environment", "token"), [("prod", None), (None, 3), ("prod", 0), ("Prod", 3), ("prod", True)],
)
def test_admissao_exige_ambiente_e_token_juntos_e_validos(
    environment: str | None, token: int | None,
) -> None:
    with pytest.raises(ValueError, match="reason="):
        UnitAdmission(environment, token)


def test_admissao_com_gate_guarda_ambiente_e_token() -> None:
    admission = UnitAdmission(ENV, 3)

    assert (admission.environment, admission.token) == (ENV, 3)


@pytest.mark.parametrize(
    ("presence", "execution_ref"),
    [(ExecutionPresence.RUNNING, "arn:exec"), (ExecutionPresence.TERMINAL, "arn:exec"),
     (ExecutionPresence.ABSENT, None)],
)
def test_observacao_de_execucao_coerente_e_aceita(
    presence: ExecutionPresence, execution_ref: str | None,
) -> None:
    observation = ExecutionObservation(presence, execution_ref)

    assert (observation.presence, observation.execution_ref) == (presence, execution_ref)


@pytest.mark.parametrize(
    ("presence", "execution_ref"),
    [(ExecutionPresence.RUNNING, None), (ExecutionPresence.TERMINAL, None),
     (ExecutionPresence.ABSENT, "arn:exec")],
)
def test_observacao_de_execucao_incoerente_e_rejeitada(
    presence: ExecutionPresence, execution_ref: str | None,
) -> None:
    with pytest.raises(ValueError, match="reason=execution_ref_presence_mismatch"):
        ExecutionObservation(presence, execution_ref)
