from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, cast

import pytest

from cnes_domain.control_plane.commands import ClaimRunDispatchStart
from cnes_domain.control_plane.dispatch_start import (
    DispatchStartClaim,
    StartAdmission,
    claim_dispatch_start,
    claim_matches,
    execute_dispatch_start,
    request_sha256,
)
from cnes_domain.control_plane.entities import RunDispatch
from cnes_domain.control_plane.enums import DispatchOutcome, DispatchState
from cnes_domain.ports.environment_gate import DeferralReason, DispatchDeferred, UnitAdmission
from cnes_domain.ports.processing import StartRunExecution

if TYPE_CHECKING:
    from cnes_domain.ports.control_plane import ControlPlanePort
    from cnes_domain.ports.processing import ProcessorExecutorPort

NOW = datetime(2026, 11, 9, 12, tzinfo=UTC)


def _claim() -> DispatchStartClaim:
    return DispatchStartClaim(
        dispatch_id="0123456789abcdef",
        generation=1,
        permit_token=7,
        fence_version=3,
        request_sha256="a" * 64,
        promotion_id="r1",
    )


def test_claim_model_exposes_starting_state_facts() -> None:
    dispatch = RunDispatch(
        tenant_id="tenant",
        run_id="run",
        wave_id="fedcba9876543210",
        dispatch_id="0123456789abcdef",
        generation=1,
        unit_ids=("unit",),
        state=DispatchState.STARTING,
        lease_until=NOW + timedelta(minutes=5),
        start_claim=_claim(),
    )

    assert dispatch.state is DispatchState.STARTING
    assert dispatch.start_claim == _claim()


def test_dispatch_start_claim_round_trips_through_json() -> None:
    dispatch = RunDispatch(
        tenant_id="tenant",
        run_id="run",
        wave_id="fedcba9876543210",
        dispatch_id="0123456789abcdef",
        generation=1,
        unit_ids=("unit",),
        state=DispatchState.STARTING,
        lease_until=NOW + timedelta(minutes=5),
        start_claim=_claim(),
    )

    restored = RunDispatch.model_validate_json(dispatch.model_dump_json())

    assert restored.start_claim == _claim()


def test_claim_rejeita_dispatch_diferente() -> None:
    with pytest.raises(ValueError, match="dispatch_id"):
        DispatchStartClaim(
            dispatch_id="bad",
            generation=1,
            permit_token=7,
            fence_version=3,
            request_sha256="a" * 64,
            promotion_id="r1",
        )


def test_claim_allows_unknown_fence_version() -> None:
    claim = DispatchStartClaim(
        dispatch_id="0123456789abcdef",
        generation=1,
        permit_token=7,
        fence_version=0,
        request_sha256="a" * 64,
        promotion_id=None,
    )

    assert claim.fence_version == 0


def test_claim_rejects_non_canonical_hex() -> None:
    with pytest.raises(ValueError, match="invalid_dispatch_id"):
        DispatchStartClaim(
            dispatch_id="0123456789ABCDEF",
            generation=1,
            permit_token=7,
            fence_version=0,
            request_sha256="a" * 64,
            promotion_id=None,
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("dispatch_id", "0" * 15, "invalid_dispatch_id"),
        ("dispatch_id", "g" * 16, "invalid_dispatch_id"),
        ("dispatch_id", "0" * 17, "invalid_dispatch_id"),
        ("request_sha256", "a" * 63, "invalid_sha256"),
        ("request_sha256", "A" * 64, "invalid_sha256"),
        ("request_sha256", "g" * 64, "invalid_sha256"),
        ("request_sha256", "a" * 65, "invalid_sha256"),
        ("promotion_id", " \t", "blank_promotion_id"),
        ("generation", 0, "positive_claim_counter_required"),
        ("permit_token", -1, "positive_claim_counter_required"),
        ("fence_version", -1, "non_negative_fence_version_required"),
        ("generation", True, "int_type"),
        ("permit_token", False, "int_type"),
        ("fence_version", True, "int_type"),
    ],
)
def test_claim_rejeita_dados_nao_canonicos(field: str, value: object, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        DispatchStartClaim.model_validate(_claim().model_dump() | {field: value})


@pytest.mark.parametrize("digit", ["0", "f"])
def test_claim_aceita_limites_do_alfabeto_hexadecimal(digit: str) -> None:
    claim = DispatchStartClaim.model_validate(
        _claim().model_dump() | {"dispatch_id": digit * 16, "request_sha256": digit * 64}
    )
    assert claim.dispatch_id == digit * 16
    assert claim.request_sha256 == digit * 64


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"state": DispatchState.STARTING, "start_claim": None}, "start_claim_required"),
        ({"canceled_before_send": True}, "cancel_before_send_requires_terminal_claim"),
        ({"dispatch_id": "fedcba9876543210"}, "start_claim_dispatch_mismatch"),
        ({"generation": 2}, "start_claim_generation_mismatch"),
    ],
)
def test_dispatch_rejeita_claim_inconsistente(updates: dict[str, object], message: str) -> None:
    values = {
        "tenant_id": "tenant",
        "run_id": "run",
        "wave_id": "fedcba9876543210",
        "dispatch_id": _claim().dispatch_id,
        "generation": 1,
        "unit_ids": ("unit",),
        "state": DispatchState.STARTING,
        "lease_until": NOW,
        "start_claim": _claim(),
    }
    with pytest.raises(ValueError, match=message):
        RunDispatch.model_validate(values | updates)


def _dispatch() -> RunDispatch:
    return RunDispatch(
        tenant_id="tenant",
        run_id="run",
        wave_id="fedcba9876543210",
        dispatch_id=_claim().dispatch_id,
        generation=1,
        unit_ids=("unit",),
        state=DispatchState.RESERVED,
        lease_until=NOW + timedelta(minutes=5),
    )


def _request() -> StartRunExecution:
    dispatch = _dispatch()
    return StartRunExecution(
        tenant_id=dispatch.tenant_id,
        run_id=dispatch.run_id,
        wave_id=dispatch.wave_id,
        dispatch_id=dispatch.dispatch_id,
        unit_ids=dispatch.unit_ids,
        max_concurrency=1,
    )


class _StartBoundary:
    def __init__(self, accepted: bool) -> None:
        self.accepted = accepted
        self.commands: list[ClaimRunDispatchStart] = []

    def claim_run_dispatch_start(self, command: ClaimRunDispatchStart) -> RunDispatch | None:
        self.commands.append(command)
        return _dispatch() if self.accepted else None


@pytest.mark.parametrize("accepted", [False, True])
def test_claim_transmite_identidade_permit_e_relogio_e_respeita_cas(accepted: bool) -> None:
    boundary = _StartBoundary(accepted)
    attempt = StartAdmission(UnitAdmission("prod", 7, 3, "r1"), NOW, 300)
    result = claim_dispatch_start(
        cast("ControlPlanePort", boundary), _dispatch(), _request(), attempt
    )
    assert len(boundary.commands) == 1
    command = boundary.commands[0]
    assert (command.tenant_id, command.run_id, command.dispatch_id) == (
        "tenant",
        "run",
        _claim().dispatch_id,
    )
    assert (command.now, command.lease_seconds, command.environment) == (NOW, 300, "prod")
    assert (
        command.claim.permit_token,
        command.claim.fence_version,
        command.claim.promotion_id,
    ) == (
        7,
        3,
        "r1",
    )
    assert command.claim.request_sha256 == request_sha256(_request())
    expected = command.claim if accepted else DispatchDeferred(DeferralReason.PERMIT_HELD)
    assert result == expected


def test_admissao_sem_gate_nao_reivindica_start() -> None:
    boundary = _StartBoundary(True)
    result = claim_dispatch_start(
        cast("ControlPlanePort", boundary),
        _dispatch(),
        _request(),
        StartAdmission(UnitAdmission(None, None), NOW, 300),
    )
    assert result is None
    assert boundary.commands == []


def test_admissao_compoe_bind_e_cancelamento_com_o_mesmo_claim_e_relogio() -> None:
    attempt = StartAdmission(UnitAdmission("prod", 7), NOW, 300)
    bind = attempt.bind(_dispatch(), "execution-1", _claim())
    cancel = attempt.cancel(_dispatch(), _claim())
    assert bind.claim == cancel.claim == _claim()
    assert bind.dispatch_id == cancel.dispatch_id == _dispatch().dispatch_id
    assert bind.now == cancel.finished_at == NOW
    assert bind.lease_seconds == 300
    assert bind.execution_ref == "execution-1"
    assert cancel.outcome is DispatchOutcome.CANCELED
    assert (cancel.tenant_id, cancel.run_id) == ("tenant", "run")


@pytest.mark.parametrize(
    ("dispatch_id", "generation", "payload", "expected"),
    [
        ("0123456789abcdef", 1, {"a": 1, "b": 2}, True),
        ("fedcba9876543210", 1, {"a": 1, "b": 2}, False),
        ("0123456789abcdef", 2, {"a": 1, "b": 2}, False),
        ("0123456789abcdef", 1, {"a": 2, "b": 2}, False),
    ],
)
def test_claim_vincula_identidade_geracao_e_payload(
    dispatch_id, generation, payload, expected
) -> None:
    claim = _claim().model_copy(update={"request_sha256": request_sha256({"b": 2, "a": 1})})
    assert (
        claim_matches(claim, dispatch_id=dispatch_id, generation=generation, request=payload)
        is expected
    )


class _Executor:
    def __init__(self) -> None:
        self.sent: list[tuple[StartRunExecution, DispatchStartClaim | None]] = []

    def start(self, request: StartRunExecution) -> str:
        self.sent.append((request, None))
        return "legacy-execution"


class _ClaimExecutor(_Executor):
    def start_with_claim(self, request: StartRunExecution, claim: DispatchStartClaim) -> str:
        self.sent.append((request, claim))
        return "claimed-execution"


@pytest.mark.parametrize(
    ("executor_type", "claim", "ref", "sent_claim"),
    [
        (_Executor, None, "legacy-execution", None),
        (_Executor, _claim(), "legacy-execution", None),
        (_ClaimExecutor, None, "legacy-execution", None),
        (_ClaimExecutor, _claim(), "claimed-execution", _claim()),
    ],
)
def test_executor_recebe_claim_quando_suportado(executor_type, claim, ref, sent_claim) -> None:
    executor = executor_type()
    request = _request()
    assert execute_dispatch_start(cast("ProcessorExecutorPort", executor), request, claim) == ref
    assert executor.sent == [(request, sent_claim)]
