"""Testes das regras de fence e da política de publicação."""

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from cnes_domain.billing.commands import PublishGateRequest
from cnes_domain.billing.errors import BillingDependencyError, EntitlementDenied, PublishDenied
from cnes_domain.billing.execution import PublicationGuard, RunBillingState
from cnes_domain.billing.models import (
    AccessLevel,
    EntitlementAction,
    EntitlementDecision,
    RunAuthorization,
    SubscriptionStatus,
)
from cnes_domain.billing.publication import (
    BillingPublicationPolicy,
    PublicationPolicyDependencies,
    PublishAuthorizer,
    RunBillingStateReader,
    require_publication_companion,
    require_publication_snapshot,
    unit_companion_allows,
)
from cnes_domain.control_plane.commands import PublicationPermit
from cnes_domain.control_plane.entities import Run, RunDependency
from cnes_domain.control_plane.enums import DispatchState, RunState
from cnes_domain.profiles import BillingMode

NOW = datetime(2026, 9, 30, 12, tzinfo=UTC)
LATER = datetime(2026, 9, 30, 13, tzinfo=UTC)
DISPATCH = "fedcba9876543210"
STRIPE = BillingMode.STRIPE
DISABLED = BillingMode.DISABLED


def _state(**overrides: Any) -> RunBillingState:
    values: dict[str, Any] = {
        "billing_account_id": "acct-1",
        "tenant_id": "tenant-1",
        "run_id": "run-1",
        "authorization": RunAuthorization("acct-1", "plan-1", 3, 2, "res-1", NOW),
        "execution_generation": 0,
        "execution_wave_id": None,
        "execution_dispatch_id": None,
        "execution_ref": None,
        "execution_unit_ids": (),
        "execution_status": None,
        "execution_terminal_outcome": None,
        "fencing_token": 7,
        "cancel_requested": False,
        "updated_at": NOW,
    }
    return RunBillingState(**{**values, **overrides})


def _bound_state(cancel: bool) -> RunBillingState:
    return _state(
        execution_generation=1,
        execution_wave_id="0123456789abcdef",
        execution_dispatch_id=DISPATCH,
        execution_ref="exec-1",
        execution_unit_ids=("unit-1",),
        execution_status=DispatchState.STARTED,
        cancel_requested=cancel,
    )


def _run() -> Run:
    return Run(
        tenant_id="tenant-1",
        run_id="run-1",
        competencia="2026-09",
        dataset_name="cnes",
        state=RunState.PROCESSING,
        dependencies=(RunDependency(source_type="cnes", file_subtype="pf", required=True),),
        missing_sources=(),
        created_at=NOW,
    )


def _guard(**overrides: Any) -> PublicationGuard:
    values: dict[str, Any] = {
        "billing_account_id": "acct-1",
        "expected_entitlement_version": 3,
        "expected_run_fencing_token": 7,
        "checked_at": NOW,
    }
    return PublicationGuard(**{**values, **overrides})


def _permit(binding_context: object | None = None, **overrides: Any) -> PublicationPermit:
    values: dict[str, Any] = {
        "tenant_id": "tenant-1",
        "run_id": "run-1",
        "policy_version": 3,
        "fencing_token": 7,
        "binding_context": binding_context,
    }
    return PublicationPermit(**{**values, **overrides})


def _decision(version: int = 4) -> EntitlementDecision:
    return EntitlementDecision(
        action=EntitlementAction.PUBLISH_RUN,
        allowed=True,
        access_level=AccessLevel.FULL,
        reason="allowed",
        entitlement_version=version,
        quota_limit=None,
    )


class _FakeControlPlane:
    def __init__(self, state: RunBillingState | None) -> None:
        self.state = state
        self.calls: list[tuple[str, str]] = []

    def get_run_billing_state(self, tenant_id: str, run_id: str) -> RunBillingState | None:
        self.calls.append((tenant_id, run_id))
        return self.state


class _FakeGate:
    def __init__(self, outcome: EntitlementDecision | Exception) -> None:
        self.outcome = outcome
        self.requests: list[PublishGateRequest] = []

    def authorize_publish_run(self, request: PublishGateRequest) -> EntitlementDecision:
        self.requests.append(request)
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome


def _policy(
    state: RunBillingState | None,
    mode: BillingMode,
    outcome: EntitlementDecision | Exception | None = None,
) -> tuple[BillingPublicationPolicy, _FakeControlPlane, _FakeGate]:
    control_plane = _FakeControlPlane(state)
    gate = _FakeGate(outcome or _decision())
    dependencies = PublicationPolicyDependencies(control_plane, gate, lambda: LATER, mode)
    return BillingPublicationPolicy(dependencies), control_plane, gate


def test_fakes_satisfazem_protocolos_da_politica():
    assert isinstance(_FakeControlPlane(None), RunBillingStateReader)
    assert isinstance(_FakeGate(_decision()), PublishAuthorizer)


def test_emite_permit_legado_sem_companion_em_disabled():
    policy, control_plane, gate = _policy(None, DISABLED)
    permit = policy(_run())
    assert (permit.tenant_id, permit.run_id) == ("tenant-1", "run-1")
    assert (permit.policy_version, permit.fencing_token) == (0, 0)
    assert permit.binding_context is None
    assert control_plane.calls == [("tenant-1", "run-1")]
    assert gate.requests == []


def test_rejeita_companion_ausente_em_stripe():
    policy, _, gate = _policy(None, STRIPE)
    with pytest.raises(PublishDenied, match="reason=run_billing_state_missing"):
        policy(_run())
    assert gate.requests == []


@pytest.mark.parametrize("mode", [STRIPE, DISABLED])
@pytest.mark.parametrize("overrides", [{"tenant_id": "tenant-2"}, {"run_id": "run-2"}])
def test_rejeita_companion_de_outra_identidade(mode, overrides):
    policy, _, gate = _policy(_state(**overrides), mode)
    with pytest.raises(PublishDenied, match="reason=run_billing_state_mismatch"):
        policy(_run())
    assert gate.requests == []


@pytest.mark.parametrize("mode", [STRIPE, DISABLED])
def test_rejeita_companion_cancelado(mode):
    policy, _, gate = _policy(_state(cancel_requested=True), mode)
    with pytest.raises(PublishDenied, match="reason=run_cancel_requested"):
        policy(_run())
    assert gate.requests == []


def test_emite_permit_do_companion_sem_chamar_gate_em_disabled():
    policy, control_plane, gate = _policy(_state(), DISABLED)
    permit = policy(_run())
    assert (permit.policy_version, permit.fencing_token) == (3, 7)
    assert permit.binding_context is None
    assert len(control_plane.calls) == 1
    assert gate.requests == []


def test_emite_permit_com_guard_em_stripe():
    policy, control_plane, gate = _policy(_state(), STRIPE, _decision(version=4))
    permit = policy(_run())
    assert gate.requests == [PublishGateRequest("acct-1", "tenant-1", "run-1", 3, 7)]
    assert len(control_plane.calls) == 1
    assert (permit.tenant_id, permit.run_id) == ("tenant-1", "run-1")
    assert (permit.policy_version, permit.fencing_token) == (4, 7)
    assert permit.binding_context == PublicationGuard("acct-1", 4, 7, LATER)


def test_converte_entitlement_denied_em_publish_denied_com_mesma_mensagem():
    error = EntitlementDenied("reason=admin_revoked")
    policy, _, _ = _policy(_state(), STRIPE, error)
    with pytest.raises(PublishDenied, match="reason=admin_revoked") as raised:
        policy(_run())
    assert raised.value.__cause__ is error


def test_propaga_erro_de_dependencia_do_gate_sem_converter():
    policy, _, _ = _policy(_state(), STRIPE, BillingDependencyError("store_down"))
    with pytest.raises(BillingDependencyError):
        policy(_run())


def test_guard_invalido_em_stripe_eh_rejeitado():
    for context in (None, object()):
        with pytest.raises(PublishDenied, match="reason=publication_guard_invalid"):
            require_publication_companion(_state(), _permit(context), STRIPE)


def test_companion_ausente_respeita_o_modo():
    with pytest.raises(PublishDenied, match="reason=run_billing_state_missing"):
        require_publication_companion(None, _permit(_guard()), STRIPE)
    require_publication_companion(None, _permit(), DISABLED)


def test_rejeita_companion_divergente_cancelado_ou_velho():
    cases = [
        (_state(run_id="run-2"), "reason=run_billing_state_mismatch"),
        (_state(tenant_id="tenant-2"), "reason=run_billing_state_mismatch"),
        (_state(cancel_requested=True), "reason=run_cancel_requested"),
        (_state(fencing_token=8), "reason=stale_fence"),
    ]
    for state, reason in cases:
        with pytest.raises(PublishDenied, match=reason):
            require_publication_companion(state, _permit(), DISABLED)


def test_rejeita_guard_de_outra_conta_ou_fence():
    with pytest.raises(PublishDenied, match="reason=billing_account_mismatch"):
        require_publication_companion(
            _state(), _permit(_guard(billing_account_id="acct-2")), STRIPE
        )
    with pytest.raises(PublishDenied, match="reason=stale_fence"):
        require_publication_companion(
            _state(), _permit(_guard(expected_run_fencing_token=6)), STRIPE
        )


def test_aceita_guard_consistente_em_stripe_e_disabled():
    require_publication_companion(_state(), _permit(_guard()), STRIPE)
    require_publication_companion(_state(), _permit(_guard()), DISABLED)


def test_aceita_companion_consistente_sem_guard_em_disabled():
    require_publication_companion(_state(), _permit(), DISABLED)


def _snapshot(status: SubscriptionStatus, version: int, valid_until: datetime = LATER) -> Any:
    return SimpleNamespace(
        subscription_status=status, entitlement_version=version, valid_until=valid_until,
    )


def test_snapshot_ausente_eh_rejeitado():
    with pytest.raises(PublishDenied, match="reason=snapshot_missing"):
        require_publication_snapshot(None, _guard(), NOW)


def test_snapshot_revogado_eh_rejeitado():
    snapshot = _snapshot(SubscriptionStatus.ADMIN_REVOKED, 3)
    with pytest.raises(PublishDenied, match="reason=admin_revoked"):
        require_publication_snapshot(snapshot, _guard(), NOW)


def test_snapshot_de_outra_versao_eh_rejeitado():
    snapshot = _snapshot(SubscriptionStatus.ACTIVE, 4)
    with pytest.raises(PublishDenied, match="reason=stale_entitlement"):
        require_publication_snapshot(snapshot, _guard(), NOW)


def test_snapshot_vigente_eh_aceito():
    require_publication_snapshot(_snapshot(SubscriptionStatus.ACTIVE, 3), _guard(), NOW)


@pytest.mark.parametrize("valid_until", [NOW, NOW - (LATER - NOW)])
def test_snapshot_expirado_eh_rejeitado(valid_until: datetime):
    snapshot = _snapshot(SubscriptionStatus.ACTIVE, 3, valid_until)
    with pytest.raises(PublishDenied, match="reason=snapshot_expired"):
        require_publication_snapshot(snapshot, _guard(), NOW)


@pytest.mark.parametrize(
    ("mode", "bound", "cancel", "dispatch", "expected"),
    [
        (STRIPE, False, False, DISPATCH, False),
        (DISABLED, False, False, DISPATCH, True),
        (STRIPE, True, True, DISPATCH, False),
        (DISABLED, True, True, DISPATCH, False),
        (STRIPE, True, False, DISPATCH, True),
        (STRIPE, True, False, "aaaaaaaaaaaaaaaa", False),
        (DISABLED, True, False, "aaaaaaaaaaaaaaaa", True),
    ],
)
def test_unit_companion_allows_cobre_tabela_verdade(mode, bound, cancel, dispatch, expected):
    state = _bound_state(cancel) if bound else None
    assert unit_companion_allows(state, dispatch, mode) is expected
