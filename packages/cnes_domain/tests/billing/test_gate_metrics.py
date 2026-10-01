"""Testes da métrica EntitlementChecksDenied emitida pelo EntitlementGate."""

from dataclasses import replace
from typing import Any

import pytest

from cnes_domain.billing.errors import BillingDependencyError, EntitlementDenied
from cnes_domain.billing.gate import (
    EntitlementGate,
    EntitlementGateDependencies,
    RunReservationSettings,
)
from cnes_domain.billing.models import EntitlementSnapshot
from packages.cnes_domain.tests.billing.test_gate import (
    _ALL,
    _GATE_REQUEST,
    _NOW,
    _RUN_REQUEST,
    _S,
    _TTL,
    _Harness,
    _publish_request,
    _snapshot,
)


class _SpyMetrics:
    def __init__(self) -> None:
        self.emitted: list[Any] = []

    def emit(self, metric: Any) -> None:
        self.emitted.append(metric)


def _metered_gate(snapshot: EntitlementSnapshot | None) -> tuple[EntitlementGate, _SpyMetrics]:
    metrics = _SpyMetrics()
    harness = _Harness(snapshot)
    return EntitlementGate(replace(_dependencies_of(harness), metrics=metrics)), metrics


def _dependencies_of(harness: _Harness) -> EntitlementGateDependencies:
    return EntitlementGateDependencies(
        projection=harness.projection,
        quotas=harness.quotas,
        clock=lambda: _NOW,
        run_settings=RunReservationSettings(4, harness.factory, _TTL),
    )


@pytest.mark.parametrize("operation", list(_ALL))
def test_negacao_por_snapshot_ausente_emite_metrica_uma_vez(operation: str) -> None:
    gate, metrics = _metered_gate(None)
    with pytest.raises(EntitlementDenied):
        _ALL[operation](gate)
    assert len(metrics.emitted) == 1
    metric = metrics.emitted[0]
    assert (metric.name, metric.value, metric.unit) == ("EntitlementChecksDenied", 1.0, "Count")
    assert dict(metric.dimensions) == {"Reason": "snapshot_missing"}
    assert metric.occurred_at == _NOW


def test_negacao_por_politica_emite_motivo_da_politica() -> None:
    gate, metrics = _metered_gate(_snapshot(_S.ADMIN_REVOKED))
    with pytest.raises(EntitlementDenied):
        gate.authorize_register_agent(_GATE_REQUEST)
    assert [dict(m.dimensions) for m in metrics.emitted] == [{"Reason": "admin_revoked"}]


def test_negacao_por_conta_divergente_emite_metrica() -> None:
    gate, metrics = _metered_gate(_snapshot(billing_account_id="ba-outra"))
    with pytest.raises(EntitlementDenied):
        gate.authorize_create_run(_RUN_REQUEST)
    assert [dict(m.dimensions) for m in metrics.emitted] == [
        {"Reason": "snapshot_account_mismatch"}
    ]


def test_negacao_por_versao_regredida_emite_metrica() -> None:
    gate, metrics = _metered_gate(_snapshot())
    with pytest.raises(EntitlementDenied):
        gate.authorize_publish_run(_publish_request(expected_version=99))
    assert [dict(m.dimensions) for m in metrics.emitted] == [
        {"Reason": "snapshot_version_regressed"}
    ]


def test_negacao_sem_motivo_na_mensagem_usa_motivo_padrao() -> None:
    gate, metrics = _metered_gate(_snapshot())
    gate._projection.get_snapshot = lambda *_: (_ for _ in ()).throw(EntitlementDenied("x"))
    with pytest.raises(EntitlementDenied):
        gate.authorize_tenant_creation(_GATE_REQUEST)
    assert [dict(m.dimensions) for m in metrics.emitted] == [{"Reason": "entitlement_denied"}]


def test_autorizacao_permitida_nao_emite_metrica() -> None:
    gate, metrics = _metered_gate(_snapshot())
    gate.authorize_register_agent(_GATE_REQUEST)
    assert metrics.emitted == []


def test_erro_que_nao_e_negacao_nao_emite_metrica() -> None:
    gate, metrics = _metered_gate(_snapshot())
    gate._projection.get_snapshot = lambda *_: (_ for _ in ()).throw(
        BillingDependencyError("dynamodb_unavailable")
    )
    with pytest.raises(BillingDependencyError):
        gate.authorize_register_agent(_GATE_REQUEST)
    assert metrics.emitted == []


def test_negacao_sem_metrics_configurado_apenas_repropaga() -> None:
    with pytest.raises(EntitlementDenied):
        _Harness(None).gate.authorize_register_agent(_GATE_REQUEST)
