"""Testes do sink CloudWatch EMF de métricas de billing."""

import io
import json
import logging
from collections.abc import Iterator
from dataclasses import replace
from typing import Any, cast

import pytest

from cnes_domain.billing.models import BillingMetric, SubscriptionStatus
from cnes_domain.billing.ports import BillingMetricsPort
from cnes_infra.billing.metrics import (
    ALLOWED_DIMENSIONS,
    BILLING_METRICS_NAMESPACE,
    METRIC_UNITS,
    BillingMetricName,
    CloudWatchBillingMetrics,
    DiscardBillingMetrics,
    billing_metric,
    build_billing_metrics,
)
from cnes_infra.observability.json_logging import configure_json_stdout

from .billing_factories import NOW

LOGGER_NAME = "cnes_infra.billing.metrics"


@pytest.fixture
def sink() -> CloudWatchBillingMetrics:
    return CloudWatchBillingMetrics("prod")


@pytest.fixture
def restored_root_logging() -> Iterator[None]:
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    yield
    root.handlers[:] = handlers
    root.setLevel(level)


def _emitted(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.getMessage() == "billing_metric"]


def _rejections(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]


def test_metric_emf_nao_contem_identificador_sensivel(
    restored_root_logging: None,
) -> None:
    stream = io.StringIO()
    configure_json_stdout("billing-worker", stream)
    metric = billing_metric(
        BillingMetricName.WEBHOOK_FAILURES, 1, NOW, {"Reason": "signature_invalid"}
    )

    CloudWatchBillingMetrics("prod").emit(metric)

    lines = stream.getvalue().splitlines()
    assert len(lines) == 1
    document = json.loads(lines[0])
    emf = document["_aws"]
    assert type(emf["Timestamp"]) is int
    assert emf["Timestamp"] == int(NOW.timestamp() * 1000)
    directive = emf["CloudWatchMetrics"][0]
    assert directive["Namespace"] == "CnesData/Billing"
    assert directive["Dimensions"] == [["Environment", "Reason"]]
    assert directive["Metrics"] == [{"Name": "WebhookFailures", "Unit": "Count"}]
    assert document["WebhookFailures"] == 1
    assert document["Environment"] == "prod"
    assert document["Reason"] == "signature_invalid"
    assert document["event"] == "billing_metric"
    forbidden = {"payload", "stripe_secret", "billing_account_id", "tenant_id"}
    assert forbidden.isdisjoint(document)


def test_namespace_e_dimensoes_permitidas_sao_fixos() -> None:
    assert BILLING_METRICS_NAMESPACE == "CnesData/Billing"
    assert {"Environment", "EventType", "Reason", "SubscriptionStatus"} == ALLOWED_DIMENSIONS


@pytest.mark.parametrize("name", list(BillingMetricName))
def test_emite_metrica_com_unidade_correta(
    name: BillingMetricName, sink: CloudWatchBillingMetrics, caplog: pytest.LogCaptureFixture
) -> None:
    expected = {
        BillingMetricName.WEBHOOK_LATENCY_MS: "Milliseconds",
        BillingMetricName.ENTITLEMENT_SNAPSHOT_AGE_SECONDS: "Seconds",
    }.get(name, "Count")
    caplog.set_level(logging.INFO, logger=LOGGER_NAME)

    sink.emit(billing_metric(name, 2.5, NOW))

    (record,) = _emitted(caplog)
    directive = record._aws["CloudWatchMetrics"][0]  # type: ignore[attr-defined]
    assert directive["Metrics"] == [{"Name": name.value, "Unit": expected}]
    assert METRIC_UNITS[name] == expected
    assert getattr(record, name.value) == 2.5
    assert directive["Dimensions"] == [["Environment"]]


def test_catalogo_cobre_onze_metricas() -> None:
    assert len(BillingMetricName) == 11
    assert set(METRIC_UNITS) == set(BillingMetricName)


def test_dimensoes_saem_ordenadas(
    sink: CloudWatchBillingMetrics, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=LOGGER_NAME)
    dims = {
        "SubscriptionStatus": SubscriptionStatus.ACTIVE.value,
        "EventType": "invoice.paid",
        "Reason": "ok_code",
    }

    sink.emit(billing_metric(BillingMetricName.WEBHOOK_DUPLICATES, 1, NOW, dims))

    (record,) = _emitted(caplog)
    names = record._aws["CloudWatchMetrics"][0]["Dimensions"]  # type: ignore[attr-defined]
    assert names == [["Environment", "EventType", "Reason", "SubscriptionStatus"]]
    assert record.EventType == "invoice.paid"  # type: ignore[attr-defined]


def _bad(**changes: object) -> BillingMetric:
    base = billing_metric(BillingMetricName.WEBHOOK_FAILURES, 1, NOW)
    return replace(base, **changes)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("metric", "reason"),
    [
        (_bad(name="Inexistente"), "metric_unknown"),
        (_bad(unit="Seconds"), "unit_mismatch"),
        (_bad(dimensions={"TenantId": "x"}), "dimension_not_allowed"),
        (_bad(dimensions={"Environment": "prod"}), "dimension_not_allowed"),
        (_bad(dimensions={"SubscriptionStatus": "ativa"}), "dimension_value_invalid"),
        (_bad(dimensions={"EventType": "Invoice.Paid"}), "dimension_value_invalid"),
        (_bad(dimensions={"Reason": "cus_AbC123"}), "dimension_value_invalid"),
    ],
)
def test_descarta_metrica_invalida_sem_lancar(
    metric: BillingMetric,
    reason: str,
    sink: CloudWatchBillingMetrics,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger=LOGGER_NAME)

    sink.emit(metric)

    assert _emitted(caplog) == []
    assert _rejections(caplog) == [f"billing_metric_rejected reason={reason}"]
    assert "cus_AbC123" not in caplog.text
    assert "Inexistente" not in caplog.text


@pytest.mark.parametrize("environment", ["", "Prod", "1prod", "a b", "x" * 33])
def test_construtor_rejeita_ambiente_invalido(environment: str) -> None:
    with pytest.raises(ValueError, match="reason=environment_invalid"):
        CloudWatchBillingMetrics(environment)


def test_usa_logger_injetado(caplog: pytest.LogCaptureFixture) -> None:
    custom = logging.getLogger("teste.custom")
    caplog.set_level(logging.INFO, logger="teste.custom")

    CloudWatchBillingMetrics("dev", custom).emit(
        billing_metric(BillingMetricName.RECOVERY_BACKLOG, 3, NOW)
    )

    assert [r.name for r in _emitted(caplog)] == ["teste.custom"]


def test_sink_satisfaz_billing_metrics_port(sink: CloudWatchBillingMetrics) -> None:
    assert isinstance(sink, BillingMetricsPort)


def test_build_sem_ambiente_devolve_sink_que_descarta(
    caplog: pytest.LogCaptureFixture,
) -> None:
    metrics = build_billing_metrics(None)

    with caplog.at_level(logging.INFO):
        metrics.emit(billing_metric(BillingMetricName.WEBHOOK_FAILURES, 1, NOW))

    assert isinstance(metrics, DiscardBillingMetrics)
    assert caplog.records == []


def test_build_com_ambiente_devolve_sink_cloudwatch() -> None:
    assert isinstance(build_billing_metrics("prod"), CloudWatchBillingMetrics)


def test_sink_configurado_escreve_documento_emf_json_no_stdout(capsys) -> None:
    build_billing_metrics("prod").emit(
        billing_metric(BillingMetricName.WEBHOOK_FAILURES, 1, NOW, {"Reason": "signature_invalid"})
    )
    document = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    directive = document["_aws"]["CloudWatchMetrics"][0]
    assert directive["Metrics"] == [{"Name": "WebhookFailures", "Unit": "Count"}]
    assert (document["Environment"], document["Reason"]) == ("prod", "signature_invalid")
    assert document["WebhookFailures"] == 1


def test_sink_configurado_nao_duplica_handler_nem_propaga(capsys) -> None:
    build_billing_metrics("prod")
    sink = build_billing_metrics("dev")
    emf_logger = cast("Any", sink)._logger
    assert len(emf_logger.handlers) == 1
    assert emf_logger.propagate is False
