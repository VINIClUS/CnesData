"""Billing local desativado não lê segredos, não importa SDK remoto e não abre rede."""

import subprocess
import sys
from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from cnes_domain.billing.commands import CreateRunRequest, ReserveRunCommand
from cnes_domain.billing.models import ReadConsistency
from cnes_domain.control_plane.entities import RunDependency
from cnes_infra.billing.disabled import DisabledEntitlementProjection, DisabledQuotaReservations

_NOW = datetime(2026, 9, 1, 12, tzinfo=UTC)
_REMOTE_MODULES = ("stripe", "boto3", "botocore")


def _create_run_request() -> CreateRunRequest:
    return CreateRunRequest(
        billing_account_id="local",
        tenant_id="354130",
        run_id="run-1",
        competencia="2026-08",
        dataset_name="cnes",
        dependencies=(RunDependency(source_type="CNES", file_subtype="LOCAL", required=True),),
        idempotency_key="req-1",
        request_hash="a" * 64,
        requested_concurrency=2,
        estimated_scan_bytes=0,
    )


@pytest.mark.negative
def test_local_disabled_nao_le_secrets_nem_abre_rede(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BILLING_MODE", "disabled")
    monkeypatch.delenv("STRIPE_SECRET_KEY", raising=False)
    monkeypatch.delenv("STRIPE_WEBHOOK_SECRET", raising=False)
    network = AssertionError("network_called")
    with (
        patch("socket.create_connection", side_effect=network) as connect,
        patch("socket.getaddrinfo", side_effect=network) as resolve,
    ):
        snapshot = DisabledEntitlementProjection(lambda: _NOW).get_snapshot(
            "local", ReadConsistency.STRONG,
        )
        command = ReserveRunCommand(_create_run_request(), snapshot, 4, "r-1", _NOW)
        result = DisabledQuotaReservations(lambda: _NOW).reserve_and_create_run(command)
    assert result.plan_version_id == "local-unmetered-v1"
    assert result.budget_reservation_id is None
    connect.assert_not_called()
    resolve.assert_not_called()


@pytest.mark.negative
def test_import_do_modo_disabled_nao_carrega_sdk_remoto() -> None:
    probe = (
        "import sys, cnes_infra.billing.disabled; "
        f"print(','.join(m for m in {_REMOTE_MODULES!r} if m in sys.modules))"
    )
    loaded = subprocess.run(  # noqa: S603
        [sys.executable, "-c", probe], capture_output=True, text=True, check=True,
    ).stdout.strip()
    assert loaded == ""
