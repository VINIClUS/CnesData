"""Pydantic OAuth response models."""
from datetime import UTC, datetime, timedelta

from cnes_infra.auth import (
    CertProvisionResponse,
    DeviceAuthorizationResponse,
    TokenResponse,
)


def test_device_authorization_response_serializa_campos_rfc8628():
    resp = DeviceAuthorizationResponse(
        device_code="abc123",
        user_code="WDJB-MJHT",
        verification_uri="https://central/activate",
        verification_uri_complete="https://central/activate?code=WDJB-MJHT",
        expires_in=600,
        interval=5,
    )
    payload = resp.model_dump()
    assert payload["device_code"] == "abc123"
    assert payload["user_code"] == "WDJB-MJHT"
    assert payload["expires_in"] == 600


def test_token_response_serializa_bearer():
    resp = TokenResponse(
        access_token="at_xyz",  # noqa: S106
        token_type="Bearer",  # noqa: S106
        expires_in=300,
        refresh_token="rt_xyz",  # noqa: S106
    )
    assert resp.token_type == "Bearer"  # noqa: S105
    assert resp.refresh_token == "rt_xyz"  # noqa: S105


def test_cert_provision_response_serializa_pem():
    resp = CertProvisionResponse(
        cert_pem="-----BEGIN CERTIFICATE-----\nMIIB...",
        ca_chain_pem="-----BEGIN CERTIFICATE-----\nMIIC...",
        refresh_token="rt_xyz",  # noqa: S106
        expires_at=datetime.now(UTC) + timedelta(days=90),
    )
    assert "BEGIN CERTIFICATE" in resp.cert_pem
