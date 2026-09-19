"""Tests for the explicit CORS allow-list on the app factory."""
from unittest.mock import patch

import pytest

from cnes_infra import config


def _make_app():
    with (
        patch("central_api.app.init_telemetry"),
        patch("central_api.deps.install_rls_listener"),
        patch("central_api.deps.instrument_engine"),
        patch("central_api.deps.install_query_counter"),
        patch("central_api.deps.create_engine"),
    ):
        from central_api.app import create_app
        return create_app()


def _preflight(app, origin: str):
    from fastapi.testclient import TestClient

    client = TestClient(app)
    return client.options(
        "/api/v1/public/leads",
        headers={
            "Origin": origin,
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "content-type",
        },
    )


def test_cors_origins_ignora_wildcard_e_vazios(monkeypatch: pytest.MonkeyPatch) -> None:
    from central_api.app import cors_origins

    monkeypatch.setattr(
        config, "CORS_ALLOWED_ORIGINS", " https://a.example , *, ,https://b.example",
    )
    assert cors_origins() == ["https://a.example", "https://b.example"]


def test_cors_origins_vazio_por_padrao(monkeypatch: pytest.MonkeyPatch) -> None:
    from central_api.app import cors_origins

    monkeypatch.setattr(config, "CORS_ALLOWED_ORIGINS", "")
    assert cors_origins() == []


def test_preflight_aceito_para_origin_permitida(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "CORS_ALLOWED_ORIGINS", "https://dev.cnesdata.example")
    r = _preflight(_make_app(), "https://dev.cnesdata.example")
    assert r.status_code == 200
    assert r.headers["access-control-allow-origin"] == "https://dev.cnesdata.example"
    assert "POST" in r.headers["access-control-allow-methods"]


def test_preflight_negado_para_origin_desconhecida(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "CORS_ALLOWED_ORIGINS", "https://dev.cnesdata.example")
    r = _preflight(_make_app(), "https://evil.example")
    assert r.status_code == 400
    assert "access-control-allow-origin" not in r.headers


def test_sem_origins_configuradas_nenhuma_origin_e_aceita(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "CORS_ALLOWED_ORIGINS", "")
    r = _preflight(_make_app(), "https://dev.cnesdata.example")
    assert "access-control-allow-origin" not in r.headers
