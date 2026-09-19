"""Tests for POST /api/v1/public/leads (unauthenticated lead capture)."""
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from slowapi.errors import RateLimitExceeded
from sqlalchemy.exc import OperationalError

from central_api.ratelimit import limiter, rate_limit_handler
from central_api.repositories.leads_repo import LeadRecord
from central_api.routes import public_leads
from cnes_infra import config

_URL = "/api/v1/public/leads"


def _payload(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "name": "  Pessoa de teste ",
        "email": "pessoa@example.org",
        "interest": "early_access",
        "organization": "Prefeitura X",
        "municipality": "",
        "role": "Cadastradora",
        "message": "Conferir vínculos",
        "source_path": "/contato",
        "source_cta": "acesso-antecipado",
        "privacy_notice_version": "1",
        "newsletter_opt_in": False,
    }
    base.update(overrides)
    return base


def _build(repo: MagicMock) -> TestClient:
    app = FastAPI()
    app.state.leads_repo = repo
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, rate_limit_handler)
    app.include_router(public_leads.router, prefix="/api/v1/public")
    return TestClient(app)


@pytest.fixture(autouse=True)
def _reset_limiter() -> None:
    limiter.reset()


def test_post_responde_202_e_persiste_registro_normalizado() -> None:
    repo = MagicMock()
    repo.create.return_value = uuid4()
    client = _build(repo)
    r = client.post(_URL, json=_payload())
    assert r.status_code == 202
    assert r.json() == {"status": "received"}
    record = repo.create.call_args.args[0]
    assert isinstance(record, LeadRecord)
    assert record.name == "Pessoa de teste"
    assert record.interest == "early_access"
    assert record.privacy_notice_version == "1"
    assert record.newsletter_opt_in is False


def test_post_nao_exige_autenticacao() -> None:
    repo = MagicMock()
    repo.create.return_value = uuid4()
    r = _build(repo).post(_URL, json=_payload(), headers={})
    assert r.status_code == 202


@pytest.mark.parametrize(
    "overrides",
    [
        {"name": ""},
        {"name": "a" * 121},
        {"email": "nao-e-email"},
        {"email": "a" * 250 + "@x.org"},
        {"interest": "outro"},
        {"message": "m" * 2001},
        {"organization": "o" * 201},
        {"privacy_notice_version": ""},
    ],
)
def test_post_responde_422_para_payload_invalido(overrides: dict[str, object]) -> None:
    repo = MagicMock()
    r = _build(repo).post(_URL, json=_payload(**overrides))
    assert r.status_code == 422
    repo.create.assert_not_called()


def test_post_responde_422_sem_campos_obrigatorios() -> None:
    repo = MagicMock()
    r = _build(repo).post(_URL, json={"interest": "contact"})
    assert r.status_code == 422
    repo.create.assert_not_called()


def test_post_responde_503_quando_banco_falha() -> None:
    repo = MagicMock()
    repo.create.side_effect = OperationalError("insert", {}, Exception("down"))
    r = _build(repo).post(_URL, json=_payload())
    assert r.status_code == 503
    assert r.json() == {"detail": "leads_unavailable"}


def test_post_responde_429_apos_limite_com_retry_after(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "LEADS_RATE_LIMIT", "2/minute")
    repo = MagicMock()
    repo.create.return_value = uuid4()
    client = _build(repo)
    assert client.post(_URL, json=_payload()).status_code == 202
    assert client.post(_URL, json=_payload()).status_code == 202
    r = client.post(_URL, json=_payload())
    assert r.status_code == 429
    assert r.headers["Retry-After"] == "60"
    assert r.json()["detail"] == "rate_limited"
    assert repo.create.call_count == 2


def test_limite_e_por_ip_do_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "LEADS_RATE_LIMIT", "1/minute")
    repo = MagicMock()
    repo.create.return_value = uuid4()
    client = _build(repo)
    h1 = {"X-Forwarded-For": "203.0.113.1"}
    h2 = {"X-Forwarded-For": "203.0.113.2"}
    assert client.post(_URL, json=_payload(), headers=h1).status_code == 202
    assert client.post(_URL, json=_payload(), headers=h1).status_code == 429
    assert client.post(_URL, json=_payload(), headers=h2).status_code == 202


def test_log_nao_inclui_campos_da_requisicao(caplog: pytest.LogCaptureFixture) -> None:
    """Request fields must never reach the log line (S5145: forged log entries)."""
    repo = MagicMock()
    repo.create.return_value = uuid4()
    client = _build(repo)
    injected = "contato\nWARNING falso-alerta"
    with caplog.at_level("INFO", logger="central_api.routes.public_leads"):
        r = client.post(_URL, json=_payload(source_cta=injected, name="Fulano\nINFO forjado"))
    assert r.status_code == 202
    logged = "\n".join(rec.getMessage() for rec in caplog.records)
    assert "falso-alerta" not in logged
    assert "forjado" not in logged
    assert "lead_received" in logged


def test_log_de_falha_nao_inclui_campos_da_requisicao(caplog: pytest.LogCaptureFixture) -> None:
    repo = MagicMock()
    repo.create.side_effect = OperationalError("insert", {}, Exception("down"))
    client = _build(repo)
    with caplog.at_level("ERROR", logger="central_api.routes.public_leads"):
        r = client.post(_URL, json=_payload(source_cta="piloto\nERROR forjado"))
    assert r.status_code == 503
    logged = "\n".join(rec.getMessage() for rec in caplog.records)
    assert "forjado" not in logged
    assert "lead_persist_failed" in logged
