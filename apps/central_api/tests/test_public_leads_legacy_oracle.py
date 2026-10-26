"""Executable capture of the legacy public-leads HTTP and SQL behavior."""

from __future__ import annotations

import importlib
import ipaddress
import os
from datetime import UTC, datetime
from pathlib import Path
from runpy import run_path
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from fastapi import FastAPI
from fastapi.testclient import TestClient
from slowapi.errors import RateLimitExceeded
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import SQLAlchemyError

from central_api.middleware import AuthMiddleware
from central_api.ratelimit import limiter, rate_limit_handler
from central_api.repositories.leads_repo import LeadRecord, LeadsRepo
from central_api.routes import public_leads
from cnes_infra.auth import TokenInvalid

if TYPE_CHECKING:
    from collections.abc import Iterator

    from starlette.types import ExceptionHandler

_FIXTURE_PATH = (
    Path(__file__).resolve().parents[3]
    / "packages/cnes_infra/tests/fixtures/public_leads_legacy_cases.py"
)
SYNTHETIC_CASES = cast(
    "dict[str, dict[str, object]]",
    run_path(str(_FIXTURE_PATH))["SYNTHETIC_CASES"],
)

pytestmark = pytest.mark.postgres

_URL = "/api/v1/public/leads"
_ROW_FIELDS = (
    "id",
    "created_at",
    "name",
    "email",
    "interest",
    "organization",
    "municipality",
    "role",
    "message",
    "source_path",
    "source_cta",
    "privacy_notice_version",
    "newsletter_opt_in",
)
_ROW_SELECT = (
    "SELECT id, created_at, name, email, interest, organization, municipality, role, "
    "message, source_path, source_cta, privacy_notice_version, newsletter_opt_in "
    "FROM marketing.leads WHERE id = :id"
)


def _validated_pg_url() -> URL:
    raw = os.environ.get("PG_TEST_URL")
    if not raw:
        raise RuntimeError("PG_TEST_URL is required for the legacy oracle")
    url = make_url(raw)
    host = url.host or ""
    try:
        loopback = ipaddress.ip_address(host).is_loopback
    except ValueError:
        loopback = host == "localhost"
    if url.get_backend_name() != "postgresql" or not loopback:
        raise RuntimeError("PG_TEST_URL must target loopback PostgreSQL")
    if not url.database or not url.database.endswith("_test"):
        raise RuntimeError("PG_TEST_URL database must end with _test")
    if url.port is None:
        raise RuntimeError("PG_TEST_URL must specify the disposable port")
    return url


def _payload(case_id: str, **overrides: object) -> dict[str, object]:
    payload = dict(SYNTHETIC_CASES[case_id])
    payload.update(overrides)
    return payload


def _application(repo: LeadsRepo) -> FastAPI:
    app = FastAPI()
    app.state.leads_repo = repo
    app.state.limiter = limiter
    app.add_middleware(AuthMiddleware)
    app.add_exception_handler(RateLimitExceeded, cast("ExceptionHandler", rate_limit_handler))
    app.include_router(public_leads.router, prefix="/api/v1/public")
    return app


class _RejectingVerifier:
    def __init__(self) -> None:
        self.calls = 0

    def verify(self, _token: str) -> dict[str, object]:
        self.calls += 1
        raise TokenInvalid("test_invalid")


def _read_row(engine: Engine, lead_id: UUID) -> dict[str, object] | None:
    with engine.connect() as connection:
        row = (
            connection.execute(
                text(_ROW_SELECT),
                {"id": lead_id},
            )
            .mappings()
            .one_or_none()
        )
    return dict(row) if row is not None else None


def _count_email(engine: Engine, email: str) -> int:
    with engine.connect() as connection:
        value = connection.execute(
            text("SELECT count(*) FROM marketing.leads WHERE email = :email"),
            {"email": email},
        ).scalar_one()
    return int(value)


def _record(case_id: str, **overrides: object) -> LeadRecord:
    fields = _payload(case_id, **overrides)
    for field in (
        "organization",
        "municipality",
        "role",
        "message",
        "source_path",
        "source_cta",
    ):
        fields.setdefault(field, "")
    fields.setdefault("newsletter_opt_in", False)
    return LeadRecord(**cast("dict[str, Any]", fields))


@pytest.fixture(scope="session")
def pg_engine() -> Iterator[Engine]:
    url = _validated_pg_url()
    engine = create_engine(url)
    migration = importlib.import_module(
        "cnes_infra.alembic.versions.019_marketing_leads",
    )
    try:
        with engine.begin() as connection:
            connection.execute(text("DROP SCHEMA IF EXISTS marketing CASCADE"))
            context = MigrationContext.configure(connection)
            with Operations.context(context):
                migration.upgrade()
        yield engine
    finally:
        try:
            with engine.begin() as connection:
                connection.execute(text("DROP SCHEMA IF EXISTS marketing CASCADE"))
        finally:
            engine.dispose()


@pytest.fixture(autouse=True)
def _reset_limiter() -> Iterator[None]:
    limiter.reset()
    yield
    limiter.reset()


@pytest.fixture
def client(pg_engine: Engine) -> Iterator[TestClient]:
    app = _application(LeadsRepo(pg_engine))
    with TestClient(app, client=("127.0.0.1", 54017)) as value:
        yield value


def test_oraculo_registra_caso_minimo_com_uuid_e_timestamp(
    client: TestClient,
    pg_engine: Engine,
) -> None:
    before = datetime.now(UTC)
    response = client.post(_URL, json=_payload("S01"))
    after = datetime.now(UTC)
    assert response.status_code == 202
    assert response.json() == {"status": "received"}
    with pg_engine.connect() as connection:
        row = (
            connection.execute(
                text("SELECT * FROM marketing.leads WHERE email = :email"),
                {"email": "s01@example.org"},
            )
            .mappings()
            .one()
        )
    assert set(row) == set(_ROW_FIELDS)
    assert isinstance(UUID(str(row["id"])), UUID)
    created_at = cast("datetime", row["created_at"])
    assert created_at.tzinfo is not None
    assert before <= created_at <= after
    assert row["name"] == "Ana Minima"
    assert row["email"] == "s01@example.org"
    assert row["interest"] == "early_access"
    assert row["organization"] == ""
    assert row["municipality"] == ""
    assert row["role"] == ""
    assert row["message"] == ""
    assert row["source_path"] == ""
    assert row["source_cta"] == ""
    assert row["privacy_notice_version"] == "1"
    assert row["newsletter_opt_in"] is False


@pytest.mark.parametrize(("case_id", "interest"), [("S02", "pilot"), ("S03", "contact")])
def test_oraculo_preserva_os_tres_interesses(
    case_id: str,
    interest: str,
    client: TestClient,
    pg_engine: Engine,
) -> None:
    response = client.post(_URL, json=_payload(case_id))
    assert response.status_code == 202
    assert response.json() == {"status": "received"}
    with pg_engine.connect() as connection:
        stored = connection.execute(
            text("SELECT interest FROM marketing.leads WHERE email = :email"),
            {"email": f"{case_id.lower()}@example.org"},
        ).scalar_one()
    assert stored == interest


def test_oraculo_registra_todas_as_fronteiras_sql_validas(
    client: TestClient,
    pg_engine: Engine,
) -> None:
    response = client.post(_URL, json=_payload("S04"))
    assert response.status_code == 202
    lead_id = _id_by_email(pg_engine, "b" * 242 + "@example.org")
    assert lead_id is not None
    row = _read_row(pg_engine, lead_id)
    assert row is not None
    assert row["name"] == "N" * 120
    assert row["email"] == "b" * 242 + "@example.org"
    assert row["interest"] == "contact"
    assert row["organization"] == "O" * 200
    assert row["municipality"] == "M" * 120
    assert row["role"] == "R" * 120
    assert row["message"] == "X" * 2000
    assert row["source_path"] == "P" * 200
    assert row["source_cta"] == "C" * 50
    assert row["privacy_notice_version"] == "v" * 10
    assert row["newsletter_opt_in"] is True


def _id_by_email(engine: Engine, email: str) -> UUID | None:
    with engine.connect() as connection:
        value = connection.execute(
            text("SELECT id FROM marketing.leads WHERE email = :email"),
            {"email": email},
        ).scalar_one_or_none()
    return UUID(str(value)) if value is not None else None


def test_oraculo_rejeita_payload_invalido_antes_do_sql(
    client: TestClient,
    pg_engine: Engine,
) -> None:
    response = client.post(_URL, json=_payload("S05"))
    assert response.status_code == 422
    assert _count_email(pg_engine, "s05@example.org") == 0
    too_long = client.post(
        _URL,
        json=_payload("S01", name="n" * 121, email="invalid-name@example.org"),
    )
    assert too_long.status_code == 422
    assert _count_email(pg_engine, "invalid-name@example.org") == 0


def test_oraculo_real_registra_whitespace_no_nome(client: TestClient, pg_engine: Engine) -> None:
    response = client.post(_URL, json=_payload("S06"))
    assert response.status_code == 503
    assert response.json() == {"detail": "leads_unavailable"}
    assert _count_email(pg_engine, "s06@example.org") == 0


def test_oraculo_real_registra_whitespace_na_privacidade(
    client: TestClient,
    pg_engine: Engine,
) -> None:
    response = client.post(_URL, json=_payload("S07"))
    assert response.status_code == 202
    assert response.json() == {"status": "received"}
    lead_id = _id_by_email(pg_engine, "s07@example.org")
    assert lead_id is not None
    row = _read_row(pg_engine, lead_id)
    assert row is not None
    assert row["privacy_notice_version"] == ""


def test_oraculo_preserva_email_misto_e_duplicado(
    client: TestClient,
    pg_engine: Engine,
) -> None:
    first = client.post(_URL, json=_payload("S08"))
    second = client.post(_URL, json=_payload("S08"))
    assert first.status_code == second.status_code == 202
    with pg_engine.connect() as connection:
        rows = (
            connection.execute(
                text(
                    "SELECT id, email, created_at FROM marketing.leads "
                    "WHERE email = :email ORDER BY created_at, id"
                ),
                {"email": "Lead.Mixed@Example.org"},
            )
            .mappings()
            .all()
        )
    assert len(rows) == 2
    assert UUID(str(rows[0]["id"])) != UUID(str(rows[1]["id"]))
    assert all(row["email"] == "Lead.Mixed@Example.org" for row in rows)
    assert all(cast("datetime", row["created_at"]).tzinfo is not None for row in rows)


def test_oraculo_preserva_newlines_e_defaults_sem_vazar_log(
    client: TestClient,
    pg_engine: Engine,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level("INFO", logger="central_api.routes.public_leads"):
        response = client.post(_URL, json=_payload("S09"))
    assert response.status_code == 202
    row_id = _id_by_email(pg_engine, "s09@example.org")
    assert row_id is not None
    row = _read_row(pg_engine, row_id)
    assert row is not None
    assert row["name"] == "Linha 1\nLinha 2"
    assert row["message"] == "Mensagem\ncom nova linha"
    assert row["source_cta"] == "cta\ncom quebra"
    assert row["organization"] == "Organizacao"
    assert row["municipality"] == ""
    assert row["source_path"] == "/contato"
    logged = "\n".join(record.getMessage() for record in caplog.records)
    assert "lead_received" in logged
    assert "com nova linha" not in logged
    assert "com quebra" not in logged


def test_rota_publica_ignora_auth_e_controle_nao_publico_exige_auth(
    pg_engine: Engine,
) -> None:
    verifier = _RejectingVerifier()
    app = _application(LeadsRepo(pg_engine))
    app.state.oidc_verifier = verifier

    @app.get("/auth-sentinel")
    def auth_sentinel() -> dict[str, str]:
        return {"status": "unexpected"}

    with TestClient(app, client=("127.0.0.1", 54017)) as client:
        absent = client.post(_URL, json=_payload("S01"), headers={})
        invalid = client.post(
            _URL,
            json=_payload("S01", email="auth-invalid@example.org"),
            headers={"Authorization": "Bearer token-invalido"},
        )
        control = client.get(
            "/auth-sentinel",
            headers={"Authorization": "Bearer token-invalido"},
        )
    assert absent.status_code == invalid.status_code == 202
    assert absent.json() == invalid.json() == {"status": "received"}
    assert verifier.calls == 1
    assert control.status_code == 401
    assert control.json() == {"detail": "token_invalid"}


def test_ddl_rejeita_interesse_invalido_e_faz_rollback(
    pg_engine: Engine,
) -> None:
    repo = LeadsRepo(pg_engine)
    with pytest.raises(SQLAlchemyError):
        repo.create(_record("S05", email="ddl-interest@example.org"))
    assert _count_email(pg_engine, "ddl-interest@example.org") == 0


def test_ddl_rejeita_limites_e_faz_rollback(pg_engine: Engine) -> None:
    repo = LeadsRepo(pg_engine)
    invalid = (
        ("ddl-name@example.org", {"name": "n" * 121}),
        ("ddl-email@example.org", {"email": "e" * 243 + "@example.org"}),
        ("ddl-message@example.org", {"message": "m" * 2001}),
    )
    for email, overrides in invalid:
        fields = {"email": email, **overrides}
        with pytest.raises(SQLAlchemyError):
            repo.create(_record("S01", **fields))
        assert _count_email(pg_engine, email) == 0
