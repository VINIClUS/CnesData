"""Postgres-backed tests for LeadsRepo (marketing.leads)."""
import pytest
from sqlalchemy import text

from central_api.repositories.leads_repo import LeadRecord, LeadsRepo

pytestmark = pytest.mark.postgres


@pytest.fixture
def repo(pg_engine):
    yield LeadsRepo(pg_engine)
    with pg_engine.begin() as conn:
        conn.execute(text("DELETE FROM marketing.leads WHERE email LIKE '%@leads-test.example'"))


def _record(**overrides: object) -> LeadRecord:
    base: dict[str, object] = {
        "name": "Pessoa",
        "email": "pessoa@leads-test.example",
        "interest": "pilot",
        "organization": "",
        "municipality": "",
        "role": "",
        "message": "",
        "source_path": "/contato",
        "source_cta": "piloto",
        "privacy_notice_version": "1",
        "newsletter_opt_in": False,
    }
    base.update(overrides)
    return LeadRecord(**base)  # type: ignore[arg-type]


def test_create_persiste_e_retorna_id(repo, pg_engine) -> None:
    lead_id = repo.create(_record(message="Quero testar"))
    with pg_engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT name, email, interest, message, created_at "
                "FROM marketing.leads WHERE id = :i"
            ),
            {"i": lead_id},
        ).mappings().one()
    assert row["name"] == "Pessoa"
    assert row["interest"] == "pilot"
    assert row["message"] == "Quero testar"
    assert row["created_at"] is not None


def test_create_aceita_duplicados_de_email(repo) -> None:
    first = repo.create(_record())
    second = repo.create(_record())
    assert first != second


def test_create_rejeita_interesse_invalido(repo) -> None:
    from sqlalchemy.exc import IntegrityError

    with pytest.raises(IntegrityError):
        repo.create(_record(interest="outro"))
