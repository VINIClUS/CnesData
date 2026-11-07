"""Migration 019: marketing.leads exists with the expected columns and checks."""
import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

pytestmark = pytest.mark.postgres

_COLUMNS = {
    "id", "created_at", "name", "email", "interest", "organization", "municipality",
    "role", "message", "source_path", "source_cta", "privacy_notice_version",
    "newsletter_opt_in",
}


def test_tabela_marketing_leads_tem_colunas_esperadas(pg_conn) -> None:
    rows = pg_conn.execute(
        text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'marketing' AND table_name = 'leads'"
        )
    ).scalars().all()
    assert set(rows) == _COLUMNS


def test_check_de_interesse_rejeita_valor_desconhecido(pg_conn) -> None:
    with pytest.raises(IntegrityError):
        pg_conn.execute(
            text(
                "INSERT INTO marketing.leads (name, email, interest, privacy_notice_version) "
                "VALUES ('x', 'x@y.z', 'outro', '1')"
            )
        )
