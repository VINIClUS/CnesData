"""Migration 011 upgrade + downgrade tests."""
from __future__ import annotations

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import inspect, text

pytestmark = pytest.mark.postgres


def _alembic_cfg(url: str) -> Config:
    cfg = Config()
    cfg.set_main_option("sqlalchemy.url", url)
    cfg.set_main_option(
        "script_location",
        "packages/cnes_infra/src/cnes_infra/alembic",
    )
    return cfg


def _url(pg_engine) -> str:
    return pg_engine.url.render_as_string(hide_password=False)


class TestMigration011:
    @pytest.fixture(autouse=True)
    def _ensure_head(self, pg_engine) -> None:
        command.upgrade(_alembic_cfg(_url(pg_engine)), "head")

    def test_upgrade_cria_files_jsonb(self, pg_engine) -> None:
        cfg = _alembic_cfg(_url(pg_engine))
        command.upgrade(cfg, "011_bpa_sia_sources")

        insp = inspect(pg_engine)
        cols = {c["name"]: c for c in insp.get_columns(
            "extractions", schema="landing",
        )}
        assert "files" in cols
        assert "minio_key" not in cols
        assert "depends_on" in cols

    def test_upgrade_cria_dim_misses(self, pg_engine) -> None:
        command.upgrade(
            _alembic_cfg(_url(pg_engine)), "011_bpa_sia_sources",
        )
        insp = inspect(pg_engine)
        assert insp.has_table("dim_misses", schema="landing")

    def test_source_type_aceita_bpa_mag(self, pg_engine) -> None:
        command.upgrade(
            _alembic_cfg(_url(pg_engine)), "011_bpa_sia_sources",
        )
        with pg_engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO landing.extractions "
                "(job_id, tenant_id, source_type, competencia, "
                " files, status, created_at) "
                "VALUES (gen_random_uuid(), 't1', 'BPA_MAG', '2026-01-01', "
                "        '[]'::jsonb, 'PENDING', NOW())"
            ))

    def test_source_type_rejeita_desconhecido(self, pg_engine) -> None:
        command.upgrade(
            _alembic_cfg(_url(pg_engine)), "011_bpa_sia_sources",
        )
        with pg_engine.begin() as conn, pytest.raises(Exception):
            conn.execute(text(
                "INSERT INTO landing.extractions "
                "(job_id, tenant_id, source_type, competencia, "
                " files, status, created_at) "
                "VALUES (gen_random_uuid(), 't1', 'UNKNOWN', '2026-01-01', "
                "        '[]'::jsonb, 'PENDING', NOW())"
            ))

    def test_downgrade_remove_files_restaura_minio_key(
        self, pg_engine,
    ) -> None:
        cfg = _alembic_cfg(_url(pg_engine))
        command.upgrade(cfg, "011_bpa_sia_sources")
        command.downgrade(cfg, "010")
        try:
            insp = inspect(pg_engine)
            cols = {c["name"] for c in insp.get_columns(
                "extractions", schema="landing",
            )}
            assert "files" not in cols
            assert "minio_key" in cols
        finally:
            command.upgrade(cfg, "head")
