"""Tests for producao_ambulatorial_repo upsert idempotente (Task 6)."""
from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import text

from cnes_contracts.fatos import ProducaoAmbulatorial
from cnes_infra.storage.repositories import producao_ambulatorial_repo

pytestmark = pytest.mark.postgres


@pytest.fixture
def pg_dims_seeded(pg_engine):
    """No-op: fato_producao_ambulatorial has no FKs to dim_* tables."""


def _sk_comp_2026_01(pg_engine) -> int:
    with pg_engine.begin() as conn:
        return conn.execute(
            text(
                "SELECT sk_competencia FROM gold.dim_competencia "
                "WHERE competencia = 202601",
            ),
        ).scalar_one()


def _fato(
    sk_comp: int, job_id, sk_proc: int = 1, fonte: str = "BPA_I",
) -> ProducaoAmbulatorial:
    return ProducaoAmbulatorial(
        sk_profissional=1,
        sk_estabelecimento=1,
        sk_procedimento=sk_proc,
        sk_competencia=sk_comp,
        qtd=5,
        valor_aprov_cents=1000,
        job_id=job_id,
        fonte_sistema=fonte,
        extracao_ts=datetime.now(UTC),
        fontes_reportadas={"BPA_MAG": {"qtd": 5}},
    )


def _truncate(pg_engine) -> None:
    with pg_engine.begin() as conn:
        conn.execute(
            text(
                "TRUNCATE gold.fato_producao_ambulatorial "
                "RESTART IDENTITY CASCADE",
            ),
        )


class TestProducaoAmbulatorialRepo:
    def test_gravar_insere(self, pg_engine, pg_dims_seeded) -> None:
        _truncate(pg_engine)
        sk_comp = _sk_comp_2026_01(pg_engine)
        fato = _fato(sk_comp, uuid4())
        with pg_engine.begin() as conn:
            producao_ambulatorial_repo.gravar(conn, fato)
        with pg_engine.begin() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM gold.fato_producao_ambulatorial"),
            ).scalar_one()
        assert count == 1

    def test_gravar_idempotente(self, pg_engine, pg_dims_seeded) -> None:
        _truncate(pg_engine)
        sk_comp = _sk_comp_2026_01(pg_engine)
        fato = _fato(sk_comp, uuid4())
        with pg_engine.begin() as conn:
            producao_ambulatorial_repo.gravar(conn, fato)
            producao_ambulatorial_repo.gravar(conn, fato)
        with pg_engine.begin() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM gold.fato_producao_ambulatorial"),
            ).scalar_one()
        assert count == 1

    def test_gravar_merge_fontes_reportadas(
        self, pg_engine, pg_dims_seeded,
    ) -> None:
        _truncate(pg_engine)
        sk_comp = _sk_comp_2026_01(pg_engine)
        job = uuid4()
        f1 = _fato(sk_comp, job, fonte="BPA_I")
        f2 = f1.model_copy(
            update={"fontes_reportadas": {"SIA_BPI": {"qtd": 3}}},
        )
        with pg_engine.begin() as conn:
            producao_ambulatorial_repo.gravar(conn, f1)
            producao_ambulatorial_repo.gravar(conn, f2)
        with pg_engine.begin() as conn:
            row = conn.execute(
                text(
                    "SELECT fontes_reportadas "
                    "FROM gold.fato_producao_ambulatorial",
                ),
            ).scalar_one()
        assert "BPA_MAG" in row
        assert "SIA_BPI" in row

    def test_gravar_cross_job_gera_duas_linhas(
        self, pg_engine, pg_dims_seeded,
    ) -> None:
        _truncate(pg_engine)
        sk_comp = _sk_comp_2026_01(pg_engine)
        f1 = _fato(sk_comp, uuid4())
        f2 = _fato(sk_comp, uuid4())
        with pg_engine.begin() as conn:
            producao_ambulatorial_repo.gravar(conn, f1)
            producao_ambulatorial_repo.gravar(conn, f2)
        with pg_engine.begin() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM gold.fato_producao_ambulatorial"),
            ).scalar_one()
        assert count == 2
