"""CID10 dim seed script tests (real schema)."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from sqlalchemy import text

from scripts.seed_dim_cid10 import seed_cid10

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.postgres


class TestSeedDimCid10:
    def test_seed_insere(self, pg_engine, tmp_path: Path) -> None:
        csv = tmp_path / "cid.csv"
        csv.write_text(
            "cod_cid,descricao,capitulo\n"
            "Z999,RINOFARINGITE TESTE A,10\n"
            "Z998,CARIE TESTE B,11\n"
        )
        inserted = seed_cid10(pg_engine, csv)
        assert inserted == 2

    def test_idempotente(self, pg_engine, tmp_path: Path) -> None:
        csv = tmp_path / "cid.csv"
        csv.write_text(
            "cod_cid,descricao,capitulo\n"
            "Z997,TESTE A,10\n"
        )
        seed_cid10(pg_engine, csv)
        seed_cid10(pg_engine, csv)
        with pg_engine.begin() as conn:
            count = conn.execute(text(
                "SELECT COUNT(*) FROM gold.dim_cid10 WHERE cod_cid='Z997'"
            )).scalar_one()
        assert count == 1
