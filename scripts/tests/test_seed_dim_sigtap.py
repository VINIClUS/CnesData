"""SIGTAP dim seed script tests (real schema)."""
from __future__ import annotations

from textwrap import dedent
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import text

from scripts.seed_dim_sigtap import seed_sigtap

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.postgres


CSV_HEADER = ("cod_sigtap,descricao,complexidade,"
              "financiamento,modalidade,"
              "competencia_vigencia_ini,competencia_vigencia_fim")


class TestSeedDimSigtap:
    def test_seed_inicial_insere_linhas(
        self, pg_engine, tmp_path: Path,
    ) -> None:
        csv = tmp_path / "sigtap.csv"
        csv.write_text(dedent(f"""
            {CSV_HEADER}
            0301010056,CONSULTA MEDICA EM ATEN BAS,1,PAB,AMB,202601,209912
            0301010064,CONSULTA ODONTOLOGICA,1,PAB,AMB,202601,209912
            0401010074,EXAME LAB HEMOGRAMA,2,MAC,AMB,202601,209912
        """).strip())

        inserted = seed_sigtap(pg_engine, csv)
        assert inserted == 3

        with pg_engine.begin() as conn:
            result = conn.execute(text(
                "SELECT COUNT(*) FROM gold.dim_procedimento_sus "
                "WHERE cod_sigtap IN ('0301010056','0301010064','0401010074')"
            )).scalar_one()
        assert result == 3

    def test_reseed_idempotente(
        self, pg_engine, tmp_path: Path,
    ) -> None:
        csv = tmp_path / "sigtap.csv"
        csv.write_text(dedent(f"""
            {CSV_HEADER}
            0399010056,CONSULTA MEDICA TESTE A,1,PAB,AMB,202601,209912
        """).strip())

        seed_sigtap(pg_engine, csv)
        seed_sigtap(pg_engine, csv)

        with pg_engine.begin() as conn:
            count = conn.execute(text(
                "SELECT COUNT(*) FROM gold.dim_procedimento_sus "
                "WHERE cod_sigtap = '0399010056'"
            )).scalar_one()
        assert count == 1

    def test_update_muda_descricao(
        self, pg_engine, tmp_path: Path,
    ) -> None:
        csv1 = tmp_path / "v1.csv"
        csv2 = tmp_path / "v2.csv"
        csv1.write_text(
            f"{CSV_HEADER}\n"
            "0399010099,OLD NAME,1,PAB,AMB,202601,209912"
        )
        csv2.write_text(
            f"{CSV_HEADER}\n"
            "0399010099,NEW NAME,1,PAB,AMB,202601,209912"
        )

        seed_sigtap(pg_engine, csv1)
        seed_sigtap(pg_engine, csv2)

        with pg_engine.begin() as conn:
            name = conn.execute(text(
                "SELECT descricao FROM gold.dim_procedimento_sus "
                "WHERE cod_sigtap = '0399010099'"
            )).scalar_one()
        assert name == "NEW NAME"
