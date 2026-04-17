"""Testes do QuarantineBuffer e quarentinar_linhas."""

from unittest.mock import MagicMock

import polars as pl
import pytest

from cnes_domain.quarantine import (
    QuarantineBuffer,
    QuarantineRecord,
    quarentinar_linhas,
)


def _record(idx: int = 0) -> QuarantineRecord:
    return QuarantineRecord(
        competencia="2025-01",
        source_system="FIREBIRD",
        record_identifier=f"cpf_{idx}",
        error_category="CPF_INVALIDO",
        failure_reason="len!=11",
        raw_payload={"CPF": f"cpf_{idx}"},
    )


class TestQuarantineBufferAppendLen:

    def test_len_zero_sem_registros(self):
        buf = QuarantineBuffer()
        assert len(buf) == 0

    def test_len_cresce_apos_append(self):
        buf = QuarantineBuffer()
        buf.append(_record(0))
        buf.append(_record(1))
        assert len(buf) == 2


class TestFlushToDuckdb:

    def test_flush_buffer_vazio_retorna_zero(self):
        buf = QuarantineBuffer()
        con = MagicMock()
        assert buf.flush_to_duckdb(con) == 0
        con.executemany.assert_not_called()

    def test_flush_persiste_registros_e_retorna_contagem(self):
        buf = QuarantineBuffer()
        buf.append(_record(0))
        buf.append(_record(1))
        con = MagicMock()
        assert buf.flush_to_duckdb(con) == 2
        con.executemany.assert_called_once()

    def test_flush_limpa_buffer_apos_persistir(self):
        buf = QuarantineBuffer()
        buf.append(_record())
        buf.flush_to_duckdb(MagicMock())
        assert len(buf) == 0

    def test_flush_serial_acumula_zero_na_segunda_chamada(self):
        buf = QuarantineBuffer()
        buf.append(_record())
        con = MagicMock()
        buf.flush_to_duckdb(con)
        assert buf.flush_to_duckdb(con) == 0


class TestQuarantineRatio:

    def test_ratio_zero_sem_registros_nem_validos(self):
        buf = QuarantineBuffer()
        assert buf.quarantine_ratio(0) == 0.0

    def test_ratio_calcula_proporcao_correta(self):
        buf = QuarantineBuffer()
        buf.append(_record())
        ratio = buf.quarantine_ratio(3)
        assert ratio == pytest.approx(0.25)

    def test_ratio_um_quando_todos_quarentenados(self):
        buf = QuarantineBuffer()
        buf.append(_record())
        assert buf.quarantine_ratio(0) == 1.0


class TestQuarentinarLinhas:

    def _df(self) -> pl.DataFrame:
        return pl.DataFrame({
            "CPF": ["11111111111", "22222222222", "33333333333"],
            "NOME": ["Ana", "Bia", "Cau"],
        })

    def test_sem_indices_nao_adiciona_registros(self):
        buf = QuarantineBuffer()
        quarentinar_linhas(self._df(), [], buf, "2025-01", "FW", "ERR", "why")
        assert len(buf) == 0

    def test_com_indices_adiciona_registros_corretos(self):
        buf = QuarantineBuffer()
        df = self._df()
        quarentinar_linhas(df, [0, 2], buf, "2025-01", "FW", "ERR", "why")
        assert len(buf) == 2

    def test_record_identifier_usa_coluna_id_col(self):
        buf = QuarantineBuffer()
        df = self._df()
        quarentinar_linhas(df, [1], buf, "2025-01", "FW", "ERR", "why", id_col="CPF")
        records = list(buf._records)
        assert records[0].record_identifier == "22222222222"

    def test_payload_contem_linha_completa(self):
        buf = QuarantineBuffer()
        df = self._df()
        quarentinar_linhas(df, [0], buf, "2025-01", "FW", "ERR", "why")
        payload = buf._records[0].raw_payload
        assert payload["CPF"] == "11111111111"
        assert payload["NOME"] == "Ana"

    def test_metadata_propagada_corretamente(self):
        buf = QuarantineBuffer()
        df = self._df()
        quarentinar_linhas(df, [0], buf, "2025-01", "DATASUS", "CPF_NULL", "motivo")
        rec = buf._records[0]
        assert rec.competencia == "2025-01"
        assert rec.source_system == "DATASUS"
        assert rec.error_category == "CPF_NULL"
        assert rec.failure_reason == "motivo"
