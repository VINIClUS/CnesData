"""Testes para iterar_query_em_lotes."""

from unittest.mock import MagicMock

import polars as pl

from cnes_infra.ingestion.cnes_client import iterar_query_em_lotes


def _mock_connection(batches: list[list[tuple]]):
    con = MagicMock()
    cur = MagicMock()
    con.cursor.return_value = cur
    cur.description = [("COL_A",), ("COL_B",)]
    cur.fetchmany.side_effect = batches + [[]]
    return con


class TestIterarQueryEmLotes:

    def test_retorna_batches_como_dataframes(self):
        con = _mock_connection([
            [("a", "1"), ("b", "2")],
            [("c", "3")],
        ])
        frames = list(iterar_query_em_lotes(con, "SELECT 1", 2))
        assert len(frames) == 2
        assert isinstance(frames[0], pl.DataFrame)
        assert len(frames[0]) == 2
        assert len(frames[1]) == 1

    def test_cursor_vazio_retorna_nenhum_frame(self):
        con = _mock_connection([])
        frames = list(iterar_query_em_lotes(con, "SELECT 1"))
        assert frames == []

    def test_cursor_fechado_no_finally(self):
        con = _mock_connection([[("a", "1")]])
        list(iterar_query_em_lotes(con, "SELECT 1"))
        con.cursor.return_value.close.assert_called_once()
