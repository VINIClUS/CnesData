"""Testes do db_client — load_from_sql com SQLAlchemy."""
import logging
from unittest.mock import MagicMock, patch

import pandas as pd

from cnes_infra.ingestion.db_client import load_from_sql

_CONN_STRING = "sqlite:///:memory:"
_QUERY = "SELECT 1 AS valor"


class TestLoadFromSql:

    def test_retorna_dataframe_em_sucesso(self):
        df_mock = pd.DataFrame({"valor": [1]})
        with patch("cnes_infra.ingestion.db_client.create_engine") as mock_engine, \
             patch("cnes_infra.ingestion.db_client.pd.read_sql") as mock_read:
            mock_read.return_value = df_mock
            mock_engine.return_value = MagicMock()
            resultado = load_from_sql(_QUERY, _CONN_STRING)
        assert not resultado.empty
        assert resultado["valor"][0] == 1

    def test_loga_quantidade_de_linhas(self, caplog):
        df_mock = pd.DataFrame({"valor": [1, 2]})
        with patch("cnes_infra.ingestion.db_client.create_engine") as mock_engine, \
             patch("cnes_infra.ingestion.db_client.pd.read_sql") as mock_read, \
             caplog.at_level(logging.INFO, logger="cnes_infra.ingestion.db_client"):
            mock_read.return_value = df_mock
            mock_engine.return_value = MagicMock()
            load_from_sql(_QUERY, _CONN_STRING)
        assert "rows=2" in caplog.text

    def test_retorna_dataframe_vazio_em_erro(self):
        with patch("cnes_infra.ingestion.db_client.create_engine") as mock_engine, \
             patch("cnes_infra.ingestion.db_client.pd.read_sql") as mock_read:
            mock_engine.return_value = MagicMock()
            mock_read.side_effect = Exception("falha de conexão")
            resultado = load_from_sql(_QUERY, _CONN_STRING)
        assert resultado.empty

    def test_loga_erro_em_excecao(self, caplog):
        with patch("cnes_infra.ingestion.db_client.create_engine") as mock_engine, \
             patch("cnes_infra.ingestion.db_client.pd.read_sql") as mock_read, \
             caplog.at_level(logging.ERROR, logger="cnes_infra.ingestion.db_client"):
            mock_engine.return_value = MagicMock()
            mock_read.side_effect = Exception("timeout")
            load_from_sql(_QUERY, _CONN_STRING)
        assert "sql_error" in caplog.text
