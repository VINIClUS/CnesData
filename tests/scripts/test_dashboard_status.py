"""Testes de dashboard_status.carregar_status — diagnóstico de dependências."""
import json
import sys
from unittest.mock import MagicMock, patch

import pandas as pd

from dashboard_status import DepStatus, carregar_status


class TestCarregarStatus:

    def test_retorna_nao_configurada_quando_sem_env_e_sem_last_run(self, tmp_path):
        path = tmp_path / "last_run.json"
        with patch.dict("os.environ", {}, clear=True):
            status = carregar_status(path, tmp_path / "cnesdata.duckdb")

        assert status["firebird"].ok is None
        assert status["bigquery"].ok is None
        assert status["hr"].ok is None

    def test_duckdb_erro_quando_arquivo_nao_existe(self, tmp_path):
        path = tmp_path / "last_run.json"
        with patch.dict("os.environ", {}, clear=True):
            status = carregar_status(path, tmp_path / "inexistente.duckdb")

        assert status["duckdb"].ok is False

    def test_duckdb_ok_none_quando_arquivo_existe_mas_sem_last_run(self, tmp_path):
        duckdb_path = tmp_path / "cnesdata.duckdb"
        duckdb_path.write_bytes(b"")
        path = tmp_path / "last_run.json"

        with patch.dict("os.environ", {}, clear=True):
            status = carregar_status(path, duckdb_path)

        assert status["duckdb"].ok is None

    def test_le_status_firebird_do_last_run(self, tmp_path):
        duckdb_path = tmp_path / "cnesdata.duckdb"
        duckdb_path.write_bytes(b"")
        path = tmp_path / "last_run.json"
        path.write_text(
            json.dumps({"firebird": {"ts": "2026-03-28T14:32:00", "ok": True}}),
            encoding="utf-8",
        )
        env = {"DB_PATH": "/db", "DB_PASSWORD": "x", "FIREBIRD_DLL": "/fbclient.dll"}
        with patch.dict("os.environ", env, clear=False):
            status = carregar_status(path, duckdb_path)

        assert status["firebird"].ok is True
        assert status["firebird"].ts == "2026-03-28T14:32:00"

    def test_le_status_bigquery_false_do_last_run(self, tmp_path):
        duckdb_path = tmp_path / "cnesdata.duckdb"
        duckdb_path.write_bytes(b"")
        path = tmp_path / "last_run.json"
        path.write_text(
            json.dumps({"bigquery": {"ts": None, "ok": False}}),
            encoding="utf-8",
        )
        env = {"GCP_PROJECT_ID": "proj-123"}
        with patch.dict("os.environ", env, clear=False):
            status = carregar_status(path, duckdb_path)

        assert status["bigquery"].ok is False
        assert status["bigquery"].ts is None

    def test_arquivo_corrompido_retorna_status_desconhecido(self, tmp_path):
        duckdb_path = tmp_path / "cnesdata.duckdb"
        duckdb_path.write_bytes(b"")
        path = tmp_path / "last_run.json"
        path.write_text("{ JSON INVÁLIDO", encoding="utf-8")

        with patch.dict("os.environ", {}, clear=True):
            status = carregar_status(path, duckdb_path)

        assert isinstance(status["firebird"], DepStatus)


class TestExecutarRangeBigquery:

    def test_retorna_none_quando_project_id_vazio(self):
        from dashboard_status import _executar_range_query
        assert _executar_range_query("", "3523008") is None

    def test_retorna_none_quando_id_municipio_vazio(self):
        from dashboard_status import _executar_range_query
        assert _executar_range_query("proj-123", "") is None

    def test_retorna_none_quando_bd_levanta_excecao(self, monkeypatch):
        mock_bd = MagicMock()
        mock_bd.read_sql.side_effect = Exception("BQ error")
        monkeypatch.setitem(sys.modules, "basedosdados", mock_bd)

        from importlib import reload
        import dashboard_status
        reload(dashboard_status)
        from dashboard_status import _executar_range_query

        result = _executar_range_query("proj-123", "3523008")
        assert result is None

    def test_retorna_range_quando_bd_disponivel(self, monkeypatch):
        mock_bd = MagicMock()
        mock_bd.read_sql.return_value = pd.DataFrame({
            "min_comp": ["2024-01"],
            "max_comp": ["2026-03"],
        })
        monkeypatch.setitem(sys.modules, "basedosdados", mock_bd)

        from importlib import reload
        import dashboard_status
        reload(dashboard_status)
        from dashboard_status import _executar_range_query

        result = _executar_range_query("proj-123", "3523008")
        assert result == ("2024-01", "2026-03")


class TestExecutarHealthCheckDatasus:

    def test_retorna_ok_true_quando_resposta_2xx(self, monkeypatch):
        mock_requests = MagicMock()
        mock_requests.get.return_value.status_code = 200
        monkeypatch.setitem(sys.modules, "requests", mock_requests)
        from importlib import reload
        import dashboard_status
        reload(dashboard_status)
        from dashboard_status import _executar_health_check_datasus
        assert _executar_health_check_datasus("https://url", "token-abc").ok is True

    def test_retorna_false_token_invalido_quando_401(self, monkeypatch):
        mock_requests = MagicMock()
        mock_requests.get.return_value.status_code = 401
        monkeypatch.setitem(sys.modules, "requests", mock_requests)
        from importlib import reload
        import dashboard_status
        reload(dashboard_status)
        from dashboard_status import _executar_health_check_datasus
        result = _executar_health_check_datasus("https://url", "token-abc")
        assert result.ok is False
        assert "token" in result.erro

    def test_retorna_false_inacessivel_quando_excecao(self, monkeypatch):
        mock_requests = MagicMock()
        mock_requests.get.side_effect = Exception("timeout")
        monkeypatch.setitem(sys.modules, "requests", mock_requests)
        from importlib import reload
        import dashboard_status
        reload(dashboard_status)
        from dashboard_status import _executar_health_check_datasus
        result = _executar_health_check_datasus("https://url", "token-abc")
        assert result.ok is False
        assert "inacessível" in result.erro

    def test_retorna_false_com_codigo_quando_503(self, monkeypatch):
        mock_requests = MagicMock()
        mock_requests.get.return_value.status_code = 503
        monkeypatch.setitem(sys.modules, "requests", mock_requests)
        from importlib import reload
        import dashboard_status
        reload(dashboard_status)
        from dashboard_status import _executar_health_check_datasus
        result = _executar_health_check_datasus("https://url", "token-abc")
        assert result.ok is False
        assert "503" in result.erro
