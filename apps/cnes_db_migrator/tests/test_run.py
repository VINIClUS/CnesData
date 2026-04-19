"""Testes unitários do cnes_db_migrator."""
from unittest.mock import MagicMock, patch

import pytest


class TestMain:
    def test_sai_com_erro_quando_db_url_nao_definida(self, monkeypatch):
        monkeypatch.delenv("DB_URL", raising=False)
        from cnes_db_migrator.run import main
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    def test_executa_upgrade_quando_db_url_configurada(self, monkeypatch):
        monkeypatch.setenv("DB_URL", "postgresql://test:test@localhost/test")
        with (
            patch("cnes_db_migrator.run.Config") as mock_cfg_cls,
            patch("cnes_db_migrator.run.command") as mock_cmd,
        ):
            mock_cfg = MagicMock()
            mock_cfg_cls.return_value = mock_cfg
            from cnes_db_migrator.run import main
            main()
        mock_cmd.upgrade.assert_called_once_with(mock_cfg, "head")

    def test_configura_script_location_e_url(self, monkeypatch):
        monkeypatch.setenv("DB_URL", "postgresql://test:test@localhost/test")
        captured = {}
        with (
            patch("cnes_db_migrator.run.Config") as mock_cfg_cls,
            patch("cnes_db_migrator.run.command"),
        ):
            mock_cfg = MagicMock()
            mock_cfg_cls.return_value = mock_cfg

            def _capture_set(key, val):
                captured[key] = val

            mock_cfg.set_main_option.side_effect = _capture_set
            from cnes_db_migrator.run import main
            main()
        assert captured.get("sqlalchemy.url") == (
            "postgresql://test:test@localhost/test"
        )
        assert captured.get("script_location") == "cnes_infra:alembic"
