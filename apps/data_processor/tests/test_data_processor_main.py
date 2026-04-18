"""Testes do ponto de entrada main do data_processor."""
import logging
from unittest.mock import MagicMock, patch

import pytest


class TestSetupLogging:
    def test_setup_logging_cria_handlers(self, tmp_path, monkeypatch):
        from cnes_infra import config as infra_config
        monkeypatch.setattr(infra_config, "LOGS_DIR", tmp_path)
        monkeypatch.setattr(
            infra_config, "LOG_FILE", tmp_path / "test.log",
        )
        root = logging.getLogger()
        original_handlers = root.handlers[:]
        try:
            from data_processor.main import _setup_logging
            _setup_logging(verbose=True)
            assert len(root.handlers) > len(original_handlers)
        finally:
            for h in root.handlers[:]:
                if h not in original_handlers:
                    root.removeHandler(h)

    def test_setup_logging_verbose_false(self, tmp_path, monkeypatch):
        from cnes_infra import config as infra_config
        monkeypatch.setattr(infra_config, "LOGS_DIR", tmp_path)
        monkeypatch.setattr(
            infra_config, "LOG_FILE", tmp_path / "test.log",
        )
        root = logging.getLogger()
        original_handlers = root.handlers[:]
        try:
            from data_processor.main import _setup_logging
            _setup_logging(verbose=False)
        finally:
            for h in root.handlers[:]:
                if h not in original_handlers:
                    root.removeHandler(h)


class TestCreateStorage:
    def test_retorna_minio_quando_disponivel(self):
        mock_instance = MagicMock()
        with (
            patch("cnes_infra.config.MINIO_ENDPOINT", "http://minio:9000"),
            patch("cnes_infra.config.MINIO_ACCESS_KEY", "user"),
            patch("cnes_infra.config.MINIO_SECRET_KEY", "pass"),
            patch("cnes_infra.config.MINIO_SECURE", False),
            patch(
                "cnes_infra.storage.object_storage.MinioObjectStorage",
                return_value=mock_instance,
            ),
        ):
            from data_processor.main import _create_storage
            storage = _create_storage()
        assert storage is mock_instance

    def test_retorna_null_quando_minio_indisponivel(self):
        from cnes_domain.ports.object_storage import NullObjectStoragePort
        with patch(
            "cnes_infra.storage.object_storage.MinioObjectStorage",
            side_effect=Exception("minio_down"),
        ):
            from data_processor.main import _create_storage
            storage = _create_storage()
        assert isinstance(storage, NullObjectStoragePort)


class TestMain:
    @pytest.mark.asyncio
    async def test_main_executa_run_processor(self, tmp_path, monkeypatch):
        import sys

        from cnes_infra import config as infra_config
        monkeypatch.setattr(infra_config, "LOGS_DIR", tmp_path)
        monkeypatch.setattr(
            infra_config, "LOG_FILE", tmp_path / "test.log",
        )
        monkeypatch.setattr(sys, "argv", ["data_processor"])

        with (
            patch("data_processor.main._setup_logging"),
            patch("data_processor.main.init_telemetry"),
            patch("data_processor.main.create_engine"),
            patch("data_processor.main._create_storage"),
            patch("data_processor.main.run_processor") as mock_run,
        ):
            mock_run.return_value = None
            mock_run.side_effect = None

            async def _fake_run(*a, **kw):
                pass

            mock_run.side_effect = _fake_run
            from data_processor.main import main
            rc = await main()

        assert rc == 0
        mock_run.assert_called_once()
