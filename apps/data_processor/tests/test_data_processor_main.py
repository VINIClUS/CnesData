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
        monkeypatch.delenv("PROFILE", raising=False)
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


class TestMainProfileLocal:
    @pytest.mark.asyncio
    async def test_main_profile_local_compoe_runtime_sem_run_processor(
        self, tmp_path, monkeypatch
    ):
        from unittest.mock import AsyncMock, patch

        from cnes_infra import config as infra_config
        monkeypatch.setenv("PROFILE", "local")
        monkeypatch.setenv("TENANT_ID", "354130")
        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        monkeypatch.setattr(infra_config, "LOGS_DIR", tmp_path)
        monkeypatch.setattr(infra_config, "LOG_FILE", tmp_path / "test.log")

        with (
            patch("data_processor.main._setup_logging"),
            patch("data_processor.main.init_telemetry"),
            patch("data_processor.main.run_processor") as mock_run,
            patch("data_processor.main._poll_until_shutdown", new_callable=AsyncMock),
        ):
            from data_processor.main import main
            rc = await main()

        assert rc == 0
        mock_run.assert_not_called()


class TestPollUntilShutdown:
    @pytest.mark.asyncio
    async def test_recover_tick_loga_quantidade_de_runs_recuperados(self, caplog):
        from data_processor.main import _recover_tick

        class _Coordinator:
            def recover(self):
                return ("run-a", "run-b")

        with caplog.at_level("INFO", logger="data_processor.main"):
            await _recover_tick(_Coordinator())

        assert "local_profile_recover_tick runs=2" in caplog.text

    @pytest.mark.asyncio
    async def test_recover_tick_absorve_excecao_e_continua(self, caplog):
        from data_processor.main import _recover_tick

        class _Coordinator:
            def recover(self):
                raise RuntimeError("sqlite_busy")

        with caplog.at_level("ERROR", logger="data_processor.main"):
            await _recover_tick(_Coordinator())

        assert "local_profile_recover_tick_error" in caplog.text

    @pytest.mark.asyncio
    async def test_poll_until_shutdown_encerra_apos_sinalizado(self):
        import asyncio

        from data_processor.main import _poll_until_shutdown

        ticks = []

        class _Coordinator:
            def recover(self):
                ticks.append(1)
                if len(ticks) == 2:
                    shutdown.set()
                return ()

        shutdown = asyncio.Event()
        await asyncio.wait_for(
            _poll_until_shutdown(_Coordinator(), shutdown, interval=0.001), timeout=2
        )

        assert len(ticks) == 2
