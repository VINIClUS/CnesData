"""Testes do ponto de entrada main do data_processor."""
import logging
from datetime import UTC, datetime
from typing import Any, cast
from unittest.mock import MagicMock, Mock, patch

import pytest

_NOW = datetime(2026, 9, 27, 12, tzinfo=UTC)


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
    def test_retorna_s3_presigned_storage(self):
        mock_instance = MagicMock()
        with (
            patch("cnes_infra.config.S3_REGION", "sa-east-1"),
            patch("cnes_infra.config.S3_ENDPOINT_URL", "http://minio:9000"),
            patch("cnes_infra.config.S3_ADDRESSING_STYLE", "path"),
            patch("data_processor.main.build_s3_client", return_value=MagicMock()),
            patch(
                "data_processor.main.S3PresignedStorage",
                return_value=mock_instance,
            ),
        ):
            from data_processor.main import _create_storage
            storage = _create_storage()
        assert storage is mock_instance

    def test_propaga_erro_de_construcao_do_client_em_vez_de_engolir(self):
        """Falha ao construir o client de storage tem que subir alto — em S3,
        engolir e cair para um storage nulo vira perda silenciosa de dado."""
        with patch(
            "data_processor.main.build_s3_client",
            side_effect=RuntimeError("s3_client_unavailable"),
        ):
            from data_processor.main import _create_storage
            with pytest.raises(RuntimeError, match="s3_client_unavailable"):
                _create_storage()

    def test_usa_client_publico_quando_diverge_do_interno(self, monkeypatch):
        """Mesmo split de central_api/deps.py (H9): a URL presigned
        entregue ao worker precisa de um host diferente do usado
        internamente quando S3_PUBLIC_ENDPOINT_URL diverge."""
        from cnes_infra import config as infra_config
        monkeypatch.setattr(infra_config, "S3_ENDPOINT_URL", "http://minio:9000")
        monkeypatch.setattr(
            infra_config, "S3_PUBLIC_ENDPOINT_URL", "https://storage.dev.example.com",
        )
        with (
            patch(
                "data_processor.main.build_s3_client", return_value=MagicMock(),
            ) as fake_build,
            patch("data_processor.main.S3PresignedStorage") as fake_storage_cls,
        ):
            from data_processor.main import _create_storage
            _create_storage()
        assert fake_build.call_count == 2
        _, kwargs = fake_storage_cls.call_args
        assert kwargs["public_client"] is not None

    def test_sem_client_publico_quando_igual_ao_interno(self, monkeypatch):
        from cnes_infra import config as infra_config
        monkeypatch.setattr(infra_config, "S3_ENDPOINT_URL", "http://minio:9000")
        monkeypatch.setattr(infra_config, "S3_PUBLIC_ENDPOINT_URL", "http://minio:9000")
        with (
            patch(
                "data_processor.main.build_s3_client", return_value=MagicMock(),
            ) as fake_build,
            patch("data_processor.main.S3PresignedStorage") as fake_storage_cls,
        ):
            from data_processor.main import _create_storage
            _create_storage()
        assert fake_build.call_count == 1
        _, kwargs = fake_storage_cls.call_args
        assert kwargs["public_client"] is None


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

        mock_engine = MagicMock()
        with (
            patch("data_processor.main._setup_logging"),
            patch("data_processor.main.init_telemetry"),
            patch("data_processor.main.create_engine", return_value=mock_engine),
            patch("data_processor.main._create_storage"),
            patch("data_processor.main.install_rls_listener") as mock_rls,
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
        mock_rls.assert_called_once_with(mock_engine)

    @pytest.mark.asyncio
    async def test_main_instala_rls_listener_antes_de_rodar_o_processor(
        self, tmp_path, monkeypatch,
    ):
        """B1: sem o listener, set_tenant_id() vira no-op e RLS bloqueia/vaza
        entre tenants. install_rls_listener() precisa rodar antes de
        run_processor() usar o engine."""
        import sys

        from cnes_infra import config as infra_config
        monkeypatch.delenv("PROFILE", raising=False)
        monkeypatch.setattr(infra_config, "LOGS_DIR", tmp_path)
        monkeypatch.setattr(
            infra_config, "LOG_FILE", tmp_path / "test.log",
        )
        monkeypatch.setattr(sys, "argv", ["data_processor"])

        calls = []
        mock_engine = MagicMock()

        async def _fake_run(*a, **kw):
            calls.append("run_processor")

        with (
            patch("data_processor.main._setup_logging"),
            patch("data_processor.main.init_telemetry"),
            patch("data_processor.main.create_engine", return_value=mock_engine),
            patch("data_processor.main._create_storage"),
            patch(
                "data_processor.main.install_rls_listener",
                side_effect=lambda _e: calls.append("install_rls_listener"),
            ),
            patch("data_processor.main.run_processor", side_effect=_fake_run),
        ):
            from data_processor.main import main
            await main()

        assert calls == ["install_rls_listener", "run_processor"]


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
    def test_audit_tick_registra_eventos_entregues(self, caplog):
        from types import SimpleNamespace

        from data_processor.main import _audit_tick

        with patch(
            "data_processor.main.dispatch_once",
            return_value=SimpleNamespace(delivered=1, failed=0),
        ) as dispatch:
            with caplog.at_level("INFO", logger="data_processor.main"):
                _audit_tick(cast("Any", object()), cast("Any", object()))

        dispatch.assert_called_once()
        assert "local_profile_audit_tick delivered=1 failed=0" in caplog.text

    @pytest.mark.asyncio
    async def test_poll_until_shutdown_executa_tick_de_auditoria(self):
        import asyncio

        from data_processor.main import _poll_until_shutdown

        audit_ticks = []

        class _Coordinator:
            def recover(self):
                shutdown.set()
                return ()

        def audit_tick():
            audit_ticks.append(1)

        shutdown = asyncio.Event()
        await _poll_until_shutdown(
            cast("Any", _Coordinator()), shutdown, interval=0.001, audit_tick=audit_tick
        )

        assert audit_ticks == [1]

    @pytest.mark.asyncio
    async def test_poll_until_shutdown_isola_falha_do_tick_de_auditoria(self, caplog):
        import asyncio

        from data_processor.main import _poll_until_shutdown

        audit_ticks = []

        class _Coordinator:
            def recover(self):
                return ()

        def audit_tick():
            audit_ticks.append(1)
            if len(audit_ticks) == 1:
                raise RuntimeError("sink_down")
            shutdown.set()

        shutdown = asyncio.Event()
        with caplog.at_level("ERROR", logger="data_processor.main"):
            await asyncio.wait_for(
                _poll_until_shutdown(
                    cast("Any", _Coordinator()), shutdown, interval=0.001, audit_tick=audit_tick
                ),
                timeout=2,
            )

        assert audit_ticks == [1, 1]
        assert "local_profile_audit_tick_error" in caplog.text

    @pytest.mark.asyncio
    async def test_recover_tick_loga_quantidade_de_runs_recuperados(self, caplog):
        from data_processor.main import _recover_tick

        class _Coordinator:
            def recover(self):
                return ("run-a", "run-b")

        with caplog.at_level("INFO", logger="data_processor.main"):
            await _recover_tick(cast("Any", _Coordinator()))

        assert "local_profile_recover_tick runs=2" in caplog.text

    @pytest.mark.asyncio
    async def test_recover_tick_absorve_excecao_e_continua(self, caplog):
        from data_processor.main import _recover_tick

        class _Coordinator:
            def recover(self):
                raise RuntimeError("sqlite_busy")

        with caplog.at_level("ERROR", logger="data_processor.main"):
            await _recover_tick(cast("Any", _Coordinator()))

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
            _poll_until_shutdown(cast("Any", _Coordinator()), shutdown, interval=0.001), timeout=2
        )

        assert len(ticks) == 2


def _control_plane_com_run_falho():
    from cnes_domain.control_plane.entities import Run, RunDependency
    from cnes_domain.control_plane.enums import RunState
    from cnes_domain.ports.control_plane import ControlPlanePort

    def run(run_id: str, state: RunState) -> Run:
        return Run(
            tenant_id="354130", run_id=run_id, competencia="2026-08", dataset_name="cnes",
            state=state, dependencies=(
                RunDependency(source_type="CNES_LOCAL", file_subtype="CNES_VINCULO", required=True),
            ), missing_sources=(), created_at=_NOW,
        )

    def get_run(tenant_id: str, run_id: str) -> Run:
        if run_id == "r1":
            raise ConnectionError("control_plane=unavailable")
        return run(run_id, RunState.PUBLISHED)

    control_plane = Mock(spec=ControlPlanePort)
    control_plane.list_recoverable_runs.return_value = (
        run("r1", RunState.PROCESSING), run("r2", RunState.PROCESSING),
    )
    control_plane.get_run.side_effect = get_run
    return control_plane


def _runtime_com_run_falho():
    from cnes_domain.ports.processing import (
        ExecutionCallbacks,
        ExecutionPolicyConfig,
        ProcessorExecutorPort,
    )
    from data_processor.composition import AwsProcessorServices, ProcessorRuntimeComponents
    from data_processor.orchestration.coordinator import (
        CoordinatorDependencies,
        PipelineCoordinator,
        allow_execution,
        noop_execution_started,
    )
    from data_processor.orchestration.publisher import DatasetPublisher
    from data_processor.recovery import ProcessorRecovery

    control_plane = _control_plane_com_run_falho()
    coordinator = PipelineCoordinator(
        CoordinatorDependencies(
            control_plane=control_plane, executor=Mock(spec=ProcessorExecutorPort),
            publisher=Mock(spec=DatasetPublisher), clock=lambda: _NOW,
        ),
        ExecutionPolicyConfig(2, 300, ExecutionCallbacks(allow_execution, noop_execution_started)),
    )
    runtime = ProcessorRuntimeComponents(
        control_plane=control_plane, object_store=Mock(), executor=Mock(), publisher=Mock(),
        source_registry=Mock(), stage_processor=Mock(), coordinator=coordinator,
        unit_worker=Mock(), unit_handler=Mock(),
        services=AwsProcessorServices(
            recovery=ProcessorRecovery(control_plane, coordinator, clock=lambda: _NOW),
            recovery_batch_size=10,
        ),
    )
    return runtime, control_plane


class TestMainProfileAws:
    @pytest.fixture(autouse=True)
    def _aws_profile(self, monkeypatch):
        import sys

        monkeypatch.setenv("PROFILE", "aws")
        monkeypatch.setattr(sys, "argv", ["data_processor", "recover-once"])

    @pytest.mark.asyncio
    async def test_main_aws_entrega_runtime_ao_entrypoint(self):
        import os

        from data_processor.composition import ProcessorRuntimeComponents

        expected = MagicMock(spec=ProcessorRuntimeComponents)
        with (
            patch("data_processor.main.configure_json_stdout"),
            patch("data_processor.main.init_telemetry"),
            patch("data_processor.main.Session") as session_cls,
            patch(
                "data_processor.main.build_processor_runtime", return_value=expected,
            ) as build,
            patch("data_processor.main.run_aws_entrypoint", return_value=0) as entrypoint,
        ):
            from data_processor.main import main
            rc = await main()

        assert rc == 0
        build.assert_called_once_with("aws", os.environ, session_cls.return_value)
        entrypoint.assert_called_once_with(expected, os.environ, ["recover-once"])

    @pytest.mark.asyncio
    async def test_main_aws_nao_instala_file_handler(self):
        from logging.handlers import RotatingFileHandler

        root = logging.getLogger()
        handlers_before = root.handlers[:]
        with (
            patch("data_processor.main.configure_json_stdout") as configure,
            patch("data_processor.main._setup_logging") as setup,
            patch("data_processor.main.init_telemetry"),
            patch("data_processor.main.Session"),
            patch("data_processor.main.build_processor_runtime"),
            patch("data_processor.main.run_aws_entrypoint", return_value=0),
            patch("data_processor.main.create_engine") as create_engine,
            patch("data_processor.main.run_processor") as run_processor,
        ):
            from data_processor.main import main
            await main()

        configure.assert_called_once_with("data-processor")
        setup.assert_not_called()
        create_engine.assert_not_called()
        run_processor.assert_not_called()
        assert root.handlers == handlers_before
        assert not any(isinstance(h, RotatingFileHandler) for h in root.handlers)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("failing", ["build_processor_runtime", "run_aws_entrypoint"])
    async def test_main_aws_falha_retorna_nao_zero_com_evento_json(self, failing, caplog):
        caplog.set_level(logging.ERROR, logger="data_processor.main")
        with (
            patch("data_processor.main.configure_json_stdout"),
            patch("data_processor.main.init_telemetry"),
            patch("data_processor.main.Session"),
            patch("data_processor.main.build_processor_runtime"),
            patch("data_processor.main.run_aws_entrypoint", return_value=0),
            patch(f"data_processor.main.{failing}", side_effect=RuntimeError("aws=down")),
        ):
            from data_processor.main import main
            rc = await main()

        assert rc == 1
        assert [record.getMessage() for record in caplog.records] == [
            "processor_entrypoint_failed",
        ]

    @pytest.mark.asyncio
    async def test_main_aws_recover_once_sai_1_quando_um_run_da_passada_falha(
        self, monkeypatch, caplog,
    ):
        from data_processor.aws_entrypoint import _ENVELOPE_NAMES
        from data_processor.main import main

        for name in _ENVELOPE_NAMES:
            monkeypatch.delenv(name, raising=False)
        runtime, control_plane = _runtime_com_run_falho()
        caplog.set_level(logging.INFO)
        with (
            patch("data_processor.main.configure_json_stdout"),
            patch("data_processor.main.init_telemetry"),
            patch("data_processor.main.Session"),
            patch("data_processor.main.build_processor_runtime", return_value=runtime),
        ):
            rc = await main()

        assert rc == 1
        assert [call.args for call in control_plane.get_run.call_args_list] == [
            ("354130", "r1"), ("354130", "r2"),
        ]
        events = [
            record.getMessage() for record in caplog.records
            if record.name in {"data_processor.recovery", "data_processor.main"}
        ]
        assert events == [
            "processor_recovery_scanned", "processor_execution_observed",
            "processor_recovery_completed", "processor_entrypoint_failed",
        ]
