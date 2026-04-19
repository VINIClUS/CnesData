"""Testes do módulo telemetry — branches de inicialização OTEL."""
import importlib
import sys
from unittest.mock import MagicMock, patch


def _reset_initialized():
    import cnes_infra.telemetry as tel
    tel._initialized = False


class TestInitTelemetry:

    def test_sem_endpoint_nao_inicializa_provider(self, monkeypatch):
        _reset_initialized()
        monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
        from cnes_infra.telemetry import init_telemetry
        init_telemetry("svc-teste")
        import cnes_infra.telemetry as tel
        assert tel._initialized is True

    def test_ja_inicializado_retorna_imediatamente(self):
        import cnes_infra.telemetry as tel
        tel._initialized = True
        from cnes_infra.telemetry import init_telemetry
        init_telemetry("svc-teste")
        assert tel._initialized is True
        _reset_initialized()

    def test_sem_pacote_otel_loga_warning(self, monkeypatch):
        _reset_initialized()
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
        with patch.dict(sys.modules, {
            "opentelemetry": None,
            "opentelemetry.trace": None,
            "opentelemetry.exporter": None,
            "opentelemetry.exporter.otlp": None,
            "opentelemetry.exporter.otlp.proto": None,
            "opentelemetry.exporter.otlp.proto.grpc": None,
            "opentelemetry.exporter.otlp.proto.grpc.trace_exporter": None,
            "opentelemetry.sdk": None,
            "opentelemetry.sdk.resources": None,
            "opentelemetry.sdk.trace": None,
            "opentelemetry.sdk.trace.export": None,
        }):
            _reset_initialized()
            from cnes_infra import telemetry
            importlib.reload(telemetry)
            telemetry.init_telemetry("svc-teste")
            assert telemetry._initialized is True
        _reset_initialized()


class TestInstrumentEngine:

    def test_sem_instrumentor_nao_levanta(self):
        from cnes_infra.telemetry import instrument_engine
        with patch.dict(sys.modules, {
            "opentelemetry.instrumentation.sqlalchemy": None,
        }):
            instrument_engine(MagicMock())


class TestGetTracer:

    def test_sem_otel_retorna_noop(self):
        with patch.dict(sys.modules, {"opentelemetry": None, "opentelemetry.trace": None}):
            from cnes_infra import telemetry
            importlib.reload(telemetry)
            tracer = telemetry.get_tracer("noop")
            assert isinstance(tracer, telemetry._NoopTracer)
        importlib.reload(telemetry)


class TestNoopSpan:

    def test_context_manager(self):
        from cnes_infra.telemetry import _NoopSpan
        span = _NoopSpan()
        span.set_attribute("key", "value")
        with span as s:
            assert s is span

    def test_exit_sem_excecao(self):
        from cnes_infra.telemetry import _NoopSpan
        span = _NoopSpan()
        span.__exit__(None, None, None)


class TestNoopTracer:

    def test_start_as_current_span_retorna_noop_span(self):
        from cnes_infra.telemetry import _NoopSpan, _NoopTracer
        tracer = _NoopTracer()
        result = tracer.start_as_current_span("op")
        assert isinstance(result, _NoopSpan)
