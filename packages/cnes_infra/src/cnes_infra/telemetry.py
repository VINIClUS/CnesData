"""OpenTelemetry bootstrap — configure once at process startup."""

import logging
import os

logger = logging.getLogger(__name__)

_initialized = False


def init_telemetry(service_name: str) -> None:
    global _initialized
    if _initialized:
        return

    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    if not endpoint:
        logger.info("otel_disabled OTEL_EXPORTER_OTLP_ENDPOINT not set")
        _initialized = True
        return

    try:
        from opentelemetry import trace  # pragma: no cover - otel[sdk] required
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (  # pragma: no cover
            OTLPSpanExporter,  # pragma: no cover
        )  # pragma: no cover
        from opentelemetry.sdk.resources import Resource  # pragma: no cover
        from opentelemetry.sdk.trace import TracerProvider  # pragma: no cover
        from opentelemetry.sdk.trace.export import BatchSpanProcessor  # pragma: no cover
    except ImportError:
        logger.warning("otel_not_installed install cnes-infra[otel]")
        _initialized = True
        return

    resource = Resource.create({"service.name": service_name})  # pragma: no cover
    provider = TracerProvider(resource=resource)  # pragma: no cover
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))  # pragma: no cover
    trace.set_tracer_provider(provider)  # pragma: no cover
    _initialized = True  # pragma: no cover
    logger.info("otel_initialized service=%s", service_name)  # pragma: no cover


def instrument_engine(engine: object) -> None:
    try:
        from opentelemetry.instrumentation.sqlalchemy import (
            SQLAlchemyInstrumentor,
        )
        SQLAlchemyInstrumentor().instrument(engine=engine)  # pragma: no cover
    except ImportError:
        pass


def get_tracer(name: str) -> object:
    try:
        from opentelemetry import trace
        return trace.get_tracer(name)  # pragma: no cover - otel required
    except ImportError:
        return _NoopTracer()


class _NoopSpan:
    def set_attribute(self, key: str, value: object) -> None:
        pass

    def __enter__(self) -> "_NoopSpan":
        return self

    def __exit__(self, *args: object) -> None:
        pass


class _NoopTracer:
    def start_as_current_span(
        self, name: str, **kwargs: object,
    ) -> _NoopSpan:
        return _NoopSpan()
