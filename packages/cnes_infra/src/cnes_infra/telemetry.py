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
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
            OTLPSpanExporter,
        )
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError:
        logger.warning("otel_not_installed install cnes-infra[otel]")
        _initialized = True
        return

    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)
    _initialized = True
    logger.info("otel_initialized service=%s", service_name)


def instrument_engine(engine: object) -> None:
    try:
        from opentelemetry.instrumentation.sqlalchemy import (
            SQLAlchemyInstrumentor,
        )
        SQLAlchemyInstrumentor().instrument(engine=engine)
    except ImportError:
        pass


def get_tracer(name: str) -> object:
    try:
        from opentelemetry import trace
        return trace.get_tracer(name)
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
