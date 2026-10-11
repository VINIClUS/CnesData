from opentelemetry.sdk.resources import Resource
from opentelemetry.trace import Tracer
from opentelemetry.trace import TracerProvider as _ApiTracerProvider
from opentelemetry.util.types import Attributes

class SpanProcessor: ...

class TracerProvider(_ApiTracerProvider):
    def __init__(self, resource: Resource | None = None) -> None: ...
    def add_span_processor(self, span_processor: SpanProcessor) -> None: ...
    def get_tracer(
        self,
        instrumenting_module_name: str,
        instrumenting_library_version: str | None = None,
        schema_url: str | None = None,
        attributes: Attributes | None = None,
    ) -> Tracer: ...
