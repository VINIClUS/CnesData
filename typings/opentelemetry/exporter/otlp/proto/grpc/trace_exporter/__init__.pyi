from opentelemetry.sdk.trace.export import SpanExporter

class OTLPSpanExporter(SpanExporter):
    def __init__(self, endpoint: str | None = None, insecure: bool | None = None) -> None: ...
