"""Observabilidade — tracer OpenTelemetry para spans do pipeline."""

from opentelemetry import trace

tracer = trace.get_tracer("cnesdata")
