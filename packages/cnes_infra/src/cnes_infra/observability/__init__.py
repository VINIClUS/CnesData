"""Logs operacionais JSON em stdout."""

from cnes_infra.observability.json_logging import JsonLogFormatter, configure_json_stdout

__all__ = ("JsonLogFormatter", "configure_json_stdout")
