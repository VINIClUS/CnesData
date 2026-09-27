"""Logs operacionais JSON por linha em stdout, com redação de campos sensíveis."""

from __future__ import annotations

import json
import logging
import sys
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from typing import TextIO

_REDACTED = "[REDACTED]"
_REDACTED_FIELDS = frozenset({
    "authorization",
    "token",
    "signed_url",
    "aws_access_key_id",
    "aws_secret_access_key",
    "email",
})
_BASE_FIELDS = frozenset({"timestamp", "level", "service", "logger", "event"})
_RESERVED = frozenset(logging.makeLogRecord({}).__dict__) | {"message", "asctime"} | _BASE_FIELDS


def _is_sensitive(key: object) -> bool:
    return isinstance(key, str) and key.lower() in _REDACTED_FIELDS


def _sanitize(value: Any) -> Any:
    if isinstance(value, BaseException):
        return type(value).__name__
    if isinstance(value, Mapping):
        return {
            key: _REDACTED if _is_sensitive(key) else _sanitize(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_sanitize(item) for item in value]
    return value


def _safe_message(record: logging.LogRecord) -> str:
    message = str(_sanitize(record.msg))
    args = record.args
    if isinstance(args, Mapping):
        return message % _sanitize(args)
    if args:
        return message % tuple(_sanitize(value) for value in args)
    return message


def _safe_extra(record: logging.LogRecord) -> dict[str, Any]:
    return _sanitize({
        key: value for key, value in record.__dict__.items() if key not in _RESERVED
    })


class JsonLogFormatter(logging.Formatter):
    """Serializa um ``LogRecord`` como um objeto JSON de uma linha."""

    def __init__(self, service_name: str) -> None:
        super().__init__()
        self._service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.fromtimestamp(record.created, tz=UTC)
        event: dict[str, Any] = {
            "timestamp": timestamp.isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            "service": self._service_name,
            "logger": record.name,
            "event": _safe_message(record),
        }
        event.update(_safe_extra(record))
        if record.exc_info and record.exc_info[0] is not None:
            event["exception_type"] = record.exc_info[0].__name__
        return json.dumps(event, ensure_ascii=False, separators=(",", ":"), default=str)


def configure_json_stdout(service_name: str, stream: TextIO | None = None) -> None:
    """Substitui todos os handlers do root por um único handler JSON.

    Args:
        service_name: Nome do serviço emitido em cada evento.
        stream: Destino dos eventos; ``sys.stdout`` quando omitido.
    """
    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setFormatter(JsonLogFormatter(service_name))
    root = logging.getLogger()
    for previous in root.handlers[:]:
        previous.close()
    root.handlers[:] = [handler]
    root.setLevel(logging.INFO)
