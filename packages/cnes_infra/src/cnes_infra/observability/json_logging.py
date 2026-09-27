"""Logs operacionais JSON por linha em stdout, com redação de campos sensíveis."""

from __future__ import annotations

import json
import logging
import re
import sys
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from typing import TextIO

_REDACTED = "[REDACTED]"
_TRUNCATED = "[TRUNCATED]"
_MAX_DEPTH = 16
_REDACTED_FIELDS = frozenset({
    "authorization",
    "token",
    "signed_url",
    "aws_access_key_id",
    "aws_secret_access_key",
    "email",
})
_BASE_FIELDS = frozenset({"timestamp", "level", "service", "logger", "event"})
_SPEC = re.compile(
    r"%(?:\([^)]*\))?[#0 +\-]*(?:\*|\d*)(?:\.(?:\*|\d*))?[hlL]?([diouxXeEfFgGcrsa%])"
)
_LABEL = re.compile(r"(\w+)\s*[=:]\s*[\"']?$")
_RESERVED = frozenset(logging.makeLogRecord({}).__dict__) | {"message", "asctime"} | _BASE_FIELDS


def _is_sensitive(key: str) -> bool:
    name = key.lower()
    return name in _REDACTED_FIELDS or name.rpartition("_")[2] in _REDACTED_FIELDS


def _sanitize(value: Any, depth: int = 0) -> Any:
    if depth > _MAX_DEPTH:
        return _TRUNCATED
    if isinstance(value, BaseException):
        return type(value).__name__
    if isinstance(value, Mapping):
        return _sanitize_mapping(value, depth)
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_sanitize(item, depth + 1) for item in value]
    return value


def _sanitize_mapping(value: Mapping[Any, Any], depth: int) -> dict[str, Any]:
    sanitized: dict[str, Any] = {}
    for key, item in value.items():
        name = str(_sanitize(key, depth + 1))
        sanitized[name] = _REDACTED if _is_sensitive(name) else _sanitize(item, depth + 1)
    return sanitized


def _safe_message(record: logging.LogRecord) -> str:
    template = str(_sanitize(record.msg))
    args = record.args
    if not args:
        return template
    if isinstance(args, Mapping) and "%(" in template:
        template, _ = _redact_labeled(template, ())
        values: Any = _sanitize(args)
    else:
        positional = args if isinstance(args, tuple) else (args,)
        template, kept = _redact_labeled(template, positional)
        values = tuple(_sanitize(value) for value in kept)
    try:
        return template % values
    except (KeyError, TypeError, ValueError):
        return template


def _redact_labeled(message: str, args: tuple[Any, ...]) -> tuple[str, list[Any]]:
    parts: list[str] = []
    kept: list[Any] = []
    cursor = 0
    values = iter(args)
    for spec in _SPEC.finditer(message):
        if spec.group(1) == "%":
            continue
        consumed = [next(values, None) for _ in range(spec.group(0).count("*") + 1)]
        label = _LABEL.search(message, cursor, spec.start())
        if label is not None and _is_sensitive(label.group(1)):
            parts.append(message[cursor:spec.start()] + _REDACTED)
            cursor = spec.end()
        else:
            kept.extend(consumed)
    return "".join(parts) + message[cursor:], kept


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
        event = self._base(record)
        try:
            event.update(_safe_extra(record))
            event["event"] = _safe_message(record)
            if record.exc_info and record.exc_info[0] is not None:
                event["exception_type"] = record.exc_info[0].__name__
            return json.dumps(event, ensure_ascii=False, separators=(",", ":"), default=str)
        except Exception as error:
            # Raising here makes logging print the raw, unredacted record args to stderr.
            failed = self._base(record) | {
                "event": "log_format_failed",
                "error_type": type(error).__name__,
            }
            return json.dumps(failed, separators=(",", ":"))

    def _base(self, record: logging.LogRecord) -> dict[str, Any]:
        timestamp = datetime.fromtimestamp(record.created, tz=UTC)
        return {
            "timestamp": timestamp.isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            "service": self._service_name,
            "logger": record.name,
            "event": "",
        }


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
