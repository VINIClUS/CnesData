"""Logs JSON estruturados e redigidos em stdout."""

from __future__ import annotations

import json
import logging
import re
from datetime import date
from io import StringIO
from typing import TYPE_CHECKING

import pytest

from cnes_infra.observability.json_logging import JsonLogFormatter, configure_json_stdout

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

logger = logging.getLogger("cnes.test.json")

_SENSITIVE = [
    "authorization",
    "token",
    "signed_url",
    "aws_access_key_id",
    "aws_secret_access_key",
    "email",
]


@pytest.fixture(autouse=True)
def _restaura_root() -> Iterator[None]:
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    yield
    root.handlers[:] = handlers
    root.setLevel(level)


def _linhas(stream: StringIO) -> list[dict[str, object]]:
    return [json.loads(line) for line in stream.getvalue().splitlines()]


def test_emite_um_json_por_linha_com_contexto() -> None:
    stream = StringIO()
    configure_json_stdout("central-api", stream)

    logger.info("serving_access_granted", extra={"tenant_id": "tenant-a", "run_id": "run-01"})
    logger.warning("serving_access_denied")

    first, second = _linhas(stream)
    assert first["service"] == "central-api"
    assert first["event"] == "serving_access_granted"
    assert first["tenant_id"] == "tenant-a"
    assert first["run_id"] == "run-01"
    assert second["level"] == "WARNING"


def test_usa_stdout_por_padrao(capsys: pytest.CaptureFixture[str]) -> None:
    configure_json_stdout("central-api")

    logger.info("stdout_default")

    assert json.loads(capsys.readouterr().out)["event"] == "stdout_default"


@pytest.mark.parametrize("field", _SENSITIVE)
def test_remove_campos_sensiveis(field: str) -> None:
    record = logging.makeLogRecord({"msg": "denied", field: "secret-value"})

    rendered = JsonLogFormatter("central-api").format(record)

    assert "secret-value" not in rendered
    assert json.loads(rendered)[field] == "[REDACTED]"


def test_emite_timestamp_utc_rfc3339_e_metadados() -> None:
    record = logging.makeLogRecord(
        {"msg": "ok", "name": "cnes.x", "levelname": "INFO", "created": 0.5}
    )

    event = json.loads(JsonLogFormatter("worker").format(record))

    assert event["timestamp"] == "1970-01-01T00:00:00.500000Z"
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z", event["timestamp"])
    assert (event["level"], event["logger"], event["service"]) == ("INFO", "cnes.x", "worker")


def test_excecao_sai_so_com_tipo_sem_stack_nem_locals() -> None:
    stream = StringIO()
    configure_json_stdout("worker", stream)
    local_value = "local-secret-value"

    try:
        raise RuntimeError("message-secret-value")
    except RuntimeError:
        logger.exception("falhou")

    output = stream.getvalue()
    assert local_value not in output
    assert "message-secret-value" not in output
    assert "Traceback" not in output
    assert _linhas(stream)[0]["exception_type"] == "RuntimeError"


def test_ignora_exc_info_vazio() -> None:
    record = logging.makeLogRecord({"msg": "ok", "exc_info": (None, None, None)})

    event = json.loads(JsonLogFormatter("worker").format(record))

    assert "exception_type" not in event


def test_extra_nao_sobrescreve_campos_base_e_serializa_objetos() -> None:
    record = logging.makeLogRecord(
        {"msg": "ok", "service": "forjado", "level": "DEBUG", "competencia": date(2026, 7, 1)}
    )

    event = json.loads(JsonLogFormatter("worker").format(record))

    assert event["service"] == "worker"
    assert event["level"] == record.levelname
    assert event["competencia"] == "2026-07-01"


def test_substitui_handlers_existentes_de_forma_idempotente(tmp_path: Path) -> None:
    root = logging.getLogger()
    file_handler = logging.FileHandler(tmp_path / "app.log")
    root.addHandler(file_handler)
    stream = StringIO()

    configure_json_stdout("worker", stream)
    configure_json_stdout("worker", stream)

    assert len(root.handlers) == 1
    assert not isinstance(root.handlers[0], logging.FileHandler)
    assert file_handler.stream is None
    assert root.level == logging.INFO
    logger.info("uma_vez")
    assert len(_linhas(stream)) == 1
