"""INSERT suprimido por trigger não representa colisão de refresh."""

import logging
import sqlite3
from pathlib import Path
from traceback import format_exception

import pytest

from cnes_domain.refresh_tokens import RefreshTokenUnavailable
from cnes_infra.auth.sqlite_refresh_tokens import SQLiteRefreshTokenStore

from .test_sqlite_refresh_token_failure_origins import (
    _CANARY,
    _PLAINTEXT,
    _SEED,
    _Database,
    _invoke,
    _SecretFactory,
    _Snapshot,
)

_TRIGGER = """
CREATE TRIGGER ignore_refresh BEFORE INSERT ON auth_refresh_tokens BEGIN
    UPDATE synthetic_refresh_failure_canary SET state = 'corrupt';
    SELECT RAISE(IGNORE);
END
"""


@pytest.fixture
def ignored_database(tmp_path: Path, caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.DEBUG)
    boundary = _Database(tmp_path / "ignored-insert.sqlite")
    boundary.prepare("ignored-insert")
    connection = sqlite3.connect(boundary.path)
    try:
        connection.execute(_TRIGGER)
        connection.commit()
    finally:
        connection.close()
    try:
        yield boundary
    finally:
        for connection in boundary.connections:
            connection.close()


@pytest.mark.parametrize("operation", ["create", "import"])
def test_insert_ignorado_sem_identidade_ocupada_vira_unavailable(
    ignored_database: _Database,
    operation: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    assert ignored_database.snapshot() == _Snapshot([_SEED], [(1, "original")])
    factory = _SecretFactory()
    store = SQLiteRefreshTokenStore(lambda: ignored_database.connect("ignored-insert"), factory)
    with pytest.raises(Exception) as caught:
        _invoke(store, operation)
    assert ignored_database.attempts == 1
    assert ignored_database.delivered == 1
    assert factory.calls == (1 if operation == "create" else 0)
    ignored_database.assert_closed("ignored-insert")
    assert ignored_database.snapshot() == _Snapshot([_SEED], [(1, "original")])
    assert isinstance(caught.value, RefreshTokenUnavailable)
    for sensitive in (_PLAINTEXT, _CANARY):
        assert sensitive not in str(caught.value)
        assert sensitive not in "".join(format_exception(caught.value))
        assert sensitive not in caplog.text
