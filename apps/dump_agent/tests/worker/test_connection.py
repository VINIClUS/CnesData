"""Tests for worker.connection Firebird wiring."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


@patch("dump_agent.worker.connection.fdb")
@patch("dump_agent.worker.connection.fbclient_dll_path")
def test_conectar_firebird_usa_platform_runtime_dll_path(
    mock_dll, mock_fdb, monkeypatch,
):
    fake_dll = Path("/fake/fbclient.so")
    mock_dll.return_value = fake_dll
    monkeypatch.setenv("DB_PATH", "/var/db/cnes.gdb")
    monkeypatch.setenv("DB_PASSWORD", "pwd")
    mock_fdb.connect.return_value = MagicMock()

    from dump_agent.worker.connection import conectar_firebird
    conectar_firebird()

    mock_fdb.load_api.assert_called_once_with(str(fake_dll))
    mock_fdb.connect.assert_called_once()


@patch("dump_agent.worker.connection.fbclient_dll_path")
def test_conectar_firebird_propaga_file_not_found(mock_dll, monkeypatch):
    mock_dll.side_effect = FileNotFoundError("fbclient_missing")
    monkeypatch.setenv("DB_PATH", "/var/db/cnes.gdb")
    monkeypatch.setenv("DB_PASSWORD", "pwd")

    from dump_agent.worker.connection import conectar_firebird
    with pytest.raises(FileNotFoundError, match="fbclient_missing"):
        conectar_firebird()
