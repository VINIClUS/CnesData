from datetime import UTC, datetime

import pytest

from cnes_infra.control_plane import sqlite_schema
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane, _SQLiteFilesystemError
from packages.cnes_infra.tests.contracts.clock import MutableClock


@pytest.fixture
def clock() -> MutableClock:
    return MutableClock(datetime(2026, 7, 15, 12, tzinfo=UTC))


@pytest.mark.parametrize("filesystem", [pytest.param("nfs"), pytest.param("cifs")])
def test_rejeita_banco_symlink_para_filesystem_de_rede(
    tmp_path, clock, monkeypatch, filesystem
) -> None:
    mount = tmp_path / "network"
    mount.mkdir()
    database_path = tmp_path / "local" / "control.sqlite3"
    database_path.parent.mkdir()
    database_path.symlink_to(mount / "control.sqlite3")

    def mounts(_path, **_kwargs) -> str:
        return f"server:/share {mount} {filesystem} rw 0 0\\n"

    monkeypatch.setattr(sqlite_schema.Path, "read_text", mounts)
    adapter = SQLiteControlPlane(database_path, clock.now)
    with pytest.raises(_SQLiteFilesystemError, match="sqlite_network_filesystem"):
        adapter.initialize()
