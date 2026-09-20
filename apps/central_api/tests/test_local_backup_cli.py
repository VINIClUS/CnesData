"""TDD do CLI de backup/restore local (main/_build_parser/_cli_*)."""
from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

import pytest

from central_api.local_backup import main
from cnes_domain.profiles import parse_profile

if TYPE_CHECKING:
    from pathlib import Path

_TENANT = "354130"


def _env(data_dir: Path) -> dict[str, str]:
    return {"TENANT_ID": _TENANT, "DATA_DIR": str(data_dir)}


def _seed_state_db(data_dir: Path) -> None:
    settings = parse_profile(_env(data_dir))
    settings.state_db.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(settings.state_db)
    connection.execute("CREATE TABLE tenants (tenant_id TEXT PRIMARY KEY, data TEXT)")
    connection.execute("INSERT INTO tenants VALUES (?, ?)", (_TENANT, "{}"))
    connection.commit()
    connection.close()


def test_main_create_grava_backup_no_target(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _seed_state_db(data_dir)
    target = tmp_path / "b1.tar"

    exit_code = main(["create", "--target", str(target)], env=_env(data_dir))

    assert exit_code == 0
    assert target.exists()


def test_main_restore_recompoe_state_db(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _seed_state_db(data_dir)
    target = tmp_path / "b1.tar"
    main(["create", "--target", str(target)], env=_env(data_dir))

    restore_data_dir = tmp_path / "restored"
    exit_code = main(["restore", "--archive", str(target)], env=_env(restore_data_dir))

    assert exit_code == 0
    settings = parse_profile(_env(restore_data_dir))
    assert settings.state_db.exists()


def test_main_exige_subcomando(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        main([], env=_env(tmp_path / "data"))
