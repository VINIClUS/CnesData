"""Script do probe executado de verdade em diretórios temporários, sem rede nem AWS."""

# ruff: noqa: S108, S603
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

from tests.integration.aws_capability._fargate_probe import parse_probe_line, probe_script

BODY_KEYS = {"uid", "user", "root_write", "tempfile", "/tmp", "/logs", "/data"}
MOUNT_KEYS = {"mode", "uid", "gid", "write"}
WRITE = re.compile(r"ok|E[A-Z0-9]+")
TEMPFILE = re.compile(r"ok:(/tmp|/logs|/data|other)|[A-Za-z]+Error")


def _mounts(base: Path, tmp_mode: int, other_mode: int) -> dict[str, Path]:
    paths = {"tmp": base / "tmp", "logs": base / "logs", "data": base / "data"}
    for name, path in paths.items():
        path.mkdir()
        path.chmod(tmp_mode if name == "tmp" else other_mode)
    return paths


@pytest.fixture
def real_processes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.undo()


def _execute(paths: dict[str, Path]) -> dict[str, object]:
    script = probe_script(str(paths["tmp"]), str(paths["logs"]), str(paths["data"]))
    environment = {"PATH": os.environ.get("PATH", ""), "TMPDIR": str(paths["tmp"])}
    done = subprocess.run(
        [sys.executable, "-I", "-c", script],
        env=environment, capture_output=True, text=True, timeout=30, check=False,
    )
    assert done.returncode == 0, done.stderr[-300:]
    return parse_probe_line(done.stdout)


def _assert_closed(body: dict[str, object]) -> None:
    assert set(body) == BODY_KEYS
    assert body["uid"] == os.getuid()
    assert isinstance(body["user"], str)
    assert WRITE.fullmatch(str(body["root_write"]))
    assert TEMPFILE.fullmatch(str(body["tempfile"]))
    for logical in ("/tmp", "/logs", "/data"):
        entry = body[logical]
        assert isinstance(entry, dict)
        assert set(entry) == MOUNT_KEYS
        assert re.fullmatch(r"0o[0-7]{3,4}", str(entry["mode"]))
        assert isinstance(entry["uid"], int)
        assert isinstance(entry["gid"], int)
        assert WRITE.fullmatch(str(entry["write"]))


@pytest.mark.usefixtures("real_processes")
def test_script_em_diretorios_gravaveis_reporta_ok_modo_dono_e_tempfile_em_tmp(
    tmp_path: Path,
) -> None:
    paths = _mounts(tmp_path, 0o1777, 0o755)
    body = _execute(paths)
    _assert_closed(body)
    assert body["/tmp"] == {
        "mode": "0o1777", "uid": os.getuid(), "gid": os.getgid(), "write": "ok",
    }
    assert body["/logs"] == {
        "mode": "0o755", "uid": os.getuid(), "gid": os.getgid(), "write": "ok",
    }
    assert body["tempfile"] == "ok:/tmp"
    assert list(paths["tmp"].iterdir()) == []
    assert list(paths["logs"].iterdir()) == []
    assert list(paths["data"].iterdir()) == []


@pytest.mark.usefixtures("real_processes")
@pytest.mark.skipif(os.getuid() == 0, reason="root ignora permissões de diretório")
def test_script_em_diretorios_somente_leitura_reporta_eacces_e_nao_cria_arquivos(
    tmp_path: Path,
) -> None:
    paths = _mounts(tmp_path, 0o555, 0o555)
    try:
        body = _execute(paths)
    finally:
        for path in paths.values():
            path.chmod(0o755)
    _assert_closed(body)
    for logical in ("/tmp", "/logs", "/data"):
        entry = body[logical]
        assert isinstance(entry, dict)
        assert entry["write"] == "EACCES"
        assert entry["mode"] == "0o555"
    assert body["tempfile"] != "ok:/tmp"
    assert all(list(path.iterdir()) == [] for path in paths.values())
