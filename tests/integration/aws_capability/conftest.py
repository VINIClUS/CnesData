"""Fixtures offline; rede e subprocessos reais são proibidos neste ciclo."""

import socket
import subprocess
from pathlib import Path

import pytest

from tests.integration.aws_capability._testing import Lab, make_lab


def _forbid(*args: object, **kwargs: object) -> None:
    pytest.fail("boundary=real_io_forbidden")


@pytest.fixture(autouse=True)
def offline(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "dummy")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "dummy")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "dummy")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setattr(subprocess, "run", _forbid)
    monkeypatch.setattr(subprocess, "Popen", _forbid)
    monkeypatch.setattr(socket.socket, "connect", _forbid)
    monkeypatch.setattr(socket.socket, "connect_ex", _forbid)


@pytest.fixture
def lab(tmp_path: Path) -> Lab:
    return make_lab(tmp_path / "private")
