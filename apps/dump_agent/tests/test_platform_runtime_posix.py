"""Linux-only tests for platform_runtime POSIX branch."""
from __future__ import annotations

import os
import signal
import threading
import time
from pathlib import Path  # noqa: F401

import pytest

from dump_agent import platform_runtime

pytestmark = pytest.mark.linux_only


class TestInstallPosixHandler:
    def test_callback_chamado_em_sigterm(self):
        called = threading.Event()
        platform_runtime._temp_dirs.clear()
        platform_runtime._install_posix_handler(on_stop=called.set)
        os.kill(os.getpid(), signal.SIGTERM)
        assert called.wait(timeout=1.0)

    def test_callback_chamado_em_sigint(self):
        called = threading.Event()
        platform_runtime._temp_dirs.clear()
        platform_runtime._install_posix_handler(on_stop=called.set)
        os.kill(os.getpid(), signal.SIGINT)
        assert called.wait(timeout=1.0)

    def test_limpa_tempdirs_registrados(self, tmp_path):
        subdir = tmp_path / "dump_abc"
        subdir.mkdir()
        (subdir / "file.tmp").write_bytes(b"x")
        platform_runtime._temp_dirs.clear()
        platform_runtime.register_temp_dir(subdir)
        platform_runtime._install_posix_handler(on_stop=lambda: None)
        os.kill(os.getpid(), signal.SIGTERM)
        time.sleep(0.2)
        assert not subdir.exists()


class TestInstallShutdownHandler:
    def test_dispatcher_chama_posix_handler_no_linux(self):
        called = threading.Event()
        platform_runtime._temp_dirs.clear()
        platform_runtime.install_shutdown_handler(on_stop=called.set)
        os.kill(os.getpid(), signal.SIGTERM)
        assert called.wait(timeout=1.0)


class TestPosixFileLock:
    def test_bloqueia_segunda_aquisicao(self, tmp_path, monkeypatch):
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        with platform_runtime._PosixFileLock("test_lock"):
            with pytest.raises(RuntimeError, match="already_running"):
                with platform_runtime._PosixFileLock("test_lock"):
                    pass

    def test_libera_apos_exit(self, tmp_path, monkeypatch):
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        with platform_runtime._PosixFileLock("test_release"):
            pass
        with platform_runtime._PosixFileLock("test_release"):
            pass
