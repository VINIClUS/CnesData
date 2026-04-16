"""Single boundary for platform-specific detection and APIs.

All Windows/frozen/POSIX specifics are encapsulated here. Callers
(main.py, worker/*) stay portable.
"""
from __future__ import annotations

import logging
import os
import shutil
import sys
import threading
import uuid
from collections.abc import Callable  # noqa: TC003
from contextlib import AbstractContextManager  # noqa: F401
from pathlib import Path

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_temp_dirs: set[Path] = set()
_on_stop_callback: Callable[[], None] | None = None


def is_frozen() -> bool:
    return bool(getattr(sys, "frozen", False))


def app_data_dir() -> Path:
    if sys.platform == "win32":
        base = os.environ.get("LOCALAPPDATA")
        if not base:
            base = str(Path.home() / "AppData" / "Local")
        path = Path(base) / "CnesAgent"
    else:
        base = os.environ.get("XDG_STATE_HOME")
        if not base:
            base = str(Path.home() / ".local" / "state")
        path = Path(base) / "cnes-agent"
    path.mkdir(parents=True, exist_ok=True)
    return path


def logs_dir() -> Path:
    override = os.environ.get("DUMP_LOGS_DIR")
    path = Path(override) if override else app_data_dir() / "logs"
    path.mkdir(parents=True, exist_ok=True)
    return path


def register_temp_dir(path: Path) -> None:
    with _lock:
        _temp_dirs.add(path)


def unregister_temp_dir(path: Path) -> None:
    with _lock:
        _temp_dirs.discard(path)


def resolve_machine_id() -> str:
    env_value = os.environ.get("MACHINE_ID")
    if env_value:
        return env_value
    store = app_data_dir() / "machine_id"
    if store.exists():
        existing = store.read_text(encoding="utf-8").strip()
        if existing:
            return existing
    new_id = uuid.uuid4().hex[:8]
    store.write_text(new_id, encoding="utf-8")
    return new_id


def fbclient_dll_path() -> Path:
    env_value = os.environ.get("FIREBIRD_DLL")
    if env_value:
        env_path = Path(env_value)
        if not env_path.exists():
            raise FileNotFoundError(f"fbclient_not_found path={env_path}")
        return env_path

    if is_frozen():
        meipass = getattr(sys, "_MEIPASS", None)
        if meipass:
            candidate = Path(meipass) / "fbclient.dll"
            if candidate.exists():
                return candidate
        exe_sibling = Path(sys.executable).parent / "fbclient.dll"
        if exe_sibling.exists():
            return exe_sibling
        raise FileNotFoundError("fbclient_not_found_in_frozen_bundle")

    if sys.platform == "win32":
        raise FileNotFoundError(
            "fbclient_windows_requires_FIREBIRD_DLL_env",
        )

    from ctypes import util

    found = util.find_library("fbclient")
    if found:
        candidate = Path(found)
        if candidate.exists():
            return candidate

    for fallback in (
        Path("/usr/lib/x86_64-linux-gnu/libfbclient.so.2"),
        Path("/usr/lib/libfbclient.so.2"),
        Path("/usr/local/lib/libfbclient.so.2"),
    ):
        if fallback.exists():
            return fallback

    raise FileNotFoundError("fbclient_not_found_on_linux")


if sys.platform != "win32":
    import signal as _signal

    def _posix_handler(signum: int, _frame: object) -> None:
        kind = _signal.Signals(signum).name
        logger.warning("shutdown_signal kind=%s", kind)
        with _lock:
            cb = _on_stop_callback
            dirs = list(_temp_dirs)
        if cb is not None:
            try:
                cb()
            except Exception:
                logger.exception("on_stop_error")
        for path in dirs:
            shutil.rmtree(path, ignore_errors=True)

    def _install_posix_handler(on_stop: Callable[[], None]) -> None:
        global _on_stop_callback
        with _lock:
            _on_stop_callback = on_stop
        _signal.signal(_signal.SIGTERM, _posix_handler)
        _signal.signal(_signal.SIGINT, _posix_handler)
