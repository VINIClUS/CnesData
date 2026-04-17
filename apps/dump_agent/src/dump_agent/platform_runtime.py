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
from contextlib import AbstractContextManager  # noqa: TC003
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
        try:
            if cb is not None:
                try:
                    cb()
                except Exception:
                    logger.exception("on_stop_error")
        finally:
            for path in dirs:
                shutil.rmtree(path, ignore_errors=True)

    def _install_posix_handler(on_stop: Callable[[], None]) -> None:
        global _on_stop_callback
        with _lock:
            _on_stop_callback = on_stop
        _signal.signal(_signal.SIGTERM, _posix_handler)
        _signal.signal(_signal.SIGINT, _posix_handler)

    import fcntl as _fcntl

    class _PosixFileLock:
        def __init__(self, name: str) -> None:
            self._path = app_data_dir() / f"{name}.lock"
            self._fd = self._path.open("w", encoding="utf-8")
            try:
                _fcntl.flock(self._fd, _fcntl.LOCK_EX | _fcntl.LOCK_NB)
            except BlockingIOError as err:
                self._fd.close()
                raise RuntimeError(
                    f"already_running lock={name}",
                ) from err

        def __enter__(self) -> None:
            return None

        def __exit__(self, *exc: object) -> None:
            try:
                _fcntl.flock(self._fd, _fcntl.LOCK_UN)
            finally:
                self._fd.close()


if sys.platform == "win32":
    import ctypes
    from ctypes import wintypes

    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

    _HANDLER_ROUTINE = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.DWORD)
    _kernel32.SetConsoleCtrlHandler.argtypes = [
        _HANDLER_ROUTINE, wintypes.BOOL,
    ]
    _kernel32.SetConsoleCtrlHandler.restype = wintypes.BOOL

    _CTRL_CODES = {
        0: "CTRL_C",
        1: "CTRL_BREAK",
        2: "CTRL_CLOSE",
        5: "CTRL_LOGOFF",
        6: "CTRL_SHUTDOWN",
    }

    _handler_ref: _HANDLER_ROUTINE | None = None

    def _windows_handler(ctrl_type: int) -> bool:
        name = _CTRL_CODES.get(ctrl_type, f"UNKNOWN_{ctrl_type}")
        logger.warning("shutdown_signal kind=%s", name)
        with _lock:
            cb = _on_stop_callback
            dirs = list(_temp_dirs)
        try:
            if cb is not None:
                try:
                    cb()
                except Exception:
                    logger.exception("on_stop_error")
        finally:
            for path in dirs:
                shutil.rmtree(path, ignore_errors=True)
        return True

    def _install_windows_handler(on_stop: Callable[[], None]) -> None:
        global _on_stop_callback, _handler_ref
        with _lock:
            _on_stop_callback = on_stop
        _handler_ref = _HANDLER_ROUTINE(_windows_handler)
        if not _kernel32.SetConsoleCtrlHandler(_handler_ref, True):
            raise OSError(
                ctypes.get_last_error(),
                "SetConsoleCtrlHandler_failed",
            )

    _ERROR_ALREADY_EXISTS = 183

    _kernel32.CreateMutexW.argtypes = [
        ctypes.c_void_p, wintypes.BOOL, wintypes.LPCWSTR,
    ]
    _kernel32.CreateMutexW.restype = wintypes.HANDLE
    _kernel32.ReleaseMutex.argtypes = [wintypes.HANDLE]
    _kernel32.ReleaseMutex.restype = wintypes.BOOL
    _kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    _kernel32.CloseHandle.restype = wintypes.BOOL

    class _WindowsMutex:
        def __init__(self, name: str) -> None:
            self._handle = _kernel32.CreateMutexW(
                None, False, f"Global\\CnesAgent_{name}",
            )
            if not self._handle:
                raise OSError(
                    ctypes.get_last_error(),
                    "CreateMutexW_failed",
                )
            if ctypes.get_last_error() == _ERROR_ALREADY_EXISTS:
                _kernel32.CloseHandle(self._handle)
                self._handle = None
                raise RuntimeError(
                    f"already_running mutex={name}",
                )

        def __enter__(self) -> None:
            return None

        def __exit__(self, *exc: object) -> None:
            if self._handle:
                _kernel32.ReleaseMutex(self._handle)
                _kernel32.CloseHandle(self._handle)
                self._handle = None


def install_shutdown_handler(on_stop: Callable[[], None]) -> None:
    if sys.platform == "win32":
        _install_windows_handler(on_stop)
    else:
        _install_posix_handler(on_stop)


def acquire_single_instance_lock(
    name: str,
) -> AbstractContextManager[None]:
    if sys.platform == "win32":
        return _WindowsMutex(name)
    return _PosixFileLock(name)
