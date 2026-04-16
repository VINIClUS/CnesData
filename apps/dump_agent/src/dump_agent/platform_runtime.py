"""Single boundary for platform-specific detection and APIs.

All Windows/frozen/POSIX specifics are encapsulated here. Callers
(main.py, worker/*) stay portable.
"""
from __future__ import annotations

import logging
import os
import shutil  # noqa: F401
import sys
import threading
import uuid  # noqa: F401
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
