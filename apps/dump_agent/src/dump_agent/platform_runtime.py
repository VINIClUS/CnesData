"""Single boundary for platform-specific detection and APIs.

All Windows/frozen/POSIX specifics are encapsulated here. Callers
(main.py, worker/*) stay portable.
"""
from __future__ import annotations

import logging
import shutil  # noqa: F401
import sys  # noqa: F401
import threading
import uuid  # noqa: F401
from collections.abc import Callable  # noqa: TC003
from contextlib import AbstractContextManager  # noqa: F401
from pathlib import Path  # noqa: TC003

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_temp_dirs: set[Path] = set()
_on_stop_callback: Callable[[], None] | None = None
