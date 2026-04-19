"""Config do batch_watcher — thresholds via env."""

import os

SIZE_THRESHOLD_MB: int = int(
    os.getenv("WATCHER_SIZE_THRESHOLD_MB", "100"),
)
AGE_THRESHOLD_DAYS: int = int(
    os.getenv("WATCHER_AGE_THRESHOLD_DAYS", "2"),
)
