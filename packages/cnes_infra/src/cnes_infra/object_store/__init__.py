"""Adapters públicos de armazenamento de objetos."""

import os

if os.name == "nt":  # pragma: no cover - Windows smoke
    from cnes_infra.object_store.windows_filesystem import (
        WindowsFilesystemObjectStore as FilesystemObjectStore,
    )
else:
    from cnes_infra.object_store.filesystem import FilesystemObjectStore
from cnes_infra.object_store.s3 import S3ObjectStore, S3Retention

__all__ = ("FilesystemObjectStore", "S3ObjectStore", "S3Retention")
