"""Adapters públicos de armazenamento de objetos."""

from cnes_infra.object_store.filesystem import FilesystemObjectStore
from cnes_infra.object_store.s3 import S3ObjectStore, S3Retention

__all__ = ("FilesystemObjectStore", "S3ObjectStore", "S3Retention")
