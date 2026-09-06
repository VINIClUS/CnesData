"""Adapters públicos de auditoria."""

from cnes_infra.audit.local_sink import LocalAuditSink
from cnes_infra.audit.s3_object_lock_sink import S3ObjectLockAuditSink

__all__ = ("LocalAuditSink", "S3ObjectLockAuditSink")
