"""Adapters públicos de auditoria."""

from cnes_governance.local_sink import LocalAuditSink
from cnes_governance.s3_object_lock_sink import S3ObjectLockAuditSink

__all__ = ("LocalAuditSink", "S3ObjectLockAuditSink")
