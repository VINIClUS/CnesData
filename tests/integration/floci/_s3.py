"""Buckets efêmeros com Object Lock e limpeza que espera retenções COMPLIANCE curtas vencerem."""
from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any

from tests.integration.floci._floci import REGION

_MAX_COMPLIANCE_WAIT_SECONDS = 60


def create_bucket(s3: Any, name: str, *, object_lock: bool = True) -> None:
    """Cria o bucket na região do cenário, com Object Lock por padrão."""
    options = {"ObjectLockEnabledForBucket": True} if object_lock else {}
    s3.create_bucket(
        Bucket=name, CreateBucketConfiguration={"LocationConstraint": REGION}, **options,
    )


def delete_bucket(s3: Any, name: str) -> None:
    """Libera legal holds, espera COMPLIANCE curto vencer e remove versões e o bucket.

    Raises:
        RuntimeError: retenção COMPLIANCE além da espera máxima da limpeza.
    """
    versions, markers = _listing(s3, name)
    released = [_release(s3, name, *version) for version in versions]
    deadlines = [deadline for deadline in released if deadline is not None]
    if deadlines:
        _wait_until(max(deadlines), name)
    for key, version_id in [*versions, *markers]:
        s3.delete_object(
            Bucket=name, Key=key, VersionId=version_id, BypassGovernanceRetention=True,
        )
    s3.delete_bucket(Bucket=name)


def _listing(s3: Any, name: str) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    versions: list[tuple[str, str]] = []
    markers: list[tuple[str, str]] = []
    for page in s3.get_paginator("list_object_versions").paginate(Bucket=name):
        versions += [(item["Key"], item["VersionId"]) for item in page.get("Versions", ())]
        markers += [(item["Key"], item["VersionId"]) for item in page.get("DeleteMarkers", ())]
    return versions, markers


def _release(s3: Any, name: str, key: str, version_id: str) -> datetime | None:
    head = s3.head_object(Bucket=name, Key=key, VersionId=version_id)
    if head.get("ObjectLockLegalHoldStatus") == "ON":
        s3.put_object_legal_hold(
            Bucket=name, Key=key, VersionId=version_id, LegalHold={"Status": "OFF"},
        )
    if head.get("ObjectLockMode") == "COMPLIANCE":
        return head["ObjectLockRetainUntilDate"]
    return None


def _wait_until(deadline: datetime, name: str) -> None:
    remaining = (deadline - datetime.now(UTC)).total_seconds() + 1
    if remaining > _MAX_COMPLIANCE_WAIT_SECONDS:
        raise RuntimeError(f"cleanup=compliance_retention_too_long bucket={name}")
    if remaining > 0:
        time.sleep(remaining)
