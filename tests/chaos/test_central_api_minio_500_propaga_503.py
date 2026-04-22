"""MinIO 500 during presign -> API responds 503 or propagates error cleanly."""
from __future__ import annotations

import pytest


@pytest.mark.chaos
def test_minio_500_nao_crasha_servidor(inject_minio_failure):
    """Invariant: MinIO 500 bubbled as clean exception, not server crash."""
    try:
        from minio import Minio
    except ImportError:
        pytest.skip("minio not installed")

    with inject_minio_failure(mode="get_500"):
        fake_secret = "y"  # noqa: S105 — test-only dummy
        client = Minio(
            "localhost:9000",
            access_key="x",
            secret_key=fake_secret,
            secure=False,
        )
        with pytest.raises(RuntimeError, match="500"):
            client.presigned_put_object("bucket", "key", 3600)
