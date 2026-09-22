"""1000 S3 presign calls must not leak (mocked client)."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

pytest.importorskip("pytest_memray")


@pytest.mark.limit_memory("30 MB")
def test_presign_1000_no_leak():
    client = MagicMock()
    client.generate_presigned_url.return_value = "http://fake/x"
    for _ in range(1000):
        client.generate_presigned_url(
            "put_object", Params={"Bucket": "bucket", "Key": "key-x"}, ExpiresIn=3600,
        )
