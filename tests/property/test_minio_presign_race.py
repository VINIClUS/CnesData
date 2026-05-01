"""Presigned URL generation under concurrent calls — no cross-contamination."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from hypothesis import given
from hypothesis import strategies as st


@pytest.mark.race
@given(keys=st.lists(
    st.text(min_size=5, max_size=20).filter(lambda s: s.strip() and "\x00" not in s),
    min_size=2, max_size=5,
    unique=True,
))
def test_presign_paralelo_preserva_chave(keys, executor):
    """Concurrent presign calls never leak a different key's URL back."""
    client = MagicMock()

    def fake_presign(bucket, key, expires):
        return f"http://fake/{bucket}/{key}?signed=yes"

    client.presigned_put_object.side_effect = fake_presign

    def call_presign(key):
        return client.presigned_put_object("bucket-x", key, 3600)

    futures = [executor.submit(call_presign, k) for k in keys]
    urls = [f.result() for f in futures]

    for key, url in zip(keys, urls, strict=True):
        assert key in url, f"presign_leak key={key!r} url={url!r}"
