"""Tests for scripts/compare_cnes_data_plane_cli.py."""
from __future__ import annotations

import hashlib
from io import BytesIO
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError


def _not_found_error() -> ClientError:
    return ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")


def _make_client() -> MagicMock:
    client = MagicMock()
    client.get_object.side_effect = _not_found_error()
    client.put_object.return_value = {}
    return client


@patch("cnes_infra.storage.s3_presigned.build_s3_client")
def test_shadow_store_isola_prefixo_por_run(mock_build_client):
    from scripts.compare_cnes_data_plane_cli import _shadow_store

    client_a = _make_client()
    client_b = _make_client()
    mock_build_client.side_effect = [client_a, client_b]
    body = b"conteudo"
    digest = hashlib.sha256(body).hexdigest()

    store_a = _shadow_store("compare-shadow-aaaaaaaa")
    store_b = _shadow_store("compare-shadow-bbbbbbbb")

    store_a.put("foo.txt", BytesIO(body), digest)
    store_b.put("foo.txt", BytesIO(body), digest)

    key_a = client_a.put_object.call_args.kwargs["Key"]
    key_b = client_b.put_object.call_args.kwargs["Key"]

    assert key_a == "compare-cnes-data-plane/compare-shadow-aaaaaaaa/foo.txt"
    assert key_b == "compare-cnes-data-plane/compare-shadow-bbbbbbbb/foo.txt"
    assert key_a != key_b
