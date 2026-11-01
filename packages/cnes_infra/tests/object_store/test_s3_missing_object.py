"""Chave ausente no S3 vira None em qualquer codigo de erro de ausencia."""

from typing import Any

import boto3
import pytest
from botocore import UNSIGNED
from botocore.config import Config
from botocore.stub import Stubber

from cnes_infra.object_store.s3 import S3ObjectStore

MISSING_CODES = ["NoSuchKey", "404", "NotFound"]
KEYS = [
    "normalized/tenant/run/unidade.parquet",
    "tmp/attempt/objeto",
    "reconciliation/tenant/run/relatorio.json",
    "serving/tenant/run/saida.parquet",
]


def _client() -> Any:
    return boto3.client(
        "s3",
        region_name="us-east-1",
        config=Config(signature_version=UNSIGNED),
    )


@pytest.mark.parametrize("key", KEYS)
@pytest.mark.parametrize("code", MISSING_CODES)
def test_stat_de_chave_ausente_devolve_none(code: str, key: str) -> None:
    client = _client()
    with Stubber(client) as stubber:
        stubber.add_client_error(
            "get_object",
            service_error_code=code,
            http_status_code=404,
            expected_params={"Bucket": "bucket", "Key": key},
        )

        stat = S3ObjectStore(client, "bucket").stat(key)

        stubber.assert_no_pending_responses()
    assert stat is None, f"stat code={code} key={key}"
