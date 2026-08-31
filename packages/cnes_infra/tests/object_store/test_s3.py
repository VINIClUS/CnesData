"""Conformidade do armazenamento de objetos no S3."""

from base64 import b64encode
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from io import BytesIO
from typing import Any

import boto3
import pytest
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.stub import ANY, Stubber
from moto import mock_aws

from cnes_domain.control_plane.errors import Conflict
from cnes_infra.object_store.s3 import S3ObjectStore, S3Retention
from packages.cnes_infra.tests.contracts.clock import MutableClock
from packages.cnes_infra.tests.contracts.object_store_contract import (
    ObjectStoreCase,
    object_store_cases,
)


def _client() -> Any:
    return boto3.client(
        "s3",
        region_name="us-east-1",
        config=Config(signature_version=UNSIGNED),
    )


def _put_params(key: str, digest: str) -> dict[str, Any]:
    return {
        "Body": ANY,
        "Bucket": "bucket",
        "IfNoneMatch": "*",
        "Key": key,
        "Metadata": {"sha256": digest},
    }


def _object_response(body: bytes, metadata_sha256: str | None) -> dict[str, Any]:
    metadata = {} if metadata_sha256 is None else {"sha256": metadata_sha256}
    return {"Body": BytesIO(body), "ContentLength": len(body), "Metadata": metadata}


def _retention_params(body: bytes, retain_until: datetime) -> dict[str, Any]:
    digest = sha256(body).hexdigest()
    return {
        **_put_params("locked/objeto", digest),
        "ChecksumAlgorithm": "SHA256",
        "ChecksumSHA256": b64encode(bytes.fromhex(digest)).decode(),
        "ObjectLockMode": "COMPLIANCE",
        "ObjectLockRetainUntilDate": retain_until,
    }


def _stub_retention_replay(
    stubber: Stubber, body: bytes, retain_until: datetime, response: dict[str, Any]
) -> None:
    digest = sha256(body).hexdigest()
    stubber.add_client_error(
        "put_object",
        service_error_code="PreconditionFailed",
        http_status_code=412,
        expected_params=_retention_params(body, retain_until),
    )
    stubber.add_response(
        "get_object",
        _object_response(body, digest),
        {"Bucket": "bucket", "Key": "locked/objeto"},
    )
    stubber.add_response(
        "get_object_retention",
        response,
        {"Bucket": "bucket", "Key": "locked/objeto"},
    )


@mock_aws
@pytest.mark.parametrize("case", object_store_cases(), ids=lambda case: case.name)
def test_cumpre_contrato_compartilhado(case: ObjectStoreCase) -> None:
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="contract-bucket")
    clock = MutableClock(datetime(2026, 7, 15, tzinfo=UTC))

    case.run(S3ObjectStore(client, "contract-bucket"), clock)


def test_repete_put_condicional_limitado_apos_409_sem_destino() -> None:
    body = b"conteudo"
    digest = sha256(body).hexdigest()
    client = _client()
    params = _put_params("raw/objeto", digest)

    with Stubber(client) as stubber:
        stubber.add_client_error(
            "put_object",
            service_error_code="ConditionalRequestConflict",
            http_status_code=409,
            expected_params=params,
        )
        stubber.add_client_error(
            "get_object",
            service_error_code="NoSuchKey",
            http_status_code=404,
            expected_params={"Bucket": "bucket", "Key": "raw/objeto"},
        )
        stubber.add_response("put_object", {}, params)

        stat = S3ObjectStore(client, "bucket").put("raw/objeto", BytesIO(body), digest)

    assert (stat.size_bytes, stat.sha256) == (len(body), digest)


def test_interrompe_retry_condicional_apos_tres_respostas_409() -> None:
    body = b"conteudo"
    digest = sha256(body).hexdigest()
    client = _client()
    params = _put_params("raw/objeto", digest)

    with Stubber(client) as stubber:
        for _ in range(3):
            stubber.add_client_error(
                "put_object",
                service_error_code="ConditionalRequestConflict",
                http_status_code=409,
                expected_params=params,
            )
            stubber.add_client_error(
                "get_object",
                service_error_code="NoSuchKey",
                http_status_code=404,
                expected_params={"Bucket": "bucket", "Key": "raw/objeto"},
            )

        with pytest.raises(Conflict, match="conditional_request=conflict"):
            S3ObjectStore(client, "bucket").put("raw/objeto", BytesIO(body), digest)


def test_propaga_erro_put_nao_condicional_e_preserva_prefixo() -> None:
    body = b"conteudo"
    digest = sha256(body).hexdigest()
    client = _client()
    params = _put_params("tenant/raw/objeto", digest)

    with Stubber(client) as stubber:
        stubber.add_client_error(
            "put_object",
            service_error_code="AccessDenied",
            http_status_code=403,
            expected_params=params,
        )

        with pytest.raises(ClientError, match="AccessDenied"):
            S3ObjectStore(client, "bucket", prefix="tenant").put(
                "raw/objeto", BytesIO(body), digest
            )


def test_propaga_erro_get_diferente_de_nao_encontrado() -> None:
    client = _client()

    with Stubber(client) as stubber:
        stubber.add_client_error(
            "get_object",
            service_error_code="AccessDenied",
            http_status_code=403,
            expected_params={"Bucket": "bucket", "Key": "raw/objeto"},
        )

        with pytest.raises(ClientError, match="AccessDenied"):
            S3ObjectStore(client, "bucket").stat("raw/objeto")


def test_rejeita_sha256_local_antes_de_acessar_s3() -> None:
    client = _client()

    with Stubber(client):
        with pytest.raises(ValueError, match="sha256=mismatch"):
            S3ObjectStore(client, "bucket").put(
                "raw/objeto", BytesIO(b"conteudo"), sha256(b"outro").hexdigest()
            )


def test_promote_faz_get_e_put_condicional_sem_copy() -> None:
    body = b"promovido"
    digest = sha256(body).hexdigest()
    client = _client()

    with Stubber(client) as stubber:
        stubber.add_response(
            "get_object",
            _object_response(body, digest),
            {"Bucket": "bucket", "Key": "staging/objeto"},
        )
        stubber.add_response("put_object", {}, _put_params("raw/objeto", digest))

        stat = S3ObjectStore(client, "bucket").promote("staging/objeto", "raw/objeto", digest)

    assert (stat.size_bytes, stat.sha256) == (len(body), digest)


def test_aceita_412_quando_metadata_e_corpo_confirmam_destino() -> None:
    body = b"conteudo"
    digest = sha256(body).hexdigest()
    client = _client()
    params = _put_params("raw/objeto", digest)

    with Stubber(client) as stubber:
        stubber.add_client_error(
            "put_object",
            service_error_code="PreconditionFailed",
            http_status_code=412,
            expected_params=params,
        )
        stubber.add_response(
            "get_object",
            _object_response(body, digest),
            {"Bucket": "bucket", "Key": "raw/objeto"},
        )

        stat = S3ObjectStore(client, "bucket").put("raw/objeto", BytesIO(body), digest)

    assert (stat.size_bytes, stat.sha256) == (len(body), digest)


@pytest.mark.parametrize("metadata_sha256", [None, "0" * 64], ids=["ausente", "divergente"])
def test_rejeita_412_quando_metadata_nao_confirma_destino(
    metadata_sha256: str | None,
) -> None:
    body = b"conteudo"
    digest = sha256(body).hexdigest()
    client = _client()
    params = _put_params("raw/objeto", digest)

    with Stubber(client) as stubber:
        stubber.add_client_error(
            "put_object",
            service_error_code="PreconditionFailed",
            http_status_code=412,
            expected_params=params,
        )
        stubber.add_response(
            "get_object",
            _object_response(body, metadata_sha256),
            {"Bucket": "bucket", "Key": "raw/objeto"},
        )

        with pytest.raises(Conflict, match="object=immutable"):
            S3ObjectStore(client, "bucket").put("raw/objeto", BytesIO(body), digest)


def test_envia_retencao_e_sha256_explicito_e_valida_resposta() -> None:
    body = b"conteudo-retido"
    digest = sha256(body).hexdigest()
    checksum = b64encode(bytes.fromhex(digest)).decode()
    retain_until = datetime(2036, 1, 1, tzinfo=UTC)
    client = _client()

    with Stubber(client) as stubber:
        stubber.add_response(
            "put_object",
            {"ChecksumSHA256": checksum},
            _retention_params(body, retain_until),
        )
        adapter = S3ObjectStore(
            client,
            "bucket",
            retention=S3Retention(mode="COMPLIANCE", retain_until=retain_until),
        )

        stat = adapter.put("locked/objeto", BytesIO(body), digest)

    assert (stat.size_bytes, stat.sha256) == (len(body), digest)


@pytest.mark.parametrize("extra_days", [0, 1], ids=["igual", "maior"])
def test_aceita_replay_412_quando_retencao_existente_satisfaz_pedido(
    extra_days: int,
) -> None:
    body = b"conteudo-retido"
    digest = sha256(body).hexdigest()
    retain_until = datetime(2036, 1, 1, tzinfo=UTC)
    response = {
        "Retention": {
            "Mode": "COMPLIANCE",
            "RetainUntilDate": retain_until + timedelta(days=extra_days),
        }
    }
    client = _client()

    with Stubber(client) as stubber:
        _stub_retention_replay(stubber, body, retain_until, response)
        adapter = S3ObjectStore(
            client,
            "bucket",
            retention=S3Retention(mode="COMPLIANCE", retain_until=retain_until),
        )
        stat = adapter.put("locked/objeto", BytesIO(body), digest)
        stubber.assert_no_pending_responses()

    assert (stat.size_bytes, stat.sha256) == (len(body), digest)


@pytest.mark.parametrize(
    "response",
    [
        {},
        {
            "Retention": {
                "Mode": "COMPLIANCE",
                "RetainUntilDate": datetime(2035, 12, 31, tzinfo=UTC),
            }
        },
        {
            "Retention": {
                "Mode": "GOVERNANCE",
                "RetainUntilDate": datetime(2036, 1, 1, tzinfo=UTC),
            }
        },
    ],
    ids=["ausente", "menor", "modo-divergente"],
)
def test_rejeita_replay_412_quando_retencao_existente_nao_satisfaz_pedido(
    response: dict[str, Any],
) -> None:
    body = b"conteudo-retido"
    digest = sha256(body).hexdigest()
    retain_until = datetime(2036, 1, 1, tzinfo=UTC)
    client = _client()

    with Stubber(client) as stubber:
        _stub_retention_replay(stubber, body, retain_until, response)
        adapter = S3ObjectStore(
            client,
            "bucket",
            retention=S3Retention(mode="COMPLIANCE", retain_until=retain_until),
        )
        with pytest.raises(Conflict, match="retention=insufficient"):
            adapter.put("locked/objeto", BytesIO(body), digest)
        stubber.assert_no_pending_responses()


@pytest.mark.parametrize(
    ("response", "message"),
    [({}, "checksum_response=missing"), ({"ChecksumSHA256": "AAAA"}, "checksum_response=mismatch")],
    ids=["ausente", "divergente"],
)
def test_rejeita_checksum_de_resposta_ausente_ou_divergente(
    response: dict[str, str], message: str
) -> None:
    body = b"conteudo-retido"
    digest = sha256(body).hexdigest()
    retain_until = datetime(2036, 1, 1, tzinfo=UTC)
    client = _client()

    with Stubber(client) as stubber:
        stubber.add_response("put_object", response, _retention_params(body, retain_until))
        adapter = S3ObjectStore(
            client,
            "bucket",
            retention=S3Retention(mode="COMPLIANCE", retain_until=retain_until),
        )

        with pytest.raises(ValueError, match=message):
            adapter.put("locked/objeto", BytesIO(body), digest)


def test_rejeita_baddigest_do_s3() -> None:
    body = b"conteudo-retido"
    digest = sha256(body).hexdigest()
    retain_until = datetime(2036, 1, 1, tzinfo=UTC)
    client = _client()

    with Stubber(client) as stubber:
        stubber.add_client_error(
            "put_object",
            service_error_code="BadDigest",
            http_status_code=400,
            expected_params=_retention_params(body, retain_until),
        )
        adapter = S3ObjectStore(
            client,
            "bucket",
            retention=S3Retention(mode="COMPLIANCE", retain_until=retain_until),
        )

        with pytest.raises(ValueError, match="checksum=rejected"):
            adapter.put("locked/objeto", BytesIO(body), digest)


@pytest.mark.skip(reason="retention_enforcement_requires_real_aws")
def test_enforcement_de_retencao_requer_aws_real() -> None:
    raise AssertionError("real_aws=required")
