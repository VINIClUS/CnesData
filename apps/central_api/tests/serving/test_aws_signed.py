"""Assinatura tenant-safe somente de objetos serving do pointer ativo."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError, NoCredentialsError

from central_api.services.serving_access import ServingUnavailable
from central_api.serving.aws_signed import (
    S3SignedServingAccess,
    ServingKeyForbidden,
    ServingSigningUnavailable,
    SignedServingRequest,
    SignedServingSettings,
)
from cnes_domain.ports.object_store import ObjectStat, ObjectStorePort
from cnes_domain.ports.serving import ServingAccessPort, ServingGrant, ServingRequest

NOW = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)
SERVING_KEY = "serving/tenant-a/run-01/overview.json"
SIGNED_URL = "https://data-bucket.s3.amazonaws.com/serving?X-Amz-Signature=abc"


def _grant() -> ServingGrant:
    return ServingGrant(
        tenant_id="tenant-a", run_id="run-01", version_id="run-01", object_keys=(SERVING_KEY,)
    )


def _buggy_grant(keys: tuple[str, ...], tenant_id: str = "tenant-a") -> ServingGrant:
    return ServingGrant.model_construct(
        tenant_id=tenant_id, run_id="run-01", version_id="run-01", object_keys=keys
    )


def _policy(grant: ServingGrant | None = None) -> Mock:
    policy = Mock(spec=ServingAccessPort)
    policy.authorize.return_value = grant or _grant()
    return policy


def _store(stat: ObjectStat | None = ObjectStat(SERVING_KEY, 10, "a" * 64)) -> Mock:
    store = Mock(spec=ObjectStorePort)
    store.stat.return_value = stat
    return store


def _signer() -> Mock:
    signer = Mock()
    signer.generate_presigned_url.return_value = SIGNED_URL
    return signer


def _access(
    policy: Mock | None = None, store: Mock | None = None, signer: Mock | None = None
) -> S3SignedServingAccess:
    return S3SignedServingAccess(
        policy or _policy(),
        store or _store(),
        signer or _signer(),
        SignedServingSettings(bucket="data-bucket", ttl_seconds=300),
    )


def _request(relative_name: str = "overview.json") -> SignedServingRequest:
    access = ServingRequest(user_id="user-a", tenant_id="tenant-a", dataset_name="cnes")
    return SignedServingRequest(access=access, relative_name=relative_name)


def _assert_nada_lido(store: Mock, signer: Mock) -> None:
    store.stat.assert_not_called()
    store.open.assert_not_called()
    signer.generate_presigned_url.assert_not_called()


def test_assina_somente_objeto_serving_do_pointer_ativo() -> None:
    policy, store, signer = _policy(), _store(), _signer()

    grant = _access(policy, store, signer).grant(_request(), NOW)

    policy.authorize.assert_called_once_with(
        ServingRequest(user_id="user-a", tenant_id="tenant-a", dataset_name="cnes")
    )
    store.stat.assert_called_once_with(SERVING_KEY)
    signer.generate_presigned_url.assert_called_once_with(
        "get_object",
        Params={
            "Bucket": "data-bucket",
            "Key": SERVING_KEY,
            "ResponseContentType": "application/json",
        },
        ExpiresIn=300,
    )
    assert grant.object_key == SERVING_KEY
    assert (grant.version_id, grant.run_id, grant.url) == ("run-01", "run-01", SIGNED_URL)
    assert grant.expires_at == NOW + timedelta(seconds=300)


@pytest.mark.parametrize(
    "key",
    [
        "raw/tenant-a/cnes/2026-07/s1/data.parquet",
        "serving/tenant-b/run-01/overview.json",
        "serving/tenant-a/run-02/overview.json",
        "serving/tenant-a/run-01/../../raw/data",
    ],
)
def test_rejeita_key_fora_do_pointer_e_prefixo(key: str) -> None:
    store, signer = _store(), _signer()

    with pytest.raises(ServingKeyForbidden, match="serving_key_forbidden"):
        _access(_policy(_buggy_grant((key,))), store, signer).grant(_request(), NOW)

    _assert_nada_lido(store, signer)


@pytest.mark.parametrize(
    "relative_name",
    [
        "",
        "/overview.json",
        "a//overview.json",
        "./overview.json",
        "../run-02/overview.json",
        "../../tenant-b/run-01/overview.json",
        "other.json",
    ],
)
def test_rejeita_nome_relativo_hostil(relative_name: str) -> None:
    store, signer = _store(), _signer()

    with pytest.raises(ServingKeyForbidden) as error:
        _access(store=store, signer=signer).grant(_request(relative_name), NOW)

    assert error.value.code == "serving_key_forbidden"
    _assert_nada_lido(store, signer)


def test_rejeita_grant_de_outro_tenant() -> None:
    other = _buggy_grant(("serving/tenant-b/run-01/overview.json",), tenant_id="tenant-b")
    store, signer = _store(), _signer()

    with pytest.raises(ServingKeyForbidden, match="serving_key_forbidden"):
        _access(_policy(other), store, signer).grant(_request(), NOW)

    _assert_nada_lido(store, signer)


@pytest.mark.parametrize(
    "code", ["membership_denied", "active_pointer_missing", "active_version_missing"]
)
def test_erro_da_policy_propaga_sem_stat_nem_assinatura(code: str) -> None:
    policy, store, signer = _policy(), _store(), _signer()
    policy.authorize.side_effect = ServingUnavailable(code)

    with pytest.raises(ServingUnavailable) as error:
        _access(policy, store, signer).grant(_request(), NOW)

    assert error.value.code == code
    _assert_nada_lido(store, signer)


def test_objeto_ausente_gera_indisponivel() -> None:
    signer = _signer()

    with pytest.raises(ServingSigningUnavailable) as error:
        _access(store=_store(stat=None), signer=signer).grant(_request(), NOW)

    assert error.value.code == "serving_object_missing"
    signer.generate_presigned_url.assert_not_called()


@pytest.mark.parametrize(
    "failure",
    [
        NoCredentialsError(),
        ClientError({"Error": {"Code": "AccessDenied", "Message": "x"}}, "GetObject"),
    ],
)
def test_falha_de_assinatura_gera_indisponivel(failure: Exception) -> None:
    signer = _signer()
    signer.generate_presigned_url.side_effect = failure

    with pytest.raises(ServingSigningUnavailable) as error:
        _access(signer=signer).grant(_request(), NOW)

    assert error.value.code == "serving_signing_failed"
    assert error.value.__cause__ is failure


@pytest.mark.parametrize(
    "failure",
    [
        EndpointConnectionError(endpoint_url="https://s3.amazonaws.com"),
        ClientError({"Error": {"Code": "AccessDenied", "Message": "x"}}, "GetObject"),
        OSError("disk"),
    ],
)
def test_falha_na_consulta_do_objeto_gera_indisponivel(failure: Exception) -> None:
    store, signer = _store(), _signer()
    store.stat.side_effect = failure

    with pytest.raises(ServingSigningUnavailable) as error:
        _access(store=store, signer=signer).grant(_request(), NOW)

    assert error.value.code == "serving_object_lookup_failed"
    assert error.value.__cause__ is failure
    signer.generate_presigned_url.assert_not_called()
