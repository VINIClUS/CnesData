"""Assina URLs somente para objetos serving autorizados pelo pointer ativo."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from botocore.exceptions import BotoCoreError, ClientError

if TYPE_CHECKING:
    from botocore.client import BaseClient

    from cnes_domain.ports.object_store import ObjectStorePort
    from cnes_domain.ports.serving import ServingAccessPort, ServingGrant, ServingRequest


class ServingKeyForbidden(Exception):
    """Chave solicitada fora do grant ativo."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class ServingSigningUnavailable(Exception):
    """Objeto ausente ou assinatura indisponível."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class SignedServingRequest:
    access: ServingRequest
    relative_name: str


@dataclass(frozen=True, slots=True)
class SignedServingSettings:
    bucket: str
    ttl_seconds: int


@dataclass(frozen=True, slots=True)
class SignedServingGrant:
    version_id: str
    run_id: str
    object_key: str
    url: str
    expires_at: datetime


def _resolve_key(authorized: ServingGrant, relative_name: str) -> str:
    segments = relative_name.split("/")
    if relative_name.startswith("/") or any(s in {"", ".", ".."} for s in segments):
        raise ServingKeyForbidden("serving_key_forbidden")
    key = f"serving/{authorized.tenant_id}/{authorized.run_id}/{relative_name}"
    if key not in authorized.object_keys:
        raise ServingKeyForbidden("serving_key_forbidden")
    return key


class S3SignedServingAccess:
    def __init__(
        self,
        access_policy: ServingAccessPort,
        object_store: ObjectStorePort,
        signer: BaseClient,
        settings: SignedServingSettings,
    ) -> None:
        self._access_policy = access_policy
        self._object_store = object_store
        self._signer = signer
        self._settings = settings

    def grant(self, request: SignedServingRequest, now: datetime) -> SignedServingGrant:
        """Autoriza pela policy estável e assina a chave exata do grant ativo.

        Args: Requisição com identidade e nome relativo; instante de emissão.
        Returns: URL assinada com expiração ``now + ttl``.
        Raises: ServingKeyForbidden, ServingSigningUnavailable, erros da policy.
        """
        authorized = self._access_policy.authorize(request.access)
        if authorized.tenant_id != request.access.tenant_id:
            raise ServingKeyForbidden("serving_key_forbidden")
        key = _resolve_key(authorized, request.relative_name)
        if self._object_store.stat(key) is None:
            raise ServingSigningUnavailable("serving_object_missing")
        return SignedServingGrant(
            version_id=authorized.version_id,
            run_id=authorized.run_id,
            object_key=key,
            url=self._sign(key),
            expires_at=now + timedelta(seconds=self._settings.ttl_seconds),
        )

    def _sign(self, key: str) -> str:
        try:
            return self._signer.generate_presigned_url(
                "get_object",
                Params={
                    "Bucket": self._settings.bucket,
                    "Key": key,
                    "ResponseContentType": "application/json",
                },
                ExpiresIn=self._settings.ttl_seconds,
            )
        except (BotoCoreError, ClientError) as error:
            raise ServingSigningUnavailable("serving_signing_failed") from error
