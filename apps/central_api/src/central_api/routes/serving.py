"""Rota BFF que autoriza e entrega documentos serving do Run ativo."""

from __future__ import annotations

import re
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Annotated

from fastapi import APIRouter, Depends, HTTPException, Response
from fastapi.responses import RedirectResponse, StreamingResponse

from central_api.services.serving_access import ServingUnavailable
from central_api.serving.aws_signed import (
    ServingKeyForbidden,
    ServingSigningUnavailable,
    SignedServingRequest,
)
from cnes_domain.ports.object_store import ObjectStorePort  # noqa: TC001
from cnes_domain.ports.serving import ServingAccessPort, ServingRequest

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from datetime import datetime

    from central_api.serving.aws_signed import S3SignedServingAccess
    from cnes_domain.ports.serving import ServingGrant

router = APIRouter(prefix="/api/v1/dashboard/serving", tags=["dashboard-serving"])

_SAFE_SEGMENT = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_CHUNK_SIZE = 1024 * 1024


@dataclass(frozen=True, slots=True)
class ServingPrincipal:
    tenant_id: str
    user_id: str


@dataclass(frozen=True, slots=True)
class _DocumentPath:
    dataset_name: str
    document_name: str


type ServingDelivery = Callable[[ServingPrincipal, _DocumentPath], Response]


def get_serving_principal() -> ServingPrincipal:
    """Falha fechado até a composição fornecer a identidade da sessão."""

    raise HTTPException(status_code=401, detail="auth_required")


def get_serving_access() -> ServingAccessPort:
    """Falha fechado até a composição fornecer o serviço de autorização."""

    raise HTTPException(status_code=503, detail="serving_access_not_configured")


def get_serving_object_store() -> ObjectStorePort:
    """Falha fechado até a composição fornecer o object store."""

    raise HTTPException(status_code=503, detail="object_store_not_configured")


def get_serving_delivery(
    access: Annotated[ServingAccessPort, Depends(get_serving_access)],
    store: Annotated[ObjectStorePort, Depends(get_serving_object_store)],
) -> ServingDelivery:
    """Entrega local: transmite o documento concedido pelo próprio BFF."""

    return partial(_stream_document, access, store)


def signed_serving_delivery(
    signed: S3SignedServingAccess, clock: Callable[[], datetime]
) -> ServingDelivery:
    """Args: signed: serviço de GET assinado; clock: instante de emissão.
    Returns: Entrega aws por redirect 307 para a URL assinada.
    """
    return partial(_redirect_signed, signed, clock)


def _validated_document(dataset_name: str, document_name: str) -> _DocumentPath:
    if not _SAFE_SEGMENT.fullmatch(document_name):
        raise HTTPException(status_code=422, detail="document_name_invalid")
    return _DocumentPath(dataset_name=dataset_name, document_name=document_name)


def _stream(store: ObjectStorePort, key: str) -> Iterator[bytes]:
    with store.open(key) as source:
        while chunk := source.read(_CHUNK_SIZE):
            yield chunk


def _serving_request(principal: ServingPrincipal, path: _DocumentPath) -> ServingRequest:
    return ServingRequest(
        user_id=principal.user_id,
        tenant_id=principal.tenant_id,
        dataset_name=path.dataset_name,
    )


def _policy_error(error: ServingUnavailable) -> HTTPException:
    if error.code == "membership_denied":
        return HTTPException(status_code=403, detail="serving_forbidden")
    return HTTPException(status_code=503, detail="active_serving_unavailable")


def _authorize_or_raise(
    access: ServingAccessPort, principal: ServingPrincipal, path: _DocumentPath
) -> ServingGrant:
    try:
        return access.authorize(_serving_request(principal, path))
    except ServingUnavailable as error:
        raise _policy_error(error) from error


def _stream_document(
    access: ServingAccessPort,
    store: ObjectStorePort,
    principal: ServingPrincipal,
    path: _DocumentPath,
) -> StreamingResponse:
    grant = _authorize_or_raise(access, principal, path)
    key = f"serving/{grant.tenant_id}/{grant.run_id}/{path.document_name}.json"
    if key not in grant.object_keys:
        raise HTTPException(status_code=404, detail="serving_document_not_found")
    stat = store.stat(key)
    if stat is None:
        raise HTTPException(status_code=503, detail="active_serving_unavailable")
    headers = {
        "ETag": f'"{stat.sha256}"',
        "X-Dataset-Version": grant.version_id,
        "Cache-Control": "private, max-age=30",
    }
    return StreamingResponse(_stream(store, key), media_type="application/json", headers=headers)


def _redirect_signed(
    signed: S3SignedServingAccess,
    clock: Callable[[], datetime],
    principal: ServingPrincipal,
    path: _DocumentPath,
) -> RedirectResponse:
    request = SignedServingRequest(
        access=_serving_request(principal, path),
        relative_name=f"{path.document_name}.json",
    )
    try:
        grant = signed.grant(request, clock())
    except ServingUnavailable as error:
        raise _policy_error(error) from error
    except ServingKeyForbidden as error:
        raise HTTPException(status_code=404, detail="serving_document_not_found") from error
    except ServingSigningUnavailable as error:
        raise HTTPException(status_code=503, detail="active_serving_unavailable") from error
    headers = {"X-Dataset-Version": grant.version_id, "Cache-Control": "private, no-store"}
    return RedirectResponse(grant.url, status_code=307, headers=headers)


@router.get("/{dataset_name}/{document_name}")
def read_serving_document(
    path: Annotated[_DocumentPath, Depends(_validated_document)],
    principal: Annotated[ServingPrincipal, Depends(get_serving_principal)],
    deliver: Annotated[ServingDelivery, Depends(get_serving_delivery)],
) -> Response:
    """Transmite o documento serving concedido, sem fallback e sem URL assinada."""

    return deliver(principal, path)
