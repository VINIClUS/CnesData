"""Rota BFF que autoriza e transmite documentos serving do Run ativo."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse

from central_api.services.serving_access import ServingUnavailable
from cnes_domain.ports.object_store import ObjectStorePort  # noqa: TC001
from cnes_domain.ports.serving import ServingAccessPort, ServingRequest

if TYPE_CHECKING:
    from collections.abc import Iterator

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


def get_serving_principal() -> ServingPrincipal:
    """Falha fechado até a composição fornecer a identidade da sessão."""

    raise HTTPException(status_code=401, detail="auth_required")


def get_serving_access() -> ServingAccessPort:
    """Falha fechado até a composição fornecer o serviço de autorização."""

    raise HTTPException(status_code=503, detail="serving_access_not_configured")


def get_serving_object_store() -> ObjectStorePort:
    """Falha fechado até a composição fornecer o object store."""

    raise HTTPException(status_code=503, detail="object_store_not_configured")


def _validated_document(dataset_name: str, document_name: str) -> _DocumentPath:
    if not _SAFE_SEGMENT.fullmatch(document_name):
        raise HTTPException(status_code=422, detail="document_name_invalid")
    return _DocumentPath(dataset_name=dataset_name, document_name=document_name)


def _stream(store: ObjectStorePort, key: str) -> Iterator[bytes]:
    with store.open(key) as source:
        while chunk := source.read(_CHUNK_SIZE):
            yield chunk


def _authorize_or_raise(
    access: ServingAccessPort, principal: ServingPrincipal, path: _DocumentPath
) -> ServingGrant:
    request = ServingRequest(
        user_id=principal.user_id,
        tenant_id=principal.tenant_id,
        dataset_name=path.dataset_name,
    )
    try:
        return access.authorize(request)
    except ServingUnavailable as error:
        if error.code == "membership_denied":
            raise HTTPException(status_code=403, detail="serving_forbidden") from error
        raise HTTPException(status_code=503, detail="active_serving_unavailable") from error


@router.get("/{dataset_name}/{document_name}")
def read_serving_document(
    path: Annotated[_DocumentPath, Depends(_validated_document)],
    principal: Annotated[ServingPrincipal, Depends(get_serving_principal)],
    access: Annotated[ServingAccessPort, Depends(get_serving_access)],
    store: Annotated[ObjectStorePort, Depends(get_serving_object_store)],
) -> StreamingResponse:
    """Transmite o documento serving concedido, sem fallback e sem URL assinada."""

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
