"""Autoriza e concede acesso somente aos objetos serving do Run ativo."""

from __future__ import annotations

from hashlib import sha256
from typing import TYPE_CHECKING

from pydantic import ValidationError

from cnes_contracts.manifests.outputs import OutputManifest, RunManifest
from cnes_domain.ports.serving import ServingGrant, ServingRequest

if TYPE_CHECKING:
    from cnes_domain.control_plane.entities import DatasetVersion
    from cnes_domain.ports.control_plane import ControlPlanePort
    from cnes_domain.ports.object_store import ObjectStorePort


class ServingUnavailable(RuntimeError):
    """Falha estável de autorização/disponibilidade de serving."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _require_membership(control_plane: ControlPlanePort, request: ServingRequest) -> None:
    found = control_plane.get_membership(request.tenant_id, request.user_id)
    if found is None:
        raise ServingUnavailable("membership_denied")
    if (found.tenant_id, found.user_id) != (request.tenant_id, request.user_id):
        raise ServingUnavailable("membership_denied")


def _resolve_active_version(
    control_plane: ControlPlanePort, request: ServingRequest
) -> DatasetVersion:
    pointer = control_plane.get_dataset_pointer(request.tenant_id, request.dataset_name)
    if pointer is None or pointer.pointer_name != "current":
        raise ServingUnavailable("active_pointer_missing")
    if (pointer.tenant_id, pointer.dataset_name) != (request.tenant_id, request.dataset_name):
        raise ServingUnavailable("pointer_identity_mismatch")
    version = control_plane.get_dataset_version(
        request.tenant_id, request.dataset_name, pointer.version_id
    )
    if version is None:
        raise ServingUnavailable("active_version_missing")
    if (version.tenant_id, version.dataset_name) != (request.tenant_id, request.dataset_name):
        raise ServingUnavailable("version_identity_mismatch")
    if version.version_id != version.run_id:
        raise ServingUnavailable("version_run_mismatch")
    return version


def _load_run_manifest(object_store: ObjectStorePort, version: DatasetVersion) -> RunManifest:
    key = version.run_manifest_key
    stat = object_store.stat(key)
    if stat is None or stat.key != key:
        raise ServingUnavailable("run_manifest_missing")
    with object_store.open(key) as stream:
        body = stream.read()
    if sha256(body).hexdigest() != stat.sha256:
        raise ServingUnavailable("run_manifest_hash_mismatch")
    try:
        return RunManifest.model_validate_json(body)
    except ValidationError as error:
        raise ServingUnavailable("run_manifest_invalid") from error


def _validate_manifest_identity(
    manifest: RunManifest, request: ServingRequest, version: DatasetVersion
) -> None:
    expected = (version.tenant_id, version.dataset_name, version.run_id)
    actual = (manifest.tenant_id, manifest.dataset_name, manifest.run_id)
    if actual != expected or request.tenant_id != version.tenant_id:
        raise ServingUnavailable("manifest_identity_mismatch")


def _collect_serving_keys(
    object_store: ObjectStorePort, manifest: RunManifest
) -> tuple[str, ...]:
    keys: list[str] = []
    for item in manifest.outputs:
        if item.layer != "serving":
            continue
        keys.append(_require_serving_object(object_store, item))
    return tuple(keys)


def _require_serving_object(object_store: ObjectStorePort, item: OutputManifest) -> str:
    stat = object_store.stat(item.object_key)
    if stat is None or stat.sha256 != item.object_sha256:
        raise ServingUnavailable("serving_object_unavailable")
    return item.object_key


def _build_grant(version: DatasetVersion, keys: tuple[str, ...]) -> ServingGrant:
    try:
        return ServingGrant(
            tenant_id=version.tenant_id,
            run_id=version.run_id,
            version_id=version.version_id,
            object_keys=keys,
        )
    except ValidationError as error:
        raise ServingUnavailable("serving_outputs_missing") from error


class LocalServingAccess:
    def __init__(
        self, control_plane: ControlPlanePort, object_store: ObjectStorePort
    ) -> None:
        self._control_plane = control_plane
        self._object_store = object_store

    def authorize(self, request: ServingRequest) -> ServingGrant:
        _require_membership(self._control_plane, request)
        version = _resolve_active_version(self._control_plane, request)
        manifest = _load_run_manifest(self._object_store, version)
        _validate_manifest_identity(manifest, request, version)
        keys = _collect_serving_keys(self._object_store, manifest)
        return _build_grant(version, keys)
