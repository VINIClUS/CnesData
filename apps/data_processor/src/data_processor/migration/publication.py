"""Leitura verificada da publicacao do candidato: ponteiro, versao, manifest e saidas."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from cnes_contracts.manifests.outputs import OutputManifest, RunManifest
from data_processor.migration.report import sha256_hex

if TYPE_CHECKING:
    from cnes_domain.ports.control_plane import ControlPlanePort
    from cnes_domain.ports.object_store import ObjectStorePort


class ShadowRunError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class Expected:
    tenant_id: str
    dataset: str
    competencia: str
    run_id: str


@dataclass(frozen=True, slots=True)
class PublishedRun:
    version_id: str
    manifest_key: str
    manifest_sha256: str
    manifest: RunManifest
    outputs: dict[str, bytes]


def _read_checked(store: ObjectStorePort, output: OutputManifest) -> bytes:
    with store.open(output.object_key) as stream:
        data = stream.read()
    if sha256_hex(data) != output.object_sha256:
        raise ShadowRunError(f"output_sha256_mismatch key={output.object_key}")
    stat = store.stat(output.object_key)
    if stat is None or stat.sha256 != output.object_sha256:
        raise ShadowRunError(f"output_stat_mismatch key={output.object_key}")
    return data


def read_verified_outputs(
    store: ObjectStorePort, manifest: RunManifest, stored: bytes, serving: tuple[str, ...]
) -> dict[str, bytes]:
    """Valida o RunManifest publicado e le cada saida conferindo o hash.

    Args: store: object store. manifest: interpretado. stored: bytes gravados. serving: catalogo.
    Returns: bytes de cada saida por chave de objeto.
    Raises: ShadowRunError: manifest nao canonico, serving fora do catalogo ou hash divergente.
    """
    if manifest.model_dump_json(exclude_none=False, by_alias=False).encode() != stored:
        raise ShadowRunError(f"manifest_not_canonical run_id={manifest.run_id}")
    expected = {f"serving/{manifest.tenant_id}/{manifest.run_id}/{name}.json" for name in serving}
    actual = {item.object_key for item in manifest.outputs if item.layer == "serving"}
    if actual != expected:
        raise ShadowRunError(
            f"serving_keys_mismatch run_id={manifest.run_id} "
            f"missing={sorted(expected - actual)} extra={sorted(actual - expected)}"
        )
    return {item.object_key: _read_checked(store, item) for item in manifest.outputs}


def _verify_identity(manifest: RunManifest, expected: Expected) -> None:
    pairs = (
        ("tenant_id", manifest.tenant_id, expected.tenant_id),
        ("dataset_name", manifest.dataset_name, expected.dataset),
        ("competencia", manifest.competencia, expected.competencia),
        ("run_id", manifest.run_id, expected.run_id),
    )
    for name, actual, wanted in pairs:
        if actual != wanted:
            raise ShadowRunError(
                f"manifest_identity_mismatch field={name} expected={wanted} actual={actual}"
            )


def _publication_mismatch(expected: Expected) -> ShadowRunError:
    return ShadowRunError(
        f"publication_mismatch dataset={expected.dataset} run_id={expected.run_id}"
    )


def read_published(
    reader: ControlPlanePort,
    store: ObjectStorePort,
    expected: Expected,
    serving: tuple[str, ...],
) -> PublishedRun:
    """Le a publicacao pelo ponteiro ativo e confere identidade, hashes e catalogo.

    Args: reader: ponteiro e versao. store: object store. expected: identidade do job.
        serving: documentos de serving do catalogo.
    Returns: run publicado, com o version_id do ponteiro e os bytes de cada saida.
    Raises: ShadowRunError: ponteiro, versao ou manifest com identidade diferente do job, ou
        bytes do manifest divergentes do stat do object store.
    """
    pointer = reader.get_dataset_pointer(expected.tenant_id, expected.dataset)
    version = (
        None if pointer is None
        else reader.get_dataset_version(expected.tenant_id, expected.dataset, pointer.version_id)
    )
    if pointer is None or version is None:
        raise _publication_mismatch(expected)
    if {pointer.version_id, version.run_id} != {expected.run_id}:
        raise _publication_mismatch(expected)
    with store.open(version.run_manifest_key) as stream:
        stored = stream.read()
    stat = store.stat(version.run_manifest_key)
    if stat is None or (stat.sha256, stat.size_bytes) != (sha256_hex(stored), len(stored)):
        raise ShadowRunError(f"manifest_stat_mismatch key={version.run_manifest_key}")
    manifest = RunManifest.model_validate_json(stored)
    _verify_identity(manifest, expected)
    outputs = read_verified_outputs(store, manifest, stored, serving)
    return PublishedRun(
        pointer.version_id, version.run_manifest_key, sha256_hex(stored), manifest, outputs
    )
