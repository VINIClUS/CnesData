"""Leitura verificada da publicacao: ponteiro, identidade do manifest, hashes e catalogo."""
from __future__ import annotations

import json
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import UTC, datetime
from hashlib import sha256
from io import BytesIO
from typing import TYPE_CHECKING, cast

import pytest

from cnes_contracts.manifests.outputs import OutputManifest, RunManifest
from cnes_domain.control_plane.entities import DatasetPointer, DatasetVersion
from cnes_domain.ports.object_store import ObjectStat
from data_processor.migration.publication import (
    Expected,
    ShadowRunError,
    read_published,
    read_verified_outputs,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from contextlib import AbstractContextManager
    from typing import BinaryIO

    from cnes_domain.ports.control_plane import ControlPlanePort

_NOW = datetime(2026, 10, 10, 12, tzinfo=UTC)
_TENANT = "354130"
_RUN_ID = "r1"
_KEY = f"serving/{_TENANT}/{_RUN_ID}/overview.json"
_MANIFEST_KEY = f"reconciliation/{_TENANT}/2026-01/{_RUN_ID}/run-manifest.json"
_BODY = b'{"valor": 1}'
_BODY_SHA = sha256(_BODY).hexdigest()
_EXPECTED = Expected(_TENANT, "demo", "2026-01", _RUN_ID)


@dataclass
class _FakeStore:
    objects: dict[str, bytes]
    stat_sha: dict[str, str] = field(default_factory=dict[str, str])
    stat_size: dict[str, int] = field(default_factory=dict[str, int])

    def put(self, key: str, body: BinaryIO, expected_sha256: str) -> ObjectStat:
        raise AssertionError(f"put_unexpected key={key}")

    def open(self, key: str) -> AbstractContextManager[BinaryIO]:
        return nullcontext(BytesIO(self.objects[key]))

    def stat(self, key: str) -> ObjectStat | None:
        data = self.objects.get(key)
        if data is None:
            return None
        digest = self.stat_sha.get(key, sha256(data).hexdigest())
        return ObjectStat(key, self.stat_size.get(key, len(data)), digest)

    def promote(self, source_key: str, destination_key: str, expected_sha256: str) -> ObjectStat:
        raise AssertionError(f"promote_unexpected key={source_key}")

    def delete(self, key: str) -> None:
        raise AssertionError(f"delete_unexpected key={key}")


@dataclass
class _Reader:
    pointer: DatasetPointer | None
    version: DatasetVersion | None

    def get_dataset_pointer(self, tenant_id: str, dataset_name: str) -> DatasetPointer | None:
        return self.pointer

    def get_dataset_version(
        self, tenant_id: str, dataset_name: str, version_id: str
    ) -> DatasetVersion | None:
        return self.version


def _reader(pointer: DatasetPointer | None, version: DatasetVersion | None) -> ControlPlanePort:
    return cast("ControlPlanePort", _Reader(pointer, version))


def _manifest(
    object_sha256: str = _BODY_SHA, **identity: str
) -> tuple[RunManifest, bytes]:
    who = {"tenant": _TENANT, "dataset": "demo", "competencia": "2026-01", "run_id": _RUN_ID}
    who.update(identity)
    output = OutputManifest(
        manifest_version=1, manifest_id="serving-overview", tenant_id=who["tenant"],
        layer="serving", source_type=None, competencia=who["competencia"], run_id=who["run_id"],
        unit_id="unit-1", attempt=1, schema_version="demo-v1",
        object_key=f"serving/{who['tenant']}/{who['run_id']}/overview.json",
        object_sha256=object_sha256, row_count=1, created_at=_NOW,
    )
    manifest = RunManifest(
        manifest_version=1, tenant_id=who["tenant"], dataset_name=who["dataset"],
        run_id=who["run_id"], competencia=who["competencia"], outputs=(output,),
        missing_sources=(), published_at=_NOW,
    )
    return manifest, manifest.model_dump_json(exclude_none=False, by_alias=False).encode()


def _pointer(version_id: str = _RUN_ID) -> DatasetPointer:
    return DatasetPointer(
        tenant_id=_TENANT, dataset_name="demo", pointer_name="current",
        version_id=version_id, updated_at=_NOW,
    )


def _version(run_id: str = _RUN_ID) -> DatasetVersion:
    return DatasetVersion(
        tenant_id=_TENANT, dataset_name="demo", version_id=run_id, run_id=run_id,
        run_manifest_key=f"reconciliation/{_TENANT}/2026-01/{run_id}/run-manifest.json",
        created_at=_NOW,
    )


def _published_store(stored: bytes, key: str = _KEY) -> _FakeStore:
    return _FakeStore({_MANIFEST_KEY: stored, key: _BODY})


def test_le_a_publicacao_pelo_ponteiro_e_devolve_o_version_id_dele() -> None:
    manifest, stored = _manifest()

    published = read_published(
        _reader(_pointer(), _version()), _published_store(stored), _EXPECTED, ("overview",)
    )

    assert published.version_id == _pointer().version_id
    assert published.manifest == manifest
    assert published.manifest_key == _MANIFEST_KEY
    assert published.manifest_sha256 == sha256(stored).hexdigest()
    assert published.outputs == {_KEY: _BODY}


@pytest.mark.parametrize(("pointer", "version"), [
    (None, _version()),
    (_pointer(), None),
    (_pointer("r2"), _version("r2")),
    (_pointer("r1"), _version("r2")),
])
def test_rejeita_ponteiro_ou_versao_que_nao_apontam_para_o_run_do_job(
    pointer: DatasetPointer | None, version: DatasetVersion | None
) -> None:
    store = _published_store(_manifest()[1])

    with pytest.raises(ShadowRunError, match=r"publication_mismatch dataset=demo run_id=r1"):
        read_published(_reader(pointer, version), store, _EXPECTED, ("overview",))


@pytest.mark.parametrize(("identity", "field_name", "wanted", "actual"), [
    ({"tenant": "999999"}, "tenant_id", _TENANT, "999999"),
    ({"dataset": "outro"}, "dataset_name", "demo", "outro"),
    ({"competencia": "2026-02"}, "competencia", "2026-01", "2026-02"),
    ({"run_id": "r9"}, "run_id", _RUN_ID, "r9"),
])
def test_rejeita_manifest_com_identidade_diferente_do_job(
    identity: dict[str, str], field_name: str, wanted: str, actual: str
) -> None:
    _, stored = _manifest(**identity)
    message = rf"manifest_identity_mismatch field={field_name} expected={wanted} actual={actual}"

    with pytest.raises(ShadowRunError, match=message):
        read_published(
            _reader(_pointer(), _version()), _published_store(stored), _EXPECTED, ("overview",)
        )


def _manifest_sem_stat(store: _FakeStore) -> None:
    store.stat = lambda key: None  # type: ignore[method-assign]


def _manifest_com_digest_divergente(store: _FakeStore) -> None:
    store.stat_sha[_MANIFEST_KEY] = "f" * 64


def _manifest_com_tamanho_divergente(store: _FakeStore) -> None:
    store.stat_size[_MANIFEST_KEY] = 1


@pytest.mark.parametrize("diverge", [
    _manifest_sem_stat, _manifest_com_digest_divergente, _manifest_com_tamanho_divergente,
])
def test_rejeita_run_manifest_que_diverge_do_stat_do_object_store(
    diverge: Callable[[_FakeStore], None],
) -> None:
    store = _published_store(_manifest()[1])
    diverge(store)

    with pytest.raises(ShadowRunError, match=r"manifest_stat_mismatch key=reconciliation/354130/"):
        read_published(_reader(_pointer(), _version()), store, _EXPECTED, ("overview",))


def test_le_as_saidas_quando_hash_e_bytes_conferem() -> None:
    manifest, stored = _manifest()

    outputs = read_verified_outputs(_FakeStore({_KEY: _BODY}), manifest, stored, ("overview",))

    assert outputs == {_KEY: _BODY}


@pytest.mark.parametrize("adulterado", ["manifest", "objeto"])
def test_rejeita_hash_de_manifest_adulterado(adulterado: str) -> None:
    manifest, stored = _manifest("0" * 64 if adulterado == "manifest" else _BODY_SHA)
    body = b"adulterado" if adulterado == "objeto" else _BODY

    with pytest.raises(ShadowRunError, match=r"output_sha256_mismatch key=serving/354130/r1/"):
        read_verified_outputs(_FakeStore({_KEY: body}), manifest, stored, ("overview",))


def test_rejeita_hash_do_stat_divergente_do_manifest() -> None:
    manifest, stored = _manifest()
    store = _FakeStore({_KEY: _BODY}, {_KEY: "f" * 64})

    with pytest.raises(ShadowRunError, match=r"output_stat_mismatch key=serving/354130/r1/"):
        read_verified_outputs(store, manifest, stored, ("overview",))


def test_rejeita_saida_sem_stat_no_object_store() -> None:
    manifest, stored = _manifest()
    store = _FakeStore({_KEY: _BODY})
    store.stat = lambda key: None  # type: ignore[method-assign]

    with pytest.raises(ShadowRunError, match=r"output_stat_mismatch key=serving/354130/r1/"):
        read_verified_outputs(store, manifest, stored, ("overview",))


def test_rejeita_run_manifest_nao_canonico() -> None:
    manifest, stored = _manifest()
    pretty = json.dumps(json.loads(stored), indent=2).encode()

    with pytest.raises(ShadowRunError, match=r"manifest_not_canonical run_id=r1"):
        read_verified_outputs(_FakeStore({_KEY: _BODY}), manifest, pretty, ("overview",))


def test_rejeita_chaves_de_serving_fora_do_catalogo() -> None:
    manifest, stored = _manifest()
    documents = ("overview", "by-establishment")

    with pytest.raises(ShadowRunError, match=r"serving_keys_mismatch run_id=r1"):
        read_verified_outputs(_FakeStore({_KEY: _BODY}), manifest, stored, documents)
