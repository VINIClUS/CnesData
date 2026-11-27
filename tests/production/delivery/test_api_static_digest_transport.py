"""publish_dashboard no transporte por digest do gate: referência do membro, readback e ordem."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest

from scripts.promotion.publish_dashboard import (
    PublicationError,
    StaticChannel,
    StaticTarget,
    UploadWindow,
    publish_release,
    read_candidate,
    take_inventory,
)
from tests.production.delivery import _ia_static_digest_consumer as ia
from tests.production.delivery._api_static_fixtures import (
    BUCKET,
    DISTRIBUTION,
    FONT,
    INDEX_CSS,
    NOW,
    FakeStaticPort,
    baseline,
    checksum,
    dist_tar,
    release_files,
    sha,
)

TARGET = StaticTarget(BUCKET, DISTRIBUTION)
ROOT = ("index.html", "theme-init.js")


@dataclass
class TamperedRead:
    inner: FakeStaticPort
    key: str
    changes: dict[str, Any]

    def guard(self) -> None:
        self.inner.guard()

    def call(self, role: str, service: str, operation: str, parameters: dict[str, Any]) -> Any:
        result = self.inner.call(role, service, operation, parameters)
        if operation == "GetObject" and parameters["Key"] == self.key:
            return result | self.changes
        return result


def _planned(files: dict[str, bytes]) -> tuple[FakeStaticPort, Any, Any]:
    port = FakeStaticPort(artifact=dist_tar(files))
    candidate = read_candidate(port.artifact)
    window = UploadWindow(port.now, NOW + timedelta(seconds=180))
    inventory = take_inventory(StaticChannel(port, TARGET), baseline(None, None), candidate, window)
    port.scope(inventory.decision.upload_order, ())
    return port, inventory, candidate


def _publish(port: Any, inventory: Any, candidate: Any) -> Any:
    window = UploadWindow(port.inner.now if isinstance(port, TamperedRead) else port.now,
                          NOW + timedelta(seconds=180))
    return publish_release(StaticChannel(port, TARGET), inventory, candidate, window)


def _normal(port: FakeStaticPort) -> list[tuple[str, str, dict[str, Any]]]:
    return [(op, params.get("Key", ""), params) for role, _, op, params in port.calls
            if role == "normal"]


def test_put_object_manda_a_referencia_do_membro_do_dist_tar_e_nunca_os_bytes() -> None:
    files = release_files()
    port, inventory, candidate = _planned(files)
    _publish(port, inventory, candidate)
    puts = [params for op, _, params in _normal(port) if op == "PutObject"]
    assert {params["Key"] for params in puts} == set(files)
    for params in puts:
        body = files[params["Key"]]
        assert params["Body"] == {"member": params["Key"], "size": len(body), "sha256": sha(body)}
        assert params["ChecksumSHA256"] == checksum(body)
        expected = {"Bucket", "Key", "Body", "ContentType", "CacheControl", "ChecksumSHA256"}
        if params["Key"] not in ROOT:
            expected.add("IfNoneMatch")
            assert params["IfNoneMatch"] == "*"
        assert set(params) == expected
    assert "base64" not in json.dumps(port.calls)


def test_readback_com_digest_e_tamanho_iguais_ao_item_conclui_a_publicacao() -> None:
    port, inventory, candidate = _planned(release_files())
    receipt = _publish(port, inventory, candidate)
    reads = [params for op, _, params in _normal(port) if op == "GetObject"]
    assert {params["Key"] for params in reads} == set(release_files())
    assert receipt.invalidation_id is not None
    assert dict(receipt.uploaded)["index.html"] == port.version("index.html")


@pytest.mark.parametrize("mudanca", ["sha256", "size", "campo-a-mais", "corpo-em-base64"])
def test_readback_divergente_do_item_recusa_sem_tocar_raiz(mudanca: str) -> None:
    files = release_files()
    port, inventory, candidate = _planned(files)
    body = files[INDEX_CSS]
    stored: dict[str, Any] = {"sha256": sha(body), "size": len(body)}
    if mudanca == "sha256":
        stored["sha256"] = sha(body + b"!")
    elif mudanca == "size":
        stored["size"] = len(body) + 1
    elif mudanca == "campo-a-mais":
        stored["base64"] = ""
    else:
        stored = {"base64": "eA=="}
    with pytest.raises(PublicationError, match=f"^readback key={INDEX_CSS}$"):
        _publish(TamperedRead(port, INDEX_CSS, {"Body": stored}), inventory, candidate)
    assert not set(port.writes()) & set(ROOT)


@pytest.mark.parametrize("chave", [INDEX_CSS, "theme-init.js"])
@pytest.mark.parametrize("campo", ["VersionId", "ContentType", "CacheControl"])
def test_readback_com_metadado_divergente_do_put_recusa_sem_nenhuma_chamada_depois(
    campo: str, chave: str
) -> None:
    port, inventory, candidate = _planned(release_files())
    with pytest.raises(PublicationError, match=f"^readback key={chave}$"):
        _publish(TamperedRead(port, chave, {campo: "divergente"}), inventory, candidate)
    calls = [(op, key) for op, key, _ in _normal(port)]
    assert calls[-1] == ("GetObject", chave)
    assert ("PutObject", "index.html") not in calls


def test_referencias_do_css_sao_conferidas_nos_bytes_locais_apos_o_readback_por_digest() -> None:
    files = release_files()
    del files[FONT]
    port, inventory, candidate = _planned(files)
    with pytest.raises(PublicationError, match=f"^reference={FONT} css={INDEX_CSS}$"):
        _publish(port, inventory, candidate)
    assert [op for op, key, _ in _normal(port) if key == INDEX_CSS] == ["PutObject", "GetObject"]
    assert not set(port.writes()) & set(ROOT)


def test_assets_antes_das_raizes_theme_init_antes_do_index_e_invalidacao_no_fim() -> None:
    port, inventory, candidate = _planned(release_files())
    _publish(port, inventory, candidate)
    sequence = [(op, key) for op, key, _ in _normal(port)]
    first_root = sequence.index(("PutObject", "theme-init.js"))
    assert all(key not in ROOT for _, key in sequence[:first_root])
    assert {key for _, key in sequence[:first_root]} == set(release_files()) - set(ROOT)
    assert sequence[first_root:] == [
        ("PutObject", "theme-init.js"), ("GetObject", "theme-init.js"),
        ("PutObject", "index.html"), ("GetObject", "index.html"), ("CreateInvalidation", ""),
    ]


def test_pedidos_do_produto_sao_admitidos_pelas_regras_do_gate_no_pin_espelhado() -> None:
    """Regras do infra-ansible no pin do espelho: lifecycle_contract._static_put, _static_get e
    s3_put_body (com o ChecksumSHA256), e static_inputs.tar_member sobre o dist.tar."""
    files = release_files()
    port, inventory, candidate = _planned(files)
    _publish(port, inventory, candidate)
    keys = list(inventory.decision.upload_order)
    calls = _normal(port)
    for operation, key, params in calls:
        if operation == "PutObject":
            ia.static_put(params, BUCKET, keys)
            assert ia.tar_member(port.artifact, ia.s3_put_body(params)) == files[key]
        elif operation == "GetObject":
            ia.static_get(params, BUCKET, keys)
    assert {operation for operation, _, _ in calls} == {"PutObject", "GetObject",
                                                        "CreateInvalidation"}
