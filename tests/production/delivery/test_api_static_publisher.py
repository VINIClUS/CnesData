"""Publicador estático: adapter S3 do contrato static_release, atomicidade e prazos."""

from __future__ import annotations

import tarfile
from datetime import timedelta

import pytest
from scripts.promotion.publish_dashboard import (
    CandidateError,
    PublicationError,
    StaticTarget,
    UploadDeadlineError,
    UploadWindow,
    publish_release,
    read_candidate,
    stored_versions,
    take_inventory,
)

from cnes_domain.orchestration.static_release import ReleaseOutcome, StoredVersion, VersionRef
from tests.production.delivery._api_static_fixtures import (
    BUCKET,
    DISTRIBUTION,
    FONT,
    INDEX_CSS,
    INDEX_JS,
    NOW,
    FakeStaticPort,
    baseline,
    checksum,
    dist_tar,
    index_html,
    release_files,
    sha,
    stamp,
)

TARGET = StaticTarget(BUCKET, DISTRIBUTION)
OLD = NOW - timedelta(days=30)
ROOT = ("index.html", "theme-init.js")


def _publish_existing(
    port: FakeStaticPort, files: dict[str, bytes], when=OLD
) -> list[tuple[str, str]]:
    return [(key, port.seed(key, body, when)) for key, body in sorted(files.items())]


def _window(port: FakeStaticPort, seconds: int = 180) -> UploadWindow:
    return UploadWindow(port.now, NOW + timedelta(seconds=seconds))


def _inventory(port: FakeStaticPort, files: dict[str, bytes], current=None, prior=None):
    candidate = read_candidate(dist_tar(files))
    return take_inventory(port, TARGET, baseline(current, prior), candidate, NOW), candidate


def test_list_object_versions_vira_stored_version_sem_delete_marker() -> None:
    first, second = NOW - timedelta(days=9), NOW - timedelta(days=8)
    marker = NOW - timedelta(days=2)
    page = {
        "Versions": [
            {"Key": INDEX_JS, "VersionId": "b", "IsLatest": True, "LastModified": stamp(second),
             "Size": 4},
            {"Key": INDEX_JS, "VersionId": "a", "IsLatest": False, "LastModified": stamp(first),
             "Size": 3},
            {"Key": FONT, "VersionId": "f", "IsLatest": False, "LastModified": stamp(first),
             "Size": 5},
        ],
        "DeleteMarkers": [
            {"Key": FONT, "VersionId": "m", "IsLatest": True, "LastModified": stamp(marker)},
        ],
        "IsTruncated": False,
    }
    known = {(INDEX_JS, "a"): checksum(b"old"), (INDEX_JS, "b"): checksum(b"news"),
             (FONT, "f"): None}
    versions = {v.ref: v for v in stored_versions([page], known)}
    assert set(versions) == {VersionRef(INDEX_JS, "a"), VersionRef(INDEX_JS, "b"),
                             VersionRef(FONT, "f")}
    assert versions[VersionRef(INDEX_JS, "a")] == StoredVersion(
        VersionRef(INDEX_JS, "a"), 3, sha(b"old"), first, second)
    assert versions[VersionRef(INDEX_JS, "b")].superseded_at is None
    assert versions[VersionRef(INDEX_JS, "b")].sha256 == sha(b"news")
    assert versions[VersionRef(FONT, "f")].superseded_at == marker
    assert versions[VersionRef(FONT, "f")].sha256 is None


def test_lista_truncada_ou_checksum_composto_nao_vira_hash_conhecido() -> None:
    page = {"Versions": [{"Key": INDEX_JS, "VersionId": "a", "IsLatest": True,
                          "LastModified": stamp(OLD), "Size": 3}],
            "DeleteMarkers": [], "IsTruncated": False}
    versions = stored_versions([page], {(INDEX_JS, "a"): "abc=-3"})
    assert versions[0].sha256 is None
    with pytest.raises(PublicationError, match="listing"):
        stored_versions([page | {"IsTruncated": True}], {(INDEX_JS, "a"): None})


def test_hash_desconhecido_recusa_sobrescrita() -> None:
    port = FakeStaticPort()
    port.seed(INDEX_JS, b"published-elsewhere", OLD, known=False)
    inventory, candidate = _inventory(port, release_files())
    assert inventory.decision.outcome is ReleaseOutcome.REJECTED_OVERWRITE
    with pytest.raises(PublicationError, match="REJECTED_OVERWRITE"):
        publish_release(port, TARGET, inventory, candidate, _window(port))
    assert port.writes() == []


def test_release_misturada_deixa_raiz_inalterada() -> None:
    port = FakeStaticPort()
    old = _publish_existing(port, release_files("v0", font=False))
    files = release_files("v1")
    files["index.html"] = index_html(INDEX_JS, "assets/other-ZzZz0000.js")
    inventory, candidate = _inventory(port, files, current=old)
    assert inventory.decision.outcome is ReleaseOutcome.REJECTED_INVALID
    with pytest.raises(PublicationError, match="REJECTED_INVALID"):
        publish_release(port, TARGET, inventory, candidate, _window(port))
    assert port.writes() == []
    assert port.latest("index.html").version_id == dict(old)["index.html"]


def test_sobrescrita_de_prefixo_imutavel_e_asset_corrompido_deixam_raiz_inalterada() -> None:
    port = FakeStaticPort()
    old = _publish_existing(port, release_files("v0", font=False))
    files = release_files("v1", font=False)
    files[INDEX_JS] = b"different bytes, same immutable key"
    inventory, candidate = _inventory(port, files, current=old)
    assert inventory.decision.outcome is ReleaseOutcome.REJECTED_OVERWRITE
    clean = FakeStaticPort()
    inventory, candidate = _inventory(clean, release_files("v1", font=False))
    clean.corrupt.add(INDEX_JS)
    with pytest.raises(PublicationError, match="readback"):
        publish_release(clean, TARGET, inventory, candidate, _window(clean))
    assert not set(clean.writes()) & set(ROOT)


def test_publicacao_sobe_assets_antes_e_entrypoints_por_ultimo() -> None:
    port = FakeStaticPort()
    inventory, candidate = _inventory(port, release_files())
    receipt = publish_release(port, TARGET, inventory, candidate, _window(port))
    writes = port.writes()
    assert writes[-2:] == ["theme-init.js", "index.html"]
    assert set(writes[:-2]) == {INDEX_JS, INDEX_CSS, FONT}
    puts = [params for _, _, op, params in port.calls if op == "PutObject"]
    assert all(p.get("IfNoneMatch") == "*" for p in puts if p["Key"].startswith("assets/"))
    assert all("IfNoneMatch" not in p for p in puts if p["Key"] in ROOT)
    assert {role for role, _, op, _ in port.calls if op == "PutObject"} == {"normal"}
    invalidations = [p for _, _, op, p in port.calls if op == "CreateInvalidation"]
    assert [p["InvalidationBatch"]["Paths"]["Items"] for p in invalidations] == [
        ["/index.html", "/theme-init.js"]]
    assert receipt.invalidation_id is not None
    assert dict(receipt.uploaded)["index.html"] == port.latest("index.html").version_id
    assert port.guards >= len(writes) + 1


def test_upload_vencido_aborta_sem_tocar_raiz() -> None:
    port = FakeStaticPort()
    inventory, candidate = _inventory(port, release_files())
    port.latency[INDEX_CSS] = timedelta(seconds=400)
    with pytest.raises(UploadDeadlineError) as raised:
        publish_release(port, TARGET, inventory, candidate, _window(port))
    assert raised.value.unknown == (INDEX_CSS,)
    assert not set(port.writes()) & set(ROOT)
    assert port.latest("index.html") is None
    assert not [op for _, _, op, _ in port.calls if op == "AbortMultipartUpload"]


def test_prazo_insuficiente_recusa_antes_de_enviar() -> None:
    port = FakeStaticPort()
    inventory, candidate = _inventory(port, release_files())
    before = len(port.calls)
    with pytest.raises(UploadDeadlineError):
        publish_release(port, TARGET, inventory, candidate, UploadWindow(port.now, port.now()))
    assert len(port.calls) == before


def test_current_e_prior_vem_do_manifest_da_release() -> None:
    port = FakeStaticPort()
    prior = _publish_existing(port, release_files("v0"), OLD - timedelta(days=5))
    current = _publish_existing(port, {key: body + b"1" if key in ROOT else body
                                       for key, body in release_files("v0").items()})
    inventory, _ = _inventory(port, release_files("v2", font=False), current=current, prior=prior)
    snapshot = inventory.snapshot
    assert snapshot.current == frozenset(VersionRef(k, v) for k, v in current)
    assert snapshot.prior == frozenset(VersionRef(k, v) for k, v in prior)
    assert FONT in {ref.key for ref in snapshot.current}
    assert all(ref.key != FONT for ref in inventory.decision.retire_latest)


def test_baseline_sem_index_ou_fora_do_bucket_e_recusado() -> None:
    port = FakeStaticPort()
    old = _publish_existing(port, release_files("v0"))
    candidate = read_candidate(dist_tar(release_files("v1")))
    raw = baseline([ref for ref in old if ref[0] != "index.html"], None)
    assert take_inventory(port, TARGET, raw, candidate, NOW).decision.outcome is (
        ReleaseOutcome.REJECTED_INVALID)
    missing = baseline([*old, (INDEX_JS + "x", "nope")], None)
    with pytest.raises(PublicationError, match="baseline"):
        take_inventory(port, TARGET, missing, candidate, NOW)


def test_current_antigo_continua_alvo_de_rollback() -> None:
    port = FakeStaticPort()
    current = _publish_existing(port, release_files("v0"))
    port.seed("index.html", b"<html>interrupted</html>", NOW - timedelta(days=1))
    inventory, _ = _inventory(port, release_files("v1"), current=current)
    assert inventory.decision.rollback_entrypoint == VersionRef("index.html",
                                                                dict(current)["index.html"])
    assert inventory.rollback_versions == (
        VersionRef("index.html", dict(current)["index.html"]),
        VersionRef("theme-init.js", dict(current)["theme-init.js"]),
    )


def test_retire_nao_presume_regra_gc() -> None:
    port = FakeStaticPort()
    stale = port.seed(INDEX_JS, b"stale", NOW - timedelta(days=20))
    current = _publish_existing(port, release_files("v0"), NOW - timedelta(days=10))
    inventory, _ = _inventory(port, release_files("v0"), current=current)
    assert VersionRef(INDEX_JS, stale) in inventory.decision.retire
    assert sum(ref.key == INDEX_JS for ref in inventory.decision.retire) == 1


def test_sem_current_nao_retira_e_release_fica_planned() -> None:
    port = FakeStaticPort()
    port.seed("assets/orphan-Aaaa1111.js", b"x", NOW - timedelta(days=40))
    port.seed("assets/orphan-Aaaa1111.js", b"y", NOW - timedelta(days=30))
    inventory, _ = _inventory(port, release_files())
    assert inventory.decision.outcome is ReleaseOutcome.PLANNED
    assert (inventory.decision.retire, inventory.decision.retire_latest) == ((), ())
    assert inventory.rollback_versions == ()


def test_bucket_cheio_devolve_retirada_sem_upload() -> None:
    port = FakeStaticPort()
    stale = port.seed(INDEX_JS, b"stale", NOW - timedelta(days=20))
    current = _publish_existing(port, release_files("v0"), NOW - timedelta(days=10))
    port.uploads.append(("assets/big-Bbbb2222.js", "u1", [5_000_000_000]))
    inventory, candidate = _inventory(port, release_files("v1"), current=current)
    assert inventory.decision.outcome is ReleaseOutcome.REJECTED_CAPACITY
    assert VersionRef(INDEX_JS, stale) in inventory.decision.retire
    assert inventory.snapshot.incomplete_upload_bytes == 5_000_000_000
    with pytest.raises(PublicationError, match="REJECTED_CAPACITY"):
        publish_release(port, TARGET, inventory, candidate, _window(port))
    assert port.writes() == []


def test_bucket_cheio_sem_current_recusa_sem_retirada() -> None:
    port = FakeStaticPort()
    port.seed(INDEX_JS, b"stale", NOW - timedelta(days=20))
    port.seed(INDEX_JS, b"newer", NOW - timedelta(days=19))
    port.uploads.append(("assets/big-Bbbb2222.js", "u1", [5_000_000_000]))
    inventory, _ = _inventory(port, release_files("v1"))
    assert inventory.decision.outcome is ReleaseOutcome.REJECTED_CAPACITY
    assert (inventory.decision.retire, inventory.decision.retire_latest) == ((), ())
    assert inventory.multipart_keys == ("assets/big-Bbbb2222.js",)


def test_objeto_com_mesmo_sha256_vai_para_already_published() -> None:
    port = FakeStaticPort()
    files = release_files("v1")
    current = _publish_existing(port, files)
    inventory, candidate = _inventory(port, files, current=current)
    assert set(inventory.decision.already_published) == set(files)
    receipt = publish_release(port, TARGET, inventory, candidate, _window(port))
    assert port.writes() == []
    assert receipt.invalidation_id is None
    assert set(receipt.already_published) == set(files)


def test_readback_confere_referencias_fora_do_index() -> None:
    port = FakeStaticPort()
    files = release_files("v1")
    del files[FONT]
    inventory, candidate = _inventory(port, files)
    assert inventory.decision.outcome is ReleaseOutcome.PLANNED
    with pytest.raises(PublicationError, match="reference"):
        publish_release(port, TARGET, inventory, candidate, _window(port))
    assert not set(port.writes()) & set(ROOT)
    complete = FakeStaticPort()
    inventory, candidate = _inventory(complete, release_files("v1"))
    publish_release(complete, TARGET, inventory, candidate, _window(complete))
    assert complete.latest(FONT) is not None


@pytest.mark.parametrize("theme", ["./theme-init.js", "theme-init.js", "//cdn.example/x.js"])
def test_build_com_base_relativa_e_recusado(theme: str) -> None:
    files = release_files("v1")
    files["index.html"] = index_html(INDEX_JS, INDEX_CSS, theme=theme)
    with pytest.raises(CandidateError, match="relative_reference"):
        read_candidate(dist_tar(files))
    files["index.html"] = b"<html><script src=/theme-init.js></script></html>"
    with pytest.raises(CandidateError, match="relative_reference"):
        read_candidate(dist_tar(files))


def test_dist_tar_com_link_traversal_ou_chave_fora_do_contrato_e_recusado() -> None:
    link = tarfile.TarInfo("./assets/link-Cccc3333.js")
    link.type = tarfile.SYMTYPE
    link.linkname = "/etc/passwd"
    with pytest.raises(CandidateError, match="member"):
        read_candidate(dist_tar(release_files(), extra=[link]))
    with pytest.raises(CandidateError, match="key"):
        read_candidate(dist_tar(release_files() | {"robots.txt": b"x"}))
    with pytest.raises(CandidateError, match="key"):
        read_candidate(dist_tar(release_files() | {"../index.html": b"x"}))
