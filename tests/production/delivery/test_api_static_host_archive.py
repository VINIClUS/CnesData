"""Archive de host EXT-3: três frames ligados à origem, digests e validade, sem segredo."""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import tarfile
from datetime import UTC, datetime, timedelta

import pytest

from scripts.promotion.build_host_archive import (
    ArchiveError,
    HostRelease,
    build_archive,
    build_envelope,
    host_manifest,
    render_compose,
    verify_archive,
)

NOW = datetime(2026, 11, 13, 12, 0, tzinfo=UTC)
IMAGE = "ghcr.io/viniclus/cnesdata/central_api@sha256:" + "e" * 64
PLAN = {
    "evidence_id": "plan-20261113-1",
    "evidence_sha256": "1" * 64,
    "inventory_sha256": "2" * 64,
    "dependencies_sha256": "3" * 64,
    "host_preflight_sha256": "4" * 64,
    "host_key_fingerprint": "SHA256:abcdefghijklmnop",
}
CI = {"repository": "VINIClUS/CnesData", "workflow": ".github/workflows/promote-production.yml",
      "run_id": 77, "artifact_id": 991, "artifact_digest": "sha256:" + "5" * 64}
CONFIG = json.dumps({"api_base_url": "https://api.cnesdata.com.br/api/v1"}).encode()


def release(**changes) -> HostRelease:
    values = {"source_sha": "a" * 40, "api_image": IMAGE, "platform_plan": PLAN,
              "requested_action": "deploy", "expires_at": NOW + timedelta(hours=6),
              "static_release": "candidate-" + "a" * 40 + "-77-1"} | changes
    return HostRelease(**values)


def _parts(**changes):
    compose = render_compose(IMAGE, 18081)
    manifest = host_manifest(release(**changes), compose, CONFIG, NOW)
    return manifest, compose, build_archive(manifest, compose, CONFIG)


def _canonical(value) -> bytes:
    return (json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
            + "\n").encode()


def test_archive_de_host_liga_origem_digest_e_validade() -> None:
    manifest, compose, archive = _parts()
    assert manifest["images"] == {"api": IMAGE}
    assert manifest["compose_sha256"] == hashlib.sha256(compose).hexdigest()
    assert manifest["config_sha256"] == hashlib.sha256(CONFIG).hexdigest()
    assert manifest["platform_plan"] == PLAN
    assert (manifest["project"], manifest["source_repository"]) == ("cnesdata", "VINIClUS/CnesData")
    assert manifest["expires_at"] == "2026-11-13T18:00:00Z"
    members = verify_archive(archive, manifest)
    assert members == {"release-manifest.json": _canonical(manifest), "compose.yml": compose,
                       "config.json": CONFIG}
    envelope = build_envelope(CI, manifest, NOW)
    assert envelope == _canonical({"schema_version": 1, "ci_evidence": CI,
                                   "release_manifest": manifest})
    assert build_archive(manifest, compose, CONFIG) == archive


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"api_image": "ghcr.io/viniclus/cnesdata/central_api:latest"}, "image"),
        ({"expires_at": NOW + timedelta(hours=25)}, "expires_at"),
        ({"expires_at": NOW - timedelta(seconds=1)}, "expires_at"),
        ({"requested_action": "deploy-now"}, "requested_action"),
        ({"source_sha": "A" * 40}, "source_sha"),
        ({"platform_plan": PLAN | {"evidence_sha256": "x"}}, "platform_plan"),
    ],
)
def test_manifest_fora_do_contrato_do_host_e_recusado(changes: dict, reason: str) -> None:
    with pytest.raises(ArchiveError, match=reason):
        _parts(**changes)


def test_envelope_exige_artefato_e_run_exatos() -> None:
    manifest, _, _ = _parts()
    for bad in ({"artifact_digest": "5" * 64}, {"run_id": 0}, {"repository": "other/repo"},
                {"workflow": ".github/workflows/x.yml"}):
        with pytest.raises(ArchiveError, match="ci_evidence"):
            build_envelope(CI | bad, manifest, NOW)
    with pytest.raises(ArchiveError, match="expires_at"):
        build_envelope(CI, manifest, NOW + timedelta(hours=7))


def test_archive_sem_segredo_nem_credencial_estatica() -> None:
    compose = render_compose(IMAGE, 18081)
    secret = b'{"key":"AKIA' + b"ABCDEFGHIJKLMNOP" + b'"}'
    with pytest.raises(ArchiveError, match="secret"):
        host_manifest(release(), compose, secret, NOW)
    leaked = compose.replace(b'"AWS_SHARED_CREDENTIALS_FILE"', b'"AWS_ACCESS_KEY_ID"')
    with pytest.raises(ArchiveError, match="environment"):
        host_manifest(release(), leaked, CONFIG, NOW)


def test_compose_renderizado_segue_contrato_do_host() -> None:
    service = json.loads(render_compose(IMAGE, 18081))["services"]["api"]
    assert service["image"] == IMAGE
    assert service["ports"] == [{"host_ip": "127.0.0.1", "published": 18081, "target": 8000,
                                 "protocol": "tcp"}]
    assert (service["read_only"], service["cap_drop"]) == (True, ["ALL"])
    assert service["environment"] == {"AWS_SHARED_CREDENTIALS_FILE": "/run/credentials/credentials"}
    targets = {m["target"] for m in service["volumes"]}
    assert targets == {"/run/credentials", "/run/config/config.json"}
    assert all(m["read_only"] is True for m in service["volumes"])
    with pytest.raises(ArchiveError, match="image"):
        render_compose("cnesdata-api:production", 18081)


def test_archive_tem_tres_membros_regulares_sem_extensao_e_com_padding_zerado() -> None:
    manifest, _, archive = _parts()
    unpacked = gzip.decompress(archive)
    with tarfile.open(fileobj=io.BytesIO(unpacked), mode="r:") as tar:
        items = tar.getmembers()
        tail = unpacked[tar.offset:]
    assert [i.name for i in items] == ["release-manifest.json", "compose.yml", "config.json"]
    assert all(i.type == tarfile.REGTYPE and not i.pax_headers and i.mtime == 0 for i in items)
    assert all(i.offset == i.offset_data - 512 for i in items)
    assert len(tail) >= 1024
    assert not any(tail)
    tampered = archive[:-8] + b"\0" * 8
    with pytest.raises(ArchiveError):
        verify_archive(tampered, manifest)
    other = host_manifest(release(), render_compose(IMAGE, 18082), CONFIG, NOW)
    with pytest.raises(ArchiveError, match="manifest"):
        verify_archive(archive, other)
