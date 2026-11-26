"""Jobs do caller lidos pelo keeper: static-inputs e a sonda static-inventory (P15b, P28*)."""

from __future__ import annotations

import hashlib
import io
import json
import zipfile
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import pytest
import yaml

from scripts.promotion import static_inputs_job, static_inventory_probe
from scripts.promotion._api_static_records import canonical
from scripts.promotion.build_host_archive import (
    HostRelease,
    build_archive,
    host_manifest,
    render_compose,
)
from scripts.promotion.closed_ready_probe import Target
from tests.production.delivery.test_api_static_host_archive import CONFIG, IMAGE, PLAN
from tests.production.delivery.test_promote_production_gates import WORKFLOW

if TYPE_CHECKING:
    from pathlib import Path

SOURCE = "a" * 40
RELEASE = f"candidate-{SOURCE}-77-1"
NOW = datetime.now(UTC)
TARGET = Target(7, 1, "application / execute")
ARN = "arn:aws:cloudfront::111122223333:distribution/E2ABCDEF123456"


def _zip(members: dict[str, bytes]) -> bytes:
    raw = io.BytesIO()
    with zipfile.ZipFile(raw, "w") as archive:
        for name, body in members.items():
            archive.writestr(name, body)
    return raw.getvalue()


def _candidate(**change: Any) -> bytes:
    return canonical({"source_sha": SOURCE, "release_id": RELEASE} | change)


def _candidate_zip(raw: bytes | None = None, checksum: str | None = None) -> bytes:
    body = raw if raw is not None else _candidate()
    digest = checksum if checksum is not None else hashlib.sha256(body).hexdigest() + "\n"
    return _zip({"candidate.json": body, "candidate.json.sha256": digest.encode()})


def _archive(source: str = SOURCE, release: str | None = RELEASE) -> bytes:
    value = HostRelease(source, IMAGE, PLAN, "deploy", NOW + timedelta(hours=6), release)
    compose = render_compose(IMAGE, 18081)
    return build_archive(host_manifest(value, compose, CONFIG, NOW), compose, CONFIG)


def _host_zip(archive: bytes | None = None) -> tuple[bytes, str]:
    raw = _zip({"host-archive.tgz": archive or _archive()})
    return raw, "sha256:" + hashlib.sha256(raw).hexdigest()


def test_static_inputs_monta_os_tres_membros_exatos_ligados_ao_candidato() -> None:
    raw, digest = _host_zip()
    candidate = static_inputs_job.candidate_member(_candidate_zip())
    files = static_inputs_job.static_inputs(
        SOURCE, candidate, static_inputs_job.host_archive(raw, digest))
    assert set(files) == {"candidate.json", "archive.tgz", "static-baseline.json"}
    assert files["candidate.json"] == _candidate()
    assert files["archive.tgz"] == _archive_bytes(raw)
    assert json.loads(files["static-baseline.json"]) == {
        "schema": "cnesdata.static-baseline/v1", "current": None, "prior": None}
    assert files["static-baseline.json"] == canonical(json.loads(files["static-baseline.json"]))


def _archive_bytes(raw: bytes) -> bytes:
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        return archive.read("host-archive.tgz")


@pytest.mark.parametrize(("archive", "candidate", "field"), [
    (dict(source="b" * 40), {}, "archive_binding"),
    (dict(release=f"candidate-{SOURCE}-78-1"), {}, "archive_binding"),
    (dict(release=None), {}, "archive_binding"),
    ({}, {"source_sha": "b" * 40}, "candidate_source"),
], ids=["fonte-do-archive", "release-do-archive", "archive-sem-release", "fonte-do-candidato"])
def test_static_inputs_recusa_archive_ou_candidato_de_outra_release(
    archive: dict[str, Any], candidate: dict[str, Any], field: str,
) -> None:
    with pytest.raises(ValueError, match=rf"^static_inputs_job field={field}$"):
        static_inputs_job.static_inputs(SOURCE, _candidate(**candidate), _archive(**archive))


def test_host_archive_recusa_digest_divergente_e_zip_com_outro_membro() -> None:
    raw, _ = _host_zip()
    with pytest.raises(ValueError, match=r"^static_inputs_job field=host_artifact$"):
        static_inputs_job.host_archive(raw, "sha256:" + "0" * 64)
    two = _zip({"a.tgz": b"x", "b.tgz": b"y"})
    with pytest.raises(ValueError, match=r"^static_inputs_job field=host_artifact$"):
        static_inputs_job.host_archive(two, "sha256:" + hashlib.sha256(two).hexdigest())


def test_candidato_recusa_checksum_divergente_ou_membro_ausente() -> None:
    with pytest.raises(ValueError, match=r"^static_inputs_job field=candidate$"):
        static_inputs_job.candidate_member(_candidate_zip(checksum="0" * 64 + "\n"))
    with pytest.raises(ValueError, match=r"^static_inputs_job field=candidate$"):
        static_inputs_job.candidate_member(_zip({"candidate.json": _candidate()}))


def test_cli_do_static_inputs_escreve_os_tres_arquivos(tmp_path: Path) -> None:
    raw, digest = _host_zip()
    (tmp_path / "candidate.zip").write_bytes(_candidate_zip())
    (tmp_path / "host.zip").write_bytes(raw)
    static_inputs_job.main(["--source-sha", SOURCE, "--candidate-artifact",
                            str(tmp_path / "candidate.zip"), "--host-artifact",
                            str(tmp_path / "host.zip"), "--host-digest", digest,
                            "--output", str(tmp_path / "out")])
    assert sorted(path.name for path in (tmp_path / "out").iterdir()) == [
        "archive.tgz", "candidate.json", "static-baseline.json"]


class Api:
    def __init__(self, listings: list[list[dict[str, Any]]], archive: bytes) -> None:
        self.listings, self.archive_raw, self.names = listings, archive, []

    def artifacts(self, name: str) -> list[dict[str, Any]]:
        self.names.append(name)
        return self.listings.pop(0) if len(self.listings) > 1 else self.listings[0]

    def archive(self, artifact_id: int) -> bytes:
        assert artifact_id == 31
        return self.archive_raw


class Clock:
    def __init__(self) -> None:
        self.now, self.sleeps = 0.0, []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


INVENTORY = canonical({"schema": "cnesdata.static-inventory/v1", "candidate_sha256": "1" * 64,
                       "dashboard_sha256": "2" * 64, "baseline": {}, "outcome": "PLANNED",
                       "upload_order": ["assets/app.js", "index.html"], "already_published": [],
                       "retire": [], "retire_latest": [], "rollback_versions": [],
                       "multipart_keys": [], "projected_bytes": 10})


def test_sonda_espera_o_inventory_do_keeper_pelo_nome_exato_do_run() -> None:
    api, clock = Api([[], [], [{"id": 31}]], _zip({"inventory.json": INVENTORY})), Clock()
    assert static_inventory_probe.wait_for_inventory(api, TARGET, clock) == INVENTORY
    assert set(api.names) == {"cnesdata-inventory-7-1"}
    assert clock.sleeps == [static_inventory_probe.POLL_SECONDS] * 2


@pytest.mark.parametrize(("listing", "members", "reason"), [
    ([{"id": 31}, {"id": 32}], {"inventory.json": INVENTORY}, "artifact_count"),
    ([{"id": 31}], {"inventory.json": INVENTORY, "x": b"1"}, "members"),
    ([{"id": 31}], {"outro.json": INVENTORY}, "members"),
], ids=["dois-artefatos", "membro-extra", "membro-errado"])
def test_sonda_recusa_artefato_duplicado_ou_fora_dos_membros(
    listing: list[dict[str, Any]], members: dict[str, bytes], reason: str,
) -> None:
    with pytest.raises(ValueError, match=rf"^static_inventory_probe={reason}$"):
        static_inventory_probe.wait_for_inventory(Api([listing], _zip(members)), TARGET, Clock())


def test_sonda_falha_fechado_no_prazo_sem_inventory() -> None:
    with pytest.raises(ValueError, match=r"^static_inventory_probe=deadline$"):
        static_inventory_probe.wait_for_inventory(Api([[]], b""), TARGET, Clock(), 30)


def test_plano_estatico_e_o_carrier_derivado_do_inventory_e_da_distribuicao() -> None:
    plan = json.loads(static_inventory_probe.static_plan_file(INVENTORY, ARN))
    assert plan["inventory_sha256"] == hashlib.sha256(INVENTORY).hexdigest()
    assert plan["distribution_arn"] == ARN
    assert plan["candidate_keys"] == ["assets/app.js", "index.html"]


def _job(name: str) -> dict[str, Any]:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))["jobs"][name]


def _uploads(job: dict[str, Any]) -> dict[str, str]:
    return {step["with"]["name"]: step["with"]["path"] for step in job["steps"]
            if str(step.get("uses", "")).startswith("actions/upload-artifact@")}


def test_jobs_estaticos_rodam_em_paralelo_sem_environment_e_publicam_os_nomes_do_keeper() -> None:
    inputs, inventory = _job("static-inputs"), _job("static-inventory")
    for job in (inputs, inventory):
        assert set(job["needs"]) == {"admit", "preamble"}
        assert "environment" not in job
        assert job["permissions"] == {"contents": "read", "actions": "read"}
        assert job["runs-on"] == "ubuntu-latest"
    run = "${{ github.run_id }}-${{ github.run_attempt }}"
    assert set(_uploads(inputs)) == {f"cnesdata-static-inputs-{run}"}
    assert set(_uploads(inventory)) == {f"cnesdata-static-plan-{run}"}
    assert "scripts.promotion.static_inputs_job" in json.dumps(inputs)
    assert "scripts.promotion.static_inventory_probe" in json.dumps(inventory)
    assert "secrets.CNESDATA_STATIC_DISTRIBUTION_ARN" in json.dumps(inventory)
