"""Job static-inputs: candidate, archive de host e baseline estático do run, nos membros do keeper."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import zipfile
from pathlib import Path

from scripts.promotion._api_static_records import canonical
from scripts.promotion.build_host_archive import ArchiveError, archive_manifest
from scripts.promotion.publish_dashboard import BASELINE_SCHEMA

CANDIDATE_MEMBER = "candidate.json"
# First delivery: no v2 static release has been published, so current and prior are empty.
FIRST_BASELINE = canonical({"schema": BASELINE_SCHEMA, "current": None, "prior": None})


def _refuse(field: str) -> ValueError:
    return ValueError(f"static_inputs_job field={field}")


def _members(raw: bytes, field: str) -> dict[str, bytes]:
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            return {item.filename: archive.read(item) for item in archive.infolist()
                    if not item.is_dir()}
    except (zipfile.BadZipFile, OSError) as error:
        raise _refuse(field) from error


def candidate_member(raw: bytes) -> bytes:
    """Returns: candidate.json do artefato do release-candidate, conferido com o .sha256.

    Raises: ValueError: membro ausente ou checksum divergente.
    """
    members = _members(raw, "candidate")
    body, checksum = members.get(CANDIDATE_MEMBER), members.get(CANDIDATE_MEMBER + ".sha256")
    if body is None or checksum != (hashlib.sha256(body).hexdigest() + "\n").encode():
        raise _refuse("candidate")
    return body


def host_archive(raw: bytes, digest: str) -> bytes:
    """Returns: o archive de host, membro único do artefato com o digest do dispatch.

    Raises: ValueError: digest divergente ou artefato fora de um membro único.
    """
    if "sha256:" + hashlib.sha256(raw).hexdigest() != digest:
        raise _refuse("host_artifact")
    members = _members(raw, "host_artifact")
    if len(members) != 1 or any("/" in name for name in members):
        raise _refuse("host_artifact")
    return next(iter(members.values()))


def static_inputs(source_sha: str, candidate: bytes, archive: bytes) -> dict[str, bytes]:
    """Returns: os três membros do artefato cnesdata-static-inputs do run.

    Raises: ValueError: candidato de outra fonte ou archive de outra release.
    """
    try:
        value = json.loads(candidate)
    except ValueError as error:
        raise _refuse("candidate_source") from error
    if not isinstance(value, dict) or value.get("source_sha") != source_sha:
        raise _refuse("candidate_source")
    try:
        manifest = archive_manifest(archive)
    except ArchiveError as error:
        raise _refuse("archive_binding") from error
    if (manifest.get("source_sha"), manifest.get("static_release")) != (
            source_sha, value.get("release_id")):
        raise _refuse("archive_binding")
    return {"candidate.json": candidate, "archive.tgz": archive,
            "static-baseline.json": FIRST_BASELINE}


def main(argv: list[str] | None = None) -> int:
    """Raises: ValueError: qualquer insumo fora do contrato, antes de escrever."""
    parser = argparse.ArgumentParser(description="Insumos estáticos do run")
    for name in ("source-sha", "candidate-artifact", "host-artifact", "host-digest", "output"):
        parser.add_argument(f"--{name}", required=True)
    arguments = parser.parse_args(argv)
    candidate = candidate_member(Path(arguments.candidate_artifact).read_bytes())
    archive = host_archive(Path(arguments.host_artifact).read_bytes(), arguments.host_digest)
    files = static_inputs(arguments.source_sha, candidate, archive)
    output = Path(arguments.output)
    output.mkdir(parents=True, exist_ok=False)
    for name, body in files.items():
        (output / name).write_bytes(body)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
