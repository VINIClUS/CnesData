"""Objetos privados da Phase A: stage local, artefato público sem ARN e upload SSE-KMS."""

from __future__ import annotations

import hashlib
import json
import re
from typing import TYPE_CHECKING, Any

from release.verify_candidate import canonical, require

if TYPE_CHECKING:
    from pathlib import Path

type Node = dict[str, Any]
OBJECTS = ("candidate.json", "phase-inputs.json", "promotion.json")
PUBLIC = "phase-a-public.json"
WRITER_ROLE = "cnesdata-prod-phase-inputs-writer"
BUCKET = re.compile(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]")
PREFIX = re.compile(r"(?:[A-Za-z0-9._=-]+/)+")
KMS_KEY = re.compile(r"arn:aws:kms:[a-z0-9-]+:[0-9]{12}:key/[0-9a-f-]{36}")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _read(directory: Path, name: str) -> bytes:
    return (directory / name).read_bytes()


def stage_private(directory: Path, identity: Node) -> None:
    """Args: diretório com promotion.json e phase-inputs.json; identity source_sha/run_id/attempt.

    Raises: ValueError quando o candidato diverge do binding ou do source_sha.
    """
    promotion = json.loads(_read(directory, "promotion.json"))
    candidate = canonical(promotion["candidate"])
    require(_sha(candidate) == promotion["binding"]["candidate_sha256"], "candidate=binding")
    require(promotion["candidate"]["source_sha"] == identity["source_sha"], "candidate=source_sha")
    (directory / "candidate.json").write_bytes(candidate)
    objects = [
        {"name": name, "sha256": _sha(data), "size": len(data)}
        for name in sorted(OBJECTS)
        for data in (_read(directory, name),)
    ]
    public = {"objects": objects} | {
        key: identity[key] for key in ("source_sha", "run_id", "run_attempt")
    }
    (directory / PUBLIC).write_bytes(canonical(public))


def _target(target: Node) -> str:
    require(bool(BUCKET.fullmatch(target["bucket"])), "evidence_bucket=invalid")
    prefix = target["prefix"]
    require(
        bool(PREFIX.fullmatch(prefix)) and ".." not in prefix.split("/"),
        "evidence_prefix=invalid",
    )
    require(bool(KMS_KEY.fullmatch(target["kms_key_arn"])), "evidence_kms_key_arn=invalid")
    identity = target["identity"]
    source, run, attempt = (identity[key] for key in ("source_sha", "run_id", "run_attempt"))
    return f"{prefix}phase-inputs/{source}/{run}/{attempt}/"


def validate_target(target: Node) -> None:
    """Raises: ValueError com evidence_<campo>=invalid antes de qualquer chamada AWS."""
    _target(target)


def upload_private(directory: Path, target: Node, client: Any) -> None:
    """Args: diretório staged, alvo bucket/prefix/kms_key_arn/identity, cliente S3.

    Raises: ValueError se o alvo é inválido ou um objeto diverge de phase-a-public.json.
    """
    base = _target(target)
    public = json.loads(_read(directory, PUBLIC))
    expected = {item["name"]: item["sha256"] for item in public["objects"]}
    require(sorted(expected) == sorted(OBJECTS), "private=objects")
    bodies = {name: _read(directory, name) for name in OBJECTS}
    for name, body in bodies.items():
        require(_sha(body) == expected[name], f"private=sha256 name={name}")
    for name, body in bodies.items():
        client.put_object(
            Bucket=target["bucket"],
            Key=base + name,
            Body=body,
            ContentType="application/json",
            ServerSideEncryption="aws:kms",
            SSEKMSKeyId=target["kms_key_arn"],
            IfNoneMatch="*",
        )
