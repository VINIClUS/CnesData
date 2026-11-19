"""Valida candidatos canônicos sem revisões de infraestrutura."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

MAX_CANDIDATE_BYTES = 65536
SCHEMA = Path(__file__).with_name("candidate.schema.json")


def require(condition: Any, reason: str) -> None:
    """Raises: ValueError quando a condição contratual não é satisfeita."""
    if not condition:
        raise ValueError(reason)


def canonical(value: dict[str, Any]) -> bytes:
    """Returns: JSON UTF-8 ordenado, compacto e terminado por newline."""
    text = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return (text + "\n").encode()


def timestamp(value: str) -> datetime:
    """Returns: instante UTC explícito; Raises: ValueError para formato inválido."""
    require(bool(re.fullmatch(r"20\d{2}-\d\d-\d\dT\d\d:\d\d:\d\dZ", value)), "timestamp=format")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _validate(value: Any, rule: dict[str, Any], definitions: dict[str, Any]) -> None:
    if "$ref" in rule:
        rule = definitions[rule["$ref"].rsplit("/", 1)[1]]
    expected = {"object": dict, "string": str, "integer": int}[rule["type"]]
    require(type(value) is expected, "schema=type")
    if "const" in rule:
        require(value == rule["const"], "schema=const")
    if isinstance(value, dict):
        require(set(value) == set(rule["required"]), "schema=fields")
        for key, item in value.items():
            _validate(item, rule["properties"][key], definitions)
    elif isinstance(value, str) and "pattern" in rule:
        require(re.fullmatch(rule["pattern"], value), "schema=pattern")
    elif isinstance(value, int):
        require(value >= rule.get("minimum", 0), "schema=minimum")


def _bindings(manifest: dict[str, Any], now: datetime) -> None:
    run, sha = manifest["verification"], manifest["source_sha"]
    require(manifest["release_id"] == f"candidate-{sha}-{run['id']}-{run['attempt']}", "release=id")
    created, expires = timestamp(manifest["created_at"]), timestamp(manifest["expires_at"])
    require(created <= now < expires <= created + timedelta(days=30), "manifest=expired")
    require(manifest["created_at"] == run["completed_at"], "run=timestamp")
    require(timestamp(manifest["pull_request"]["merged_at"]) <= created, "pull=after_run")
    for check in manifest["pull_request"]["checks"].values():
        require(timestamp(check["completed_at"])
                <= timestamp(manifest["pull_request"]["merged_at"]),
                "check=after_merge")
    require(len({item["id"] for item in manifest["artifacts"].values()}) == 6, "artifact=duplicate")
    for artifact in manifest["artifacts"].values():
        require((artifact["source_sha"], artifact["run_id"], artifact["run_attempt"])
                == (sha, run["id"], run["attempt"]), "artifact=identity")
        require(expires <= timestamp(artifact["expires_at"]), "artifact=expired")
    for app, image in manifest["images"].items():
        prefix = f"ghcr.io/{manifest['repository'].lower()}/{app}@sha256:"
        require(image["image"].startswith(prefix), "image=repository")


def verify_candidate(data: bytes, checksum: str, now: datetime | None = None) -> dict[str, Any]:
    """Returns: candidato validado; Raises: ValueError para bytes ou vínculos inválidos."""
    require(0 < len(data) <= MAX_CANDIDATE_BYTES, "manifest=size")
    require(re.fullmatch(r"[0-9a-f]{64}\n?", checksum), "checksum=format")
    require(hashlib.sha256(data).hexdigest() == checksum.strip(), "checksum=mismatch")
    manifest = json.loads(data)
    require(data == canonical(manifest), "json=noncanonical")
    schema = json.loads(SCHEMA.read_bytes())
    _validate(manifest, schema, schema["$defs"])
    _bindings(manifest, now or datetime.now(UTC))
    return manifest


def main() -> None:
    """Args: caminhos do candidato e sidecar SHA-256 pela linha de comando."""
    parser = argparse.ArgumentParser()
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--checksum", type=Path)
    args = parser.parse_args()
    sidecar = args.checksum or args.candidate.with_suffix(args.candidate.suffix + ".sha256")
    require(args.candidate.stat().st_size <= MAX_CANDIDATE_BYTES, "manifest=size")
    verify_candidate(args.candidate.read_bytes(), sidecar.read_text(encoding="ascii"))
    logging.info("action=verify_candidate result=success")


if __name__ == "__main__":
    main()
