"""Espelho das regras do transporte por digest do publish-static do infra-ansible, por commit."""

from __future__ import annotations

import base64
import hashlib
import io
import re
import tarfile
from typing import Any

# infra-ansible c675aa47 tools/cnesdata_delivery/lifecycle_contract.py: STATIC_MEMBER_LIMIT,
# STATIC_UPLOAD_FLOOR, STATIC_ROOT_BLOCK_SECONDS, STATIC_ENTRYPOINTS, s3_put_body, s3_body_value,
# _static_put, _static_get; tools/cnesdata_delivery/static_inputs.py: tar_member;
# tools/cnesdata_delivery/contract.py: require, closed, digest, integer.
IA_PIN = "c675aa47d28a7fac9158f2958cdd959e5b568875"
IA_MIRRORED_SOURCES = {
    "tools/cnesdata_delivery/contract.py": (
        "1f881e85cd21ab45ea5be22d12e81d3515c7ea7849b5c90aa6c54cb1e8b13d3b"
    ),
    "tools/cnesdata_delivery/lifecycle_contract.py": (
        "166197213a23f59d70bff661251d8fd1c8a9a53c8a3caadd16da66532c07dabd"
    ),
    "tools/cnesdata_delivery/static_inputs.py": (
        "ac99f3926811fedd4ebb09ab4a29de7054ef5cddd3d3c056c3146cc835840a6b"
    ),
}
STATIC_MEMBER_LIMIT = 1024**2
STATIC_UPLOAD_FLOOR = 512 * 1024
STATIC_ROOT_BLOCK_SECONDS = 5 * 20
STATIC_ENTRYPOINTS = ("index.html", "theme-init.js")


def require(condition: object, message: str) -> None:
    if not condition:
        raise ValueError(message)


def closed(value: Any, fields: str) -> Any:
    require(type(value) is dict and set(value) == set(fields.split()), "closed object fields")
    return value


def digest(value: Any, length: int = 64) -> Any:
    require(type(value) is str and re.fullmatch(f"[0-9a-f]{{{length}}}", value), "digest")
    return value


def integer(value: Any, minimum: int = 1, maximum: int = 2**53 - 1) -> Any:
    require(type(value) is int and minimum <= value <= maximum, "integer range")
    return value


def s3_put_body(parameters: Any) -> dict[str, Any]:
    require(type(parameters) is dict and type(parameters.get("ChecksumSHA256")) is str,
            "S3 Body PutObject parameters")
    body = closed(parameters.get("Body"), "member size sha256")
    require(body["member"] == parameters.get("Key"), "S3 Body member")
    integer(body["size"], 0, STATIC_MEMBER_LIMIT)
    digest(body["sha256"])
    require(parameters["ChecksumSHA256"]
            == base64.b64encode(bytes.fromhex(body["sha256"])).decode("ascii"),
            "S3 Body ChecksumSHA256")
    return dict(body)


def s3_body_value(stream: io.BufferedIOBase, *, maximum: int) -> dict[str, Any]:
    integer(maximum, 0)
    sha, size = hashlib.sha256(), 0
    while size <= maximum:
        chunk = stream.read(maximum + 1 - size)
        require(type(chunk) is bytes, "S3 Body stream bytes")
        if not chunk:
            break
        sha.update(chunk)
        size += len(chunk)
    require(size <= maximum, "S3 Body bound")
    return {"sha256": sha.hexdigest(), "size": size}


def static_put(parameters: dict[str, Any], bucket: str, candidate_keys: list[str]) -> None:
    asset = parameters.get("Key") not in STATIC_ENTRYPOINTS
    closed(parameters, "Bucket Key Body ContentType CacheControl ChecksumSHA256"
           + (" IfNoneMatch" if asset else ""))
    require(parameters["Bucket"] == bucket and parameters["Key"] in candidate_keys
            and (not asset or parameters["IfNoneMatch"] == "*")
            and all(type(parameters[name]) is str and 1 <= len(parameters[name]) <= 256
                    for name in ("ContentType", "CacheControl")), "static call parameters")
    s3_put_body(parameters)


def static_get(parameters: dict[str, Any], bucket: str, candidate_keys: list[str]) -> None:
    closed(parameters, "Bucket Key")
    require(parameters["Bucket"] == bucket and parameters["Key"] in candidate_keys,
            "static call parameters")


def tar_member(tar_raw: bytes, reference: dict[str, Any]) -> bytes:
    key = reference["member"]
    with tarfile.open(fileobj=io.BytesIO(tar_raw), mode="r:") as archive:
        found = [member for member in archive.getmembers()
                 if member.isfile() and member.name.removeprefix("./") == key]
        require(len(found) == 1, f"static_body=member_absent key={key}")
        require(found[0].size == reference["size"], f"static_body=member_digest key={key}")
        handle = archive.extractfile(found[0])
        raw = handle.read() if handle is not None else None
    require(type(raw) is bytes and len(raw) == reference["size"]
            and hashlib.sha256(raw).hexdigest() == reference["sha256"],
            f"static_body=member_digest key={key}")
    assert raw is not None
    return raw
