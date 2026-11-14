"""Publicador S3 da release estática: adapter de I/O do contrato static_release."""

from __future__ import annotations

import base64
import binascii
import hashlib
import io
import json
import re
import tarfile
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Protocol

from cnes_domain.orchestration.static_release import (
    ReleaseObject,
    ReleaseOutcome,
    StaticBucketSnapshot,
    StaticReleaseDecision,
    StoredVersion,
    VersionRef,
    describe_release_object,
    plan_static_release,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

BASELINE_SCHEMA = "cnesdata.static-baseline/v1"
ENTRYPOINTS = ("theme-init.js", "index.html")
INVALIDATION_PATHS = ("/index.html", "/theme-init.js")
CALL_SECONDS = 20
_TAR_LIMIT = 64 * 1024**2
_MAX_PAGES = 100
_TAG = re.compile(rb"<(?:script|link)\b[^>]*>", re.IGNORECASE)
_ATTRIBUTE = re.compile(rb"""\b(?:src|href)\s*=\s*(["']?)([^"'\s>]*)""", re.IGNORECASE)
_CSS_URL = re.compile(rb"""url\(\s*(["']?)([^"')]+)\1\s*\)""")


class StaticPort(Protocol):
    def call(self, role: str, service: str, operation: str, parameters: dict[str, Any]) -> Any: ...

    def guard(self) -> None: ...


class PublicationError(RuntimeError):
    pass


class CandidateError(PublicationError):
    pass


class UploadDeadlineError(PublicationError):
    def __init__(self, reason: str, unknown: Sequence[str] = ()) -> None:
        super().__init__(reason)
        self.unknown = tuple(unknown)


@dataclass(frozen=True)
class StaticTarget:
    bucket: str
    distribution_id: str


@dataclass(frozen=True)
class StaticChannel:
    port: StaticPort
    target: StaticTarget


@dataclass(frozen=True)
class Candidate:
    objects: tuple[ReleaseObject, ...]
    contents: Mapping[str, bytes]


@dataclass(frozen=True)
class Inventory:
    snapshot: StaticBucketSnapshot
    decision: StaticReleaseDecision
    rollback_versions: tuple[VersionRef, ...]
    multipart_keys: tuple[str, ...]


@dataclass(frozen=True)
class UploadWindow:
    clock: Callable[[], datetime]
    deadline: datetime
    call_seconds: int = CALL_SECONDS


@dataclass(frozen=True)
class PublicationReceipt:
    uploaded: tuple[tuple[str, str], ...]
    already_published: tuple[str, ...]
    invalidation_id: str | None


def stored_versions(
    pages: Sequence[Mapping[str, Any]], checksums: Mapping[tuple[str, str], str | None]
) -> tuple[StoredVersion, ...]:
    """Returns:
    Versões com conteúdo; delete marker só define o superseded_at da anterior.
    Raises:
    PublicationError: listagem truncada ou sem versão mais recente única por chave.
    """
    if not pages or pages[-1].get("IsTruncated") is not False:
        raise PublicationError("listing=truncated")
    entries: dict[str, list[tuple[datetime, bool, str, int | None]]] = {}
    for page in pages:
        for item in page.get("Versions", []):
            _listed(entries, item, int(item["Size"]))
        for item in page.get("DeleteMarkers", []):
            _listed(entries, item, None)
    result: list[StoredVersion] = []
    for key, items in entries.items():
        ordered = sorted(items, key=lambda entry: (entry[0], entry[1], entry[2]))
        if not ordered[-1][1] or sum(entry[1] for entry in ordered) != 1:
            raise PublicationError(f"listing=latest key={key}")
        for position, (modified, _, version, size) in enumerate(ordered):
            if size is None:
                continue
            following = ordered[position + 1][0] if position + 1 < len(ordered) else None
            known = _hex_checksum(checksums.get((key, version)))
            result.append(StoredVersion(VersionRef(key, version), size, known, modified, following))
    return tuple(sorted(result, key=lambda version: version.ref))


def baseline_references(raw: bytes) -> tuple[frozenset[VersionRef], frozenset[VersionRef]]:
    """Returns:
    Referências (current, prior) lidas do manifest de cada release.
    Raises:
    PublicationError: baseline fora do schema ou com chave fora do contrato.
    """
    value = _json(raw, "baseline")
    if not isinstance(value, dict) or set(value) != {"schema", "current", "prior"}:
        raise PublicationError("baseline=fields")
    if value["schema"] != BASELINE_SCHEMA:
        raise PublicationError("baseline=schema")
    return _release_refs(value["current"]), _release_refs(value["prior"])


def read_candidate(dist_tar: bytes) -> Candidate:
    """Returns:
    Objetos descritos pelo contrato e seus bytes, a partir do dist.tar do build.
    Raises:
    CandidateError: membro não regular, chave fora do contrato ou referência relativa.
    """
    if len(dist_tar) > _TAR_LIMIT:
        raise CandidateError("dist=size")
    contents: dict[str, bytes] = {}
    objects: list[ReleaseObject] = []
    with tarfile.open(fileobj=io.BytesIO(dist_tar), mode="r:") as archive:
        for member in archive:
            if member.isdir():
                continue
            item = _candidate_member(archive, member, contents)
            objects.append(item)
    if "index.html" in contents:
        _reject_relative_references(contents["index.html"])
    return Candidate(tuple(sorted(objects, key=lambda item: item.key)), contents)


def take_inventory(
    channel: StaticChannel, baseline: bytes, candidate: Candidate, now: datetime
) -> Inventory:
    """Returns:
    Snapshot completo do bucket, decisão do contrato e alvos de rollback/abort.
    """
    port, target = channel.port, channel.target
    current, prior = baseline_references(baseline)
    pages = _version_pages(port, target)
    checksums = {
        (item["Key"], item["VersionId"]): _head_checksum(port, target, item)
        for page in pages
        for item in page.get("Versions", [])
    }
    uploads = _incomplete_uploads(port, target)
    snapshot = StaticBucketSnapshot(
        versions=stored_versions(pages, checksums),
        incomplete_upload_bytes=sum(size for _, size in uploads),
        current=current,
        prior=prior,
        complete=True,
    )
    decision = plan_static_release(snapshot, candidate.objects, now)
    rollback = tuple(
        ref for key in ("index.html", "theme-init.js") for ref in current if ref.key == key
    )
    return Inventory(snapshot, decision, rollback, tuple(sorted({key for key, _ in uploads})))


def publish_release(
    channel: StaticChannel, inventory: Inventory, candidate: Candidate, window: UploadWindow
) -> PublicationReceipt:
    """Returns:
    Versões escritas; assets verificados antes dos entrypoints, raiz por último.
    Raises:
    PublicationError: decisão não PLANNED, readback divergente ou referência ausente.
    UploadDeadlineError: chamada sem prazo ou vencida; nenhum entrypoint posterior.
    """
    decision = inventory.decision
    if decision.outcome is not ReleaseOutcome.PLANNED:
        raise PublicationError(f"static_release={decision.outcome.value}")
    objects = {item.key: item for item in candidate.objects}
    assets = [key for key in decision.upload_order if key not in ENTRYPOINTS]
    roots = [key for key in decision.upload_order if key in ENTRYPOINTS]
    contents = candidate.contents
    uploaded = [(key, _put(channel, objects[key], contents[key], window)) for key in assets]
    bodies = {key: _readback(channel, objects[key], version, window) for key, version in uploaded}
    _verify_css_references(objects, {**contents, **bodies})
    for key in roots:
        version = _put(channel, objects[key], contents[key], window)
        _readback(channel, objects[key], version, window)
        uploaded.append((key, version))
    invalidation = _invalidate(channel, uploaded, window) if roots else None
    return PublicationReceipt(tuple(uploaded), decision.already_published, invalidation)


def call_within(
    port: StaticPort, window: UploadWindow, request: tuple[str, str, str], parameters: dict[str, Any]
) -> Any:
    """Returns:
    Resposta da chamada feita dentro do prazo fixo do estágio.
    Raises:
    UploadDeadlineError: prazo insuficiente antes do envio ou resposta depois do limite.
    """
    role, service, operation = request
    limit = window.clock() + timedelta(seconds=window.call_seconds)
    if limit > window.deadline:
        raise UploadDeadlineError(f"deadline={operation}")
    port.guard()
    result = port.call(role, service, operation, parameters)
    if window.clock() > limit:
        raise UploadDeadlineError(f"expired={operation}", (str(parameters.get("Key", "")),))
    return result


def b64(raw: bytes) -> str:
    return base64.b64encode(raw).decode("ascii")


def sha256_b64(raw: bytes) -> str:
    return b64(hashlib.sha256(raw).digest())


def _listed(
    entries: dict[str, list[tuple[datetime, bool, str, int | None]]],
    item: Mapping[str, Any],
    size: int | None,
) -> None:
    modified = datetime.fromisoformat(str(item["LastModified"]).replace("Z", "+00:00"))
    if modified.utcoffset() is None:
        raise PublicationError(f"listing=timezone key={item['Key']}")
    record = (modified, item["IsLatest"] is True, str(item["VersionId"]), size)
    entries.setdefault(str(item["Key"]), []).append(record)


def _hex_checksum(value: str | None) -> str | None:
    if not isinstance(value, str) or "-" in value:
        return None
    try:
        raw = base64.b64decode(value, validate=True)
    except binascii.Error:
        return None
    return raw.hex() if len(raw) == 32 else None


def _json(raw: bytes, name: str) -> Any:
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PublicationError(f"{name}=json") from error


def _release_refs(release: Any) -> frozenset[VersionRef]:
    if release is None:
        return frozenset()
    if not isinstance(release, dict) or set(release) != {"release_id", "objects"}:
        raise PublicationError("baseline=release")
    refs = []
    for item in release["objects"]:
        if not isinstance(item, dict) or set(item) != {"key", "version_id"}:
            raise PublicationError("baseline=object")
        key, version = item["key"], item["version_id"]
        if describe_release_object(str(key), b"") is None or not isinstance(version, str):
            raise PublicationError(f"baseline=key key={key}")
        refs.append(VersionRef(key, version))
    if len(set(refs)) != len(refs) or not _unique_keys(refs):
        raise PublicationError("baseline=duplicate")
    return frozenset(refs)


def _unique_keys(refs: Sequence[VersionRef]) -> bool:
    return all(ref.version_id for ref in refs) and len({ref.key for ref in refs}) == len(refs)


def _candidate_member(
    archive: tarfile.TarFile, member: tarfile.TarInfo, contents: dict[str, bytes]
) -> ReleaseObject:
    if not member.isreg():
        raise CandidateError(f"member={member.name}")
    key = member.name.removeprefix("./")
    if key in contents or key.startswith("/") or ".." in key.split("/"):
        raise CandidateError(f"key={key}")
    stream = archive.extractfile(member)
    if stream is None:
        raise CandidateError(f"member={member.name}")
    body = stream.read(member.size + 1)
    item = describe_release_object(key, body)
    if item is None or len(body) != member.size:
        raise CandidateError(f"key={key}")
    contents[key] = body
    return item


def _reject_relative_references(index: bytes) -> None:
    for tag in _TAG.findall(index):
        for quote, value in _ATTRIBUTE.findall(tag):
            if not quote or not value.startswith(b"/") or value.startswith(b"//"):
                raise CandidateError("relative_reference")


def _version_pages(port: StaticPort, target: StaticTarget) -> list[Mapping[str, Any]]:
    pages: list[Mapping[str, Any]] = []
    marker: dict[str, Any] = {}
    for _ in range(_MAX_PAGES):
        port.guard()
        page = port.call("observer", "s3", "ListObjectVersions", {"Bucket": target.bucket} | marker)
        pages.append(page)
        if page.get("IsTruncated") is False:
            return pages
        marker = {"KeyMarker": page["NextKeyMarker"], "VersionIdMarker": page["NextVersionIdMarker"]}
    raise PublicationError("listing=pages")


def _head_checksum(port: StaticPort, target: StaticTarget, item: Mapping[str, Any]) -> str | None:
    port.guard()
    parameters = {"Bucket": target.bucket, "Key": item["Key"], "VersionId": item["VersionId"],
                  "ChecksumMode": "ENABLED"}
    result = port.call("observer", "s3", "HeadObject", parameters)
    if result.get("VersionId") != item["VersionId"]:
        raise PublicationError(f"head=version key={item['Key']}")
    return result.get("ChecksumSHA256")


def _incomplete_uploads(port: StaticPort, target: StaticTarget) -> list[tuple[str, int]]:
    port.guard()
    listing = port.call("observer", "s3", "ListMultipartUploads", {"Bucket": target.bucket})
    if listing.get("IsTruncated") is not False:
        raise PublicationError("multipart=truncated")
    result = []
    for upload in listing.get("Uploads", []):
        port.guard()
        parameters = {"Bucket": target.bucket, "Key": upload["Key"], "UploadId": upload["UploadId"]}
        parts = port.call("observer", "s3", "ListParts", parameters)
        if parts.get("IsTruncated") is not False:
            raise PublicationError(f"parts=truncated key={upload['Key']}")
        result.append((str(upload["Key"]), sum(int(p["Size"]) for p in parts.get("Parts", []))))
    return result


def _put(channel: StaticChannel, item: ReleaseObject, body: bytes, window: UploadWindow) -> str:
    parameters: dict[str, Any] = {
        "Bucket": channel.target.bucket,
        "Key": item.key,
        "Body": {"base64": b64(body)},
        "ContentType": item.content_type,
        "CacheControl": item.cache_control,
        "ChecksumSHA256": sha256_b64(body),
    }
    if item.key not in ENTRYPOINTS:
        parameters["IfNoneMatch"] = "*"
    result = call_within(channel.port, window, ("normal", "s3", "PutObject"), parameters)
    version = result.get("VersionId")
    if not isinstance(version, str) or not version:
        raise PublicationError(f"put=version key={item.key}")
    if result.get("ChecksumSHA256") != parameters["ChecksumSHA256"]:
        raise PublicationError(f"put=checksum key={item.key}")
    return version


def _readback(
    channel: StaticChannel, item: ReleaseObject, version: str, window: UploadWindow
) -> bytes:
    parameters = {"Bucket": channel.target.bucket, "Key": item.key}
    result = call_within(channel.port, window, ("normal", "s3", "GetObject"), parameters)
    body = base64.b64decode(result["Body"]["base64"], validate=True)
    if (
        result.get("VersionId") != version
        or hashlib.sha256(body).hexdigest() != item.sha256
        or result.get("ContentType") != item.content_type
        or result.get("CacheControl") != item.cache_control
    ):
        raise PublicationError(f"readback key={item.key}")
    return body


def _verify_css_references(objects: Mapping[str, ReleaseObject], bodies: Mapping[str, bytes]) -> None:
    for key, body in bodies.items():
        if not key.endswith(".css"):
            continue
        directory = key.rsplit("/", 1)[0]
        for _, raw in _CSS_URL.findall(body):
            value = raw.decode("utf-8", "replace").split("#", 1)[0].split("?", 1)[0]
            if value.startswith("data:") or not value:
                continue
            resolved = value[1:] if value.startswith("/") else f"{directory}/{value.removeprefix('./')}"
            if resolved not in objects:
                raise PublicationError(f"reference={resolved} css={key}")


def _invalidate(
    channel: StaticChannel, uploaded: Sequence[tuple[str, str]], window: UploadWindow
) -> str:
    reference = hashlib.sha256(json.dumps(sorted(uploaded)).encode()).hexdigest()[:32]
    parameters = {
        "DistributionId": channel.target.distribution_id,
        "InvalidationBatch": {
            "Paths": {"Quantity": len(INVALIDATION_PATHS), "Items": list(INVALIDATION_PATHS)},
            "CallerReference": reference,
        },
    }
    request = ("normal", "cloudfront", "CreateInvalidation")
    result = call_within(channel.port, window, request, parameters)
    return str(result["Invalidation"]["Id"])
