"""Contrato puro de publicação, retenção e recusa da release estática."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from hashlib import sha256
from re import compile as compile_pattern

_INDEX = "index.html"
_ENTRYPOINTS = frozenset({_INDEX, "theme-init.js"})
_ASSET_PREFIX = "assets/"
_ASSET_KEY = compile_pattern(r"assets/[A-Za-z0-9_-]+-[A-Za-z0-9_-]{8}\.(?:js|css|woff2)")
_IMMUTABLE_CACHE = "public,max-age=31536000,immutable"
_ENTRYPOINT_CACHE = "no-cache"
_CONTENT_TYPES = {
    "html": "text/html; charset=utf-8",
    "js": "text/javascript; charset=utf-8",
    "css": "text/css; charset=utf-8",
    "woff2": "font/woff2",
}
_SHA256 = compile_pattern(r"[0-9a-f]{64}")
_REFERENCE = compile_pattern(rb"""<(?:script|link)\b[^>]*?\b(?:src|href)=["']/([^"'/][^"']*)["']""")
_SECRET_MARKERS = (
    ("aws_access_key", compile_pattern(rb"\b(?:AK" rb"IA|AS" rb"IA)[0-9A-Z]{16}\b")),
    ("private_key", compile_pattern(rb"-----BEGIN " rb"[A-Z ]*PRIVATE KEY-----")),
    (
        "stripe_secret",
        compile_pattern(rb"(?<![A-Za-z0-9_])(?:(?:sk|rk)_(?:live|test)_|whsec_)[A-Za-z0-9]"),
    ),
)
_CAPACITY_BYTES = 5_000_000_000
_RETENTION = timedelta(days=7)


@dataclass(frozen=True, order=True)
class VersionRef:
    key: str
    version_id: str


@dataclass(frozen=True)
class StoredVersion:
    ref: VersionRef
    size: int
    sha256: str | None
    modified_at: datetime
    superseded_at: datetime | None


@dataclass(frozen=True)
class StaticBucketSnapshot:
    versions: tuple[StoredVersion, ...]
    incomplete_upload_bytes: int
    current: frozenset[VersionRef]
    prior: frozenset[VersionRef]
    complete: bool


@dataclass(frozen=True)
class ReleaseObject:
    key: str
    size: int
    sha256: str
    content_type: str
    cache_control: str
    references: frozenset[str]
    secret_markers: frozenset[str]


class ReleaseOutcome(StrEnum):
    PLANNED = "PLANNED"
    REJECTED_INCOMPLETE = "REJECTED_INCOMPLETE"
    REJECTED_INVALID = "REJECTED_INVALID"
    REJECTED_SECRET = "REJECTED_SECRET"  # noqa: S105
    REJECTED_OVERWRITE = "REJECTED_OVERWRITE"
    REJECTED_CAPACITY = "REJECTED_CAPACITY"


@dataclass(frozen=True)
class StaticReleaseDecision:
    outcome: ReleaseOutcome
    upload_order: tuple[str, ...] = ()
    already_published: tuple[str, ...] = ()
    retire: tuple[VersionRef, ...] = ()
    retire_latest: tuple[VersionRef, ...] = ()
    rollback_entrypoint: VersionRef | None = None
    projected_bytes: int = 0


def describe_release_object(key: str, content: bytes) -> ReleaseObject | None:
    """Returns:
    Descrição sem o conteúdo; None se a chave estiver fora do contrato.
    """
    headers = _headers(key)
    if headers is None:
        return None
    content_type, cache_control = headers
    return ReleaseObject(
        key=key,
        size=len(content),
        sha256=sha256(content).hexdigest(),
        content_type=content_type,
        cache_control=cache_control,
        references=_references(key, content),
        secret_markers=_secret_markers(content),
    )


def plan_static_release(
    snapshot: StaticBucketSnapshot,
    candidate: tuple[ReleaseObject, ...],
    now: datetime,
) -> StaticReleaseDecision:
    """Returns:
    Proposta determinística; recusas não propõem upload nem rollback.
    """
    if not _inventory_complete(snapshot):
        return StaticReleaseDecision(ReleaseOutcome.REJECTED_INCOMPLETE)
    if not _valid_snapshot(snapshot, now) or not _valid_candidate(candidate):
        return StaticReleaseDecision(ReleaseOutcome.REJECTED_INVALID)
    if any(item.secret_markers for item in candidate):
        return StaticReleaseDecision(ReleaseOutcome.REJECTED_SECRET)
    latest = {
        version.ref.key: version for version in snapshot.versions if version.superseded_at is None
    }
    if any(_overwrites(item, latest) for item in candidate):
        return StaticReleaseDecision(ReleaseOutcome.REJECTED_OVERWRITE)
    return _capacity_decision(snapshot, candidate, latest, now)


def _capacity_decision(
    snapshot: StaticBucketSnapshot,
    candidate: tuple[ReleaseObject, ...],
    latest: dict[str, StoredVersion],
    now: datetime,
) -> StaticReleaseDecision:
    published = tuple(sorted(item.key for item in candidate if _is_published(item, latest)))
    pending = tuple(item for item in candidate if item.key not in published)
    stored = sum(version.size for version in snapshot.versions)
    projected = stored + snapshot.incomplete_upload_bytes + sum(item.size for item in pending)
    retire = _retire(snapshot, now)
    retire_latest = _retire_latest(snapshot, candidate, now)
    if projected >= _CAPACITY_BYTES:
        return StaticReleaseDecision(
            ReleaseOutcome.REJECTED_CAPACITY,
            retire=retire,
            retire_latest=retire_latest,
            projected_bytes=projected,
        )
    return StaticReleaseDecision(
        ReleaseOutcome.PLANNED,
        upload_order=_upload_order(pending),
        already_published=published,
        retire=retire,
        retire_latest=retire_latest,
        rollback_entrypoint=_rollback_entrypoint(snapshot.current),
        projected_bytes=projected,
    )


def _headers(key: str) -> tuple[str, str] | None:
    if key in _ENTRYPOINTS:
        cache_control = _ENTRYPOINT_CACHE
    elif _ASSET_KEY.fullmatch(key) is not None:
        cache_control = _IMMUTABLE_CACHE
    else:
        return None
    return _CONTENT_TYPES[key.rsplit(".", 1)[1]], cache_control


def _references(key: str, content: bytes) -> frozenset[str]:
    if key != _INDEX:
        return frozenset()
    return frozenset(found.decode("utf-8", "replace") for found in _REFERENCE.findall(content))


def _secret_markers(content: bytes) -> frozenset[str]:
    return frozenset(label for label, pattern in _SECRET_MARKERS if pattern.search(content))


def _inventory_complete(snapshot: StaticBucketSnapshot) -> bool:
    listed = {version.ref for version in snapshot.versions}
    return snapshot.complete and (snapshot.current | snapshot.prior) <= listed


def _valid_snapshot(snapshot: StaticBucketSnapshot, now: datetime) -> bool:
    return (
        now.utcoffset() is not None
        and snapshot.incomplete_upload_bytes >= 0
        and all(_valid_version(version, now) for version in snapshot.versions)
        and _unique_versions(snapshot.versions)
        and _valid_references(snapshot)
    )


def _valid_version(version: StoredVersion, now: datetime) -> bool:
    superseded = version.superseded_at
    return (
        version.size >= 0
        and _not_after(version.modified_at, now)
        and (superseded is None or _not_after(superseded, now))
    )


def _not_after(timestamp: datetime, now: datetime) -> bool:
    return timestamp.utcoffset() is not None and timestamp <= now


def _unique_versions(versions: tuple[StoredVersion, ...]) -> bool:
    refs = [version.ref for version in versions]
    latest = [version.ref.key for version in versions if version.superseded_at is None]
    return len(set(refs)) == len(refs) and len(set(latest)) == len(latest)


def _valid_references(snapshot: StaticBucketSnapshot) -> bool:
    if snapshot.prior and not snapshot.current:
        return False
    return all(
        not refs or sum(ref.key == _INDEX for ref in refs) == 1
        for refs in (snapshot.current, snapshot.prior)
    )


def _valid_candidate(candidate: tuple[ReleaseObject, ...]) -> bool:
    keys = [item.key for item in candidate]
    unique = frozenset(keys)
    return (
        len(unique) == len(keys)
        and _INDEX in unique
        and all(_valid_object(item) for item in candidate)
        and all(item.references <= unique for item in candidate)
    )


def _valid_object(item: ReleaseObject) -> bool:
    return (
        item.size >= 0
        and _SHA256.fullmatch(item.sha256) is not None
        and _headers(item.key) == (item.content_type, item.cache_control)
    )


def _overwrites(item: ReleaseObject, latest: dict[str, StoredVersion]) -> bool:
    published = latest.get(item.key)
    return (
        item.cache_control == _IMMUTABLE_CACHE
        and published is not None
        and published.sha256 != item.sha256
    )


def _is_published(item: ReleaseObject, latest: dict[str, StoredVersion]) -> bool:
    published = latest.get(item.key)
    return published is not None and published.sha256 == item.sha256


def _retire(snapshot: StaticBucketSnapshot, now: datetime) -> tuple[VersionRef, ...]:
    protected = snapshot.current | snapshot.prior
    return tuple(
        sorted(
            version.ref
            for version in snapshot.versions
            if version.superseded_at is not None
            and version.ref not in protected
            and now - version.superseded_at > _RETENTION
        )
    )


def _retire_latest(
    snapshot: StaticBucketSnapshot, candidate: tuple[ReleaseObject, ...], now: datetime
) -> tuple[VersionRef, ...]:
    kept = {ref.key for ref in snapshot.current | snapshot.prior}
    kept.update(item.key for item in candidate)
    return tuple(
        sorted(
            version.ref
            for version in snapshot.versions
            if version.superseded_at is None
            and version.ref.key.startswith(_ASSET_PREFIX)
            and version.ref.key not in kept
            and now - version.modified_at > _RETENTION
        )
    )


def _upload_order(pending: tuple[ReleaseObject, ...]) -> tuple[str, ...]:
    return tuple(
        sorted(
            (item.key for item in pending),
            key=lambda key: (key == _INDEX, key in _ENTRYPOINTS, key),
        )
    )


def _rollback_entrypoint(current: frozenset[VersionRef]) -> VersionRef | None:
    return next((ref for ref in current if ref.key == _INDEX), None)
