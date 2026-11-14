"""Archive de host EXT-3: manifest, compose/config e envelope canônicos, sem segredo."""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import re
import tarfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping

REPOSITORY = "VINIClUS/CnesData"
CALLER_WORKFLOW = ".github/workflows/promote-production.yml"
# Single caller-ref point; the owner amendment of the workflow track moves it to main.
CALLER_REF = "refs/heads/develop"
PRODUCER_WORKFLOWS = frozenset({CALLER_WORKFLOW})
ACTIONS = frozenset({"validate", "deploy", "rollback-previous"})
MEMBERS = ("release-manifest.json", "compose.yml", "config.json")
LIMITS = {"release-manifest.json": 65536, "compose.yml": 1024**2, "config.json": 16 * 1024**2}
ARCHIVE_LIMIT = 16 * 1024**2
ENVELOPE_LIMIT = 65536
MAX_LIFETIME = timedelta(hours=24)
CREDENTIALS = ("/run/personal-platform/credentials/cnesdata/current", "/run/credentials")
CONFIG_MOUNT = ("./config.json", "/run/config/config.json")
_IMAGE = re.compile(r"ghcr\.io/viniclus/cnesdata/central_api@sha256:[0-9a-f]{64}")
_DIGEST = re.compile(r"[0-9a-f]{64}")
_PLAN = ("evidence_id", "evidence_sha256", "inventory_sha256", "dependencies_sha256",
         "host_preflight_sha256", "host_key_fingerprint")
_SECRETS = (
    re.compile(rb"\b(?:AK" rb"IA|AS" rb"IA)[0-9A-Z]{16}\b"),
    re.compile(rb"-----BEGIN " rb"[A-Z ]*PRIVATE KEY-----"),
    re.compile(rb"(?<![A-Za-z0-9_])(?:(?:sk|rk)_(?:live|test)_|whsec_)[A-Za-z0-9]"),
    re.compile(rb"(?<![A-Za-z0-9_])(?:gh[opsur]_[A-Za-z0-9]{20,}|github_pat_)"),
)


class ArchiveError(ValueError):
    pass


@dataclass(frozen=True)
class HostRelease:
    source_sha: str
    api_image: str
    platform_plan: Mapping[str, str]
    requested_action: str
    expires_at: datetime
    static_release: str | None = None


def canonical(value: Any) -> bytes:
    text = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return (text + "\n").encode("utf-8")


def render_compose(image: str, port: int) -> bytes:
    """Returns:
    Compose canônico (JSON é YAML válido) no escopo aceito pelo host gate.
    Raises:
    ArchiveError: imagem sem digest GHCR do API ou porta fora do intervalo.
    """
    if _IMAGE.fullmatch(image) is None:
        raise ArchiveError("image=not_pinned")
    if not 1024 < port < 65536:
        raise ArchiveError("port=range")
    mounts = [
        {"type": "bind", "source": source, "target": target, "read_only": True}
        for source, target in (CREDENTIALS, CONFIG_MOUNT)
    ]
    service = {
        "image": image,
        "user": "10001:10001",
        "init": True,
        "restart": "unless-stopped",
        "read_only": True,
        "cap_drop": ["ALL"],
        "security_opt": ["no-new-privileges:true"],
        "cpus": 1.0,
        "mem_limit": "1g",
        "pids_limit": 256,
        "ports": [{"host_ip": "127.0.0.1", "published": port, "target": 8000, "protocol": "tcp"}],
        "networks": ["cnesdata"],
        "volumes": mounts,
        "healthcheck": {"test": ["CMD", "python", "-c", _HEALTH], "interval": "15s",
                        "timeout": "15s", "retries": 3, "start_period": "60s"},
        "logging": {"driver": "json-file", "options": {"max-size": "10m", "max-file": "3"}},
    }
    value = {"name": "cnesdata", "services": {"api": service},
             "networks": {"cnesdata": {"name": "cnesdata"}}}
    return canonical(value)


def host_manifest(
    release: HostRelease, compose: bytes, config: bytes, now: datetime
) -> dict[str, Any]:
    """Returns:
    release_manifest fechado do envelope v1, ligado aos bytes de compose e config.
    Raises:
    ArchiveError: campo fora do contrato do host, segredo ou credencial estática.
    """
    _require_release(release, now)
    _require_clean(compose, config)
    manifest: dict[str, Any] = {
        "project": "cnesdata",
        "source_repository": REPOSITORY,
        "source_sha": release.source_sha,
        "expires_at": release.expires_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "platform_plan": dict(release.platform_plan),
        "images": {"api": release.api_image},
        "compose_sha256": hashlib.sha256(compose).hexdigest(),
        "config_sha256": hashlib.sha256(config).hexdigest(),
        "requested_action": release.requested_action,
    }
    if release.static_release is not None:
        manifest["static_release"] = release.static_release
    if len(canonical(manifest)) > LIMITS["release-manifest.json"]:
        raise ArchiveError("manifest=size")
    return manifest


def build_archive(manifest: Mapping[str, Any], compose: bytes, config: bytes) -> bytes:
    """Returns:
    tar.gz determinístico com os três membros exatos, sem extensão PAX/GNU.
    """
    files = dict(zip(MEMBERS, (canonical(manifest), compose, config), strict=True))
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w", format=tarfile.USTAR_FORMAT) as tar:
        for name in MEMBERS:
            info = tarfile.TarInfo(name)
            info.size, info.mode, info.mtime = len(files[name]), 0o644, 0
            info.uname = info.gname = ""
            tar.addfile(info, io.BytesIO(files[name]))
    packed = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=packed, mtime=0) as stream:
        stream.write(raw.getvalue())
    result = packed.getvalue()
    verify_archive(result, manifest)
    return result


def verify_archive(raw: bytes, manifest: Mapping[str, Any]) -> dict[str, bytes]:
    """Returns:
    Membros do archive, conferidos com as regras do host gate.
    Raises:
    ArchiveError: tamanho, membro, extensão, padding ou digest divergente.
    """
    result = _unpack(raw)
    if result["release-manifest.json"] != canonical(manifest):
        raise ArchiveError("manifest=embedded")
    if (
        hashlib.sha256(result["compose.yml"]).hexdigest() != manifest.get("compose_sha256")
        or hashlib.sha256(result["config.json"]).hexdigest() != manifest.get("config_sha256")
    ):
        raise ArchiveError("manifest=digest")
    return result


def archive_manifest(raw: bytes) -> dict[str, Any]:
    """Returns:
    release_manifest embutido, aceito só se o archive inteiro confere com ele.
    Raises:
    ArchiveError: archive inválido, manifest não canônico ou digest divergente.
    """
    try:
        manifest = json.loads(_unpack(raw)["release-manifest.json"])
    except (UnicodeDecodeError, ValueError) as error:
        raise ArchiveError("manifest=json") from error
    if not isinstance(manifest, dict):
        raise ArchiveError("manifest=object")
    verify_archive(raw, manifest)
    return manifest


def _unpack(raw: bytes) -> dict[str, bytes]:
    if len(raw) > ARCHIVE_LIMIT:
        raise ArchiveError("archive=size")
    try:
        unpacked = gzip.decompress(raw)
        if len(unpacked) > ARCHIVE_LIMIT:
            raise ArchiveError("archive=expansion")
        return _members(unpacked)
    except (OSError, EOFError, tarfile.TarError) as error:
        raise ArchiveError("archive=invalid") from error


def build_envelope(ci_evidence: Mapping[str, Any], manifest: Mapping[str, Any], now: datetime) -> bytes:
    """Returns:
    Envelope canônico v1 ligando run, artefato e digest exatos ao manifest.
    Raises:
    ArchiveError: evidência de CI fora do contrato ou manifest expirado.
    """
    if not _valid_ci(ci_evidence):
        raise ArchiveError("ci_evidence=invalid")
    expires = datetime.strptime(manifest["expires_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    if not now < expires <= now + MAX_LIFETIME:
        raise ArchiveError("expires_at=window")
    raw = canonical({"schema_version": 1, "ci_evidence": dict(ci_evidence),
                     "release_manifest": dict(manifest)})
    if len(raw) > ENVELOPE_LIMIT:
        raise ArchiveError("envelope=size")
    return raw


def _members(unpacked: bytes) -> dict[str, bytes]:
    result: dict[str, bytes] = {}
    with tarfile.open(fileobj=io.BytesIO(unpacked), mode="r:") as tar:
        for item in tar:
            if (
                item.name not in LIMITS
                or item.name in result
                or item.type != tarfile.REGTYPE
                or item.pax_headers
                or item.linkname
                or not 0 < item.size <= LIMITS[item.name]
                or item.offset != item.offset_data - 512
            ):
                raise ArchiveError(f"archive=member name={item.name}")
            stream = tar.extractfile(item)
            data = stream.read(item.size + 1) if stream is not None else b""
            if len(data) != item.size:
                raise ArchiveError(f"archive=truncated name={item.name}")
            result[item.name] = data
        tail = unpacked[tar.offset :]
    if set(result) != set(MEMBERS) or len(tail) < 1024 or any(tail):
        raise ArchiveError("archive=layout")
    return result


def _require_release(release: HostRelease, now: datetime) -> None:
    if re.fullmatch(r"[0-9a-f]{40}", release.source_sha) is None:
        raise ArchiveError("source_sha=invalid")
    if _IMAGE.fullmatch(release.api_image) is None:
        raise ArchiveError("image=not_pinned")
    if release.requested_action not in ACTIONS:
        raise ArchiveError("requested_action=invalid")
    if not now < release.expires_at <= now + MAX_LIFETIME:
        raise ArchiveError("expires_at=window")
    plan = release.platform_plan
    if tuple(sorted(plan)) != tuple(sorted(_PLAN)) or not all(
        _DIGEST.fullmatch(plan[key]) if key.endswith("sha256") else _opaque(plan[key])
        for key in _PLAN
    ):
        raise ArchiveError("platform_plan=invalid")
    if release.static_release is not None and not _opaque(release.static_release):
        raise ArchiveError("static_release=invalid")


def _require_clean(compose: bytes, config: bytes) -> None:
    for name, body in (("compose.yml", compose), ("config.json", config)):
        if not 0 < len(body) <= LIMITS[name]:
            raise ArchiveError(f"{name}=size")
        if any(pattern.search(body) for pattern in _SECRETS):
            raise ArchiveError(f"{name}=secret")
    try:
        service = json.loads(compose)["services"]["api"]
        config_value = json.loads(config)
    except (ValueError, KeyError, TypeError) as error:
        raise ArchiveError("compose.yml=structure") from error
    if not isinstance(config_value, dict):
        raise ArchiveError("config.json=structure")
    # The AWS profile is baked into the image; any compose env could reopen a credential provider.
    if "environment" in service:
        raise ArchiveError("compose.yml=environment")


def _valid_ci(value: Mapping[str, Any]) -> bool:
    fields = {"repository", "workflow", "run_id", "artifact_id", "artifact_digest"}
    digest = value.get("artifact_digest")
    return (
        set(value) == fields
        and value["repository"] == REPOSITORY
        and value["workflow"] in PRODUCER_WORKFLOWS
        and all(type(value[key]) is int and value[key] >= 1 for key in ("run_id", "artifact_id"))
        and isinstance(digest, str)
        and digest.startswith("sha256:")
        and _DIGEST.fullmatch(digest.removeprefix("sha256:")) is not None
    )


def _opaque(value: Any) -> bool:
    return (
        isinstance(value, str)
        and 0 < len(value.encode("utf-8")) <= 256
        and not any(ord(c) < 32 or ord(c) == 127 for c in value)
    )


_HEALTH = (
    "import urllib.request;"
    "urllib.request.urlopen('http://127.0.0.1:8000/api/v1/system/health', timeout=10)"
)
