"""Fakes de boundary S3/CloudFront do publicador estático no formato do context ABI2."""

from __future__ import annotations

import base64
import hashlib
import io
import json
import re
import tarfile
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

NOW = datetime(2026, 11, 13, 12, 0, tzinfo=UTC)
BUCKET = "cnesdata-prod-static-web"
DISTRIBUTION = "E2EXAMPLE123"
INDEX_JS = "assets/index-AbCd1234.js"
INDEX_CSS = "assets/index-EfGh5678.css"
FONT = "assets/inter-IjKl9012.woff2"


def stamp(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def checksum(body: bytes) -> str:
    return base64.b64encode(hashlib.sha256(body).digest()).decode()


def sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def index_html(*refs: str, theme: str = "/theme-init.js") -> bytes:
    tags = "".join(f'<script type="module" crossorigin src="/{ref}"></script>' for ref in refs)
    return (f'<!doctype html><html><head><script src="{theme}"></script>{tags}'
            '</head><body><div id="root"></div></body></html>').encode()


def release_files(tag: str = "v1", *, font: bool = True) -> dict[str, bytes]:
    css = f"@font-face{{src:url(/{FONT})}} .{tag}{{}}" if font else f".{tag}{{}}"
    files = {
        "index.html": index_html(INDEX_JS, INDEX_CSS) + tag.encode(),
        "theme-init.js": f"/*{tag}*/".encode(),
        INDEX_JS: f"console.log('{tag}')".encode(),
        INDEX_CSS: css.encode(),
    }
    if font:
        files[FONT] = b"wOF2" + tag.encode()
    return files


def dist_tar(files: dict[str, bytes], *, extra: list[tarfile.TarInfo] | None = None) -> bytes:
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w", format=tarfile.USTAR_FORMAT) as tar:
        for directory in ("./", "./assets/"):
            info = tarfile.TarInfo(directory.rstrip("/") or ".")
            info.type = tarfile.DIRTYPE
            tar.addfile(info)
        for name, body in sorted(files.items()):
            info = tarfile.TarInfo("./" + name)
            info.size = len(body)
            tar.addfile(info, io.BytesIO(body))
        for info in extra or []:
            tar.addfile(info)
    return raw.getvalue()


def baseline(current: list[tuple[str, str]] | None, prior: list[tuple[str, str]] | None) -> bytes:
    def release(refs: list[tuple[str, str]] | None, name: str) -> dict[str, Any] | None:
        if refs is None:
            return None
        objects = [{"key": key, "version_id": version} for key, version in sorted(refs)]
        return {"release_id": name, "objects": objects}

    value = {"schema": "cnesdata.static-baseline/v1", "current": release(current, "current"),
             "prior": release(prior, "prior")}
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


@dataclass
class Entry:
    version_id: str
    body: bytes | None
    modified: datetime
    content_type: str = ""
    cache_control: str = ""
    checksum: str | None = None


@dataclass
class FakeStaticPort:
    """Bucket versionado + CloudFront mínimo; cada call avança o relógio em `latency`."""

    clock: datetime = NOW
    objects: dict[str, list[Entry]] = field(default_factory=dict)
    uploads: list[tuple[str, str, list[int]]] = field(default_factory=list)
    calls: list[tuple[str, str, str, dict[str, Any]]] = field(default_factory=list)
    latency: dict[str, timedelta] = field(default_factory=dict)
    corrupt: set[str] = field(default_factory=set)
    guards: int = 0
    serial: int = 0

    def now(self) -> datetime:
        return self.clock

    def guard(self) -> None:
        self.guards += 1

    def seed(self, key: str, body: bytes | None, modified: datetime, *, known: bool = True) -> str:
        self.serial += 1
        version = f"v{self.serial:04d}"
        digest = checksum(body) if body is not None and known else None
        entry = Entry(version, body, modified, checksum=digest)
        self.objects.setdefault(key, []).append(entry)
        return version

    def latest(self, key: str) -> Entry | None:
        entries = self.objects.get(key, [])
        return entries[-1] if entries else None

    def writes(self, operation: str = "PutObject") -> list[str]:
        return [params["Key"] for _, _, op, params in self.calls if op == operation]

    def call(self, role: str, service: str, operation: str, parameters: dict[str, Any]) -> Any:
        self.calls.append((role, service, operation, json.loads(json.dumps(parameters))))
        key = parameters.get("Key", "")
        self.clock += self.latency.get(key, timedelta(seconds=1))
        handler = getattr(self, "_" + re.sub(r"(?<!^)(?=[A-Z])", "_", operation).lower())
        return handler(parameters)

    def _list_object_versions(self, params: dict[str, Any]) -> dict[str, Any]:
        versions, markers = [], []
        for key in sorted(self.objects):
            entries = self.objects[key]
            for position, entry in enumerate(reversed(entries)):
                item = {"Key": key, "VersionId": entry.version_id, "IsLatest": position == 0,
                        "LastModified": stamp(entry.modified)}
                if entry.body is None:
                    markers.append(item)
                else:
                    versions.append(item | {"Size": len(entry.body), "ETag": '"e"'})
        return {"Versions": versions, "DeleteMarkers": markers, "IsTruncated": False}

    def _find(self, key: str, version: str) -> Entry:
        return next(e for e in self.objects[key] if e.version_id == version)

    def _head_object(self, params: dict[str, Any]) -> dict[str, Any]:
        entry = self._find(params["Key"], params["VersionId"])
        body = entry.body or b""
        result = {"VersionId": entry.version_id, "ContentLength": len(body)}
        return result | ({"ChecksumSHA256": entry.checksum} if entry.checksum else {})

    def _list_multipart_uploads(self, params: dict[str, Any]) -> dict[str, Any]:
        items = [{"Key": key, "UploadId": upload} for key, upload, _ in self.uploads]
        return {"Uploads": items, "IsTruncated": False}

    def _list_parts(self, params: dict[str, Any]) -> dict[str, Any]:
        sizes = next(s for k, u, s in self.uploads if (k, u) == (params["Key"], params["UploadId"]))
        return {"Parts": [{"PartNumber": n + 1, "Size": s} for n, s in enumerate(sizes)],
                "IsTruncated": False}

    def _put_object(self, params: dict[str, Any]) -> dict[str, Any]:
        body = base64.b64decode(params["Body"]["base64"], validate=True)
        assert params["ChecksumSHA256"] == checksum(body), "checksum_divergente"
        current = self.latest(params["Key"])
        if params.get("IfNoneMatch") == "*" and current is not None and current.body is not None:
            raise RuntimeError("PreconditionFailed")
        self.serial += 1
        entry = Entry(f"v{self.serial:04d}", body, self.clock, params["ContentType"],
                      params["CacheControl"], checksum(body))
        self.objects.setdefault(params["Key"], []).append(entry)
        return {"VersionId": entry.version_id, "ChecksumSHA256": entry.checksum}

    def _get_object(self, params: dict[str, Any]) -> dict[str, Any]:
        entry = self.latest(params["Key"])
        assert entry is not None
        assert entry.body is not None
        body = entry.body + (b"!" if params["Key"] in self.corrupt else b"")
        return {"Body": {"base64": base64.b64encode(body).decode()}, "VersionId": entry.version_id,
                "ContentType": entry.content_type, "CacheControl": entry.cache_control}

    def _copy_object(self, params: dict[str, Any]) -> dict[str, Any]:
        source = params["CopySource"]
        origin = self._find(source["Key"], source["VersionId"])
        self.serial += 1
        entry = Entry(f"v{self.serial:04d}", origin.body, self.clock, origin.content_type,
                      origin.cache_control, origin.checksum)
        self.objects[params["Key"]].append(entry)
        return {"VersionId": entry.version_id,
                "CopyObjectResult": {"ChecksumSHA256": entry.checksum}}

    def _create_invalidation(self, params: dict[str, Any]) -> dict[str, Any]:
        return {"Invalidation": {"Id": "I" + str(len(self.calls)), "Status": "InProgress"}}
