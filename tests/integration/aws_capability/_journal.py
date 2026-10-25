"""Journal privado com substituição atômica e lease exclusivo por execução."""

import fcntl
import json
import os
import stat
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path

from tests.integration.aws_capability._profiles import PROFILES
from tests.integration.aws_capability._safety import SERVICES, SafetyFailure
from tests.integration.aws_capability.contracts import Resource, RunSpec


@dataclass
class Journal:
    spec: RunSpec
    phase: str = "prepared"
    baseline: list[Resource] = field(default_factory=list)
    created: list[Resource] = field(default_factory=list)
    pending_service: str | None = None
    operation_state: str = "confirmed"

    @property
    def path(self) -> Path:
        return self.spec.journal_dir / f"{self.spec.run_id}.json"

    def content(self) -> bytes:
        data = {
            "schema_version": 1,
            "account": self.spec.account,
            "region": self.spec.region,
            "run_id": self.spec.run_id,
            "deadline": self.spec.deadline,
            "phase": self.phase,
            "baseline": [asdict(ref) for ref in self.baseline],
            "created": [asdict(ref) for ref in self.created],
            "pending_service": self.pending_service,
            "operation_state": self.operation_state,
        }
        if self.spec.probe != "all":
            data.update(schema_version=2, **PROFILES[self.spec.probe].snapshot(self.spec.probe))
        return json.dumps(data, allow_nan=False).encode("utf-8")

    def save(self) -> None:
        descriptor, temporary = tempfile.mkstemp(dir=self.spec.journal_dir, prefix=".journal-")
        try:
            data = self.content()
            while data:
                written = os.write(descriptor, data)
                if written <= 0:
                    raise OSError("journal=short_write")
                data = data[written:]
            os.fsync(descriptor)
            os.replace(temporary, self.path)
            sync_directory(self.spec.journal_dir)
        finally:
            os.close(descriptor)
            Path(temporary).unlink(missing_ok=True)

    def load(self) -> None:
        if not self.path.exists():
            raise SafetyFailure("journal_missing")
        try:
            descriptor = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(descriptor, "r", encoding="utf-8") as handle:
                verify_private(os.fstat(handle.fileno()), directory=False)
                data = json.load(handle)
            self.decode(data)
        except (OSError, ValueError, TypeError, KeyError):
            raise SafetyFailure("journal_corrupt") from None

    def decode(self, data: dict) -> None:
        if not isinstance(data, dict):
            raise SafetyFailure("journal_corrupt")
        expected = {
            "schema_version": 1 if self.spec.probe == "all" else 2,
            "account": self.spec.account,
            "region": self.spec.region,
            "run_id": self.spec.run_id,
            "deadline": self.spec.deadline,
        }
        if self.spec.probe == "all":
            if data.get("probe", "all") != "all":
                raise SafetyFailure("journal_mismatch")
        else:
            expected.update(PROFILES[self.spec.probe].snapshot(self.spec.probe))
        if any(data.get(key) != value for key, value in expected.items()):
            raise SafetyFailure("journal_mismatch")
        self.phase = data["phase"]
        self.pending_service = data["pending_service"]
        self.operation_state = data["operation_state"]
        if self.phase not in {"prepared", "creating", "probing", "cleanup_requested", "complete"}:
            raise SafetyFailure("journal_corrupt")
        if self.operation_state not in {"unknown", "confirmed", "rejected"}:
            raise SafetyFailure("journal_corrupt")
        if self.pending_service is not None and self.pending_service not in SERVICES:
            raise SafetyFailure("journal_corrupt")
        if (self.operation_state == "unknown") != (self.pending_service is not None):
            raise SafetyFailure("journal_corrupt")
        if self.phase == "complete" and self.pending_service is not None:
            raise SafetyFailure("journal_corrupt")
        self.baseline = decode_resources(data["baseline"])
        self.created = decode_resources(data["created"])

    def is_baseline(self, resource: Resource) -> bool:
        return any(
            (ref.service, ref.resource_id) == (resource.service, resource.resource_id)
            for ref in self.baseline
        )


def decode_resources(rows: list) -> list[Resource]:
    if not isinstance(rows, list):
        raise SafetyFailure("journal_corrupt")
    resources = [Resource(**row) for row in rows]
    for resource in resources:
        if resource.service not in SERVICES or not resource.resource_id or not resource.name:
            raise SafetyFailure("journal_corrupt")
        if not all(isinstance(value, str) for value in asdict(resource).values()):
            raise SafetyFailure("journal_corrupt")
    return resources


def verify_private(info: os.stat_result, *, directory: bool) -> None:
    valid_type = stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
    if not valid_type or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise SafetyFailure("journal_unavailable")


def prepare_directory(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    verify_private(path.lstat(), directory=True)


def sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


@contextmanager
def lease(spec: RunSpec) -> Iterator[None]:
    path = spec.journal_dir / f"{spec.run_id}.lock"
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
            raise SafetyFailure("journal_unavailable")
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        os.fchmod(descriptor, 0o600)
        yield
    finally:
        os.close(descriptor)
