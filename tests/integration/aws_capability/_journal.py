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
from tests.integration.aws_capability._resource_ids import cognito_child
from tests.integration.aws_capability._safety import SERVICES, SafetyFailure, root_names, run_name
from tests.integration.aws_capability._service_cognito import _is_regional_pool_id
from tests.integration.aws_capability.contracts import Resource, RunSpec, ServiceFailure


@dataclass
class Journal:
    spec: RunSpec
    phase: str = "prepared"
    baseline: list[Resource] = field(default_factory=list)
    created: list[Resource] = field(default_factory=list)
    pending_service: str | None = None
    operation_state: str = "confirmed"
    dynamodb_delete_accepted: list[Resource] = field(default_factory=list)
    pending_resource: Resource | None = None
    pending_name: str | None = None

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
        if self.spec.probe == "cognito-resource-server":
            data["schema_version"] = 3
            data.pop("pending_service")
            if self.pending_service is not None:
                data["pending_service"] = self.pending_service
            if self.pending_resource is not None:
                data["pending_resource"] = asdict(self.pending_resource)
        if self.pending_name is not None and self.persists_pending_name():
            data["pending_name"] = self.pending_name
        if self.dynamodb_delete_accepted:
            data["dynamodb_delete_accepted"] = [
                asdict(ref) for ref in self.dynamodb_delete_accepted
            ]
        return json.dumps(data, allow_nan=False).encode("utf-8")

    def persists_pending_name(self) -> bool:
        return len(root_names(self.spec, self.pending_service or "")) > 1

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
        if self.spec.probe == "cognito-resource-server":
            candidate = Journal(self.spec)
            try:
                candidate.decode_server(data)
            except (KeyError, TypeError, ValueError):
                raise SafetyFailure("journal_corrupt") from None
            self.__dict__.update(vars(candidate))
            return
        self.decode_legacy(data)

    def decode_legacy(self, data: dict) -> None:
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
        self.pending_name = data.get("pending_name")
        self.operation_state = data["operation_state"]
        self.validate_legacy_state()
        self.validate_pending_name()
        self.baseline = decode_resources(data["baseline"])
        self.created = decode_resources(data["created"])
        self.dynamodb_delete_accepted = self.decode_dynamodb_deletes(
            data.get("dynamodb_delete_accepted", [])
        )

    def validate_legacy_state(self) -> None:
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

    def validate_pending_name(self) -> None:
        planned = root_names(self.spec, self.pending_service or "")
        if self.pending_name is None:
            if len(planned) > 1:
                raise SafetyFailure("journal_corrupt")
            return
        if self.pending_service is None or self.pending_name not in planned:
            raise SafetyFailure("journal_corrupt")

    def decode_server(self, data: dict) -> None:
        expected = {
            "schema_version": 3,
            "account": self.spec.account,
            "region": self.spec.region,
            "run_id": self.spec.run_id,
            "deadline": self.spec.deadline,
            **PROFILES[self.spec.probe].snapshot(self.spec.probe),
        }
        if type(data.get("schema_version")) is not int:
            raise SafetyFailure("journal_mismatch")
        if any(data.get(key) != value for key, value in expected.items()):
            raise SafetyFailure("journal_mismatch")
        if any(
            key in data and data[key] is None for key in ("pending_service", "pending_resource")
        ):
            raise SafetyFailure("journal_corrupt")
        self.phase, self.operation_state = data["phase"], data["operation_state"]
        self.baseline = decode_resources(data["baseline"])
        self.created = decode_resources(data["created"])
        self.pending_service = data.get("pending_service")
        if "pending_resource" in data:
            self.pending_resource = decode_resources([data["pending_resource"]])[0]
        self.validate_server_state()
        self.dynamodb_delete_accepted = self.decode_dynamodb_deletes(
            data.get("dynamodb_delete_accepted", [])
        )

    def validate_server_state(self) -> None:
        phases = {"prepared", "creating", "probing", "cleanup_requested", "complete"}
        if self.phase not in phases or self.operation_state not in {
            "unknown",
            "confirmed",
            "rejected",
        }:
            raise SafetyFailure("journal_corrupt")
        if self.created:
            self.validate_server_created()
        if self.operation_state == "unknown":
            self.validate_server_pending()
            return
        self.validate_server_resolved()

    def validate_server_resolved(self) -> None:
        if self.pending_service is not None or self.pending_resource is not None:
            raise SafetyFailure("journal_corrupt")
        if self.operation_state == "rejected":
            if self.phase != "cleanup_requested" or len(self.created) > 1:
                raise SafetyFailure("journal_corrupt")
            return
        valid = {
            "prepared": {0},
            "creating": {1, 2},
            "probing": {2},
            "cleanup_requested": {0, 1, 2},
            "complete": {0},
        }
        if len(self.created) not in valid[self.phase]:
            raise SafetyFailure("journal_corrupt")

    def validate_server_created(self) -> None:
        root = self.created[0]
        if (
            len(self.created) > 2
            or root.service != "cognito"
            or root.name != run_name(self.spec)
            or not _is_regional_pool_id(root.resource_id, self.spec.region)
            or any(self.is_baseline(ref) for ref in self.created)
        ):
            raise SafetyFailure("journal_corrupt")
        if len(self.created) == 2 and self.created[1] != self.server_child(root):
            raise SafetyFailure("journal_corrupt")

    def server_child(self, root: Resource) -> Resource:
        try:
            return cognito_child(
                root, self.spec, "cognito.server", {"identifier": "https://api.cnesdata.com.br"}
            )
        except ServiceFailure:
            raise SafetyFailure("journal_corrupt") from None

    def validate_server_pending(self) -> None:
        if self.phase not in {"creating", "cleanup_requested"} or self.pending_service != "cognito":
            raise SafetyFailure("journal_corrupt")
        if not self.created and self.pending_resource is None:
            return
        if len(self.created) != 1 or self.pending_resource != self.server_child(self.created[0]):
            raise SafetyFailure("journal_corrupt")

    def decode_dynamodb_deletes(self, rows: list) -> list[Resource]:
        resources = decode_resources(rows)
        identities = {(ref.service, ref.resource_id) for ref in resources}
        if len(identities) != len(resources):
            raise SafetyFailure("journal_corrupt")
        if resources and (self.operation_state != "confirmed" or self.phase == "complete"):
            raise SafetyFailure("journal_corrupt")
        for ref in resources:
            if ref.service != "dynamodb" or ref not in self.created or self.is_baseline(ref):
                raise SafetyFailure("journal_corrupt")
        return resources

    def require_dynamodb_created(self, resource: Resource) -> None:
        if (
            resource.service != "dynamodb"
            or resource not in self.created
            or self.is_baseline(resource)
            or self.operation_state != "confirmed"
            or self.phase == "complete"
        ):
            raise SafetyFailure("creation_uncertain")

    def authorize_dynamodb_delete(self, resource: Resource) -> None:
        self.require_dynamodb_created(resource)
        saved = Journal(self.spec)
        saved.load()
        saved.require_dynamodb_created(resource)
        if (
            resource in self.dynamodb_delete_accepted
            and resource not in saved.dynamodb_delete_accepted
        ):
            raise SafetyFailure("creation_uncertain")

    def record_dynamodb_delete(self, resource: Resource) -> None:
        self.require_dynamodb_created(resource)
        if resource in self.dynamodb_delete_accepted:
            return
        previous = self.dynamodb_delete_accepted
        self.dynamodb_delete_accepted = self.decode_dynamodb_deletes(
            [asdict(ref) for ref in [*previous, resource]]
        )
        try:
            self.save()
        except OSError:
            self.dynamodb_delete_accepted = previous
            raise

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
