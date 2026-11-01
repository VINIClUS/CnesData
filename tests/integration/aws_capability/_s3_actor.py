"""Ator S3 de probe: mesmas operações para o operador e para a sessão do role."""

from dataclasses import dataclass, field

from tests.integration.aws_capability._probe_attempt import Attempt, attempt, stamp
from tests.integration.aws_capability._service_contracts import CommandPort
from tests.integration.aws_capability.cli import BODY_FIXTURE, Request
from tests.integration.aws_capability.contracts import RequestContext


@dataclass(frozen=True)
class Actor:
    commands: CommandPort = field(repr=False)
    bucket: str
    account: str
    context: RequestContext

    def _run(self, operation: str, payload: dict[str, object]) -> Attempt:
        body = {"Bucket": self.bucket, **payload, "ExpectedBucketOwner": self.account}
        return attempt(self.commands, Request("s3api", operation, body), self.context)

    def put(self, key: str, until: float | None) -> Attempt:
        payload: dict[str, object] = {"Key": key, "Body": str(BODY_FIXTURE)}
        if until is not None:
            payload["ObjectLockMode"] = "GOVERNANCE"
            payload["ObjectLockRetainUntilDate"] = stamp(until)
        return self._run("put-object", payload)

    def head(self, key: str) -> Attempt:
        return self._run("head-object", {"Key": key})

    def delete(self, key: str, version: str, bypass: bool) -> Attempt:
        payload: dict[str, object] = {"Key": key, "VersionId": version}
        if bypass:
            payload["BypassGovernanceRetention"] = True
        return self._run("delete-object", payload)

    def hold(self, key: str, version: str, on: bool) -> Attempt:
        status = {"Status": "ON" if on else "OFF"}
        payload: dict[str, object] = {"Key": key, "VersionId": version, "LegalHold": status}
        return self._run("put-object-legal-hold", payload)

    def retention(self, key: str, version: str, until: float, bypass: bool) -> Attempt:
        retention = {"Mode": "GOVERNANCE", "RetainUntilDate": stamp(until)}
        payload: dict[str, object] = {"Key": key, "VersionId": version, "Retention": retention}
        if bypass:
            payload["BypassGovernanceRetention"] = True
        return self._run("put-object-retention", payload)

    def read_retention(self, key: str, version: str) -> Attempt:
        return self._run("get-object-retention", {"Key": key, "VersionId": version})

    def suspend(self) -> Attempt:
        payload: dict[str, object] = {"VersioningConfiguration": {"Status": "Suspended"}}
        return self._run("put-bucket-versioning", payload)
