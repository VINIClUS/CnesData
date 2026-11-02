"""Bucket policy, listagem e gate de ausência do S3 da fronteira com estado."""

import json
from collections.abc import Callable, Mapping, Sequence
from fnmatch import fnmatchcase

from tests.integration.aws_capability._aws_fake_policy_testing import listed
from tests.integration.aws_capability._aws_fake_sts_testing import name_for, role_arn
from tests.integration.aws_capability._aws_fake_types_testing import (
    OPERATOR,
    FakeBucket,
    FakeRole,
    denied,
    good,
)
from tests.integration.aws_capability._b1_testing import OPERATOR_ARN
from tests.integration.aws_capability._service_contracts import Reply


def object_arn(payload: Mapping[str, object]) -> str:
    return f"arn:aws:s3:::{payload['Bucket']}/{payload['Key']}"


def bucket_arn(payload: Mapping[str, object]) -> str:
    return f"arn:aws:s3:::{payload['Bucket']}"


class S3PolicyHandlers:
    buckets: dict[str, FakeBucket]
    roles: dict[str, FakeRole]
    counter: int
    bucket_policy_lag: int
    bucket_policy_malformed: int
    list_gates_missing: bool
    permits: Callable[..., bool]

    def _principal_arn(self, who: str) -> str:
        return OPERATOR_ARN if who == OPERATOR else role_arn(name_for(who))

    def _bucket_denies(self, bucket: FakeBucket, who: str, action: str, resource: str) -> bool:
        if bucket.policy is None or self.counter < bucket.policy_effective:
            return False
        arn = self._principal_arn(who)
        return any(
            statement["Effect"] == "Deny"
            and action in listed(statement["Action"])
            and self._principal_matches(statement["Principal"], arn)
            and any(fnmatchcase(resource, p) for p in listed(statement["Resource"]))
            for statement in bucket.policy["Statement"]  # type: ignore[index]
        )

    @staticmethod
    def _principal_matches(principal: object, arn: str) -> bool:
        if principal == "*":
            return True
        members = principal["AWS"] if isinstance(principal, Mapping) else []
        return "*" in listed(members) or arn in listed(members)

    def _valid_principals(self, document: Mapping[str, object]) -> bool:
        known = {OPERATOR_ARN, *(role_arn(name) for name in self.roles)}
        for statement in document["Statement"]:  # type: ignore[attr-defined]
            principal = statement["Principal"]
            members = [] if principal == "*" else listed(principal["AWS"])
            if any(member != "*" and member not in known for member in members):
                return False
        return True

    def _s3api_put_bucket_policy(self, payload: dict, who: str) -> Reply:
        bucket = self.buckets.get(payload["Bucket"])
        if bucket is None:
            return denied("NoSuchBucket")
        if not self.permits(who, "s3:PutBucketPolicy", bucket_arn(payload)):
            return denied()
        document = json.loads(payload["Policy"])
        if self.bucket_policy_malformed > 0 or not self._valid_principals(document):
            self.bucket_policy_malformed = max(self.bucket_policy_malformed - 1, 0)
            return denied("MalformedPolicy", "Invalid principal in policy")
        bucket.policy = document
        bucket.policy_effective = self.counter + self.bucket_policy_lag
        return good()

    def _s3api_delete_bucket_policy(self, payload: dict, who: str) -> Reply:
        bucket = self.buckets.get(payload["Bucket"])
        if bucket is None:
            return denied("NoSuchBucket")
        if bucket.policy is None:
            return denied("NoSuchBucketPolicy")
        bucket.policy = None
        return good()

    def _s3api_list_objects_v2(self, payload: dict, who: str) -> Reply:
        bucket = self.buckets.get(payload["Bucket"])
        if bucket is None:
            return denied("NoSuchBucket")
        prefix = payload.get("Prefix", "")
        keys: Sequence[str] = sorted(
            {v.key for v in bucket.versions if v.key.startswith(prefix)}
        )
        if not self.permits(who, "s3:ListBucket", bucket_arn(payload), {"s3:prefix": [prefix]}):
            return denied()
        return good({"KeyCount": min(len(keys), payload.get("MaxKeys", 1000))})
