"""Operações S3 da fronteira com estado: versões, hold, retenção e bypass."""

from collections.abc import Callable

from tests.integration.aws_capability._aws_fake_s3_policy_testing import (
    S3PolicyHandlers,
    bucket_arn,
    object_arn,
)
from tests.integration.aws_capability._aws_fake_types_testing import (
    FakeBucket,
    FakeVersion,
    denied,
    good,
    iso,
    linger,
    parse_iso,
    still_visible,
)
from tests.integration.aws_capability._service_contracts import Reply


class S3Handlers(S3PolicyHandlers):
    buckets: dict[str, FakeBucket]
    clock: Callable[[], float]
    counter: int
    allowed: Callable[[str, str], bool]
    list_gates_missing: bool
    bucket_absent_polls: int
    lingering: dict[str, int]

    def _bucket(self, payload: dict) -> FakeBucket | None:
        return self.buckets.get(payload["Bucket"])

    def _version(self, bucket: FakeBucket, payload: dict) -> FakeVersion | None:
        for version in reversed(bucket.versions):
            same_id = "VersionId" not in payload or version.version_id == payload["VersionId"]
            if version.key == payload["Key"] and same_id:
                return version
        return None

    def _s3api_create_bucket(self, payload: dict, who: str) -> Reply:
        if payload["Bucket"] in self.buckets:
            return denied("BucketAlreadyOwnedByYou")
        assert payload["ObjectLockEnabledForBucket"] is True
        self.buckets[payload["Bucket"]] = FakeBucket()
        return good({"Location": f"http://{payload['Bucket']}.s3.amazonaws.com/"})

    def _s3api_put_bucket_tagging(self, payload: dict, who: str) -> Reply:
        bucket = self._bucket(payload)
        if bucket is None:
            return denied("NoSuchBucket")
        tag_set = payload["Tagging"]["TagSet"]
        bucket.tags = {item["Key"]: item["Value"] for item in tag_set}
        return good()

    def _s3api_get_bucket_location(self, payload: dict, who: str) -> Reply:
        gone = self._bucket(payload) is None
        if gone and not still_visible(self.lingering, f"bucket/{payload['Bucket']}"):
            return denied("NoSuchBucket")
        return good({"LocationConstraint": "us-east-2"})

    def _s3api_get_bucket_tagging(self, payload: dict, who: str) -> Reply:
        bucket = self._bucket(payload)
        if bucket is None:
            return denied("NoSuchBucket")
        if not bucket.tags:
            return denied("NoSuchTagSet")
        return good({"TagSet": [{"Key": k, "Value": v} for k, v in bucket.tags.items()]})

    def _s3api_get_bucket_versioning(self, payload: dict, who: str) -> Reply:
        return good({"Status": "Enabled"})

    def _s3api_get_object_lock_configuration(self, payload: dict, who: str) -> Reply:
        if not self.permits(who, "s3:GetBucketObjectLockConfiguration", bucket_arn(payload)):
            return denied()
        return good({"ObjectLockConfiguration": {"ObjectLockEnabled": "Enabled"}})

    def _s3api_put_bucket_versioning(self, payload: dict, who: str) -> Reply:
        return denied("InvalidBucketState")

    def _s3api_list_buckets(self, payload: dict, who: str) -> Reply:
        return good({"Buckets": [{"Name": name} for name in self.buckets]})

    def _s3api_list_object_versions(self, payload: dict, who: str) -> Reply:
        bucket = self._bucket(payload)
        if bucket is None:
            return denied("NoSuchBucket")
        reply: dict[str, object] = {
            "EncodingType": "url",
            "IsTruncated": False,
            "KeyMarker": "",
            "MaxKeys": 1000,
            "Name": payload["Bucket"],
            "Prefix": "",
            "VersionIdMarker": "",
        }
        if bucket.versions:
            reply["Versions"] = [{"Key": v.key, "VersionId": v.version_id} for v in bucket.versions]
        return good(reply)

    def _s3api_list_multipart_uploads(self, payload: dict, who: str) -> Reply:
        if self._bucket(payload) is None:
            return denied("NoSuchBucket")
        return good(
            {
                "Bucket": payload["Bucket"],
                "KeyMarker": "",
                "UploadIdMarker": "",
                "NextKeyMarker": "",
                "NextUploadIdMarker": "",
                "MaxUploads": 1000,
                "IsTruncated": False,
            }
        )

    def _s3api_put_object(self, payload: dict, who: str) -> Reply:
        bucket = self._bucket(payload)
        if bucket is None:
            return denied("NoSuchBucket")
        locked = "ObjectLockMode" in payload
        target = object_arn(payload)
        if self._bucket_denies(bucket, who, "s3:PutObject", target):
            return denied()
        if not self.permits(who, "s3:PutObject", target):
            return denied()
        if locked and not self.permits(who, "s3:PutObjectRetention", target):
            return denied()
        until = parse_iso(payload["ObjectLockRetainUntilDate"]) if locked else 0.0
        version = FakeVersion(payload["Key"], f"VERSION_ID_CANARY_{self.counter}", None, until)
        bucket.versions.append(version)
        return good({"ETag": "etag", "VersionId": version.version_id})

    def _s3api_head_object(self, payload: dict, who: str) -> Reply:
        bucket = self._bucket(payload)
        found = None if bucket is None else self._version(bucket, payload)
        target = object_arn(payload)
        if bucket is not None and self._bucket_denies(bucket, who, "s3:GetObject", target):
            return denied("403")
        if not self.permits(who, "s3:GetObject", target):
            return denied("403")
        if found is None:
            return denied(self._missing_code(payload, who))
        return good({"ContentLength": 2})

    def _missing_code(self, payload: dict, who: str) -> str:
        listing = {"s3:prefix": [payload["Key"]]}
        if self.list_gates_missing and not self.permits(
            who, "s3:ListBucket", bucket_arn(payload), listing
        ):
            return "403"
        return "404"

    def _blocked(self, version: FakeVersion, header: bool, who: str) -> bool:
        if version.hold == "ON":
            return True
        retained = version.until > self.clock()
        return retained and not (header and self.allowed(who, "s3:BypassGovernanceRetention"))

    def _s3api_delete_object(self, payload: dict, who: str) -> Reply:
        bucket = self._bucket(payload)
        if bucket is None:
            return denied("NoSuchBucket")
        header = payload.get("BypassGovernanceRetention") is True
        action = "s3:DeleteObjectVersion" if "VersionId" in payload else "s3:DeleteObject"
        bypass_missing = header and not self.allowed(who, "s3:BypassGovernanceRetention")
        if not self.permits(who, action, object_arn(payload)) or bypass_missing:
            return denied()
        version = self._version(bucket, payload)
        if version is None:
            return denied("NoSuchVersion")
        if self._blocked(version, header, who):
            return denied()
        bucket.versions.remove(version)
        return good({"VersionId": version.version_id})

    def _s3api_get_object_legal_hold(self, payload: dict, who: str) -> Reply:
        bucket = self._bucket(payload)
        version = None if bucket is None else self._version(bucket, payload)
        if version is None:
            return denied("NoSuchVersion")
        if version.hold is None:
            return denied("NoSuchObjectLockConfiguration")
        return good({"LegalHold": {"Status": version.hold}})

    def _s3api_put_object_legal_hold(self, payload: dict, who: str) -> Reply:
        bucket = self._bucket(payload)
        version = None if bucket is None else self._version(bucket, payload)
        if version is None:
            return denied("NoSuchVersion")
        version.hold = payload["LegalHold"]["Status"]
        return good()

    def _s3api_get_object_retention(self, payload: dict, who: str) -> Reply:
        bucket = self._bucket(payload)
        version = None if bucket is None else self._version(bucket, payload)
        if not self.permits(who, "s3:GetObjectRetention", object_arn(payload)):
            return denied()
        if version is None:
            return denied("NoSuchVersion")
        if version.until == 0.0:
            return denied("NoSuchObjectLockConfiguration")
        retention = {"Mode": "GOVERNANCE", "RetainUntilDate": iso(version.until)}
        return good({"Retention": retention})

    def _s3api_put_object_retention(self, payload: dict, who: str) -> Reply:
        bucket = self._bucket(payload)
        version = None if bucket is None else self._version(bucket, payload)
        header = payload.get("BypassGovernanceRetention") is True
        if not self.permits(who, "s3:PutObjectRetention", object_arn(payload)):
            return denied()
        if version is None:
            return denied("NoSuchVersion")
        until = parse_iso(payload["Retention"]["RetainUntilDate"])
        shortening = version.until > self.clock() and until < version.until
        if shortening and not (header and self.allowed(who, "s3:BypassGovernanceRetention")):
            return denied()
        version.until = until
        return good()

    def _s3api_delete_bucket(self, payload: dict, who: str) -> Reply:
        bucket = self._bucket(payload)
        if bucket is None:
            return denied("NoSuchBucket")
        if self._bucket_denies(bucket, who, "s3:DeleteBucket", bucket_arn(payload)):
            return denied()
        if bucket.versions:
            return denied("BucketNotEmpty")
        del self.buckets[payload["Bucket"]]
        linger(self.lingering, f"bucket/{payload['Bucket']}", self.bucket_absent_polls)
        return good()
