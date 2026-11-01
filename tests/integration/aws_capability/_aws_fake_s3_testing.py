"""Operações S3 da fronteira com estado: versões, hold, retenção e bypass."""

from collections.abc import Callable

from tests.integration.aws_capability._aws_fake_types_testing import (
    FakeBucket,
    FakeVersion,
    denied,
    good,
    iso,
    parse_iso,
)
from tests.integration.aws_capability._service_contracts import Reply


class S3Handlers:
    buckets: dict[str, FakeBucket]
    clock: Callable[[], float]
    counter: int
    allowed: Callable[[str, str], bool]

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
        if self._bucket(payload) is None:
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
        if not self.allowed(who, "s3:PutObject"):
            return denied()
        if locked and not self.allowed(who, "s3:PutObjectRetention"):
            return denied()
        until = parse_iso(payload["ObjectLockRetainUntilDate"]) if locked else 0.0
        version = FakeVersion(payload["Key"], f"VERSION_ID_CANARY_{self.counter}", None, until)
        bucket.versions.append(version)
        return good({"ETag": "etag", "VersionId": version.version_id})

    def _s3api_head_object(self, payload: dict, who: str) -> Reply:
        bucket = self._bucket(payload)
        found = None if bucket is None else self._version(bucket, payload)
        if not self.allowed(who, "s3:GetObject"):
            return denied()
        return denied("NotFound") if found is None else good({"ContentLength": 2})

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
        if not self.allowed(who, action) or bypass_missing:
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
        if not self.allowed(who, "s3:PutObjectRetention"):
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
        if bucket.versions:
            return denied("BucketNotEmpty")
        del self.buckets[payload["Bucket"]]
        return good()
