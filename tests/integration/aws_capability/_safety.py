"""Validação fechada e identidade de recursos do ensaio."""

import math
import re

from tests.integration.aws_capability._profiles import PROFILES
from tests.integration.aws_capability.contracts import Ownership, Ports, Resource, RunSpec

SERVICES = ("cognito", "dynamodb", "s3", "iam")


class SafetyFailure(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(f"lifecycle={reason}")
        self.reason = reason


def validate(spec: RunSpec, ports: Ports, *, recovery: bool = False) -> None:
    checks = (
        (spec.account == ports.policy.account, "account_not_authorized"),
        (spec.region in ports.policy.regions, "region_not_authorized"),
        (re.fullmatch(r"[0-9a-f]{8}", spec.run_id) is not None, "invalid_run_id"),
        (math.isfinite(spec.deadline), "invalid_deadline"),
        (recovery or spec.deadline > ports.now(), "deadline_expired"),
        (spec.retention == "GOVERNANCE", "retention_forbidden"),
        (spec.allow_governance_bypass, "cleanup_bypass_required"),
    )
    for valid, reason in checks:
        if not valid:
            raise SafetyFailure(reason)
    validate_profile(spec, ports, recovery=recovery)
    limits = ports.policy.limits
    if not 0 < limits.cleanup_reserve < limits.calls:
        raise SafetyFailure("cleanup_reserve_unavailable")
    if min(limits.pages, limits.attempts) <= 0:
        raise SafetyFailure("invalid_limits")
    if not math.isfinite(limits.cleanup_seconds) or limits.cleanup_seconds <= 0:
        raise SafetyFailure("invalid_limits")


def validate_profile(spec: RunSpec, ports: Ports, *, recovery: bool) -> None:
    profile = PROFILES.get(spec.probe)
    if profile is None:
        raise SafetyFailure("invalid_profile")
    if spec.probe == "all":
        return
    if profile.limits != ports.policy.limits or profile.limits != ports.budget.limits:
        raise SafetyFailure("profile_limits_mismatch")
    if ports.budget.deadline != spec.deadline:
        raise SafetyFailure("invalid_deadline")
    if not recovery and spec.deadline - ports.now() > 180:
        raise SafetyFailure("invalid_deadline")


def run_name(spec: RunSpec) -> str:
    return f"cnesdata-cap-{spec.run_id}"


def tags(spec: RunSpec) -> dict[str, str]:
    return {
        "Project": "CnesData",
        "Purpose": "aws-capability",
        "Run": spec.run_id,
        "ExpiresAt": format(spec.deadline, ".15g"),
    }


def matches_name(resource: Resource, spec: RunSpec) -> bool:
    name = run_name(spec)
    return resource.name == name or resource.name.startswith(name + "-")


def owned(resource: Resource, owner: Ownership, spec: RunSpec) -> bool:
    return (
        resource.service in SERVICES
        and matches_name(resource, spec)
        and owner.account == spec.account
        and getattr(owner, "region", None)
        == ("aws-global" if resource.service == "iam" else spec.region)
        and all(owner.tags.get(key) == value for key, value in tags(spec).items())
    )
