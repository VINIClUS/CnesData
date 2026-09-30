"""Stable billing domain errors."""

import re

_SANITIZED_CODE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")


class BillingError(RuntimeError):
    code: str = "billing_error"


class EntitlementDenied(BillingError):
    code = "entitlement_denied"


class ImmutablePlanConflict(BillingError):
    code = "immutable_plan_conflict"


class BillingTenantConflict(BillingError):
    code = "billing_tenant_conflict"


class QuotaExceeded(BillingError):
    code = "quota_exceeded"


class IdempotencyConflict(BillingError):
    code = "idempotency_conflict"


class BillingDisabledError(BillingError):
    code = "billing_disabled"


class PublishDenied(BillingError):
    code = "publish_denied"


class StaleInboxClaim(BillingError):
    code = "inbox_claim_stale"

    def __init__(self, event_id: str | None = None) -> None:
        suffix = "" if event_id is None else f" event_id={event_id}"
        super().__init__(f"code={self.code}{suffix}")


class _SanitizedCodeError(BillingError):
    def __init__(self, code: str) -> None:
        if not isinstance(code, str) or not _SANITIZED_CODE.fullmatch(code):
            raise ValueError("reason=unsanitized_error_code")
        super().__init__(f"code={code}")
        self.code = code


class RetryableBillingError(_SanitizedCodeError):
    pass


class PermanentBillingError(_SanitizedCodeError):
    pass


class BillingDependencyError(RetryableBillingError):
    pass
