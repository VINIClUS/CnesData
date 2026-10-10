"""Stable billing domain errors."""

import re
from typing import cast

_SANITIZED_CODE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_PAIR = r"[a-z_][a-z0-9_]*=[A-Za-z0-9_.:-]+"
_SANITIZED_DETAIL = re.compile(rf"^{_PAIR}( {_PAIR})*$")


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
    def __init__(self, code: str, *, detail: str | None = None) -> None:
        if not isinstance(cast("object", code), str) or not _SANITIZED_CODE.fullmatch(code):
            raise ValueError("reason=unsanitized_error_code")
        if detail is not None and not _SANITIZED_DETAIL.fullmatch(detail):
            raise ValueError("reason=unsanitized_error_detail")
        suffix = "" if detail is None else f" {detail}"
        super().__init__(f"code={code}{suffix}")
        self.code = code


class RetryableBillingError(_SanitizedCodeError):
    pass


class PermanentBillingError(_SanitizedCodeError):
    pass


class BillingDependencyError(RetryableBillingError):
    pass
