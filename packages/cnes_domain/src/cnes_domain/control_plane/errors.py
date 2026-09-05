"""Control-plane domain errors."""

from enum import StrEnum


class ControlPlaneErrorCode(StrEnum):
    ACCESS_REQUEST_CONFLICT = "access_request_conflict"
    ACCESS_REQUEST_CREATION_CONFLICT = "access_request_creation_conflict"
    ACCESS_REQUEST_CREATION_STATE = "access_request_creation_state"
    ACCESS_REQUEST_DECISION_CONFLICT = "access_request_decision_conflict"
    ACCESS_REQUEST_DECISION_STATE = "access_request_decision_state"
    ACCESS_REQUEST_IDENTITY_CONFLICT = "access_request_identity_conflict"
    ACCESS_REQUEST_MISSING = "access_request_missing"
    ACCESS_REQUEST_STATE_CONFLICT = "access_request_state_conflict"
    ACTIVE_DISPATCH_CONFLICT = "active_dispatch_conflict"
    AGENT_REVOKED = "agent_revoked"
    DEGRADED_ERROR_REQUIRED = "degraded_error_required"
    DEGRADED_NORMALIZE_REQUIRED = "degraded_normalize_required"
    DEGRADED_OUTPUTS_FORBIDDEN = "degraded_outputs_forbidden"
    DISPATCH_BIND_CONFLICT = "dispatch_bind_conflict"
    DISPATCH_BINDING_CONFLICT = "dispatch_binding_conflict"
    DISPATCH_EXPIRED = "dispatch_expired"
    DISPATCH_FENCE_REJECTED = "dispatch_fence_rejected"
    DISPATCH_FINISH_CONFLICT = "dispatch_finish_conflict"
    DISPATCH_ID_CONFLICT = "dispatch_id_conflict"
    DISPATCH_INACTIVE = "dispatch_inactive"
    DISPATCH_LIVE = "dispatch_live"
    DISPATCH_MISMATCH = "dispatch_mismatch"
    DISPATCH_MISSING = "dispatch_missing"
    DISPATCH_OUTCOME_CONFLICT = "dispatch_outcome_conflict"
    DISPATCH_STALE = "dispatch_stale"
    DISPATCH_TERMINAL = "dispatch_terminal"
    DISPATCH_UNIT_MISSING = "dispatch_unit_missing"
    DISPATCH_UNIT_UNAVAILABLE = "dispatch_unit_unavailable"
    DISPATCH_UNITS_CONFLICT = "dispatch_units_conflict"
    DUPLICATE_TRANSACTION_KEY = "duplicate_transaction_key"
    EVENT_DELIVERY_CONFLICT = "event_delivery_conflict"
    EVENT_ID_CONFLICT = "event_id_conflict"
    EVENT_TENANT_CONFLICT = "event_tenant_conflict"
    EXECUTION_CONFLICT = "execution_conflict"
    FENCE_MISMATCH = "fence_mismatch"
    IDEMPOTENCY_HASH_CONFLICT = "idempotency_hash_conflict"
    JOB_CANCELLATION_CONFLICT = "job_cancellation_conflict"
    JOB_CONFLICT = "job_conflict"
    JOB_CREATION_CONFLICT = "job_creation_conflict"
    JOB_FENCE_REJECTED = "job_fence_rejected"
    JOB_INITIAL_STATE_INVALID = "job_initial_state_invalid"
    JOB_LEASE_EXPIRED = "job_lease_expired"
    JOB_MISSING = "job_missing"
    JOB_NOT_LEASED = "job_not_leased"
    JOB_OWNER_LOST = "job_owner_lost"
    JOB_STATE_CONFLICT = "job_state_conflict"
    JOB_TERMINAL_CONFLICT = "job_terminal_conflict"
    LEASE_EXPIRED = "lease_expired"
    MANIFEST_IDENTITY_CONFLICT = "manifest_identity_conflict"
    MANIFEST_IDENTITY_MISMATCH = "manifest_identity_mismatch"
    MANIFEST_IMMUTABLE = "manifest_immutable"
    OPTIONAL_DEPENDENCY_REQUIRED = "optional_dependency_required"
    OUTBOX_DELIVERY_CONFLICT = "outbox_delivery_conflict"
    OUTBOX_EVENT_ALREADY_DELIVERED = "outbox_event_already_delivered"
    OUTBOX_EVENT_CONFLICT = "outbox_event_conflict"
    OUTBOX_EVENT_MISSING = "outbox_event_missing"
    OUTBOX_EVENT_TENANT_MISMATCH = "outbox_event_tenant_mismatch"
    OUTCOME_CONFLICT = "outcome_conflict"
    OWNER_MISMATCH = "owner_mismatch"
    PARENT_NOT_PROCESSING = "parent_not_processing"
    PARENT_RUN_MISMATCH = "parent_run_mismatch"
    PARENT_RUN_NOT_CANCELING = "parent_run_not_canceling"
    PARENT_RUN_REQUIRED = "parent_run_required"
    POINTER_CAS = "pointer_cas"
    POINTER_NAME_NOT_CURRENT = "pointer_name_not_current"
    POINTER_VERSION_CONFLICT = "pointer_version_conflict"
    PUBLICATION_REPLAY_CONFLICT = "publication_replay_conflict"
    PUBLICATION_REPLAY_RESPONSE_MISSING = "publication_replay_response_missing"
    PUBLICATION_RUN_CONFLICT = "publication_run_conflict"
    RAW_ANCESTRY_CONFLICT = "raw_ancestry_conflict"
    RUN_CANCELLATION_CONFLICT = "run_cancellation_conflict"
    RUN_COMPETENCIA_MISMATCH = "run_competencia_mismatch"
    RUN_CONFLICT = "run_conflict"
    RUN_DATASET_MISMATCH = "run_dataset_mismatch"
    RUN_DEPENDENCY_CONFLICT = "run_dependency_conflict"
    RUN_MISSING = "run_missing"
    RUN_NOT_CANCELING = "run_not_canceling"
    RUN_NOT_PROCESSING = "run_not_processing"
    RUN_NOT_PUBLISHING = "run_not_publishing"
    RUN_STATE_CONFLICT = "run_state_conflict"
    RUN_TRANSITION_CONFLICT = "run_transition_conflict"
    RUN_UNITS_CONFLICT = "run_units_conflict"
    TRANSACTION_CONFLICT = "transaction_conflict"
    TRANSACTION_LIMIT = "transaction_limit"
    UNIT_CONTEXT_MISSING = "unit_context_missing"
    UNIT_DISPATCH_MISMATCH = "unit_dispatch_mismatch"
    UNIT_DISPATCH_REJECTED = "unit_dispatch_rejected"
    UNIT_FENCE_REJECTED = "unit_fence_rejected"
    UNIT_LEASE_EXPIRED = "unit_lease_expired"
    UNIT_MISSING = "unit_missing"
    UNIT_NOT_LEASED = "unit_not_leased"
    UNIT_OWNER_LOST = "unit_owner_lost"
    UNIT_TERMINAL_CONFLICT = "unit_terminal_conflict"
    UNITS_CONFLICT = "units_conflict"
    VERSION_IMMUTABLE = "version_immutable"


class _CodedError:
    code: ControlPlaneErrorCode | str | None

    def __init__(self, *args: object) -> None:
        normalized_args = args
        if args and isinstance(args[0], ControlPlaneErrorCode):
            normalized_args = (args[0].value, *args[1:])
        super().__init__(*normalized_args)
        self.code = None
        if args and isinstance(args[0], str):
            try:
                self.code = ControlPlaneErrorCode(args[0])
            except ValueError:
                self.code = args[0]


class Conflict(_CodedError, RuntimeError):
    pass


class InvalidTransition(Conflict):
    pass


class LeaseLost(Conflict):
    pass


class FenceRejected(Conflict):
    pass


class NotFound(_CodedError, LookupError):
    pass
