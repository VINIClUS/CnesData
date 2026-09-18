"""Política pura de aceitação de cadeias raw DELTA."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum

from cnes_contracts import RawManifest, manifest_sha256
from cnes_domain.control_plane.entities import RawResyncState

_MAX_BASE_AGE = timedelta(days=7)
_MAX_CHAIN_LENGTH = 30


class ResyncReason(StrEnum):
    AGENT_RESYNC_REQUIRED = "AGENT_RESYNC_REQUIRED"
    BASE_UNKNOWN = "BASE_UNKNOWN"
    SEQUENCE_GAP = "SEQUENCE_GAP"
    HASH_CHAIN_MISMATCH = "HASH_CHAIN_MISMATCH"
    SCHEMA_INCOMPATIBLE = "SCHEMA_INCOMPATIBLE"
    BASE_TOO_OLD = "BASE_TOO_OLD"
    CHAIN_TOO_LONG = "CHAIN_TOO_LONG"


@dataclass(frozen=True, slots=True)
class _DeltaContext:
    manifest: RawManifest
    chain: tuple[RawManifest, ...]
    resync_state: RawResyncState | None
    now: datetime


@dataclass(frozen=True, slots=True)
class DeltaPolicy:
    max_base_age: timedelta = _MAX_BASE_AGE
    max_chain_length: int = _MAX_CHAIN_LENGTH

    def __post_init__(self) -> None:
        valid_age = timedelta(0) < self.max_base_age <= _MAX_BASE_AGE
        valid_length = 0 < self.max_chain_length <= _MAX_CHAIN_LENGTH
        if not valid_age or not valid_length:
            raise ValueError("delta_policy_limit")

    def evaluate(self, context: _DeltaContext) -> ResyncReason | None:
        manifest = context.manifest
        chain = context.chain
        reason = None
        if context.resync_state is not None:
            reason = ResyncReason.AGENT_RESYNC_REQUIRED
        elif not chain or manifest.base_snapshot_id != chain[0].snapshot_id:
            reason = ResyncReason.BASE_UNKNOWN
        elif manifest.sequence != chain[-1].sequence + 1:
            reason = ResyncReason.SEQUENCE_GAP
        elif manifest.previous_manifest_sha256 != manifest_sha256(chain[-1]):
            reason = ResyncReason.HASH_CHAIN_MISMATCH
        elif manifest.schema_version != chain[-1].schema_version:
            reason = ResyncReason.SCHEMA_INCOMPATIBLE
        elif not timedelta(0) <= context.now - chain[0].created_at <= self.max_base_age:
            reason = ResyncReason.BASE_TOO_OLD
        elif len(chain) - 1 >= self.max_chain_length:
            reason = ResyncReason.CHAIN_TOO_LONG
        return reason
