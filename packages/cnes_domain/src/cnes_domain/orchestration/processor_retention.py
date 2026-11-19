"""Contrato puro de retenção de imagens do processador."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from re import compile as compile_pattern

_DIGEST = compile_pattern(r"sha256:[0-9a-f]{64}")
_CAPACITY = 5
_MAX_AGE = timedelta(days=30)


@dataclass(frozen=True)
class RetainedProcessorDigest:
    digest: str
    first_admitted_at: datetime


@dataclass(frozen=True)
class ProcessorImageReferences:
    current: frozenset[str]
    prior: frozenset[str]
    in_flight: frozenset[str]
    retained_revisions: frozenset[str]


@dataclass(frozen=True)
class ProcessorRetentionSnapshot:
    history: tuple[RetainedProcessorDigest, ...]
    references: ProcessorImageReferences
    complete: bool


class RetentionOutcome(StrEnum):
    PLANNED = "PLANNED"
    REJECTED_INCOMPLETE = "REJECTED_INCOMPLETE"
    REJECTED_INVALID = "REJECTED_INVALID"
    REJECTED_CAPACITY = "REJECTED_CAPACITY"


@dataclass(frozen=True)
class ProcessorRetentionDecision:
    outcome: RetentionOutcome
    retained: frozenset[str]
    prune: tuple[str, ...]


def plan_processor_retention(
    snapshot: ProcessorRetentionSnapshot,
    candidate: RetainedProcessorDigest,
    now: datetime,
) -> ProcessorRetentionDecision:
    """Returns:
    Proposta determinística; recusas preservam o histórico e não propõem poda.
    """
    retained = frozenset(image.digest for image in snapshot.history)
    normalized = _validated_anchors(snapshot, candidate, now)
    if normalized is None:
        return _rejected(RetentionOutcome.REJECTED_INVALID, retained)
    anchors, candidate_anchor, instant = normalized
    refs = snapshot.references
    referenced = refs.current | refs.prior | refs.in_flight | refs.retained_revisions
    if any(_DIGEST.fullmatch(digest) is None for digest in referenced):
        return _rejected(RetentionOutcome.REJECTED_INVALID, retained)
    if not snapshot.complete or not referenced <= anchors.keys():
        return _rejected(RetentionOutcome.REJECTED_INCOMPLETE, retained)
    if not _valid_bootstrap(snapshot):
        return _rejected(RetentionOutcome.REJECTED_INVALID, retained)
    protected = referenced | {candidate.digest}
    if len(protected) > _CAPACITY:
        return _rejected(RetentionOutcome.REJECTED_CAPACITY, retained)
    anchors[candidate.digest] = candidate_anchor
    safe = sorted((anchor, digest) for digest, anchor in anchors.items() if digest not in protected)
    expired_count = sum(instant - anchor > _MAX_AGE for anchor, _ in safe)
    prune_count = max(expired_count, len(anchors) - _CAPACITY)
    prune = tuple(digest for _, digest in safe[:prune_count])
    return ProcessorRetentionDecision(
        RetentionOutcome.PLANNED, frozenset(anchors).difference(prune), prune
    )


def _validated_anchors(
    snapshot: ProcessorRetentionSnapshot, candidate: RetainedProcessorDigest, now: datetime
) -> tuple[dict[str, datetime], datetime, datetime] | None:
    instant = _utc(now)
    if instant is None:
        return None
    anchors = _history_anchors(snapshot.history, instant)
    candidate_anchor = _admission_anchor(candidate, instant)
    if anchors is None or candidate_anchor is None:
        return None
    if candidate.digest in anchors and anchors[candidate.digest] != candidate_anchor:
        return None
    return anchors, candidate_anchor, instant


def _utc(timestamp: datetime | None) -> datetime | None:
    if (
        timestamp is None
        or timestamp.utcoffset() is None
        or timestamp.replace(fold=0).utcoffset() != timestamp.replace(fold=1).utcoffset()
    ):
        return None
    try:
        return timestamp.astimezone(UTC)
    except OverflowError:
        return None


def _admission_anchor(image: RetainedProcessorDigest, now: datetime) -> datetime | None:
    anchor = _utc(image.first_admitted_at)
    if _DIGEST.fullmatch(image.digest) is None or anchor is None or anchor > now:
        return None
    return anchor


def _history_anchors(
    history: tuple[RetainedProcessorDigest, ...], now: datetime
) -> dict[str, datetime] | None:
    anchors: dict[str, datetime] = {}
    for image in history:
        anchor = _admission_anchor(image, now)
        if anchor is None or image.digest in anchors:
            return None
        anchors[image.digest] = anchor
    return anchors


def _valid_bootstrap(snapshot: ProcessorRetentionSnapshot) -> bool:
    refs = snapshot.references
    return bool(refs.current) or (
        not refs.prior
        and not refs.retained_revisions
        and frozenset(image.digest for image in snapshot.history) <= refs.in_flight
    )


def _rejected(outcome: RetentionOutcome, retained: frozenset[str]) -> ProcessorRetentionDecision:
    return ProcessorRetentionDecision(outcome, retained, ())
