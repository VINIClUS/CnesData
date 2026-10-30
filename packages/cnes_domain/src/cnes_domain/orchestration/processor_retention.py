"""Contrato puro de retenção de imagens do processador."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


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
    Proposta neutra provisória para o estágio TESTS/RED.
    """
    return ProcessorRetentionDecision(RetentionOutcome.PLANNED, frozenset(), ())
