"""Testes de contratos de jobs."""
from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from cnes_contracts.jobs import JobStatus, JobTransitionEvent


def test_job_status_valores():
    assert JobStatus.PENDING == "PENDING"
    assert JobStatus.UPLOADED == "UPLOADED"
    assert JobStatus.PROCESSING == "PROCESSING"
    assert JobStatus.INGESTED == "INGESTED"
    assert JobStatus.FAILED == "FAILED"
    assert JobStatus.DLQ == "DLQ"


def test_job_transition_event_valido():
    ev = JobTransitionEvent(
        extraction_id=uuid4(),
        from_status=JobStatus.PENDING,
        to_status=JobStatus.UPLOADED,
        actor="dump_agent",
        reason="upload_complete",
        at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert ev.from_status == JobStatus.PENDING
    assert ev.to_status == JobStatus.UPLOADED
    assert ev.reason == "upload_complete"


def test_job_transition_event_reason_opcional():
    ev = JobTransitionEvent(
        extraction_id=uuid4(),
        from_status=JobStatus.UPLOADED,
        to_status=JobStatus.PROCESSING,
        actor="data_processor",
        at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert ev.reason is None


def test_job_transition_event_rejeita_actor_nao_string():
    with pytest.raises(ValidationError):
        JobTransitionEvent(
            extraction_id=uuid4(),
            from_status=JobStatus.PENDING,
            to_status=JobStatus.UPLOADED,
            actor=123,
            at=datetime(2026, 1, 1, tzinfo=UTC),
        )


def test_job_transition_event_rejeita_status_invalido():
    with pytest.raises(ValidationError):
        JobTransitionEvent(
            extraction_id=uuid4(),
            from_status="FOO",
            to_status=JobStatus.UPLOADED,
            actor="dump_agent",
            at=datetime(2026, 1, 1, tzinfo=UTC),
        )


def test_job_transition_event_rejeita_extraction_id_invalido():
    with pytest.raises(ValidationError):
        JobTransitionEvent(
            extraction_id="nao-uuid",
            from_status=JobStatus.PENDING,
            to_status=JobStatus.UPLOADED,
            actor="dump_agent",
            at=datetime(2026, 1, 1, tzinfo=UTC),
        )


def test_job_transition_event_frozen():
    ev = JobTransitionEvent(
        extraction_id=uuid4(),
        from_status=JobStatus.PENDING,
        to_status=JobStatus.UPLOADED,
        actor="dump_agent",
        at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    with pytest.raises(ValidationError):
        ev.actor = "outro"
