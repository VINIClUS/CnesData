from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from cnes_domain.control_plane.commands import FailJob
from cnes_domain.control_plane.entities import Job, RawResyncState
from cnes_domain.control_plane.enums import JobState

NOW = datetime(2026, 7, 15, 12, tzinfo=UTC)
HASH = "a" * 64


def job_values(**updates: object) -> dict[str, object]:
    values = {
        "tenant_id": "354130", "job_id": "job-1", "agent_id": "agent-01",
        "source_type": "CNES_LOCAL", "file_subtype": "CNES_VINCULO",
        "competencia": "2026-07", "requested_snapshot_mode": "FULL",
        "state": JobState.PENDING, "attempt": 0, "fencing_token": 0,
        "lease_owner": None, "lease_until": None, "result_manifest_id": None,
        "result_manifest_key": None, "error_code": None, "created_at": NOW,
    }
    return values | updates


def test_estado_de_resync_exige_identidade_exata_e_instante_utc() -> None:
    state = RawResyncState(
        tenant_id="354130", agent_id="agent-01", source_type="CNES_LOCAL",
        file_subtype="CNES_VINCULO", competencia="2026-07", required_since=NOW,
    )
    assert state.required_since is NOW
    with pytest.raises(ValidationError, match="datetime_not_utc"):
        RawResyncState.model_validate(
            state.model_dump() | {"required_since": NOW.replace(tzinfo=None)}
        )


@pytest.mark.parametrize(
    ("updates", "error"),
    [
        ({"state": JobState.FAILED_FINAL, "error_code": "RAW_RESYNC_BASE_UNKNOWN"},
         "resync_hash_required"),
        ({"rejected_manifest_sha256": HASH}, "resync_hash_forbidden"),
        ({"state": JobState.FAILED_FINAL, "error_code": "other",
          "rejected_manifest_sha256": HASH}, "resync_hash_forbidden"),
        ({"state": JobState.FAILED_FINAL, "error_code": "RAW_RESYNC_BASE_UNKNOWN",
          "rejected_manifest_sha256": "A" * 64}, "invalid_sha256"),
    ],
)
def test_job_vincula_hash_rejeitado_somente_a_resync_final(
    updates: dict[str, object], error: str
) -> None:
    with pytest.raises(ValidationError, match=error):
        Job.model_validate(job_values(**updates))
    valid = Job.model_validate(job_values(
        state=JobState.FAILED_FINAL,
        error_code="RAW_RESYNC_BASE_UNKNOWN",
        rejected_manifest_sha256=HASH,
    ))
    assert valid.rejected_manifest_sha256 == HASH


@pytest.mark.parametrize(
    ("updates", "error"),
    [
        ({"rejected_manifest_sha256": HASH}, "resync_hash_forbidden"),
        ({"error_code": "RAW_RESYNC_BASE_UNKNOWN", "retryable": False},
         "resync_hash_required"),
    ],
)
def test_comando_de_falha_vincula_hash_ao_resync(updates, error) -> None:
    values = {
        "tenant_id": "354130", "job_id": "job-1", "owner": "worker",
        "fencing_token": 1, "error_code": "other", "retryable": True,
    }
    with pytest.raises(ValidationError, match=error):
        FailJob(**(values | updates))


@pytest.mark.parametrize(
    ("updates", "error"),
    [
        ({"expected_resync_marker": False}, "resync_guard_forbidden"),
        (
            {
                "error_code": "RAW_RESYNC_BASE_UNKNOWN",
                "retryable": False,
                "rejected_manifest_sha256": HASH,
                "expected_head_manifest_id": "manifest-a",
            },
            "head_guard_requires_absent_marker",
        ),
    ],
)
def test_comando_de_falha_restringe_guarda_raw(updates, error) -> None:
    values = {
        "tenant_id": "354130", "job_id": "job-1", "owner": "worker",
        "fencing_token": 1, "error_code": "other", "retryable": True,
    }
    with pytest.raises(ValidationError, match=error):
        FailJob(**(values | updates))
