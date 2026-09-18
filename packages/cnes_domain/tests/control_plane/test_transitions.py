import ast
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from cnes_domain.control_plane.commands import PublicationPermit, PublishDataset
from cnes_domain.control_plane.entities import (
    DatasetVersion,
    Job,
    OutboxEvent,
    Run,
    RunDependency,
    RunUnit,
)
from cnes_domain.control_plane.enums import JobState, RunStage, RunState, RunUnitState
from cnes_domain.control_plane.errors import InvalidTransition
from cnes_domain.control_plane.ids import RunUnitIdentity
from cnes_domain.control_plane.transitions import (
    transition_job,
    transition_run,
    transition_run_unit,
)

NOW = datetime(2026, 7, 15, 12, tzinfo=UTC)


def job(state: JobState) -> Job:
    leased = state in {
        JobState.LEASED,
        JobState.CANCEL_REQUESTED,
    }
    carries_result = state in {JobState.LEASED, JobState.SUCCEEDED}
    return Job(
        tenant_id="354130",
        job_id="job-1",
        agent_id="agent-01",
        source_type="CNES_LOCAL",
        file_subtype="CNES_VINCULO",
        competencia="2026-07",
        requested_snapshot_mode="FULL",
        state=state,
        attempt=0,
        fencing_token=0,
        lease_owner="worker" if leased else None,
        lease_until=NOW if leased else None,
        result_manifest_id="manifest-1" if carries_result else None,
        result_manifest_key=(
            "raw/354130/CNES_LOCAL/2026-07/snapshot-1/manifest.json" if carries_result else None
        ),
        error_code=None,
        created_at=NOW,
    )


def run(state: RunState, *, required: bool = True, missing: tuple[str, ...] = ()) -> Run:
    return Run(
        tenant_id="354130",
        run_id="run-1",
        competencia="2026-07",
        dataset_name="cnes",
        state=state,
        dependencies=(
            RunDependency(
                source_type="CNES_LOCAL",
                file_subtype="CNES_VINCULO",
                required=required,
            ),
        ),
        missing_sources=missing,
        created_at=NOW,
    )


def unit(state: RunUnitState, **updates: object) -> RunUnit:
    leased = state is RunUnitState.LEASED
    values: dict[str, object] = {
        "tenant_id": "354130",
        "run_id": "run-1",
        "unit_id": "unit-1",
        "stage": RunStage.NORMALIZE,
        "source_type": "CNES_LOCAL",
        "file_subtype": "CNES_VINCULO",
        "partition": "all",
        "depends_on_unit_ids": (),
        "input_manifests": (
            {
                "manifest_id": "manifest-1",
                "manifest_key": "raw/354130/CNES_LOCAL/2026-07/snapshot-1/manifest.json",
            },
        ),
        "state": state,
        "attempt": 0,
        "fencing_token": 0,
        "lease_owner": "worker" if leased else None,
        "lease_until": NOW if leased else None,
        "dispatch_id": "fedcba9876543210" if leased else None,
        "output_manifests": (),
        "error_code": "optional_failed" if state is RunUnitState.SUCCEEDED_DEGRADED else None,
    }
    return RunUnit.model_validate(values | updates)


JOB_LEGAL = {
    (JobState.PENDING, JobState.LEASED),
    (JobState.FAILED_RETRYABLE, JobState.LEASED),
    (JobState.LEASED, JobState.SUCCEEDED),
    (JobState.LEASED, JobState.FAILED_RETRYABLE),
    (JobState.LEASED, JobState.FAILED_FINAL),
    (JobState.LEASED, JobState.CANCEL_REQUESTED),
    (JobState.CANCEL_REQUESTED, JobState.CANCELED),
}

RUN_LEGAL = {
    (RunState.PLANNED, RunState.WAITING_INPUTS),
    (RunState.PLANNED, RunState.PROCESSING),
    (RunState.PLANNED, RunState.CANCEL_REQUESTED),
    (RunState.WAITING_INPUTS, RunState.PROCESSING),
    (RunState.WAITING_INPUTS, RunState.FAILED),
    (RunState.WAITING_INPUTS, RunState.CANCEL_REQUESTED),
    (RunState.PROCESSING, RunState.PUBLISHING),
    (RunState.PROCESSING, RunState.FAILED),
    (RunState.PROCESSING, RunState.CANCEL_REQUESTED),
    (RunState.PUBLISHING, RunState.PUBLISHED),
    (RunState.PUBLISHING, RunState.PUBLISHED_DEGRADED),
    (RunState.PUBLISHING, RunState.FAILED),
    (RunState.CANCEL_REQUESTED, RunState.CANCELED),
}

UNIT_LEGAL = {
    (RunUnitState.PENDING, RunUnitState.LEASED),
    (RunUnitState.FAILED_RETRYABLE, RunUnitState.LEASED),
    (RunUnitState.LEASED, RunUnitState.SUCCEEDED),
    (RunUnitState.LEASED, RunUnitState.FAILED_RETRYABLE),
    (RunUnitState.LEASED, RunUnitState.FAILED_FINAL),
}


@pytest.mark.parametrize(
    ("old_state", "new_state"),
    [(old, new) for old in JobState for new in JobState],
)
def test_transition_job_aplica_matriz_completa(old_state: JobState, new_state: JobState) -> None:
    original = job(old_state)
    if (old_state, new_state) in JOB_LEGAL:
        changed = transition_job(original, new_state)
        assert changed.state is new_state
        assert changed is not original
        assert original.state is old_state
        assert changed.model_dump(exclude={"state"}) == original.model_dump(exclude={"state"})
    else:
        with pytest.raises(
            InvalidTransition,
            match=f"transition={old_state.value}->{new_state.value}",
        ):
            transition_job(original, new_state)


@pytest.mark.parametrize(
    ("old_state", "new_state"),
    [(old, new) for old in RunState for new in RunState],
)
def test_transition_run_aplica_matriz_completa(old_state: RunState, new_state: RunState) -> None:
    original = run(old_state)
    if (old_state, new_state) in RUN_LEGAL:
        changed = transition_run(original, new_state)
        assert changed.state is new_state
        assert changed is not original
        assert original.state is old_state
        assert changed.model_dump(exclude={"state"}) == original.model_dump(exclude={"state"})
    else:
        with pytest.raises(
            InvalidTransition,
            match=f"transition={old_state.value}->{new_state.value}",
        ):
            transition_run(original, new_state)


@pytest.mark.parametrize(
    ("old_state", "new_state"),
    [(old, new) for old in RunUnitState for new in RunUnitState],
)
def test_transition_unit_aplica_matriz_sem_contexto(
    old_state: RunUnitState,
    new_state: RunUnitState,
) -> None:
    original = unit(old_state)
    if (old_state, new_state) in UNIT_LEGAL:
        changed = transition_run_unit(original, new_state)
        assert changed.state is new_state
        assert changed is not original
        assert original.state is old_state
    else:
        with pytest.raises(InvalidTransition):
            transition_run_unit(original, new_state)


@pytest.mark.parametrize(
    "old_state",
    [
        RunUnitState.PENDING,
        RunUnitState.LEASED,
        RunUnitState.FAILED_RETRYABLE,
    ],
)
def test_cancelamento_de_unit_exige_parent_cancel_requested(old_state: RunUnitState) -> None:
    original = unit(old_state)
    changed = transition_run_unit(original, RunUnitState.CANCELED, run(RunState.CANCEL_REQUESTED))
    assert changed.state is RunUnitState.CANCELED

    with pytest.raises(InvalidTransition, match="parent_run_not_canceling"):
        transition_run_unit(original, RunUnitState.CANCELED, run(RunState.PROCESSING))


def test_cancelamento_rejeita_parent_de_outro_run() -> None:
    parent = run(RunState.CANCEL_REQUESTED).model_copy(update={"run_id": "run-2"})
    with pytest.raises(InvalidTransition, match="parent_run_mismatch"):
        transition_run_unit(unit(RunUnitState.PENDING), RunUnitState.CANCELED, parent)


def test_degradacao_aceita_apenas_normalize_opcional_final_failed() -> None:
    failed = unit(RunUnitState.LEASED, error_code="optional_source_failed")
    parent = run(RunState.PROCESSING, required=False)

    changed = transition_run_unit(failed, RunUnitState.SUCCEEDED_DEGRADED, parent)

    assert changed.state is RunUnitState.SUCCEEDED_DEGRADED
    assert changed.output_manifests == ()


@pytest.mark.parametrize(
    ("unit_updates", "parent", "message"),
    [
        ({"error_code": None}, run(RunState.PROCESSING, required=False), "degraded_error_required"),
        (
            {
                "stage": RunStage.RECONCILE,
                "source_type": None,
                "file_subtype": None,
                "input_manifests": (),
                "depends_on_unit_ids": ("unit-0",),
                "error_code": "failed",
            },
            run(RunState.PROCESSING, required=False),
            "degraded_normalize_required",
        ),
        (
            {"error_code": "failed"},
            run(RunState.PROCESSING, required=True),
            "optional_dependency_required",
        ),
        (
            {
                "error_code": "failed",
                "output_manifests": (
                    {
                        "manifest_id": "output-1",
                        "manifest_key": "normalized/354130/2026-07/run-1/unit-1/manifest.json",
                    },
                ),
            },
            run(RunState.PROCESSING, required=False),
            "degraded_outputs_forbidden",
        ),
    ],
)
def test_degradacao_rejeita_contexto_invalido(
    unit_updates: dict[str, object],
    parent: Run,
    message: str,
) -> None:
    failed = unit(RunUnitState.LEASED, **unit_updates)
    with pytest.raises(InvalidTransition, match=message):
        transition_run_unit(failed, RunUnitState.SUCCEEDED_DEGRADED, parent)


def test_modulos_do_dominio_nao_importam_infra_ou_frameworks() -> None:
    root = Path(__file__).parents[2] / "src/cnes_domain/control_plane"
    forbidden = {"boto3", "fastapi", "sqlalchemy", "cnes_infra"}
    imported: set[str] = set()
    for path in root.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0])
    assert imported.isdisjoint(forbidden)


@pytest.mark.parametrize(
    "source_type",
    [pytest.param("CNES/LOCAL"), pytest.param("CNES#LOCAL")],
)
def test_run_unit_identity_rejeita_componentes_incompativeis(source_type: str) -> None:
    with pytest.raises(ValueError, match="invalid_key_component"):
        RunUnitIdentity("run-1", RunStage.NORMALIZE, source_type, "subtype")


@pytest.mark.parametrize(
    "component",
    [pytest.param("."), pytest.param(".."), pytest.param("a/b"), pytest.param("a\\b")],
)
def test_dataset_version_rejeita_segmentos_nao_canonicos(component: str) -> None:
    values = {
        "tenant_id": component,
        "dataset_name": "cnes",
        "version_id": "run-1",
        "run_id": "run-1",
        "run_manifest_key": f"reconciliation/{component}/2026-07/run-1/run-manifest.json",
        "created_at": NOW,
    }
    with pytest.raises(ValidationError):
        DatasetVersion.model_validate(values)


def test_publicacao_rejeita_evento_de_outro_aggregate() -> None:
    version = DatasetVersion(
        tenant_id="354130",
        dataset_name="cnes",
        version_id="run-1",
        run_id="run-1",
        run_manifest_key="reconciliation/354130/2026-07/run-1/run-manifest.json",
        created_at=NOW,
    )
    permit = PublicationPermit(
        tenant_id="354130", run_id="run-1", policy_version=1, fencing_token=0
    )
    event = OutboxEvent(
        tenant_id="354130",
        event_id="event-1",
        event_type="run.published",
        aggregate_id="run-2",
        payload={},
        created_at=NOW,
        delivered_at=None,
    )
    with pytest.raises(ValidationError, match="publication_event_mismatch"):
        PublishDataset(
            version=version,
            pointer_name="CURRENT",
            expected_version_id=None,
            final_state=RunState.PUBLISHED,
            missing_sources=(),
            publication_permit=permit,
            event=event,
        )


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param({"value": float("nan")}),
        pytest.param({"value": [float("inf")]}),
        pytest.param({"value": {"nested": float("-inf")}}),
    ],
)
def test_outbox_rejeita_float_nao_finito(payload: dict[str, object]) -> None:
    with pytest.raises(ValidationError, match="non_finite_json_float"):
        OutboxEvent(
            tenant_id="354130",
            event_id="event-1",
            event_type="event",
            aggregate_id="run-1",
            payload=payload,
            created_at=NOW,
            delivered_at=None,
        )
