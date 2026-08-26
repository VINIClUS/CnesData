from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from cnes_domain.control_plane.commands import (
    BeginIdempotency,
    BindRunDispatch,
    CancelJob,
    ClaimJob,
    ClaimRunUnit,
    CommitRunUnit,
    CompleteJob,
    FailJob,
    FailRunUnit,
    FinalizeRunCancellation,
    FinishRunDispatch,
    IdempotencyOutcome,
    PublicationPermit,
    PublishDataset,
    PutRunUnits,
    RenewJobLease,
    ReserveRunDispatch,
    TransitionRun,
)
from cnes_domain.control_plane.entities import (
    DatasetVersion,
    IdempotencyRecord,
    ManifestRef,
    OutboxEvent,
    RawManifestRecord,
    RunUnit,
)
from cnes_domain.control_plane.enums import DispatchOutcome, RunStage, RunState, RunUnitState
from cnes_domain.control_plane.ids import (
    JobIdentity,
    RunUnitIdentity,
    job_id,
    run_dependency_key,
    unit_id,
)

NOW = datetime(2026, 7, 15, 12, tzinfo=UTC)
HASH = "a" * 64
RAW_KEY = "raw/354130/CNES_LOCAL/2026-07/snapshot-1/manifest.json"
OUTPUT_KEY = "normalized/354130/2026-07/run-1/unit-1/manifest.json"


def test_job_id_preserva_vetor_canonico() -> None:
    identity = JobIdentity(
        "354130",
        "agent-01",
        "CNES_LOCAL",
        "CNES_VINCULO",
        "2026-07",
        "request-1",
    )

    assert job_id(identity) == "32aebb69a9b6ef95178567cd8388ca44"


def test_unit_id_preserva_vetor_canonico() -> None:
    identity = RunUnitIdentity("run-1", RunStage.NORMALIZE, "CNES_LOCAL", "CNES_VINCULO", "all")

    assert unit_id(identity) == "d955b5df9760e95d4e1c4e66f859a13a"


def test_ids_sao_deterministicos_e_separam_componentes() -> None:
    first = JobIdentity("a", "b", "c", "d", "2026-07", "e")
    shifted = JobIdentity("ab", "c", "d", "e", "2026-07", "x")
    unit = RunUnitIdentity("run-1", RunStage.RECONCILE)

    assert job_id(first) == job_id(first)
    assert job_id(first) != job_id(shifted)
    assert unit_id(unit) == unit_id(unit)


def test_run_dependency_key_e_codec_canonico() -> None:
    assert (
        run_dependency_key("354130", "CNES_LOCAL", "CNES_VINCULO", "2026-07")
        == "RUN_DEP#354130#CNES_LOCAL#CNES_VINCULO#2026-07"
    )


@pytest.mark.parametrize(
    "values",
    [
        ("", "CNES_LOCAL", "CNES_VINCULO", "2026-07"),
        ("354130", " ", "CNES_VINCULO", "2026-07"),
        ("354130", "CNES#LOCAL", "CNES_VINCULO", "2026-07"),
        ("354130", "CNES_LOCAL", "CNES_VINCULO", "2026-7"),
        ("354130", "CNES_LOCAL", "CNES_VINCULO", "2026-13"),
    ],
)
def test_run_dependency_key_rejeita_componentes_invalidos(values: tuple[str, ...]) -> None:
    with pytest.raises(ValueError):
        run_dependency_key(*values)


@pytest.mark.parametrize(
    ("factory", "args"),
    [
        (JobIdentity, ("", "agent", "source", "subtype", "2026-07", "key")),
        (JobIdentity, ("tenant", "agent", "source", "subtype", "2026-7", "key")),
        (JobIdentity, ("tenant", "agent", "source", "subtype", "2026-07", "")),
        (RunUnitIdentity, ("", RunStage.NORMALIZE, "source", "subtype", "all")),
        (RunUnitIdentity, ("run", RunStage.NORMALIZE, "", "subtype", "all")),
        (RunUnitIdentity, ("run", RunStage.RECONCILE, "source", "", "all")),
    ],
)
def test_identidades_rejeitam_componentes_invalidos(
    factory: type, args: tuple[object, ...]
) -> None:
    with pytest.raises((TypeError, ValueError)):
        factory(*args)


def test_identidade_de_unit_exige_enum_e_campos_de_normalize() -> None:
    with pytest.raises(TypeError):
        RunUnitIdentity("run-1", "NORMALIZE")
    with pytest.raises(ValueError, match="normalize_source_required"):
        RunUnitIdentity("run-1", RunStage.NORMALIZE)
    with pytest.raises(ValueError, match="downstream_source_forbidden"):
        RunUnitIdentity("run-1", RunStage.MATERIALIZE, "CNES_LOCAL")


def _raw_record() -> RawManifestRecord:
    return RawManifestRecord(
        tenant_id="354130",
        manifest_id="manifest-1",
        manifest_key=RAW_KEY,
        agent_id="agent-01",
        source_type="CNES_LOCAL",
        file_subtype="CNES_VINCULO",
        competencia="2026-07",
        snapshot_mode="FULL",
        snapshot_id="snapshot-1",
        base_snapshot_id=None,
        sequence=1,
        previous_manifest_sha256=None,
        manifest_sha256=HASH,
        created_at=NOW,
    )


def _idempotency_record() -> IdempotencyRecord:
    return IdempotencyRecord(
        tenant_id="354130",
        scope="job",
        key="request-1",
        request_hash=HASH,
        status="CREATED",
        resource_id="job-1",
        created_at=NOW,
        expires_at=NOW + timedelta(days=1),
    )


def _publication_values() -> dict[str, object]:
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
        aggregate_id="run-1",
        payload={},
        created_at=NOW,
        delivered_at=None,
    )
    return {
        "version": version,
        "pointer_name": "CURRENT",
        "expected_version_id": None,
        "final_state": RunState.PUBLISHED,
        "missing_sources": (),
        "publication_permit": permit,
        "event": event,
    }


def _normalize_unit(**updates: object) -> RunUnit:
    values: dict[str, object] = {
        "tenant_id": "354130",
        "run_id": "run-1",
        "unit_id": "unit-1",
        "stage": RunStage.NORMALIZE,
        "source_type": "CNES_LOCAL",
        "file_subtype": "CNES_VINCULO",
        "partition": "all",
        "depends_on_unit_ids": (),
        "input_manifests": (ManifestRef(manifest_id="manifest-1", manifest_key=RAW_KEY),),
        "state": RunUnitState.PENDING,
        "attempt": 0,
        "fencing_token": 0,
        "lease_owner": None,
        "lease_until": None,
        "dispatch_id": None,
        "output_manifests": (),
        "error_code": None,
    }
    return RunUnit.model_validate(values | updates)


def test_comandos_de_job_aceitam_valores_estritos() -> None:
    record = _raw_record()
    commands = (
        ClaimJob(tenant_id="354130", job_id="job-1", owner="worker", now=NOW, lease_seconds=30),
        RenewJobLease(
            tenant_id="354130",
            job_id="job-1",
            owner="worker",
            fencing_token=1,
            now=NOW,
            lease_seconds=30,
        ),
        CompleteJob(
            tenant_id="354130",
            job_id="job-1",
            owner="worker",
            fencing_token=1,
            manifest=record,
        ),
        FailJob(
            tenant_id="354130",
            job_id="job-1",
            owner="worker",
            fencing_token=1,
            error_code="failed",
            retryable=False,
        ),
        CancelJob(tenant_id="354130", job_id="job-1", requested_by="user-1"),
    )
    assert len(commands) == 5
    with pytest.raises(ValidationError):
        commands[0].owner = "other"


def test_comandos_de_unit_aceitam_valores_estritos() -> None:
    output = ManifestRef(manifest_id="output-1", manifest_key=OUTPUT_KEY)
    common = {"tenant_id": "354130", "run_id": "run-1"}
    commands = (
        TransitionRun(
            **common,
            expected_state=RunState.PLANNED,
            new_state=RunState.PROCESSING,
        ),
        ClaimRunUnit(
            **common,
            unit_id="unit-1",
            dispatch_id="fedcba9876543210",
            owner="worker",
            now=NOW,
            lease_seconds=30,
        ),
        CommitRunUnit(
            **common,
            unit_id="unit-1",
            dispatch_id="fedcba9876543210",
            owner="worker",
            fencing_token=1,
            output_manifests=(output,),
        ),
        FailRunUnit(
            **common,
            unit_id="unit-1",
            dispatch_id="fedcba9876543210",
            owner="worker",
            fencing_token=1,
            error_code="failed",
            retryable=True,
        ),
    )
    assert len(commands) == 4


def test_comandos_de_run_aceitam_valores_estritos() -> None:
    common = {"tenant_id": "354130", "run_id": "run-1"}
    commands = (
        FinalizeRunCancellation(
            **common,
            expected_state=RunState.CANCEL_REQUESTED,
            canceled_at=NOW,
        ),
        ReserveRunDispatch(
            **common,
            wave_id="0123456789abcdef",
            unit_ids=("unit-1",),
            now=NOW,
            lease_seconds=30,
        ),
        BindRunDispatch(
            **common,
            dispatch_id="fedcba9876543210",
            execution_ref="execution-1",
            now=NOW,
            lease_seconds=30,
        ),
        FinishRunDispatch(
            **common,
            dispatch_id="fedcba9876543210",
            outcome=DispatchOutcome.SUCCEEDED,
            finished_at=NOW,
        ),
        BeginIdempotency(
            tenant_id="354130",
            scope="job",
            key="request-1",
            request_hash=HASH,
            resource_id="job-1",
            now=NOW,
            expires_at=NOW + timedelta(days=1),
        ),
        IdempotencyOutcome(record=_idempotency_record(), created=True),
        PublicationPermit(**common, policy_version=1, fencing_token=0),
        PublishDataset.model_validate(_publication_values()),
    )
    assert len(commands) == 8


def test_comandos_rejeitam_limites_invalidos() -> None:
    common = {"tenant_id": "354130", "run_id": "run-1"}
    invalid_calls = (
        lambda: ClaimJob(
            tenant_id="354130", job_id="job-1", owner="worker", now=NOW, lease_seconds=0
        ),
        lambda: RenewJobLease(
            tenant_id="354130",
            job_id="job-1",
            owner="worker",
            fencing_token=-1,
            now=NOW,
            lease_seconds=30,
        ),
        lambda: ReserveRunDispatch(
            **common,
            wave_id="0123456789abcdef",
            unit_ids=(),
            now=NOW,
            lease_seconds=30,
        ),
        lambda: ReserveRunDispatch(
            **common,
            wave_id="0123456789abcdef",
            unit_ids=("b", "a"),
            now=NOW,
            lease_seconds=30,
        ),
        lambda: CommitRunUnit(
            **common,
            unit_id="unit-1",
            dispatch_id="fedcba9876543210",
            owner="worker",
            fencing_token=1,
            output_manifests=(),
        ),
        lambda: BeginIdempotency(
            tenant_id="354130",
            scope="job",
            key="request-1",
            request_hash=HASH,
            resource_id="job-1",
            now=NOW,
            expires_at=NOW,
        ),
    )
    for call in invalid_calls:
        with pytest.raises(ValidationError):
            call()


def test_complete_job_rejeita_manifesto_de_outro_tenant() -> None:
    with pytest.raises(ValidationError, match="manifest_identity_mismatch"):
        CompleteJob(
            tenant_id="other",
            job_id="job-1",
            owner="worker",
            fencing_token=1,
            manifest=_raw_record(),
        )


def test_publicacao_rejeita_identidades_e_estados_inconsistentes() -> None:
    values = _publication_values()
    invalid_publications = (
        {"final_state": RunState.PROCESSING},
        {"publication_permit": values["publication_permit"].model_copy(update={"run_id": "x"})},
        {"event": values["event"].model_copy(update={"tenant_id": "other"})},
        {"expected_version_id": ""},
        {"missing_sources": ("CNES_LOCAL/CNES_VINCULO",)},
        {"final_state": RunState.PUBLISHED_DEGRADED},
    )
    for update in invalid_publications:
        with pytest.raises(ValidationError):
            PublishDataset.model_validate(values | update)
    assert PublishDataset.model_validate(
        values
        | {
            "final_state": RunState.PUBLISHED_DEGRADED,
            "missing_sources": ("CNES_LOCAL/CNES_VINCULO",),
        }
    )


def test_put_run_units_valida_identidade_estado_e_dag() -> None:
    normalize = _normalize_unit()
    reconcile = RunUnit.model_validate(
        normalize.model_dump()
        | {
            "unit_id": "unit-reconcile",
            "stage": RunStage.RECONCILE,
            "source_type": None,
            "file_subtype": None,
            "input_manifests": (),
            "depends_on_unit_ids": (normalize.unit_id,),
        }
    )
    materialize = RunUnit.model_validate(
        reconcile.model_dump()
        | {
            "unit_id": "unit-materialize",
            "stage": RunStage.MATERIALIZE,
            "depends_on_unit_ids": (normalize.unit_id,),
        }
    )
    valid = {
        "tenant_id": "354130",
        "run_id": "run-1",
        "expected_run_state": RunState.PLANNED,
        "units": (normalize, reconcile),
    }
    assert PutRunUnits.model_validate(valid).units == (normalize, reconcile)
    invalid_units = (
        (),
        (normalize, normalize),
        (normalize.model_copy(update={"tenant_id": "other"}),),
        (normalize.model_copy(update={"state": RunUnitState.FAILED_RETRYABLE}),),
        (normalize, reconcile.model_copy(update={"depends_on_unit_ids": ("missing",)})),
        (normalize, materialize),
    )
    for units in invalid_units:
        with pytest.raises(ValidationError):
            PutRunUnits.model_validate(valid | {"units": units})


def test_commit_rejeita_referencias_duplicadas() -> None:
    output = ManifestRef(manifest_id="output-1", manifest_key=OUTPUT_KEY)
    duplicates = (
        (output, output),
        (output, output.model_copy(update={"manifest_key": RAW_KEY})),
        (output, output.model_copy(update={"manifest_id": "output-2"})),
    )
    for manifests in duplicates:
        with pytest.raises(ValidationError, match="duplicate_manifest_ref"):
            CommitRunUnit(
                tenant_id="354130",
                run_id="run-1",
                unit_id="unit-1",
                dispatch_id="fedcba9876543210",
                owner="worker",
                fencing_token=1,
                output_manifests=manifests,
            )


def test_identidade_rejeita_separador_de_hash() -> None:
    with pytest.raises(ValueError, match="invalid_identity_component"):
        JobIdentity("tenant\x1fother", "agent", "source", "subtype", "2026-07", "key")


@pytest.mark.parametrize(
    "source_type",
    [pytest.param("CNES#LOCAL"), pytest.param("CNES/LOCAL")],
)
def test_job_identity_aplica_regras_dos_componentes(source_type: str) -> None:
    with pytest.raises(ValueError, match="invalid_key_component"):
        JobIdentity("tenant", "agent", source_type, "subtype", "2026-07", "key")
