from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from cnes_domain.control_plane.entities import (
    AccessRequest,
    Agent,
    DatasetPointer,
    DatasetVersion,
    IdempotencyRecord,
    Job,
    ManifestRef,
    Membership,
    OutboxEvent,
    RawManifestRecord,
    Run,
    RunDependency,
    RunDispatch,
    RunUnit,
    Tenant,
)
from cnes_domain.control_plane.enums import (
    AccessRequestState,
    AgentState,
    DispatchOutcome,
    DispatchState,
    JobState,
    RunStage,
    RunState,
    RunUnitState,
)

NOW = datetime(2026, 7, 15, 12, tzinfo=UTC)
RAW_KEY = "raw/354130/CNES_LOCAL/2026-07/snapshot-1/manifest.json"
HASH = "a" * 64


def manifest_ref(identifier: str = "manifest-1") -> ManifestRef:
    return ManifestRef(manifest_id=identifier, manifest_key=RAW_KEY)


def dependency(required: bool = True) -> RunDependency:
    return RunDependency(
        source_type="CNES_LOCAL",
        file_subtype="CNES_VINCULO",
        required=required,
    )


def run_values(**updates: object) -> dict[str, object]:
    values: dict[str, object] = {
        "tenant_id": "354130",
        "run_id": "run-1",
        "competencia": "2026-07",
        "dataset_name": "cnes",
        "state": RunState.PLANNED,
        "dependencies": (dependency(),),
        "missing_sources": (),
        "created_at": NOW,
    }
    return values | updates


def unit_values(**updates: object) -> dict[str, object]:
    values: dict[str, object] = {
        "tenant_id": "354130",
        "run_id": "run-1",
        "unit_id": "unit-normalize",
        "stage": RunStage.NORMALIZE,
        "source_type": "CNES_LOCAL",
        "file_subtype": "CNES_VINCULO",
        "partition": "all",
        "depends_on_unit_ids": (),
        "input_manifests": (manifest_ref(),),
        "state": RunUnitState.PENDING,
        "attempt": 0,
        "fencing_token": 0,
        "lease_owner": None,
        "lease_until": None,
        "dispatch_id": None,
        "output_manifests": (),
        "error_code": None,
    }
    return values | updates


def raw_record(**updates: object) -> RawManifestRecord:
    values: dict[str, object] = {
        "tenant_id": "354130",
        "manifest_id": "manifest-1",
        "manifest_key": RAW_KEY,
        "agent_id": "agent-01",
        "source_type": "CNES_LOCAL",
        "file_subtype": "CNES_VINCULO",
        "competencia": "2026-07",
        "snapshot_mode": "FULL",
        "snapshot_id": "snapshot-1",
        "base_snapshot_id": None,
        "sequence": 1,
        "previous_manifest_sha256": None,
        "manifest_sha256": HASH,
        "created_at": NOW,
    }
    return RawManifestRecord.model_validate(values | updates)


def job_values(**updates: object) -> dict[str, object]:
    values: dict[str, object] = {
        "tenant_id": "354130",
        "job_id": "job-1",
        "agent_id": "agent-01",
        "source_type": "CNES_LOCAL",
        "file_subtype": "CNES_VINCULO",
        "competencia": "2026-07",
        "requested_snapshot_mode": "FULL",
        "state": JobState.PENDING,
        "attempt": 0,
        "fencing_token": 0,
        "lease_owner": None,
        "lease_until": None,
        "result_manifest_id": None,
        "result_manifest_key": None,
        "error_code": None,
        "created_at": NOW,
    }
    return values | updates


def dispatch_values(**updates: object) -> dict[str, object]:
    values: dict[str, object] = {
        "tenant_id": "354130",
        "run_id": "run-1",
        "wave_id": "0123456789abcdef",
        "dispatch_id": "fedcba9876543210",
        "generation": 1,
        "unit_ids": ("unit-a", "unit-b"),
        "state": DispatchState.RESERVED,
        "lease_until": NOW + timedelta(minutes=5),
        "execution_ref": None,
        "terminal_outcome": None,
    }
    return values | updates


def idempotency_record() -> IdempotencyRecord:
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


def outbox_event() -> OutboxEvent:
    return OutboxEvent(
        tenant_id="354130",
        event_id="event-1",
        event_type="run.published",
        aggregate_id="run-1",
        payload={"count": 1, "nested": [True, None, {"source": "CNES_LOCAL"}]},
        created_at=NOW,
        delivered_at=None,
    )


def test_modelos_sao_estritos_imutaveis_e_sem_campos_extras() -> None:
    tenant = Tenant(tenant_id="354130", municipality_name="Presidente Epitácio", created_at=NOW)

    with pytest.raises(ValidationError, match="frozen"):
        tenant.tenant_id = "other"
    with pytest.raises(ValidationError, match="Extra inputs"):
        Tenant.model_validate({**tenant.model_dump(), "unknown": "value"})
    with pytest.raises(ValidationError):
        Tenant.model_validate({**tenant.model_dump(), "tenant_id": 354130})
    with pytest.raises(ValidationError):
        Job.model_validate(job_values(state="PENDING"))


@pytest.mark.parametrize(
    ("model", "values"),
    [
        (Tenant, {"tenant_id": "354130", "municipality_name": "Municipality", "created_at": NOW}),
        (
            Membership,
            {"tenant_id": "354130", "user_id": "user-1", "role": "ADMIN", "created_at": NOW},
        ),
        (
            Agent,
            {
                "tenant_id": "354130",
                "agent_id": "agent-01",
                "state": AgentState.ACTIVE,
                "version": "1.0.0",
                "certificate_fingerprint": HASH,
                "last_seen_at": NOW,
                "created_at": NOW,
            },
        ),
        (Job, job_values()),
        (Run, run_values()),
        (RawManifestRecord, raw_record().model_dump()),
        (RunUnit, unit_values()),
        (RunDispatch, dispatch_values()),
        (
            DatasetVersion,
            {
                "tenant_id": "354130",
                "dataset_name": "cnes",
                "version_id": "run-1",
                "run_id": "run-1",
                "run_manifest_key": "reconciliation/354130/2026-07/run-1/run-manifest.json",
                "created_at": NOW,
            },
        ),
        (
            DatasetPointer,
            {
                "tenant_id": "354130",
                "dataset_name": "cnes",
                "pointer_name": "CURRENT",
                "version_id": "run-1",
                "updated_at": NOW,
            },
        ),
        (
            AccessRequest,
            {
                "tenant_id": "354130",
                "request_id": "request-1",
                "user_id": "user-1",
                "state": AccessRequestState.PENDING,
                "decided_by": None,
                "decided_at": None,
            },
        ),
        (IdempotencyRecord, idempotency_record().model_dump()),
        (OutboxEvent, outbox_event().model_dump()),
    ],
)
def test_entidades_aceitam_valores_canonicos(model: type, values: dict[str, object]) -> None:
    assert model.model_validate(values)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("tenant_id", ""),
        ("job_id", " "),
        ("competencia", "2026-13"),
        ("attempt", -1),
        ("fencing_token", -1),
        ("requested_snapshot_mode", "INCREMENTAL"),
    ],
)
def test_job_rejeita_valores_invalidos(field: str, value: object) -> None:
    with pytest.raises(ValidationError):
        Job.model_validate(job_values(**{field: value}))


def test_datas_exigem_utc_e_lease_coerente() -> None:
    with pytest.raises(ValidationError, match="datetime_not_utc"):
        Tenant(
            tenant_id="354130",
            municipality_name="Municipality",
            created_at=NOW.replace(tzinfo=None),
        )
    with pytest.raises(ValidationError, match="datetime_not_utc"):
        Tenant(
            tenant_id="354130",
            municipality_name="Municipality",
            created_at=NOW.astimezone(timezone(timedelta(hours=-3))),
        )
    with pytest.raises(ValidationError, match="lease_pair_required"):
        Job.model_validate(job_values(lease_owner="worker-1"))
    assert Job.model_validate(job_values(state=JobState.LEASED)).lease_owner is None


def test_job_sucedido_exige_manifesto_canonico() -> None:
    with pytest.raises(ValidationError, match="succeeded_manifest_required"):
        Job.model_validate(job_values(state=JobState.SUCCEEDED))
    with pytest.raises(ValidationError, match="result_manifest_pair_required"):
        Job.model_validate(job_values(result_manifest_id="manifest-1"))
    succeeded = job_values(
        state=JobState.SUCCEEDED,
        result_manifest_id="manifest-1",
        result_manifest_key=RAW_KEY,
    )
    assert Job.model_validate(succeeded).result_manifest_key == RAW_KEY
    with pytest.raises(ValidationError, match="invalid_result_manifest_key"):
        Job.model_validate(succeeded | {"result_manifest_key": "raw/other/manifest.json"})
    invalid_key = RAW_KEY.replace("manifest", "run-manifest")
    with pytest.raises(ValidationError, match="invalid_result_manifest_key"):
        Job.model_validate(succeeded | {"result_manifest_key": invalid_key})


def test_run_exige_dependencias_unicas_e_competencia_valida() -> None:
    with pytest.raises(ValidationError, match="dependencies_required"):
        Run.model_validate(run_values(dependencies=()))
    with pytest.raises(ValidationError, match="duplicate_dependency"):
        Run.model_validate(run_values(dependencies=(dependency(), dependency(False))))
    with pytest.raises(ValidationError):
        Run.model_validate(run_values(dataset_name=" "))
    with pytest.raises(ValidationError):
        Run.model_validate(run_values(competencia="2026-7"))


def test_raw_manifest_valida_full_delta_hash_e_sidecar() -> None:
    with pytest.raises(ValidationError, match="invalid_sequence"):
        raw_record(sequence=0)
    with pytest.raises(ValidationError, match="invalid_full_chain"):
        raw_record(sequence=2)
    with pytest.raises(ValidationError, match="invalid_delta_chain"):
        raw_record(snapshot_mode="DELTA")
    delta = raw_record(
        snapshot_mode="DELTA",
        base_snapshot_id="snapshot-1",
        sequence=2,
        previous_manifest_sha256="b" * 64,
    )
    assert delta.sequence == 2
    with pytest.raises(ValidationError, match="invalid_sha256"):
        raw_record(manifest_sha256="A" * 64)
    with pytest.raises(ValidationError, match="invalid_manifest_key"):
        raw_record(manifest_key="raw/data.parquet")
    with pytest.raises(ValidationError, match="invalid_manifest_key"):
        raw_record(manifest_key="raw/354130/CNES_LOCAL/2026-07/other/manifest.json")
    with pytest.raises(ValidationError, match="invalid_manifest_key"):
        ManifestRef(manifest_id="manifest-1", manifest_key="/raw/path/manifest.json")


def test_run_unit_aplica_invariantes_por_estagio() -> None:
    normalize = RunUnit.model_validate(unit_values())
    reconcile = RunUnit.model_validate(
        unit_values(
            unit_id="unit-reconcile",
            stage=RunStage.RECONCILE,
            source_type=None,
            file_subtype=None,
            depends_on_unit_ids=(normalize.unit_id,),
            input_manifests=(),
        )
    )
    materialize = RunUnit.model_validate(
        unit_values(
            unit_id="unit-materialize",
            stage=RunStage.MATERIALIZE,
            source_type=None,
            file_subtype=None,
            depends_on_unit_ids=(reconcile.unit_id,),
            input_manifests=(),
        )
    )
    assert materialize.stage is RunStage.MATERIALIZE
    with pytest.raises(ValidationError, match="normalize_inputs_required"):
        RunUnit.model_validate(unit_values(input_manifests=()))
    with pytest.raises(ValidationError, match="downstream_direct_inputs_forbidden"):
        RunUnit.model_validate({**reconcile.model_dump(), "input_manifests": (manifest_ref(),)})
    with pytest.raises(ValidationError, match="self_dependency"):
        RunUnit.model_validate(
            {**reconcile.model_dump(), "depends_on_unit_ids": ("unit-reconcile",)}
        )
    with pytest.raises(ValidationError, match="initial_dispatch_forbidden"):
        RunUnit.model_validate(unit_values(dispatch_id="fedcba9876543210"))
    invalid_values = (
        unit_values(attempt=-1),
        unit_values(error_code="not sanitized"),
        unit_values(lease_owner="worker"),
        unit_values(depends_on_unit_ids=("unit-other",)),
        {**reconcile.model_dump(), "source_type": "CNES_LOCAL"},
        {**reconcile.model_dump(), "depends_on_unit_ids": ()},
        unit_values(input_manifests=(manifest_ref(), manifest_ref())),
        unit_values(
            input_manifests=(manifest_ref(), manifest_ref("manifest-2")),
        ),
    )
    for values in invalid_values:
        with pytest.raises(ValidationError):
            RunUnit.model_validate(values)


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"generation": 0}, "generation_positive"),
        ({"wave_id": "ABCDEF0123456789"}, "invalid_wave_id"),
        ({"dispatch_id": "short"}, "invalid_dispatch_id"),
        ({"unit_ids": ()}, "unit_ids_required"),
        ({"unit_ids": ("unit-b", "unit-a")}, "unit_ids_not_ordered"),
        ({"unit_ids": ("unit-a", "unit-a")}, "duplicate_unit_id"),
        ({"state": DispatchState.STARTED}, "execution_ref_required"),
        ({"execution_ref": "execution-1"}, "execution_ref_forbidden"),
        ({"terminal_outcome": DispatchOutcome.SUCCEEDED}, "terminal_outcome_forbidden"),
        ({"state": DispatchState.TERMINAL}, "terminal_outcome_required"),
    ],
)
def test_dispatch_rejeita_estado_inconsistente(updates: dict[str, object], message: str) -> None:
    with pytest.raises(ValidationError, match=message):
        RunDispatch.model_validate(dispatch_values(**updates))


def test_dispatch_started_e_terminal_sao_consistentes() -> None:
    started = RunDispatch.model_validate(
        dispatch_values(
            state=DispatchState.STARTED,
            execution_ref="execution-1",
        )
    )
    terminal = RunDispatch.model_validate(
        dispatch_values(
            state=DispatchState.TERMINAL,
            execution_ref="execution-1",
            terminal_outcome=DispatchOutcome.SUCCEEDED,
        )
    )
    assert started.terminal_outcome is None
    assert terminal.terminal_outcome is DispatchOutcome.SUCCEEDED

def test_dataset_version_vincula_run_e_manifesto() -> None:
    values = {
        "tenant_id": "354130",
        "dataset_name": "cnes",
        "version_id": "run-1",
        "run_id": "run-1",
        "run_manifest_key": "reconciliation/354130/2026-07/run-1/run-manifest.json",
        "created_at": NOW,
    }
    with pytest.raises(ValidationError, match="version_run_mismatch"):
        DatasetVersion.model_validate(values | {"version_id": "run-2"})
    with pytest.raises(ValidationError, match="invalid_run_manifest_key"):
        DatasetVersion.model_validate(values | {"run_manifest_key": "serving/overview.json"})


def test_access_request_e_idempotencia_exigem_estado_temporal_coerente() -> None:
    base = {
        "tenant_id": "354130",
        "request_id": "request-1",
        "user_id": "user-1",
    }
    with pytest.raises(ValidationError, match="decision_pair_required"):
        AccessRequest(**base, state=AccessRequestState.PENDING, decided_by="admin", decided_at=None)
    with pytest.raises(ValidationError, match="decision_required"):
        AccessRequest(**base, state=AccessRequestState.REJECTED, decided_by=None, decided_at=None)
    with pytest.raises(ValidationError, match="decision_forbidden"):
        AccessRequest(
            tenant_id="354130",
            request_id="request-1",
            user_id="user-1",
            state=AccessRequestState.PENDING,
            decided_by="admin",
            decided_at=NOW,
        )
    approved = AccessRequest(
        tenant_id="354130",
        request_id="request-1",
        user_id="user-1",
        state=AccessRequestState.APPROVED,
        decided_by="admin",
        decided_at=NOW,
    )
    assert approved.decided_by == "admin"
    with pytest.raises(ValidationError, match="invalid_expiry"):
        IdempotencyRecord.model_validate(idempotency_record().model_dump() | {"expires_at": NOW})


def test_outbox_aceita_json_estrutural_e_rejeita_objetos() -> None:
    event = outbox_event()
    event.payload["count"] = 2
    nested = event.payload["nested"]
    assert isinstance(nested, list)
    nested.append("changed")
    assert event.payload == {"count": 1, "nested": [True, None, {"source": "CNES_LOCAL"}]}
    with pytest.raises(ValidationError):
        OutboxEvent.model_validate(outbox_event().model_dump() | {"payload": {"bad": {1, 2}}})


def test_identificadores_opcionais_presentes_rejeitam_branco() -> None:
    invalid = (
        (Job, job_values(lease_owner="", lease_until=NOW)),
        (Job, job_values(result_manifest_id="", result_manifest_key=RAW_KEY)),
        (RunUnit, unit_values(lease_owner="", lease_until=NOW)),
        (RunDispatch, dispatch_values(execution_ref="")),
    )
    for model, values in invalid:
        with pytest.raises(ValidationError):
            model.model_validate(values)


def test_unit_degradada_exige_normalize_erro_e_nenhum_output() -> None:
    valid = unit_values(state=RunUnitState.SUCCEEDED_DEGRADED, error_code="optional_failed")
    assert RunUnit.model_validate(valid).state is RunUnitState.SUCCEEDED_DEGRADED
    for update in ({"error_code": None}, {"output_manifests": (manifest_ref(),)}):
        with pytest.raises(ValidationError):
            RunUnit.model_validate(valid | update)
