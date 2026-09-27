"""Montagem, seeding e condução do runtime AWS composto sobre os emuladores (AWS-014)."""
from __future__ import annotations

import json
from dataclasses import dataclass
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

from central_api.services.raw_ingestion import RegisterRawManifest
from central_api.serving.aws_signed import SignedServingRequest
from cnes_contracts.manifests.outputs import OutputManifest, RunManifest
from cnes_contracts.manifests.raw import RawManifest, SnapshotMode, SourceType
from cnes_domain.control_plane.commands import (
    ClaimJob,
    ClaimRunUnit,
    CommitRunUnit,
    PutRunUnits,
    TransitionRun,
)
from cnes_domain.control_plane.entities import (
    Agent,
    Job,
    ManifestRef,
    Membership,
    OutboxEvent,
    Run,
    RunDispatch,
    RunUnit,
)
from cnes_domain.control_plane.enums import AgentState, JobState, RunStage, RunState, RunUnitState
from cnes_domain.orchestration.planner import PlanRequest, RawManifestRef, plan_run
from cnes_domain.ports.processing import (
    ExecutionCallbacks,
    ExecutionPolicyConfig,
    RunUnitMessage,
    StartRunExecution,
)
from cnes_domain.ports.serving import ServingRequest
from cnes_infra.auth.oidc import OidcPrincipal
from cnes_infra.control_plane.dynamodb_codec import decode_model
from cnes_infra.control_plane.dynamodb_keys import (
    dispatch_key,
    entity_key,
    idempotency_key,
    item_key,
)
from data_processor.orchestration.attempt_store import attempt_object_key, unit_attempt_prefix
from data_processor.orchestration.coordinator import (
    CoordinatorDependencies,
    PipelineCoordinator,
    allow_execution,
    noop_execution_started,
)
from data_processor.orchestration.publisher import PublishRequest
from tests.integration.aws._doubles import RecordingSession

if TYPE_CHECKING:
    from datetime import datetime

    from central_api.composition import RuntimeComponents
    from cnes_domain.ports.control_plane import ControlPlanePort
    from data_processor.composition import ProcessorRuntimeComponents
    from data_processor.orchestration.publisher import PublishResult
    from packages.cnes_infra.tests.contracts.clock import MutableClock
    from tests.integration.aws._doubles import RecordingS3Client, RecordingStepFunctionsClient

TENANT = "354130"
COMPETENCIA = "2026-01"
DATASET = "cnes"
RUN_ID = "run-aws014"
LEASE_SECONDS = 300
MAX_CONCURRENCY = 8
ISSUER = "https://issuer.aws014.test"
_AGENT_ID = "agent-aws014"
_FILE_SUBTYPE = "CNES_VINCULO"
_MAX_WAVES = 8
_FIXTURES = Path(__file__).resolve().parents[3] / "docs" / "fixtures" / "data-plane"
_START_KEYS = {"tenant_id", "run_id", "wave_id", "dispatch_id", "unit_ids", "max_concurrency"}


@dataclass(frozen=True, slots=True)
class RawSource:
    stem: str
    manifest_id: str
    source_type: SourceType

    @property
    def dependency_key(self) -> str:
        return f"{self.source_type.value}/{_FILE_SUBTYPE}"


LOCAL = RawSource("cnes-local-v1", "fixture-cnes-local-v1", SourceType.CNES_LOCAL)
NACIONAL = RawSource("cnes-nacional-v1", "fixture-cnes-nacional-v1", SourceType.CNES_NACIONAL)


@dataclass(frozen=True, slots=True)
class EmulatorResources:
    table_name: str
    data_bucket: str
    audit_bucket: str
    state_machine_arn: str
    dynamodb_endpoint: str
    service_endpoint: str


@dataclass(frozen=True, slots=True)
class AwsTestRuntime:
    api: RuntimeComponents
    processor: ProcessorRuntimeComponents
    clock: MutableClock
    resources: EmulatorResources
    s3: RecordingS3Client
    step_functions: RecordingStepFunctionsClient
    dynamodb: Any


@dataclass(frozen=True, slots=True)
class RawSubmission:
    job: Job
    manifest: RawManifest


def runtime_values(
    resources: EmulatorResources, overrides: dict[str, str] | None = None,
) -> dict[str, str]:
    values = {
        "PROFILE": "aws", "AUTH_MODE": "oidc", "AWS_REGION": RecordingSession.REGION,
        "AWS_CONTROL_PLANE_TABLE": resources.table_name,
        "AWS_DATA_BUCKET": resources.data_bucket, "AWS_AUDIT_BUCKET": resources.audit_bucket,
        "AWS_STATE_MACHINE_ARN": resources.state_machine_arn,
        "AWS_PROCESSOR_CONTAINER_NAME": "processor",
        "AWS_PROCESSOR_MAX_CONCURRENCY": str(MAX_CONCURRENCY),
        "AWS_PROCESSOR_LEASE_SECONDS": str(LEASE_SECONDS),
        "AWS_PROCESSOR_RECOVERY_BATCH_SIZE": "10",
        "AWS_SERVING_URL_TTL_SECONDS": "300", "AWS_AUDIT_RETENTION_DAYS": "1",
        "OIDC_ISSUER": ISSUER, "OIDC_AUDIENCE": "cnesdata-dashboard",
        "DYNAMODB_ENDPOINT_URL": resources.dynamodb_endpoint,
        "AWS_ENDPOINT_URL": resources.service_endpoint,
    }
    return values | (overrides or {})


def new_session(resources: EmulatorResources) -> RecordingSession:
    return RecordingSession.for_emulator(resources.audit_bucket)


def principal(user_id: str) -> OidcPrincipal:
    return OidcPrincipal(issuer=ISSUER, subject=user_id, email=None, display_name=None)


def seed_membership(runtime: AwsTestRuntime, tenant_id: str, user_id: str) -> None:
    runtime.api.control_plane.put_membership(Membership(
        tenant_id=tenant_id, user_id=user_id, role="gestor",
        created_at=runtime.clock.now(), oidc_issuer=ISSUER,
    ))


def delete_membership(runtime: AwsTestRuntime, tenant_id: str, user_id: str) -> None:
    runtime.dynamodb.delete_item(
        TableName=runtime.resources.table_name,
        Key=item_key(*entity_key(tenant_id, "MEMBERSHIP", user_id)),
    )


def _event(tenant_id: str, event_id: str, now: datetime) -> OutboxEvent:
    return OutboxEvent(
        tenant_id=tenant_id, event_id=event_id, event_type="aws014.seeded",
        aggregate_id=event_id, payload={}, created_at=now, delivered_at=None,
    )


def raw_data_key(source: RawSource, tenant_id: str = TENANT) -> str:
    return f"raw/{tenant_id}/{source.source_type}/{COMPETENCIA}/{source.manifest_id}/data.parquet"


def _raw_manifest(runtime: AwsTestRuntime, source: RawSource, tenant_id: str) -> RawManifest:
    body = (_FIXTURES / f"{source.stem}.parquet").read_bytes()
    digest = sha256(body).hexdigest()
    key = raw_data_key(source, tenant_id)
    runtime.api.object_store.put(key, BytesIO(body), digest)
    files = json.loads((_FIXTURES / "fixture-manifest.json").read_text())["files"]
    meta = files[f"{source.stem}.parquet"]
    return RawManifest(
        manifest_version=1, manifest_id=source.manifest_id, tenant_id=tenant_id,
        source_type=source.source_type, file_subtype=_FILE_SUBTYPE, competencia=COMPETENCIA,
        agent_id=_AGENT_ID, agent_version="1.0.0", schema_version=meta["schema_version"],
        snapshot_mode=SnapshotMode.FULL, snapshot_id=source.manifest_id,
        base_snapshot_id=None, sequence=1, previous_manifest_sha256=None,
        object_sha256=digest, row_count=meta["row_count"], size_bytes=len(body),
        object_key=key, created_at=runtime.clock.now(),
    )


def create_raw_job(
    runtime: AwsTestRuntime, source: RawSource, tenant_id: str = TENANT,
) -> RawSubmission:
    control_plane, now = runtime.api.control_plane, runtime.clock.now()
    control_plane.put_agent(Agent(
        tenant_id=tenant_id, agent_id=_AGENT_ID, state=AgentState.ACTIVE, version="1.0.0",
        certificate_fingerprint="a" * 64, last_seen_at=None, created_at=now,
    ))
    manifest = _raw_manifest(runtime, source, tenant_id)
    job = control_plane.create_job(Job(
        tenant_id=tenant_id, job_id=f"job-{source.manifest_id}", agent_id=_AGENT_ID,
        source_type=source.source_type.value, file_subtype=_FILE_SUBTYPE,
        competencia=COMPETENCIA, requested_snapshot_mode="FULL", state=JobState.PENDING,
        attempt=0, fencing_token=0, lease_owner=None, lease_until=None,
        result_manifest_id=None, result_manifest_key=None, error_code=None, created_at=now,
    ), _event(tenant_id, f"job.created:{source.manifest_id}", now))
    return RawSubmission(job=job, manifest=manifest)


def accept_raw_job(runtime: AwsTestRuntime, submission: RawSubmission) -> Job:
    control_plane, now, job = runtime.api.control_plane, runtime.clock.now(), submission.job
    claimed = control_plane.claim_job(ClaimJob(
        tenant_id=job.tenant_id, job_id=job.job_id, owner=_AGENT_ID, now=now,
        lease_seconds=LEASE_SECONDS,
    ))
    assert claimed is not None, "raw_job_claim=missing"
    manifest = submission.manifest
    runtime.api.raw_ingestion.register(RegisterRawManifest(
        tenant_id=job.tenant_id, agent_id=job.agent_id, job_id=job.job_id, owner=_AGENT_ID,
        fencing_token=claimed.fencing_token, manifest=manifest,
        manifest_bytes=manifest.model_dump_json(exclude_none=False, by_alias=False).encode(),
        now=now,
    ))
    return control_plane.get_job(job.tenant_id, job.job_id)


def submit_frozen_raw(runtime: AwsTestRuntime) -> None:
    for source in (LOCAL, NACIONAL):
        accept_raw_job(runtime, create_raw_job(runtime, source))


def break_raw_source(runtime: AwsTestRuntime, source: RawSource) -> None:
    runtime.api.object_store.delete(raw_data_key(source))


def put_planned_run(runtime: AwsTestRuntime, run_id: str, tenant_id: str = TENANT) -> Run:
    definition = runtime.api.source_catalog.for_pipeline(DATASET)
    run = Run(
        tenant_id=tenant_id, run_id=run_id, competencia=COMPETENCIA, dataset_name=DATASET,
        state=RunState.PLANNED, dependencies=definition.dependencies, missing_sources=(),
        created_at=runtime.clock.now(),
    )
    runtime.api.control_plane.put_run(run)
    return run


def launch_frozen_cnes_run(runtime: AwsTestRuntime, run_id: str = RUN_ID) -> Run:
    submit_frozen_raw(runtime)
    run = put_planned_run(runtime, run_id)
    return runtime.api.run_planning.launch(run.tenant_id, run.run_id).run


def planned_run(runtime: AwsTestRuntime, run_id: str = RUN_ID) -> Run:
    submit_frozen_raw(runtime)
    run = put_planned_run(runtime, run_id)
    manifests = tuple(
        RawManifestRef(
            manifest_id=source.manifest_id,
            manifest_key=raw_data_key(source).removesuffix("data.parquet") + "manifest.json",
            source_type=source.source_type.value, file_subtype=_FILE_SUBTYPE,
            partition=COMPETENCIA,
        )
        for source in (LOCAL, NACIONAL)
    )
    plan = plan_run(PlanRequest(run=run, manifests=manifests, deployment_limit=MAX_CONCURRENCY))
    control_plane = runtime.api.control_plane
    control_plane.put_run_units(PutRunUnits(
        tenant_id=run.tenant_id, run_id=run.run_id, expected_run_state=RunState.PLANNED,
        units=plan.units,
    ))
    return control_plane.transition_run(TransitionRun(
        tenant_id=run.tenant_id, run_id=run.run_id, expected_state=RunState.PLANNED,
        new_state=RunState.PROCESSING, missing_sources=(),
    ), _event(run.tenant_id, f"run.processing:{run.run_id}", runtime.clock.now()))


def active_dispatch(runtime: AwsTestRuntime, run: Run) -> RunDispatch:
    dispatch = runtime.processor.control_plane.get_active_run_dispatch(run.tenant_id, run.run_id)
    assert dispatch is not None, "active_dispatch=missing"
    return dispatch


def _base_item(runtime: AwsTestRuntime, key: tuple[str, str]) -> dict[str, Any]:
    request = {"TableName": runtime.resources.table_name, "Key": item_key(*key)}
    return runtime.dynamodb.get_item(**request, ConsistentRead=True)["Item"]


def latest_dispatch(runtime: AwsTestRuntime, run: Run) -> RunDispatch:
    return decode_model(_base_item(runtime, dispatch_key(run.tenant_id, run.run_id)), RunDispatch)


def idempotency_item(runtime: AwsTestRuntime, scope: str, key: str) -> dict[str, Any]:
    return _base_item(runtime, idempotency_key(TENANT, scope, key))


def units_of(runtime: AwsTestRuntime, run: Run) -> tuple[RunUnit, ...]:
    return runtime.processor.control_plane.list_run_units(run.tenant_id, run.run_id)


def stages(runtime: AwsTestRuntime, run: Run, unit_ids: tuple[str, ...]) -> tuple[RunStage, ...]:
    by_id = {unit.unit_id: unit.stage for unit in units_of(runtime, run)}
    return tuple(sorted({by_id[unit_id] for unit_id in unit_ids}))


def unit_message(dispatch: RunDispatch, unit_id: str, now: datetime) -> RunUnitMessage:
    return RunUnitMessage(
        tenant_id=dispatch.tenant_id, run_id=dispatch.run_id, wave_id=dispatch.wave_id,
        dispatch_id=dispatch.dispatch_id, unit_id=unit_id, owner=dispatch.execution_ref,
        now=now, lease_seconds=LEASE_SECONDS,
    )


def drive_dispatch_units(
    runtime: AwsTestRuntime, dispatch: RunDispatch, status: str = "SUCCEEDED",
) -> tuple[RunUnit, ...]:
    handled = tuple(
        runtime.processor.unit_handler.handle(unit_message(dispatch, unit_id, runtime.clock.now()))
        for unit_id in dispatch.unit_ids
    )
    runtime.step_functions.set_status(dispatch.execution_ref, status)
    return handled


def resume_and_active_dispatch(runtime: AwsTestRuntime, run: Run) -> RunDispatch:
    runtime.processor.coordinator.resume(run.tenant_id, run.run_id)
    return active_dispatch(runtime, run)


def drive_run_to_terminal(runtime: AwsTestRuntime, run: Run) -> Run:
    for _ in range(_MAX_WAVES):
        result = runtime.processor.coordinator.resume(run.tenant_id, run.run_id)
        if result.state is not RunState.PROCESSING:
            return runtime.processor.control_plane.get_run(run.tenant_id, run.run_id)
        drive_dispatch_units(runtime, active_dispatch(runtime, run))
    raise AssertionError("run_terminal=unreached")


def recorded_start_requests(runtime: AwsTestRuntime, run: Run) -> tuple[StartRunExecution, ...]:
    requests = []
    for _, raw_input in runtime.step_functions.started:
        payload = json.loads(raw_input)
        assert set(payload) == _START_KEYS, f"start_input_keys={sorted(payload)}"
        request = StartRunExecution.model_validate(
            {**payload, "unit_ids": tuple(payload["unit_ids"])},
        )
        if (request.tenant_id, request.run_id) == (run.tenant_id, run.run_id):
            requests.append(request)
    return tuple(requests)


def execution_name(execution_ref: str) -> str:
    return execution_ref.rpartition(":")[2]


def claim_unit(
    runtime: AwsTestRuntime, dispatch: RunDispatch, owner: str, lease_seconds: int,
) -> RunUnit:
    claimed = runtime.processor.control_plane.claim_run_unit(ClaimRunUnit(
        tenant_id=dispatch.tenant_id, run_id=dispatch.run_id, unit_id=dispatch.unit_ids[0],
        dispatch_id=dispatch.dispatch_id, owner=owner, now=runtime.clock.now(),
        lease_seconds=lease_seconds,
    ))
    assert claimed is not None, "unit_claim=missing"
    return claimed


def commit_unit(runtime: AwsTestRuntime, unit: RunUnit, manifest_id: str) -> RunUnit:
    key = attempt_object_key(unit_attempt_prefix(unit), f"manifests/{manifest_id}/manifest.json")
    command = CommitRunUnit(
        tenant_id=unit.tenant_id, run_id=unit.run_id, unit_id=unit.unit_id,
        dispatch_id=unit.dispatch_id, owner=unit.lease_owner, fencing_token=unit.fencing_token,
        output_manifests=(ManifestRef(manifest_id=manifest_id, manifest_key=key),),
    )
    event_id = f"run_unit.succeeded:{unit.unit_id}:{unit.attempt}:{manifest_id}"
    event = _event(unit.tenant_id, event_id, runtime.clock.now())
    return runtime.processor.control_plane.commit_run_unit(command, event)


def build_coordinator(
    runtime: AwsTestRuntime, control_plane: ControlPlanePort,
) -> PipelineCoordinator:
    return PipelineCoordinator(
        CoordinatorDependencies(
            control_plane=control_plane, executor=runtime.processor.executor,
            publisher=runtime.processor.publisher, clock=runtime.clock.now,
        ),
        ExecutionPolicyConfig(
            MAX_CONCURRENCY, LEASE_SECONDS,
            ExecutionCallbacks(allow_execution, noop_execution_started),
        ),
    )


def serving_key(tenant_id: str, run_id: str) -> str:
    return f"serving/{tenant_id}/{run_id}/overview.json"


def serving_request(user_id: str, tenant_id: str, relative_name: str) -> SignedServingRequest:
    return SignedServingRequest(
        access=ServingRequest(user_id=user_id, tenant_id=tenant_id, dataset_name=DATASET),
        relative_name=relative_name,
    )


def _materialized_unit(runtime: AwsTestRuntime, run: Run) -> RunUnit:
    body = b'{"schema_version": "cnes-serving-v1"}'
    digest = sha256(body).hexdigest()
    object_key = serving_key(run.tenant_id, run.run_id)
    prefix = unit_attempt_prefix(SimpleNamespace(
        tenant_id=run.tenant_id, run_id=run.run_id, unit_id="unit-materialize", attempt=1,
    ))
    manifest = OutputManifest(
        manifest_version=1, manifest_id=f"serving-{run.run_id}", tenant_id=run.tenant_id,
        layer="serving", source_type=None, competencia=COMPETENCIA, run_id=run.run_id,
        unit_id="unit-materialize", attempt=1, schema_version="cnes-serving-v1",
        object_key=object_key, object_sha256=digest, row_count=1,
        created_at=runtime.clock.now(),
    )
    store = runtime.api.object_store
    store.put(attempt_object_key(prefix, object_key), BytesIO(body), digest)
    sidecar = attempt_object_key(prefix, f"manifests/{manifest.manifest_id}/manifest.json")
    payload = manifest.model_dump_json(exclude_none=False, by_alias=False).encode()
    store.put(sidecar, BytesIO(payload), sha256(payload).hexdigest())
    return RunUnit(
        tenant_id=run.tenant_id, run_id=run.run_id, unit_id="unit-materialize",
        stage=RunStage.MATERIALIZE, source_type=None, file_subtype=None, partition="all",
        depends_on_unit_ids=("unit-upstream",), input_manifests=(),
        state=RunUnitState.SUCCEEDED, attempt=1, fencing_token=1, lease_owner=None,
        lease_until=None, dispatch_id=None,
        output_manifests=(ManifestRef(manifest_id=manifest.manifest_id, manifest_key=sidecar),),
        error_code=None,
    )


def prepare_publication(
    runtime: AwsTestRuntime, run_id: str, tenant_id: str = TENANT,
) -> PublishRequest:
    now = runtime.clock.now()
    run = Run(
        tenant_id=tenant_id, run_id=run_id, competencia=COMPETENCIA, dataset_name=DATASET,
        state=RunState.PUBLISHING,
        dependencies=runtime.api.source_catalog.for_pipeline(DATASET).dependencies,
        missing_sources=(), created_at=now,
    )
    unit = _materialized_unit(runtime, run)
    runtime.api.control_plane.put_run(run)
    pointer = runtime.api.control_plane.get_dataset_pointer(tenant_id, DATASET)
    expected = None if pointer is None else pointer.version_id
    return PublishRequest(run=run, units=(unit,), expected_version_id=expected, now=now)


def publish_version(
    runtime: AwsTestRuntime, run_id: str, tenant_id: str = TENANT,
) -> PublishResult:
    return runtime.processor.publisher.publish(prepare_publication(runtime, run_id, tenant_id))


def run_manifest_key(run: Run) -> str:
    return f"reconciliation/{run.tenant_id}/{run.competencia}/{run.run_id}/run-manifest.json"


def run_manifest_puts(runtime: AwsTestRuntime, run: Run) -> int:
    return runtime.s3.puts.count((runtime.resources.data_bucket, run_manifest_key(run)))


def read_run_manifest(runtime: AwsTestRuntime, run: Run) -> RunManifest:
    with runtime.api.object_store.open(run_manifest_key(run)) as stream:
        return RunManifest.model_validate_json(stream.read())


def pending_events(runtime: AwsTestRuntime) -> tuple[tuple[str, str], ...]:
    events = runtime.api.control_plane.pending_outbox(100)
    return tuple((event.event_type, event.aggregate_id) for event in events)


def audit_keys(runtime: AwsTestRuntime) -> tuple[str, ...]:
    response = runtime.s3.list_objects_v2(Bucket=runtime.resources.audit_bucket, Prefix="audit/")
    return tuple(sorted(item["Key"] for item in response.get("Contents", ())))
