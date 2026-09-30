# CnesData AWS Runtime Profile Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Compose and verify the multi-tenant AWS application profile on the stable CND ports and adapters, with generic OIDC authorization, Step Functions Standard/ECS execution, CloudWatch-compatible logs, Object-Locked audit delivery, and tenant-safe signed serving access.

**Architecture:** Keep domain and orchestration contracts independent of AWS while composing their DynamoDB, S3, Step Functions, ECS, and audit implementations in thin runtime modules. OIDC establishes identity only; tenant selection is resolved and revalidated server-side through canonical membership keys, and every correctness-sensitive operation rechecks base state rather than trusting a GSI, TTL deletion, workflow retry, or object-store side effect. Production resource creation, IAM policies, networking, state-machine deployment, bucket configuration, and alarms require a separate deployment specification; this plan supplies application code, runtime validation, required resource contracts, and emulator-backed tests only.

**Tech Stack:** Python 3.13, FastAPI, PEP 544 ports, boto3/botocore, python-jose, httpx, Amazon DynamoDB/DynamoDB Local, Amazon S3/S3-compatible test service, AWS Step Functions Standard, Amazon ECS Fargate, CloudWatch Logs via JSON stdout, pytest, pytest-asyncio, Hypothesis, Docker Compose test services.

**Spec:** `docs/superpowers/specs/2026-08-16-parquet-data-plane-orchestration-design.md`; execution order and ownership: `docs/superpowers/specs/2026-08-23-cnesdata-redesign-execution-design.md`

## Global Constraints

- Start from a green `develop` containing `CND-010` through `CND-045`; this plan does not change CND domain contracts or adapter invariants.
- The inspected planning baseline is `develop@f1ca71bb4277e9b1354fa11d8997a00871fa6c36`; at that commit the target CND ports/adapters do not yet exist, so the dependency gate below is mandatory before dispatch.
- The `aws` profile is multi-tenant and uses DynamoDB and S3; PostgreSQL/RDS, MinIO, Keycloak, and BigQuery are not runtime fallbacks.
- `AUTH_MODE=oidc` is required for `PROFILE=aws`; the issuer and audience are deployment configuration. Cognito is only an optional issuer preset and no Cognito-specific claim appears in domain or application interfaces.
- Token claims establish issuer and subject only. A request cannot select or authorize a tenant from a token claim, query parameter, body field, object key, or stale GSI result.
- DynamoDB uses one control-plane table per environment. GSIs only discover candidates; membership authorization, claims, revocation, commits, idempotency, and publication use canonical base keys plus conditional operations.
- DynamoDB TTL is garbage collection only. `expires_at <= now` is evaluated by application/adapter logic even while the expired item still exists physically.
- Step Functions executions are `STANDARD`; fan-out uses Inline Map with explicit `MaxConcurrencyPath`. Distributed Map is rejected by runtime compatibility validation.
- Effective concurrency is `min(ready_units, deployment_limit, authorization_max_concurrency)`, computed before each normalize, reconcile, materialize, or retry dispatch.
- Workflow payloads contain only tenant, run, logical wave, dispatch-attempt and unit IDs plus concurrency. Object keys, attempt numbers, fencing tokens, Parquet rows, and serving bodies are loaded behind the control-plane/object-store ports and never enter Step Functions input.
- ECS processing uses Fargate. The state-machine definition owns cluster, task-definition, network, and role references; application code neither creates nor updates those resources.
- Processing is at-least-once. Execution names, unit IDs, attempt paths, idempotency records, and fencing protect retries; no task may claim exactly-once behavior.
- Dispatch identity is control-plane state, not an AWS-derived hash of a mutable worker attempt. The canonical CAS allocator reuses an unexpired `RESERVED|STARTED` dispatch for the same logical wave; after a terminal dispatch, or an expired dispatch lease with no active unit lease, it increments `generation` and reserves a new `dispatch_id`. AWS code only transports those IDs.
- S3 buckets remain private. The browser receives only a short-lived signed GET for an authorized `serving/<tenant>/<run_id>/...` object; raw, normalized, reconciliation, tmp, and audit keys are never signed for frontend access.
- Audit events are delivered through the stable outbox dispatcher to a versioned S3 bucket with Object Lock. Operational logs are JSON lines on stdout for the ECS `awslogs` driver; logs are not domain audit records.
- API and processor composition roots remain distinct: the API/Billing `RuntimeComponents` always carries raw ingestion, while `ProcessorRuntimeComponents` always carries the canonical dataset publisher and source registry. Neither root drops CND-060 fields or recreates its services.
- Application code uses the boto3 default credential/provider chain and configured region/endpoints. It never reads static AWS access-key or secret-key settings.
- Production resource provisioning, retention governance, KMS key selection, IAM policy documents, VPC/subnet/security-group creation, Route 53, certificates, CloudWatch retention/alarms, and deployment rollout are out of scope until a deployment specification is approved.
- Python requires `>=3.13`; line width is at most 100 characters; function bodies are at most 50 lines; cyclomatic complexity is at most 10; files are at most 500 lines; functions accept at most four parameters; nesting is at most three levels.
- Package tests retain 100% branch coverage where enforced; application tests retain 90% line coverage where enforced.
- Test names describe behavior in Portuguese. Logs use one JSON object per line and exclude bearer tokens, signed query strings, emails, object contents, and AWS credentials.
- Use branches named `feat/aws-010-aws-runtime`, `feat/aws-011-oidc-membership`, `feat/aws-012-step-functions-ecs`, `feat/aws-013-observability-serving`, and `test/aws-014-aws-integration`.
- Never commit directly to `main`. Each branch starts from the latest green dependency-complete `develop` head.

### Dependency Interface Gate

Before Task 1, verify these CND-owned modules and exact signatures exist on integrated `develop`. They are consumed, never redefined, by this plan:

All `BaseClient` annotations below import `botocore.client.BaseClient`, matching the CND adapter
contracts and avoiding an undeclared service-stub dependency.

| Import path | Required symbol/signature |
|---|---|
| `cnes_domain.profiles` | `parse_profile(env: Mapping[str, str]) -> ProfileSettings` for the unchanged local branch |
| `cnes_domain.ports.control_plane` | `ControlPlanePort.get_membership(tenant_id: str, user_id: str) -> Membership | None` |
| `cnes_domain.ports.control_plane` | `ControlPlanePort.get_dataset_pointer(tenant_id: str, dataset_name: str) -> DatasetPointer | None` |
| `cnes_domain.control_plane.entities` | `Membership(tenant_id: str, user_id: str, role: str, created_at: datetime)`; absence means unauthorized |
| `cnes_domain.control_plane.commands` | `ClaimRunUnit(tenant_id, run_id, unit_id, dispatch_id, owner, now, lease_seconds)`; `ReserveRunDispatch(tenant_id, run_id, wave_id, unit_ids, now, lease_seconds)`; `BindRunDispatch(tenant_id, run_id, dispatch_id, execution_ref, now, lease_seconds)`; `FinishRunDispatch(tenant_id, run_id, dispatch_id, outcome, finished_at)`; plus `ClaimJob`, `CommitRunUnit`, `BeginIdempotency`, and `PublishDataset` with the CND registry fields |
| `cnes_domain.control_plane.entities` | `RunDispatch(tenant_id, run_id, wave_id, dispatch_id, generation, unit_ids, state, lease_until, execution_ref=None, terminal_outcome=None)` using canonical `DispatchState`/`DispatchOutcome` |
| `cnes_domain.ports.serving` | `ServingRequest(user_id: str, tenant_id: str, dataset_name: str)`, `ServingGrant(tenant_id: str, run_id: str, version_id: str, object_keys: tuple[str, ...])`, and `ServingAccessPort.authorize(request) -> ServingGrant` |
| `cnes_domain.ports.object_store` | `ObjectStorePort.open(key: str) -> ContextManager[BinaryIO]`; `ObjectStorePort.stat(key: str) -> ObjectStat | None` |
| `cnes_domain.ports.processing` | `StartRunExecution(tenant_id: str, run_id: str, wave_id: str, dispatch_id: str, unit_ids: tuple[str, ...], max_concurrency: int)` and `CancelRunExecution(tenant_id: str, run_id: str, execution_ref: str | None)`; logical `wave_id` is stable for an ordered ready set, while durable `dispatch_id` is stable only for one start attempt |
| `cnes_domain.ports.processing` | `ProcessorExecutorPort.start(request: StartRunExecution) -> str`; `ProcessorExecutorPort.cancel(request: CancelRunExecution) -> None`; `ProcessorExecutorPort.status(execution_ref: str) -> ExecutionStatus` |
| `cnes_domain.ports.processing` | `ConcurrencyPolicy = Callable[[Run, RunDispatch, int], ExecutionPermit]`; `ExecutionStarted(run, request, execution_ref, permit)` plus canonical `ExecutionCallbacks` and `ExecutionPolicyConfig`; the identical permit object crosses the callback |
| `cnes_domain.orchestration.planner` | `execution_request(plan: RunPlan, dispatch: RunDispatch, max_concurrency: int) -> StartRunExecution`; it transports the already-reserved `wave_id`/`dispatch_id` and never derives an AWS retry identity |
| `cnes_domain.ports.control_plane` | `reserve_run_dispatch(command: ReserveRunDispatch) -> RunDispatch`, `bind_run_dispatch(command: BindRunDispatch) -> RunDispatch`, `finish_run_dispatch(command: FinishRunDispatch) -> RunDispatch`, and `get_active_run_dispatch(tenant_id: str, run_id: str) -> RunDispatch | None` |
| `cnes_domain.ports.control_plane` | `list_recoverable_runs(now: datetime, limit: int = 100) -> tuple[Run, ...]` returns bounded, strongly revalidated `WAITING_INPUTS|PROCESSING|PUBLISHING|CANCEL_REQUESTED` candidates; TTL/GSI state is never authoritative |
| `cnes_domain.outbox_dispatcher` | `dispatch_once(control_plane, sink, now, limit=100) -> DispatchResult` |
| `cnes_infra.control_plane.dynamodb_adapter` | `DynamoDBControlPlane(client: BaseClient, table_name: str, clock: Callable[[], datetime])` |
| `cnes_infra.object_store.s3` | `S3ObjectStore(client: BaseClient, bucket: str, prefix: str = "")` |
| `cnes_infra.audit.s3_object_lock_sink` | `S3ObjectLockAuditSink(client: BaseClient, bucket: str, retention_days: int)` with `append(event: OutboxEvent) -> None` |
| `cnes_infra.executor.step_functions` | CND-042 `StepFunctionsExecutor(client: BaseClient, state_machine_arn: str)` implementing `ProcessorExecutorPort`; AWS-012 extends this class in place and preserves the two-argument constructor |
| `central_api.composition` | `build_local_runtime(settings: ProfileSettings, clock: Callable[[], datetime]) -> LocalRuntime` |
| `data_processor.composition` | `build_local_processor_runtime(settings: ProfileSettings, clock: Callable[[], datetime]) -> LocalProcessorRuntime` |
| `central_api.services.raw_ingestion` | `RawIngestionService(control_plane, object_store, DeltaPolicy(), accepted_manifest=noop)`; reused with AWS ports, not reimplemented |
| `data_processor.orchestration.publisher` | canonical `DatasetPublisher(store=ObjectStorePort, control_plane=ControlPlanePort)` |
| `cnes_domain.orchestration.source_catalog` | canonical `build_source_catalog() -> SourceCatalog` shared by API planning and processor dispatch |
| `data_processor.composition` | canonical `build_source_registry(catalog: SourceCatalog | None = None) -> SourceRegistry`; AWS composition reuses the same definitions and stage functions |
| `cnes_domain.ports.processing` | `RunUnitMessage(tenant_id: str, run_id: str, wave_id: str, dispatch_id: str, unit_id: str, owner: str, now: datetime, lease_seconds: int)` |
| `data_processor.orchestration.unit_handler` | `RunUnitCommandHandler(unit_worker: UnitWorker).handle(message: RunUnitMessage) -> RunUnit`; it alone converts the transport message to dispatch-aware `ClaimRunUnit` and calls `UnitWorker.execute` |
| `data_processor.orchestration.coordinator` | `allow_execution(run: Run, dispatch: RunDispatch, requested_limit: int) -> ExecutionPermit`; `CoordinatorDependencies`; `PipelineCoordinator(dependencies: CoordinatorDependencies, execution: ExecutionPolicyConfig)`; `recover(limit: int = 100) -> tuple[CoordinatorResult, ...]` owns canonical status/lease/CAS recovery with its injected clock; policy bundle types are imported from `cnes_domain.ports.processing` |
| `central_api.services.run_planning` | `RunPlanningDependencies(control_plane, object_store, executor, source_catalog)` and `RunPlanningService(dependencies: RunPlanningDependencies, execution: ExecutionPolicyConfig, clock: Callable[[], datetime])`; it owns the same canonical start/bind/callback compensation for the initial wave |
| `central_api.services.serving_access` | `LocalServingAccess(control_plane, object_store).authorize(request: ServingRequest) -> ServingGrant`; despite its name, this is the storage-neutral canonical pointer/membership policy that reads the active immutable run manifest before S3 signing |
| `docker-compose.yml` | profile `aws-test`; services `dynamodb-local` at `DYNAMODB_ENDPOINT_URL=http://127.0.0.1:18000` and `aws-emulator` at `AWS_ENDPOINT_URL=http://127.0.0.1:4566` |
| `.github/workflows/python-quality.yml` | CND-025 adapter-emulator job and teardown surface, extended only by serial Task 10 |

If this gate fails, integration owns a contract-name reconciliation before feature dispatch. Feature branches do not edit the CND-owned files above.

The gate also verifies behavior, not only imports: `reserve_run_dispatch` replays the same unexpired
`RESERVED|STARTED` record for the same wave, while a terminal record or an expired dispatch lease
with no live unit lease advances `generation` by CAS and derives a new `dispatch_id` even when no
ECS claim ever occurred. `bind_run_dispatch` accepts only that generation and is idempotent for the
same execution reference; `finish_run_dispatch` fences stale generation/reference pairs.
`claim_run_unit` accepts only a `ClaimRunUnit.dispatch_id` matching the active dispatch of a
`PROCESSING` Run. AWS tasks do not reimplement these transitions or infer them from Step Functions
status.

### File Map and Ownership

| Path | Responsibility | Owner |
|---|---|---|
| `packages/cnes_infra/src/cnes_infra/aws/settings.py` | Immutable AWS application settings and validation | `AWS-010` |
| `packages/cnes_infra/src/cnes_infra/aws/runtime.py` | boto3 client bundle and stable adapter construction | `AWS-010` |
| `packages/cnes_infra/src/cnes_infra/auth/oidc.py` | Generic discovery/JWKS token verification | `AWS-011` |
| `packages/cnes_infra/src/cnes_infra/auth/dynamodb_memberships.py` | GSI membership candidate discovery only | `AWS-011` |
| `apps/central_api/src/central_api/auth/aws_oidc.py` | Server-side membership resolution and base-key authorization | `AWS-011` |
| `packages/cnes_infra/src/cnes_infra/executor/step_functions.py` | Extend the canonical CND-042 executor with Standard Inline Map/ECS validation | integration-owned, serial (`AWS-012`) |
| `packages/cnes_infra/src/cnes_infra/observability/json_logging.py` | Redacting JSON stdout formatter | `AWS-013` |
| `apps/central_api/src/central_api/serving/aws_signed.py` | Tenant-safe signed serving application service | `AWS-013` |
| `apps/central_api/src/central_api/deps.py` | Select and install the AWS runtime bundle | integration-owned, serial |
| `apps/central_api/src/central_api/composition.py` | Preserve CND local builder and add the API/Billing-compatible AWS root | integration-owned, serial |
| `apps/central_api/src/central_api/middleware.py` | Install generic OIDC principal and canonical membership authorization | integration-owned, serial |
| `apps/central_api/src/central_api/routes/serving.py` | Preserve local streaming and select AWS signed redirect delivery | integration-owned, serial |
| `apps/data_processor/src/data_processor/main.py` | Select AWS executor/runtime and JSON stdout logging | integration-owned, serial |
| `apps/data_processor/src/data_processor/composition.py` | Preserve CND local builder and add the AWS processor root | integration-owned, serial |
| `apps/data_processor/src/data_processor/aws_entrypoint.py` | Parse the seven-field ECS envelope into canonical `RunUnitMessage` | integration-owned, serial |
| `apps/data_processor/src/data_processor/recovery.py` | Observe executions and invoke bounded canonical recovery | integration-owned, serial |
| `packages/cnes_infra/src/cnes_infra/auth/__init__.py` | Shared authentication exports | integration-owned, serial |
| `packages/cnes_infra/src/cnes_infra/aws/__init__.py` | Shared AWS runtime exports | integration-owned, serial |
| `packages/cnes_infra/src/cnes_infra/observability/__init__.py` | Shared logging exports | integration-owned, serial |
| `apps/central_api/src/central_api/serving/__init__.py` | Shared signed-serving exports | integration-owned, serial |
| `.env.example` | AWS profile application settings and separate emulator endpoints | integration-owned, serial |
| `docker-compose.yml`, `.github/workflows/python-quality.yml` | Extend the CND-025 `aws-test` emulator/CI surface for Step Functions | integration-owned, serial |
| `tests/integration/aws/**` | AWS-014 security, consistency, fencing, idempotency, and recovery suites | `AWS-014` |

---

### Task 1: AWS-010 Immutable Runtime Settings

**Files:**
- Create: `packages/cnes_infra/src/cnes_infra/aws/settings.py`
- Create: `packages/cnes_infra/tests/aws/test_settings.py`

**Interfaces:**
- Consumes: `Mapping[str, str]` and the stable profile values `PROFILE=aws`, `AUTH_MODE=oidc`.
- Produces: `AwsRuntimeSettings.from_mapping(values: Mapping[str, str]) -> AwsRuntimeSettings` and `AwsRuntimeConfigurationError`.

- [ ] **Step 1: Write the failing settings tests**

```python
def _valid_values() -> dict[str, str]:
    return {
        "PROFILE": "aws",
        "AUTH_MODE": "oidc",
        "AWS_REGION": "us-east-1",
        "AWS_CONTROL_PLANE_TABLE": "cnesdata-test-control-plane",
        "AWS_DATA_BUCKET": "cnesdata-test-data",
        "AWS_AUDIT_BUCKET": "cnesdata-test-audit",
        "AWS_STATE_MACHINE_ARN": (
            "arn:aws:states:us-east-1:000000000000:stateMachine:cnesdata-test"
        ),
        "AWS_PROCESSOR_CONTAINER_NAME": "processor",
        "AWS_PROCESSOR_MAX_CONCURRENCY": "8",
        "AWS_PROCESSOR_LEASE_SECONDS": "300",
        "AWS_PROCESSOR_RECOVERY_BATCH_SIZE": "100",
        "AWS_SERVING_URL_TTL_SECONDS": "300",
        "AWS_AUDIT_RETENTION_DAYS": "365",
        "OIDC_ISSUER": "https://id.example.test",
        "OIDC_AUDIENCE": "cnesdata-dashboard",
    }


def test_aceita_configuracao_aws_sem_credenciais_estaticas() -> None:
    settings = AwsRuntimeSettings.from_mapping(_valid_values())
    assert settings.region == "us-east-1"
    assert settings.serving_url_ttl_seconds == 300
    assert settings.processor_max_concurrency == 8
    assert settings.processor_lease_seconds == 300
    assert settings.processor_recovery_batch_size == 100
    assert not hasattr(settings, "access_key_id")
    assert not hasattr(settings, "secret_access_key")


@pytest.mark.parametrize(
    ("key", "value", "error"),
    [
        ("PROFILE", "local", "profile_must_be_aws"),
        ("AUTH_MODE", "local", "auth_mode_must_be_oidc"),
        ("AWS_SERVING_URL_TTL_SECONDS", "901", "serving_ttl_out_of_range"),
        ("AWS_AUDIT_RETENTION_DAYS", "0", "audit_retention_days_invalid"),
        ("AWS_PROCESSOR_MAX_CONCURRENCY", "0", "processor_concurrency_invalid"),
        ("AWS_PROCESSOR_LEASE_SECONDS", "0", "processor_lease_invalid"),
        ("AWS_PROCESSOR_RECOVERY_BATCH_SIZE", "1001", "recovery_batch_invalid"),
    ],
)
def test_rejeita_configuracao_insegura(key: str, value: str, error: str) -> None:
    values = _valid_values() | {key: value}
    with pytest.raises(AwsRuntimeConfigurationError, match=error):
        AwsRuntimeSettings.from_mapping(values)
```

- [ ] **Step 2: Run the settings tests to verify RED**

Run: `uv run pytest packages/cnes_infra/tests/aws/test_settings.py -q`

Expected: FAIL during collection with `ModuleNotFoundError: No module named 'cnes_infra.aws'`.

- [ ] **Step 3: Implement immutable parsing and validation**

```python
@dataclass(frozen=True, slots=True)
class AwsRuntimeSettings:
    region: str
    control_plane_table: str
    data_bucket: str
    audit_bucket: str
    state_machine_arn: str
    processor_container_name: str
    oidc_issuer: str
    oidc_audience: str
    serving_url_ttl_seconds: int
    audit_retention_days: int
    processor_max_concurrency: int
    processor_lease_seconds: int
    processor_recovery_batch_size: int
    dynamodb_endpoint_url: str | None
    service_endpoint_url: str | None

    @classmethod
    def from_mapping(cls, values: Mapping[str, str]) -> "AwsRuntimeSettings":
        if values.get("PROFILE") != "aws":
            raise AwsRuntimeConfigurationError("profile_must_be_aws")
        if values.get("AUTH_MODE") != "oidc":
            raise AwsRuntimeConfigurationError("auth_mode_must_be_oidc")
        ttl = _integer(values, "AWS_SERVING_URL_TTL_SECONDS", 300)
        retention = _integer(values, "AWS_AUDIT_RETENTION_DAYS")
        concurrency = _integer(values, "AWS_PROCESSOR_MAX_CONCURRENCY", 8)
        lease_seconds = _integer(values, "AWS_PROCESSOR_LEASE_SECONDS", 300)
        recovery_batch_size = _integer(
            values, "AWS_PROCESSOR_RECOVERY_BATCH_SIZE", 100,
        )
        dynamodb_endpoint_url = values.get("DYNAMODB_ENDPOINT_URL") or None
        service_endpoint_url = values.get("AWS_ENDPOINT_URL") or None
        if not 30 <= ttl <= 900:
            raise AwsRuntimeConfigurationError("serving_ttl_out_of_range")
        if retention < 1:
            raise AwsRuntimeConfigurationError("audit_retention_days_invalid")
        if not 1 <= concurrency <= 40:
            raise AwsRuntimeConfigurationError("processor_concurrency_invalid")
        if not 30 <= lease_seconds <= 3600:
            raise AwsRuntimeConfigurationError("processor_lease_invalid")
        if not 1 <= recovery_batch_size <= 1000:
            raise AwsRuntimeConfigurationError("recovery_batch_invalid")
        return cls(
            region=_required(values, "AWS_REGION"),
            control_plane_table=_required(values, "AWS_CONTROL_PLANE_TABLE"),
            data_bucket=_required(values, "AWS_DATA_BUCKET"),
            audit_bucket=_required(values, "AWS_AUDIT_BUCKET"),
            state_machine_arn=_required(values, "AWS_STATE_MACHINE_ARN"),
            processor_container_name=_required(values, "AWS_PROCESSOR_CONTAINER_NAME"),
            oidc_issuer=_issuer(
                _required(values, "OIDC_ISSUER"),
                allow_http=service_endpoint_url is not None,
            ),
            oidc_audience=_required(values, "OIDC_AUDIENCE"),
            serving_url_ttl_seconds=ttl,
            audit_retention_days=retention,
            processor_max_concurrency=concurrency,
            processor_lease_seconds=lease_seconds,
            processor_recovery_batch_size=recovery_batch_size,
            dynamodb_endpoint_url=dynamodb_endpoint_url,
            service_endpoint_url=service_endpoint_url,
        )
```

Implement `_required`, `_integer`, and `_issuer(value: str, allow_http: bool)` in the same file. `_issuer` requires `https`, strips one trailing slash, and rejects query/fragment components; `http://` is accepted only when `allow_http` is true for the emulator test profile. AWS SDK credentials remain wholly inside boto3's standard provider chain and never become `AwsRuntimeSettings` fields.

- [ ] **Step 4: Run unit tests and package coverage to verify GREEN**

Run: `uv run pytest packages/cnes_infra/tests/aws/test_settings.py -q`

Expected: PASS with every required/malformed/range branch covered.

Run: `uv run pytest packages/cnes_infra/tests/aws/test_settings.py --cov=cnes_infra.aws.settings --cov-branch --cov-report=term-missing --cov-fail-under=100`

Expected: PASS and `100%` branch coverage for `settings.py`.

- [ ] **Step 5: Lint and commit**

Run: `uv run ruff check packages/cnes_infra/src/cnes_infra/aws/settings.py packages/cnes_infra/tests/aws/test_settings.py`

Expected: `All checks passed!`

```bash
git add packages/cnes_infra/src/cnes_infra/aws/settings.py packages/cnes_infra/tests/aws/test_settings.py
git commit -m "feat(aws): validate runtime profile settings"
```

### Task 2: AWS-010 Runtime Client and Adapter Bundle

**Files:**
- Create: `packages/cnes_infra/src/cnes_infra/aws/runtime.py`
- Create: `packages/cnes_infra/tests/aws/test_runtime.py`

**Interfaces:**
- Consumes: `AwsRuntimeSettings`; stable `DynamoDBControlPlane(client, table_name, clock)`, `S3ObjectStore(client, bucket, prefix="")`, and `S3ObjectLockAuditSink(client, bucket, retention_days)`.
- Produces: `AwsRuntimeComponents`, `create_aws_clients(settings, session) -> AwsClients`, and `build_aws_runtime(settings, clients, clock) -> AwsRuntimeComponents`.

- [ ] **Step 1: Write the failing bundle tests**

```python
def test_cria_clientes_na_regiao_e_endpoint_configurados() -> None:
    session = Mock()
    session.client.side_effect = [sentinel.dynamodb, sentinel.s3, sentinel.sfn]
    clients = create_aws_clients(
        _settings(
            dynamodb_endpoint_url="http://dynamodb-local:8000",
            service_endpoint_url="http://aws-emulator:4566",
        ),
        session,
    )
    assert clients.s3 is sentinel.s3
    assert session.client.call_args_list == [
        call(
            "dynamodb", region_name="us-east-1",
            endpoint_url="http://dynamodb-local:8000",
        ),
        call("s3", region_name="us-east-1", endpoint_url="http://aws-emulator:4566"),
        call("stepfunctions", region_name="us-east-1", endpoint_url="http://aws-emulator:4566"),
    ]


def test_compoe_adapters_com_recursos_configurados() -> None:
    components = build_aws_runtime(_settings(), _clients(), clock=lambda: NOW)
    assert isinstance(components.control_plane, DynamoDBControlPlane)
    assert isinstance(components.object_store, S3ObjectStore)
    assert isinstance(components.audit_sink, S3ObjectLockAuditSink)
```

- [ ] **Step 2: Run the bundle tests to verify RED**

Run: `uv run pytest packages/cnes_infra/tests/aws/test_runtime.py -q`

Expected: FAIL with `ModuleNotFoundError: No module named 'cnes_infra.aws.runtime'`.

- [ ] **Step 3: Implement the typed bundle without resource creation**

```python
@dataclass(frozen=True, slots=True)
class AwsClients:
    dynamodb: BaseClient
    s3: BaseClient
    step_functions: BaseClient


@dataclass(frozen=True, slots=True)
class AwsRuntimeComponents:
    control_plane: ControlPlanePort
    object_store: ObjectStorePort
    audit_sink: AuditSinkPort


def create_aws_clients(settings: AwsRuntimeSettings, session: Session) -> AwsClients:
    return AwsClients(
        dynamodb=cast("BaseClient", session.client(
            "dynamodb", region_name=settings.region,
            endpoint_url=settings.dynamodb_endpoint_url,
        )),
        s3=cast("BaseClient", session.client(
            "s3", region_name=settings.region,
            endpoint_url=settings.service_endpoint_url,
        )),
        step_functions=cast("BaseClient", session.client(
            "stepfunctions", region_name=settings.region,
            endpoint_url=settings.service_endpoint_url,
        )),
    )


def build_aws_runtime(
    settings: AwsRuntimeSettings, clients: AwsClients,
    clock: Callable[[], datetime],
) -> AwsRuntimeComponents:
    return AwsRuntimeComponents(
        control_plane=DynamoDBControlPlane(
            client=clients.dynamodb,
            table_name=settings.control_plane_table,
            clock=clock,
        ),
        object_store=S3ObjectStore(
            client=clients.s3, bucket=settings.data_bucket, prefix="",
        ),
        audit_sink=S3ObjectLockAuditSink(
            client=clients.s3,
            bucket=settings.audit_bucket,
            retention_days=settings.audit_retention_days,
        ),
    )
```

The module may call only `session.client`; it must not call create-table, create-bucket, put-bucket-policy, put-object-lock-configuration, create-state-machine, register-task-definition, or IAM APIs.

- [ ] **Step 4: Run tests and prove no provisioning call is made**

Run: `uv run pytest packages/cnes_infra/tests/aws/test_runtime.py -q`

Expected: PASS; the strict mocks report exactly three `session.client` calls and zero service calls.

- [ ] **Step 5: Run targeted lint and commit**

Run: `uv run ruff check packages/cnes_infra/src/cnes_infra/aws packages/cnes_infra/tests/aws`

Expected: `All checks passed!`

```bash
git add packages/cnes_infra/src/cnes_infra/aws/runtime.py packages/cnes_infra/tests/aws/test_runtime.py
git commit -m "feat(aws): compose managed service adapters"
```

### Task 3: AWS-011 Generic OIDC Verification

**Files:**
- Create: `packages/cnes_infra/src/cnes_infra/auth/oidc.py`
- Create: `packages/cnes_infra/tests/auth/test_oidc.py`

**Interfaces:**
- Consumes: configured `issuer`, `audience`, `httpx.Client`, and a bearer token.
- Produces: `OidcPrincipal(issuer: str, subject: str, email: str | None, display_name: str | None)` and `OidcVerifier.verify(token: str) -> OidcPrincipal`; raises existing `TokenInvalid` with sanitized codes.

- [ ] **Step 1: Write failing provider-neutral tests**

```python
def test_descobre_jwks_uri_e_valida_token_generico(httpx_mock) -> None:
    httpx_mock.add_response(
        url=f"{ISSUER}/.well-known/openid-configuration",
        json={"issuer": ISSUER, "jwks_uri": f"{ISSUER}/keys"},
    )
    httpx_mock.add_response(url=f"{ISSUER}/keys", json={"keys": [PUBLIC_JWK]})
    principal = _verifier().verify(_signed_token())
    assert principal == OidcPrincipal(
        issuer=ISSUER, subject="user-1", email="gestor@example.test",
        display_name="Gestor",
    )


def test_rejeita_discovery_com_issuer_divergente(httpx_mock) -> None:
    httpx_mock.add_response(
        url=f"{ISSUER}/.well-known/openid-configuration",
        json={"issuer": "https://evil.example", "jwks_uri": f"{ISSUER}/keys"},
    )
    with pytest.raises(TokenInvalid, match="discovery_issuer_mismatch"):
        _verifier().verify(_signed_token())


def test_nao_interpreta_claim_de_tenant(httpx_mock) -> None:
    _mock_provider(httpx_mock)
    principal = _verifier().verify(_signed_token(extra={"tenant_id": "tenant-b"}))
    assert not hasattr(principal, "tenant_id")
```

Also cover expired token, wrong audience, wrong issuer, missing `kid`, unknown `kid` with one refresh, unreachable discovery without cache, unreachable JWKS with a fresh cache, unsupported algorithm, and missing/blank `sub`.

- [ ] **Step 2: Run the OIDC tests to verify RED**

Run: `uv run pytest packages/cnes_infra/tests/auth/test_oidc.py -q`

Expected: FAIL with `ModuleNotFoundError: No module named 'cnes_infra.auth.oidc'`.

- [ ] **Step 3: Implement discovery, caching, and strict claims**

```python
@dataclass(frozen=True, slots=True)
class OidcPrincipal:
    issuer: str
    subject: str
    email: str | None
    display_name: str | None


class OidcVerifier:
    def __init__(
        self, issuer: str, audience: str, client: httpx.Client,
        cache_ttl_seconds: int = 600,
    ) -> None:
        self._issuer = issuer.rstrip("/")
        self._audience = audience
        self._client = client
        self._cache_ttl_seconds = cache_ttl_seconds
        self._provider: ProviderMetadata | None = None
        self._keys: tuple[dict[str, object], ...] = ()
        self._fetched_at = 0.0

    def verify(self, token: str) -> OidcPrincipal:
        header = _verified_header(token)
        if header.get("alg") != "RS256":
            raise TokenInvalid("unsupported_algorithm")
        claims = jose_jwt.decode(
            token,
            self._key(str(header.get("kid") or "")),
            algorithms=["RS256"],
            audience=self._audience,
            issuer=self._issuer,
            options={"require_exp": True, "require_iat": True, "require_sub": True},
        )
        subject = str(claims.get("sub") or "").strip()
        if not subject:
            raise TokenInvalid("subject_required")
        return OidcPrincipal(
            issuer=self._issuer,
            subject=subject,
            email=_optional_text(claims.get("email")),
            display_name=_optional_text(claims.get("name")),
        )
```

`_provider_metadata()` fetches `/.well-known/openid-configuration`, requires exact issuer equality and an HTTPS `jwks_uri` on the same configured trust decision; `_key()` caches keys for 600 seconds and refreshes once on `kid` miss. Convert jose/httpx failures into `TokenInvalid` codes without including tokens or response bodies.

- [ ] **Step 4: Run tests and coverage to verify GREEN**

Run: `uv run pytest packages/cnes_infra/tests/auth/test_oidc.py -q`

Expected: PASS for generic-provider, malicious-discovery, cache, and failure cases.

Run: `uv run pytest packages/cnes_infra/tests/auth/test_oidc.py --cov=cnes_infra.auth.oidc --cov-branch --cov-report=term-missing --cov-fail-under=100`

Expected: PASS and `100%` branch coverage for `oidc.py`.

- [ ] **Step 5: Lint and commit**

Run: `uv run ruff check packages/cnes_infra/src/cnes_infra/auth/oidc.py packages/cnes_infra/tests/auth/test_oidc.py`

Expected: `All checks passed!`

```bash
git add packages/cnes_infra/src/cnes_infra/auth/oidc.py packages/cnes_infra/tests/auth/test_oidc.py
git commit -m "feat(auth): validate generic oidc identities"
```

### Task 4: AWS-011 Canonical Membership Authorization

**Files:**
- Create: `apps/central_api/src/central_api/auth/aws_oidc.py`
- Create: `apps/central_api/tests/auth/test_aws_oidc.py`
- Create: `packages/cnes_infra/src/cnes_infra/auth/dynamodb_memberships.py`
- Create: `packages/cnes_infra/tests/auth/test_dynamodb_memberships.py`

**Interfaces:**
- Consumes: `OidcPrincipal`, AWS-specific `MembershipCandidateSource.list_candidates(user_id) -> tuple[str, ...]`, and canonical `ControlPlanePort.get_membership(tenant_id, user_id)`.
- Produces: `DynamoDBMembershipCandidates(client: BaseClient, table_name: str, index_name: str = "GSI1")`, `AuthorizedTenant(tenant_id: str, user_id: str, role: str)`, `MembershipAuthorizer.authorize(principal, requested_tenant_id) -> AuthorizedTenant`, and `MembershipAuthorizer.list_authorized(principal) -> tuple[AuthorizedTenant, ...]`; raises `TenantAccessDenied(code: str)`.

- [ ] **Step 1: Write failing direct-key and stale-GSI tests**

```python
def test_autoriza_por_membership_na_chave_canonica() -> None:
    control_plane = Mock()
    control_plane.get_membership.return_value = _membership("tenant-a", "user-1")
    result = MembershipAuthorizer(control_plane, Mock()).authorize(
        _principal(), "tenant-a",
    )
    assert result.tenant_id == "tenant-a"
    control_plane.get_membership.assert_called_once_with("tenant-a", "user-1")


def test_rejeita_tenant_de_claim_e_request_sem_membership() -> None:
    control_plane = Mock()
    control_plane.get_membership.return_value = None
    with pytest.raises(TenantAccessDenied, match="membership_not_active"):
        MembershipAuthorizer(control_plane, Mock()).authorize(_principal(), "tenant-b")


def test_remove_candidato_stale_do_gsi_apos_revalidar_base() -> None:
    control_plane = Mock()
    candidates = Mock()
    candidates.list_candidates.return_value = ("tenant-a", "tenant-b")
    control_plane.get_membership.side_effect = [
        _membership("tenant-a", "user-1"),
        None,
    ]
    result = MembershipAuthorizer(control_plane, candidates).list_authorized(_principal())
    assert tuple(item.tenant_id for item in result) == ("tenant-a",)
```

Also prove that a membership whose `tenant_id` or `user_id` differs from the canonical lookup is rejected, blank tenant IDs are rejected before storage access, duplicates from the GSI are revalidated once, and the token's `tenant_id` claim is unavailable.

- [ ] **Step 2: Run authorization tests to verify RED**

Run: `uv run pytest apps/central_api/tests/auth/test_aws_oidc.py packages/cnes_infra/tests/auth/test_dynamodb_memberships.py -q`

Expected: FAIL with missing `central_api.auth` and `cnes_infra.auth.dynamodb_memberships` modules.

- [ ] **Step 3: Implement fail-closed membership resolution**

```python
@dataclass(frozen=True, slots=True)
class AuthorizedTenant:
    tenant_id: str
    user_id: str
    role: str


class MembershipAuthorizer:
    def __init__(
        self, control_plane: ControlPlanePort,
        candidates: MembershipCandidateSource,
    ) -> None:
        self._control_plane = control_plane
        self._candidates = candidates

    def authorize(
        self, principal: OidcPrincipal, requested_tenant_id: str,
    ) -> AuthorizedTenant:
        tenant_id = requested_tenant_id.strip()
        if not tenant_id:
            raise TenantAccessDenied("tenant_required")
        membership = self._control_plane.get_membership(tenant_id, principal.subject)
        if not _matches_active_membership(membership, tenant_id, principal.subject):
            raise TenantAccessDenied("membership_not_active")
        return AuthorizedTenant(
            tenant_id=membership.tenant_id,
            user_id=membership.user_id,
            role=membership.role,
        )

    def list_authorized(
        self, principal: OidcPrincipal,
    ) -> tuple[AuthorizedTenant, ...]:
        tenant_ids = dict.fromkeys(
            self._candidates.list_candidates(principal.subject),
        )
        return tuple(
            grant for tenant_id in tenant_ids
            if (grant := self._authorized_or_none(principal, tenant_id)) is not None
        )
```

`_matches_active_membership` requires a non-null membership plus exact tenant/user equality; the CND model has no membership status field, so revocation removes the canonical membership item. `_authorized_or_none` catches only `TenantAccessDenied`; DynamoDB errors propagate so outages cannot degrade into authorization success.

`DynamoDBMembershipCandidates.list_candidates` performs a paginated `Query` on `GSI1` with `GSI1PK=USER#<user_id>`, projects only `GSI1SK`, parses only exact `TENANT#<tenant_id>` values, deduplicates them, and returns candidates. It never returns a role/status and never authorizes; `MembershipAuthorizer` revalidates every candidate with `get_membership` on the base key.

```python
class DynamoDBMembershipCandidates:
    def __init__(
        self, client: BaseClient, table_name: str, index_name: str = "GSI1",
    ) -> None:
        self._client = client
        self._table_name = table_name
        self._index_name = index_name

    def list_candidates(self, user_id: str) -> tuple[str, ...]:
        request = {
            "TableName": self._table_name,
            "IndexName": self._index_name,
            "KeyConditionExpression": "GSI1PK = :user",
            "ExpressionAttributeValues": {":user": {"S": f"USER#{user_id}"}},
            "ProjectionExpression": "GSI1SK",
        }
        tenant_ids: dict[str, None] = {}
        while True:
            response = self._client.query(**request)
            for item in response.get("Items", []):
                value = item.get("GSI1SK", {}).get("S", "")
                if value.startswith("TENANT#") and value != "TENANT#":
                    tenant_ids[value.removeprefix("TENANT#")] = None
            if "LastEvaluatedKey" not in response:
                return tuple(tenant_ids)
            request["ExclusiveStartKey"] = response["LastEvaluatedKey"]
```

- [ ] **Step 4: Run tests and application coverage to verify GREEN**

Run: `uv run pytest apps/central_api/tests/auth/test_aws_oidc.py packages/cnes_infra/tests/auth/test_dynamodb_memberships.py -q`

Expected: PASS, including stale-GSI and cross-tenant denial.

Run: `uv run pytest apps/central_api/tests/auth/test_aws_oidc.py --cov=central_api.auth.aws_oidc --cov-report=term-missing --cov-fail-under=90`

Expected: PASS with at least `90%` line coverage.

Run: `uv run pytest packages/cnes_infra/tests/auth/test_dynamodb_memberships.py --cov=cnes_infra.auth.dynamodb_memberships --cov-branch --cov-report=term-missing --cov-fail-under=100`

Expected: PASS with `100%` branch coverage for candidate pagination and malformed GSI values.

- [ ] **Step 5: Lint and commit**

Run: `uv run ruff check apps/central_api/src/central_api/auth/aws_oidc.py apps/central_api/tests/auth/test_aws_oidc.py packages/cnes_infra/src/cnes_infra/auth/dynamodb_memberships.py packages/cnes_infra/tests/auth/test_dynamodb_memberships.py`

Expected: `All checks passed!`

```bash
git add apps/central_api/src/central_api/auth/aws_oidc.py \
  apps/central_api/tests/auth/test_aws_oidc.py \
  packages/cnes_infra/src/cnes_infra/auth/dynamodb_memberships.py \
  packages/cnes_infra/tests/auth/test_dynamodb_memberships.py
git commit -m "feat(auth): authorize aws tenants by canonical membership"
```

### Task 5: Serial AWS-012 Extension of the Canonical Step Functions Executor

This task starts only after CND-042 is merged. It extends the existing adapter in place; it must not create `cnes_infra.execution`, `StepFunctionsEcsExecutor`, or a second implementation of `ProcessorExecutorPort`.

**Files:**
- Modify: `packages/cnes_infra/src/cnes_infra/executor/step_functions.py`
- Modify: `packages/cnes_infra/tests/executor/test_step_functions.py`
- Create: `packages/cnes_infra/tests/fixtures/step_functions/standard_inline_ecs.json`
- Create: `packages/cnes_infra/tests/fixtures/step_functions/distributed_map.json`

**Interfaces:**
- Consumes: CND-042 `StepFunctionsExecutor(client, state_machine_arn)`, wave/dispatch-aware `StartRunExecution`/`CancelRunExecution`, the ordered deterministic `unit_ids` produced by CND-041, configured container name/lease seconds, and the adapter's existing `BaseClient`.
- Produces: the same `StepFunctionsExecutor.start(request: StartRunExecution) -> str`, `cancel(request: CancelRunExecution) -> None`, and `status(execution_ref: str) -> ExecutionStatus`, plus `validate_state_machine(client: BaseClient, state_machine_arn: str, processor_container_name: str, lease_seconds: int) -> None`. Raises `IncompatibleStateMachine` or `ProcessorExecutionUnavailable`. The canonical two-argument constructor remains unchanged; bind/callback compensation remains solely in CND `PipelineCoordinator`/`RunPlanningService`.

- [ ] **Step 1: Write failing workflow contract and retry tests**

```python
def test_aceita_standard_inline_map_com_ecs_fargate() -> None:
    client = _client_for_fixture("standard_inline_ecs.json", workflow_type="STANDARD")
    validate_state_machine(client, STATE_MACHINE_ARN, "processor", 300)


@pytest.mark.parametrize(
    ("fixture_name", "workflow_type", "error"),
    [
        ("standard_inline_ecs.json", "EXPRESS", "workflow_must_be_standard"),
        ("distributed_map.json", "STANDARD", "map_must_be_inline"),
    ],
)
def test_rejeita_workflow_incompativel(
    fixture_name: str, workflow_type: str, error: str,
) -> None:
    with pytest.raises(IncompatibleStateMachine, match=error):
        validate_state_machine(
            _client_for_fixture(fixture_name, workflow_type),
            STATE_MACHINE_ARN,
            "processor",
            300,
        )


def test_inicia_execucao_deterministica_com_ids_sem_dados() -> None:
    client = Mock()
    client.start_execution.return_value = {"executionArn": EXECUTION_ARN}
    execution_ref = _executor(client).start(_request())
    payload = json.loads(client.start_execution.call_args.kwargs["input"])
    assert execution_ref == EXECUTION_ARN
    assert payload == {
        "tenant_id": "tenant-a",
        "run_id": "run-01",
        "wave_id": _request().wave_id,
        "dispatch_id": _request().dispatch_id,
        "max_concurrency": 4,
        "unit_ids": ["unit-01"],
    }


def test_dispatch_novo_da_mesma_onda_abre_nova_execucao() -> None:
    client = Mock()
    client.start_execution.side_effect = [
        {"executionArn": "arn:execution:dispatch-a"},
        {"executionArn": "arn:execution:dispatch-b"},
    ]
    executor = _executor(client)
    first = _request(wave_id=WAVE_ID, dispatch_id="1111111111111111")
    retry = _request(wave_id=WAVE_ID, dispatch_id="2222222222222222")
    assert executor.start(first) != executor.start(retry)
    assert [call.kwargs["name"] for call in client.start_execution.call_args_list] == [
        "1111111111111111", "2222222222222222",
    ]


def test_replay_da_mesma_tentativa_e_idempotente() -> None:
    client = _client_with_existing_execution(status="FAILED")
    assert _executor(client).start(_request()) == EXISTING_EXECUTION_ARN


def test_falha_ao_descrever_existente_e_normalizada() -> None:
    client = _client_with_existing_execution(describe_error="ThrottlingException")
    with pytest.raises(ProcessorExecutionUnavailable, match="ThrottlingException"):
        _executor(client).start(_request())


```

Also test `ExecutionAlreadyExists` returns the deterministic execution ARN only after
`describe_execution` confirms byte-equivalent input; all three logical stage waves use distinct
`wave_id` values, every dispatch attempt uses its own `dispatch_id`, and only replay of that exact
dispatch may return its prior ARN. Test `start_execution` and nested `describe_execution`
throttling/service errors mapping to `ProcessorExecutionUnavailable`;
`cancel(CancelRunExecution(...))` is a no-op when `execution_ref is None` and otherwise calls
`stop_execution`; invalid `wave_id`/`dispatch_id`, duplicate/empty unit IDs, zero/negative
concurrency, or
row/object-content fields are rejected before AWS access. The adapter never loads a `RunUnit`; the
ECS worker loads it by canonical `(tenant_id, run_id, unit_id)` and obtains attempt/fencing state
from the control plane.

Retain the CND-042 `status` method and add tests mapping `RUNNING`, `SUCCEEDED`,
`FAILED|TIMED_OUT`, and `ABORTED` to
`ExecutionStatus.RUNNING|SUCCEEDED|FAILED|CANCELED`; an unknown service status fails closed.
`describe_execution` `ClientError` from either `status` or `_confirm_existing` is normalized to
`ProcessorExecutionUnavailable` by its own nested `try`; neither caller reaches the raw client.

- [ ] **Step 2: Run the canonical executor tests to verify RED**

Run: `uv run pytest packages/cnes_infra/tests/executor/test_step_functions.py -q`

Expected: existing CND-042 start/cancel/status tests PASS, while the new tests FAIL importing
`validate_state_machine` and its compatibility exceptions.

- [ ] **Step 3: Add exact test state-machine definitions**

Write this accepted definition to `standard_inline_ecs.json`:

```json
{
  "StartAt": "RunUnits",
  "States": {
    "RunUnits": {
      "Type": "Map",
      "ItemsPath": "$.unit_ids",
      "MaxConcurrencyPath": "$.max_concurrency",
      "ItemSelector": {
        "tenant_id.$": "$.tenant_id",
        "run_id.$": "$.run_id",
        "wave_id.$": "$.wave_id",
        "dispatch_id.$": "$.dispatch_id",
        "unit_id.$": "$$.Map.Item.Value"
      },
      "ItemProcessor": {
        "ProcessorConfig": {"Mode": "INLINE"},
        "StartAt": "RunProcessor",
        "States": {
          "RunProcessor": {
            "Type": "Task",
            "Resource": "arn:aws:states:::ecs:runTask.sync",
            "Parameters": {
              "Cluster": "arn:aws:ecs:us-east-1:000000000000:cluster/test",
              "TaskDefinition": "arn:aws:ecs:us-east-1:000000000000:task-definition/processor:1",
              "LaunchType": "FARGATE",
              "NetworkConfiguration": {
                "AwsvpcConfiguration": {
                  "AssignPublicIp": "DISABLED",
                  "Subnets": ["subnet-00000000000000001"],
                  "SecurityGroups": ["sg-00000000000000001"]
                }
              },
              "Overrides": {
                "ContainerOverrides": [{
                  "Name": "processor",
                  "Environment": [
                    {"Name": "TENANT_ID", "Value.$": "$.tenant_id"},
                    {"Name": "RUN_ID", "Value.$": "$.run_id"},
                    {"Name": "WAVE_ID", "Value.$": "$.wave_id"},
                    {"Name": "DISPATCH_ID", "Value.$": "$.dispatch_id"},
                    {"Name": "UNIT_ID", "Value.$": "$.unit_id"},
                    {"Name": "EXECUTION_OWNER", "Value.$": "$$.Execution.Id"},
                    {"Name": "LEASE_SECONDS", "Value": "300"}
                  ]
                }]
              }
            },
            "End": true
          }
        }
      },
      "End": true
    }
  }
}
```

Write `distributed_map.json` with this same complete definition and the sole value change `"Mode": "DISTRIBUTED"`; the test asserts a normalized JSON diff of exactly `/States/RunUnits/ItemProcessor/ProcessorConfig/Mode` before calling the validator.

- [ ] **Step 4: Implement validation and deterministic execution**

```python
class StepFunctionsExecutor(ProcessorExecutorPort):
    def __init__(
        self, client: BaseClient, state_machine_arn: str,
    ) -> None:
        self._client = client
        self._state_machine_arn = state_machine_arn

    def start(self, request: StartRunExecution) -> str:
        payload = _execution_payload(request)
        name = _execution_name(request.dispatch_id)
        serialized = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        try:
            response = self._client.start_execution(
                stateMachineArn=self._state_machine_arn,
                name=name,
                input=serialized,
            )
        except self._client.exceptions.ExecutionAlreadyExists:
            execution_id = _execution_arn(self._state_machine_arn, name)
            return _confirm_existing(
                self._client, execution_id, serialized,
            )
        except ClientError as error:
            raise ProcessorExecutionUnavailable(_error_code(error)) from error
        return response["executionArn"]

    def cancel(self, request: CancelRunExecution) -> None:
        if request.execution_ref is None:
            return
        try:
            self._client.stop_execution(
                executionArn=request.execution_ref,
                cause=f"cancelled run {request.tenant_id}/{request.run_id}",
            )
        except ClientError as error:
            raise ProcessorExecutionUnavailable(_error_code(error)) from error


def _confirm_existing(
    client: BaseClient, execution_id: str, expected_input: str,
) -> str:
    try:
        existing = client.describe_execution(executionArn=execution_id)
    except ClientError as error:
        raise ProcessorExecutionUnavailable(_error_code(error)) from error
    if existing["input"] != expected_input:
        raise ProcessorExecutionUnavailable("execution_name_conflict")
    return execution_id
```

Modify the CND-042 methods rather than copying them into another module. Preserve its deterministic
execution name, IDs-only payload, no-op cancellation without `execution_ref`, and two-argument
constructor. `wave_id` remains the logical ID of one ordered ready set. The Standard execution name
is the required durable `dispatch_id`: replay of the same start attempt is idempotent even if its
execution is terminal, while retry after terminal failure receives generation+1/new `dispatch_id`
from the canonical control-plane CAS allocator and may start a new execution for the same wave. The
adapter never increments generation or derives either ID. Add duplicate-input
verification and normalized AWS error mapping to those same methods. `validate_state_machine`
parses `describe_state_machine`; requires `type == "STANDARD"`, an Inline Map with explicit
concurrency, exactly one ECS optimized `.sync` task, Fargate launch type, disabled public IP, and
the configured container override, exact lease seconds, and all seven environment bindings.
`_execution_payload` accepts only canonical tenant/run/wave/dispatch/unit IDs and concurrency,
rejects duplicate unit IDs, and serializes no manifests, keys, attempts, fences, or object contents.

- [ ] **Step 5: Run tests, coverage, and lint to verify GREEN**

Run: `uv run pytest packages/cnes_infra/tests/executor/test_step_functions.py -q`

Expected: PASS for accepted/rejected definitions, duplicate start, cancellation, and failure mapping.

Run: `uv run pytest packages/cnes_infra/tests/executor/test_step_functions.py --cov=cnes_infra.executor.step_functions --cov-branch --cov-report=term-missing --cov-fail-under=100`

Expected: PASS and `100%` branch coverage.

Run: `uv run ruff check packages/cnes_infra/src/cnes_infra/executor/step_functions.py packages/cnes_infra/tests/executor/test_step_functions.py packages/cnes_infra/tests/fixtures/step_functions`

Expected: `All checks passed!`

- [ ] **Step 6: Commit**

```bash
git add packages/cnes_infra/src/cnes_infra/executor/step_functions.py \
  packages/cnes_infra/tests/executor/test_step_functions.py \
  packages/cnes_infra/tests/fixtures/step_functions
git commit -m "feat(aws): execute run units with standard inline map"
```

### Task 6: AWS-013 CloudWatch-Compatible JSON Stdout

**Files:**
- Create: `packages/cnes_infra/src/cnes_infra/observability/json_logging.py`
- Create: `packages/cnes_infra/tests/observability/test_json_logging.py`

**Interfaces:**
- Consumes: standard `logging.LogRecord` and output stream.
- Produces: `configure_json_stdout(service_name: str, stream: TextIO = sys.stdout) -> None` and `JsonLogFormatter.format(record: LogRecord) -> str`.

- [ ] **Step 1: Write failing structured/redaction tests**

```python
def test_emite_um_json_por_linha_com_contexto(capfd) -> None:
    configure_json_stdout("central-api")
    logger.info(
        "serving_access_granted",
        extra={"tenant_id": "tenant-a", "run_id": "run-01"},
    )
    event = json.loads(capfd.readouterr().out)
    assert event["service"] == "central-api"
    assert event["event"] == "serving_access_granted"
    assert event["tenant_id"] == "tenant-a"
    assert event["run_id"] == "run-01"


@pytest.mark.parametrize(
    "field",
    ["authorization", "token", "signed_url", "aws_secret_access_key", "email"],
)
def test_remove_campos_sensiveis(field: str) -> None:
    record = logging.makeLogRecord({"msg": "denied", field: "secret-value"})
    rendered = JsonLogFormatter("central-api").format(record)
    assert "secret-value" not in rendered
    assert json.loads(rendered)[field] == "[REDACTED]"
```

Also assert UTC RFC3339 timestamp, level, logger, exception type without stack locals, deterministic replacement of existing root handlers, and no file handler in `PROFILE=aws` composition.

- [ ] **Step 2: Run logging tests to verify RED**

Run: `uv run pytest packages/cnes_infra/tests/observability/test_json_logging.py -q`

Expected: FAIL with `ModuleNotFoundError: No module named 'cnes_infra.observability'`.

- [ ] **Step 3: Implement the formatter and idempotent stdout setup**

```python
_REDACTED_FIELDS = frozenset({
    "authorization", "token", "signed_url", "aws_access_key_id",
    "aws_secret_access_key", "email",
})


class JsonLogFormatter(logging.Formatter):
    def __init__(self, service_name: str) -> None:
        super().__init__()
        self._service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        event = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=UTC,
            ).isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            "service": self._service_name,
            "logger": record.name,
            "event": record.getMessage(),
        }
        event.update(_safe_extra(record))
        if record.exc_info:
            event["exception_type"] = record.exc_info[0].__name__
        return json.dumps(event, ensure_ascii=False, separators=(",", ":"))


def configure_json_stdout(service_name: str, stream: TextIO = sys.stdout) -> None:
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonLogFormatter(service_name))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(logging.INFO)
```

- [ ] **Step 4: Run tests, coverage, and lint to verify GREEN**

Run: `uv run pytest packages/cnes_infra/tests/observability/test_json_logging.py -q`

Expected: PASS with parseable single-line output and all sensitive values redacted.

Run: `uv run pytest packages/cnes_infra/tests/observability/test_json_logging.py --cov=cnes_infra.observability.json_logging --cov-branch --cov-report=term-missing --cov-fail-under=100`

Expected: PASS and `100%` branch coverage.

Run: `uv run ruff check packages/cnes_infra/src/cnes_infra/observability packages/cnes_infra/tests/observability`

Expected: `All checks passed!`

- [ ] **Step 5: Commit**

```bash
git add packages/cnes_infra/src/cnes_infra/observability/json_logging.py \
  packages/cnes_infra/tests/observability/test_json_logging.py
git commit -m "feat(aws): emit redacted json logs to stdout"
```

### Task 7: AWS-013 Object-Locked Audit and Signed Serving Access

**Files:**
- Create: `apps/central_api/src/central_api/serving/aws_signed.py`
- Create: `apps/central_api/tests/serving/test_aws_signed.py`
- Create: `packages/cnes_infra/tests/audit/test_s3_object_lock_runtime.py`

**Interfaces:**
- Consumes: stable `ServingAccessPort.authorize(ServingRequest) -> ServingGrant`, `ObjectStorePort.stat`, boto3 `BaseClient.generate_presigned_url`, stable `S3ObjectLockAuditSink.append(event)`, and `AwsRuntimeSettings`.
- Produces: `SignedServingRequest(access: ServingRequest, relative_name: str)`, `SignedServingSettings(bucket: str, ttl_seconds: int)`, `SignedServingGrant(version_id: str, run_id: str, object_key: str, url: str, expires_at: datetime)`, and `S3SignedServingAccess.grant(request: SignedServingRequest, now: datetime) -> SignedServingGrant`; raises `ServingKeyForbidden(code: str)` or `ServingSigningUnavailable(code: str)` while preserving authorization errors from the stable serving policy.

- [ ] **Step 1: Write failing tenant-safe serving tests**

```python
def test_assina_somente_objeto_serving_do_pointer_ativo() -> None:
    policy = Mock(spec=ServingAccessPort)
    policy.authorize.return_value = _grant(keys=(SERVING_KEY,))
    access = _access(policy=policy)
    grant = access.grant(
        _request("tenant-a", "cnes", "overview.json"), NOW,
    )
    policy.authorize.assert_called_once_with(
        ServingRequest(user_id="user-a", tenant_id="tenant-a", dataset_name="cnes"),
    )
    assert grant.object_key == SERVING_KEY
    assert grant.expires_at == NOW + timedelta(seconds=300)
    _s3_client(access).generate_presigned_url.assert_called_once_with(
        "get_object",
        Params={
            "Bucket": "data-bucket",
            "Key": SERVING_KEY,
            "ResponseContentType": "application/json",
        },
        ExpiresIn=300,
    )


@pytest.mark.parametrize(
    ("key", "code"),
    [
        ("raw/tenant-a/cnes/2026-07/s1/data.parquet", "serving_key_forbidden"),
        ("serving/tenant-b/run-01/overview.json", "serving_key_forbidden"),
        ("serving/tenant-a/run-02/overview.json", "serving_key_forbidden"),
        ("serving/tenant-a/run-01/../../raw/data", "serving_key_forbidden"),
    ],
)
def test_rejeita_key_fora_do_pointer_e_prefixo(key: str, code: str) -> None:
    with pytest.raises(ServingKeyForbidden, match=code):
        _access(policy=_policy(_grant(keys=(key,)))).grant(
            _request("tenant-a", "cnes", "overview.json"), NOW,
        )
```

Also test that an absent membership, missing pointer/version, or cross-tenant request is rejected by
the stable policy before any run-manifest read, object stat, or signing; that a requested file absent
from `ServingGrant.object_keys`, failed object stat, and S3 signing failure fail closed; and that no
request parameter becomes an S3 key unless it exactly matches an already-authorized immutable key.

- [ ] **Step 2: Write the failing Object Lock runtime assertion**

```python
def test_audit_sink_envia_object_lock_e_chave_append_only() -> None:
    client = Mock()
    sink = S3ObjectLockAuditSink(client, "audit-bucket", retention_days=365)
    sink.append(_event(event_id="evt-01", tenant_id="tenant-a", created_at=NOW))
    request = client.put_object.call_args.kwargs
    assert request["Key"] == "audit/tenant-a/2026/08/23/evt-01.json"
    assert request["IfNoneMatch"] == "*"
    assert request["ObjectLockMode"] == "COMPLIANCE"
    assert request["ObjectLockRetainUntilDate"] == NOW + timedelta(days=365)
    assert len(request["Metadata"]["sha256"]) == 64
```

Add a duplicate-delivery test proving the same event produces the same key and checksum, and an AWS error test proving the outbox event remains pending because the sink raises instead of acknowledging.

- [ ] **Step 3: Run serving and audit tests to verify RED**

Run: `uv run pytest apps/central_api/tests/serving/test_aws_signed.py packages/cnes_infra/tests/audit/test_s3_object_lock_runtime.py -q`

Expected: serving test fails with missing `central_api.serving.aws_signed`; the audit runtime test passes against the stable CND sink.

- [ ] **Step 4: Implement signed serving with canonical path validation**

```python
@dataclass(frozen=True, slots=True)
class SignedServingRequest:
    access: ServingRequest
    relative_name: str


@dataclass(frozen=True, slots=True)
class SignedServingSettings:
    bucket: str
    ttl_seconds: int


@dataclass(frozen=True, slots=True)
class SignedServingGrant:
    version_id: str
    run_id: str
    object_key: str
    url: str
    expires_at: datetime


class S3SignedServingAccess:
    def __init__(
        self, access_policy: ServingAccessPort, object_store: ObjectStorePort,
        signer: BaseClient, settings: SignedServingSettings,
    ) -> None:
        self._access_policy = access_policy
        self._object_store = object_store
        self._signer = signer
        self._settings = settings

    def grant(
        self, request: SignedServingRequest, now: datetime,
    ) -> SignedServingGrant:
        authorized = self._access_policy.authorize(request.access)
        key = _resolve_key(authorized, request.relative_name)
        if self._object_store.stat(key) is None:
            raise ServingSigningUnavailable("serving_object_missing")
        try:
            url = self._signer.generate_presigned_url(
                "get_object",
                Params={
                    "Bucket": self._settings.bucket,
                    "Key": key,
                    "ResponseContentType": "application/json",
                },
                ExpiresIn=self._settings.ttl_seconds,
            )
        except ClientError as error:
            raise ServingSigningUnavailable("serving_signing_failed") from error
        return SignedServingGrant(
            version_id=authorized.version_id,
            run_id=authorized.run_id,
            object_key=key,
            url=url,
            expires_at=now + timedelta(seconds=self._settings.ttl_seconds),
        )
```

Use request/settings dataclasses so public methods stay within the four-parameter limit. `_resolve_key` first rejects absolute names, empty segments, `.` and `..`; it then builds `serving/{authorized.tenant_id}/{authorized.run_id}/{relative_name}` and returns it only if it exactly equals one member of `authorized.object_keys`. Authorization is complete before object lookup, and the signer never receives raw, normalized, reconciliation, tmp, audit, another tenant's key, or a non-active version key.

- [ ] **Step 5: Verify the stable audit adapter satisfies the AWS runtime contract**

The CND-owned sink must send canonical JSON bytes with `ContentType="application/json"`, `IfNoneMatch="*"`, `ObjectLockMode="COMPLIANCE"`, `ObjectLockRetainUntilDate=created_at + retention_days`, and a lowercase SHA-256 in object metadata. Expected: the test passes without a CND production edit. If it fails, stop this task and route the mismatch to the integration controller because the stable dependency gate was violated.

- [ ] **Step 6: Run tests, coverage, and lint to verify GREEN**

Run: `uv run pytest apps/central_api/tests/serving/test_aws_signed.py packages/cnes_infra/tests/audit/test_s3_object_lock_runtime.py -q`

Expected: PASS for tenant/path denials, pointer/object failures, deterministic audit delivery, and Object Lock fields.

Run: `uv run pytest apps/central_api/tests/serving/test_aws_signed.py --cov=central_api.serving.aws_signed --cov-report=term-missing --cov-fail-under=90`

Expected: PASS with at least `90%` line coverage for `aws_signed.py`.

Run: `uv run ruff check apps/central_api/src/central_api/serving apps/central_api/tests/serving packages/cnes_infra/tests/audit/test_s3_object_lock_runtime.py`

Expected: `All checks passed!`

- [ ] **Step 7: Commit**

```bash
git add apps/central_api/src/central_api/serving/aws_signed.py \
  apps/central_api/tests/serving/test_aws_signed.py \
  packages/cnes_infra/tests/audit/test_s3_object_lock_runtime.py
git commit -m "feat(aws): authorize signed serving and locked audit"
```

### Task 8: Serial Integration-Owned AWS Composition

This task starts only after Tasks 1 through 7 and the complete `CND-064` local gate are merged. It
is the first task after `CND-064` in the controller-owned composition queue frozen by the execution
design. The AWS feature lane may finish earlier, but composition waits for the final local roots and
serving interfaces. No feature worktree edits these files.

**Files:**
- Modify: `apps/central_api/src/central_api/composition.py`
- Create: `apps/central_api/tests/test_aws_composition.py`
- Modify: `apps/central_api/src/central_api/deps.py`
- Modify: `apps/central_api/src/central_api/middleware.py`
- Modify: `apps/central_api/src/central_api/routes/serving.py`
- Modify: `apps/central_api/tests/test_app_wiring.py`
- Modify: `apps/central_api/tests/routes/test_serving.py`
- Modify: `apps/data_processor/src/data_processor/main.py`
- Create: `apps/data_processor/src/data_processor/aws_entrypoint.py`
- Create: `apps/data_processor/src/data_processor/recovery.py`
- Modify: `apps/data_processor/src/data_processor/composition.py`
- Create: `apps/data_processor/tests/test_aws_composition.py`
- Create: `apps/data_processor/tests/test_aws_entrypoint.py`
- Create: `apps/data_processor/tests/test_recovery.py`
- Modify: `apps/data_processor/tests/test_data_processor_main.py`
- Modify: `packages/cnes_infra/src/cnes_infra/auth/__init__.py`
- Create: `packages/cnes_infra/src/cnes_infra/aws/__init__.py`
- Create: `packages/cnes_infra/src/cnes_infra/observability/__init__.py`
- Create: `apps/central_api/src/central_api/serving/__init__.py`
- Modify: `.env.example`
- Modify: `docker-compose.yml`

**Interfaces:**
- Consumes: all outputs from Tasks 1 through 7; CND-060 `LocalRuntime`, `LocalProcessorRuntime`, `build_local_runtime`, `build_local_processor_runtime`, `build_source_catalog`, `build_source_registry`, `RunPlanningService`, `StageProcessor`, `PipelineCoordinator`, `RunUnitCommandHandler`, and `UnitWorker`; CND-030 `RawIngestionService`/`DeltaPolicy`; CND-044 `DatasetPublisher`; and CND-062 `LocalServingAccess`.
- Produces in `central_api.composition`: Billing-consumed `RuntimeComponents(control_plane, object_store, executor, audit_sink, raw_ingestion, source_catalog, run_planning, services)` and backward-compatible `build_runtime(profile: str, values: Mapping[str, str], session: Session, execution_started: ExecutionStarted = noop_execution_started) -> RuntimeComponents`; existing Billing calls with three arguments remain unchanged.
- Produces in `data_processor.composition`: `AwsProcessorServices(recovery, recovery_batch_size)`, `ProcessorRuntimeComponents(control_plane, object_store, executor, publisher, source_registry, stage_processor, coordinator, unit_worker, unit_handler, services)`, and `build_processor_runtime(profile: str, values: Mapping[str, str], session: Session, execution_started: ExecutionStarted = noop_execution_started) -> ProcessorRuntimeComponents`.
- Produces in `data_processor.aws_entrypoint`: `EcsUnitEnvelope.from_mapping(values)`, `run_aws_entrypoint(runtime, values, argv) -> int`; unit mode builds one canonical `RunUnitMessage` for the handler-to-`ClaimRunUnit` path, while no-`UNIT_ID` mode accepts only `recover-once` and runs one bounded recovery pass.
- Produces in `data_processor.recovery`: `RecoveryResult(scanned: int, recovered: int)` and `ProcessorRecovery(control_plane, coordinator, clock).run_once(limit: int) -> RecoveryResult`; it calls `list_recoverable_runs(now, limit)` for bounded operational telemetry, then delegates status/lease/CAS correction to canonical `PipelineCoordinator.recover(limit)`.
- Produces at process boundaries: FastAPI `app.state.runtime`, OIDC-backed `request.state.principal`, canonical `request.state.authorized_tenant`, and AWS processor/signed-serving selection. Local branches delegate to the unchanged CND-060 builders; CND-025 continues to own dependencies, markers, and the base emulator profile.

- [ ] **Step 1: Write failing composition tests**

```python
def test_api_local_delega_ao_runtime_cnd_sem_cliente_aws(session) -> None:
    local = _local_api_runtime()
    with patch(
        "central_api.composition.build_local_runtime", return_value=local,
    ) as build_local:
        runtime = build_runtime("local", _local_values(), session)
    build_local.assert_called_once_with(parse_profile(_local_values()), utc_now)
    assert runtime.control_plane is local.control_plane
    assert runtime.raw_ingestion is local.raw_ingestion
    assert runtime.source_catalog is local.source_catalog
    assert runtime.run_planning is local.run_planning
    assert runtime.services is None
    session.client.assert_not_called()


def test_api_aws_instala_runtime_completo_oidc_e_serving(session) -> None:
    runtime = build_runtime("aws", _aws_values(), session)
    assert isinstance(runtime.control_plane, DynamoDBControlPlane)
    assert isinstance(runtime.object_store, S3ObjectStore)
    assert isinstance(runtime.executor, StepFunctionsExecutor)
    assert isinstance(runtime.raw_ingestion, RawIngestionService)
    assert isinstance(runtime.source_catalog, SourceCatalog)
    assert isinstance(runtime.run_planning, RunPlanningService)
    assert isinstance(runtime.services, AwsApiServices)
    assert isinstance(runtime.services.serving_access, S3SignedServingAccess)


def test_api_aws_falha_fechado_sem_oidc(session) -> None:
    with pytest.raises(AwsRuntimeConfigurationError, match="auth_mode_must_be_oidc"):
        build_runtime("aws", _aws_values() | {"AUTH_MODE": "local"}, session)


def test_processor_local_delega_ao_runtime_cnd(session) -> None:
    local = _local_processor_runtime()
    with patch(
        "data_processor.composition.build_local_processor_runtime",
        return_value=local,
    ) as build_local:
        runtime = build_processor_runtime("local", _local_values(), session)
    build_local.assert_called_once_with(parse_profile(_local_values()), utc_now)
    assert runtime.publisher is local.publisher
    assert runtime.source_registry is local.source_registry
    session.client.assert_not_called()


def test_processor_aws_reusa_executor_publisher_e_registry_canonicos(session) -> None:
    catalog = Mock(spec=SourceCatalog)
    registry = Mock(spec=SourceRegistry)
    with patch("data_processor.composition.build_source_catalog", return_value=catalog), \
         patch("data_processor.composition.build_source_registry",
               return_value=registry) as build_registry:
        runtime = build_processor_runtime("aws", _aws_values(), session)
    assert isinstance(runtime.control_plane, DynamoDBControlPlane)
    assert isinstance(runtime.object_store, S3ObjectStore)
    assert isinstance(runtime.executor, StepFunctionsExecutor)
    assert isinstance(runtime.publisher, DatasetPublisher)
    assert runtime.source_registry is registry
    build_registry.assert_called_once_with(catalog)
    assert isinstance(runtime.stage_processor, StageProcessor)
    assert isinstance(runtime.coordinator, PipelineCoordinator)
    assert isinstance(runtime.unit_worker, UnitWorker)
    assert isinstance(runtime.unit_handler, RunUnitCommandHandler)


def test_deps_instala_o_mesmo_runtime_consumido_por_billing() -> None:
    expected = Mock(spec=RuntimeComponents)
    with patch("central_api.deps.build_runtime", return_value=expected) as build:
        app = create_app()
        with TestClient(app):
            assert app.state.runtime is expected
    build.assert_called_once()


def test_main_aws_entrega_runtime_ao_entrypoint(monkeypatch) -> None:
    expected = Mock(spec=ProcessorRuntimeComponents)
    with patch(
        "data_processor.main.build_processor_runtime", return_value=expected,
    ) as build, patch(
        "data_processor.main.run_aws_entrypoint", return_value=0,
    ) as entrypoint:
        monkeypatch.setenv("PROFILE", "aws")
        assert asyncio.run(main()) == 0
    build.assert_called_once()
    entrypoint.assert_called_once()


def test_entrypoint_ecs_converte_env_em_claim_canonico(runtime) -> None:
    values = {
        "TENANT_ID": "tenant-a", "RUN_ID": "run-01",
        "WAVE_ID": "1111111111111111", "DISPATCH_ID": "2222222222222222",
        "UNIT_ID": "unit-01", "EXECUTION_OWNER": EXECUTION_ARN,
        "LEASE_SECONDS": "300",
    }
    assert run_aws_entrypoint(runtime, values, ()) == 0
    runtime.unit_handler.handle.assert_called_once_with(RunUnitMessage(
        tenant_id="tenant-a", run_id="run-01", wave_id="1111111111111111",
        dispatch_id="2222222222222222", unit_id="unit-01",
        owner=EXECUTION_ARN, now=NOW, lease_seconds=300,
    ))


def test_entrypoint_sem_unit_executa_recovery_once(runtime) -> None:
    assert run_aws_entrypoint(runtime, {}, ("recover-once",)) == 0
    runtime.services.recovery.run_once.assert_called_once_with(
        limit=runtime.services.recovery_batch_size,
    )


def test_recovery_lista_no_instante_injetado_e_delega_ao_coordinator() -> None:
    control_plane = Mock()
    control_plane.list_recoverable_runs.return_value = (
        _run("r1", RunState.PROCESSING),
        _run("r2", RunState.PUBLISHING),
    )
    coordinator = Mock()
    coordinator.recover.return_value = (_result(), _result())
    recovery = ProcessorRecovery(control_plane, coordinator, clock=lambda: NOW)
    assert recovery.run_once(10) == RecoveryResult(scanned=2, recovered=2)
    control_plane.list_recoverable_runs.assert_called_once_with(now=NOW, limit=10)
    coordinator.recover.assert_called_once_with(limit=10)


def test_recovery_propaga_falha_do_coordinator() -> None:
    control_plane = Mock()
    control_plane.list_recoverable_runs.return_value = (_run("r1"),)
    coordinator = Mock()
    coordinator.recover.side_effect = ProcessorExecutionUnavailable("ThrottlingException")
    recovery = ProcessorRecovery(control_plane, coordinator, clock=lambda: NOW)
    with pytest.raises(ProcessorExecutionUnavailable, match="ThrottlingException"):
        recovery.run_once(10)


def test_recovery_nao_avanca_dispatch_enquanto_lease_ativa(runtime) -> None:
    active = _dispatch(state=DispatchState.STARTED, lease_until=NOW + ONE_MINUTE)
    runtime.control_plane.get_active_run_dispatch.return_value = active
    runtime.services.recovery.run_once(limit=10)
    runtime.control_plane.reserve_run_dispatch.assert_not_called()
    runtime.coordinator.recover.assert_called_once_with(limit=10)


def test_callback_de_manifesto_aceito_descarta_retorno() -> None:
    run_planning = Mock()
    run_planning.on_raw_manifest_accepted.return_value = _launch_result()
    assert _notify_accepted(run_planning, _raw_manifest_record()) is None


@pytest.mark.parametrize(
    ("path", "names"),
    [
        (API_COMPOSITION, {
            "build_runtime", "_build_aws_api_runtime", "_validate_runtime",
            "_execution_config", "_notify_accepted",
        }),
        (PROCESSOR_COMPOSITION,
         {"build_processor_runtime", "_build_aws_processor_runtime"}),
    ],
)
def test_builders_e_helpers_tem_corpo_menor_que_cinquenta_linhas(
    path: Path, names: set[str],
) -> None:
    tree = ast.parse(path.read_text())
    functions = {node.name: node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)}
    for name in names:
        node = functions[name]
        body_lines = node.end_lineno - node.body[0].lineno + 1
        assert body_lines < 50, (name, body_lines)


def test_rota_aws_redireciona_para_get_assinado_sem_tenant_na_url(client) -> None:
    response = client.get(
        "/api/v1/dashboard/serving/cnes/overview",
        headers={"Authorization": "Bearer valid", "X-Tenant-Id": "tenant-a"},
        follow_redirects=False,
    )
    assert response.status_code == 307
    assert response.headers["location"].startswith("https://signed.example.test/")
    assert response.headers["x-dataset-version"] == "run-01"
    assert "tenant" not in response.request.url.query
```

- [ ] **Step 2: Run composition tests to verify RED**

Run: `uv run pytest apps/central_api/tests/test_aws_composition.py apps/data_processor/tests/test_aws_composition.py apps/data_processor/tests/test_aws_entrypoint.py apps/data_processor/tests/test_recovery.py apps/central_api/tests/test_app_wiring.py apps/data_processor/tests/test_data_processor_main.py -q`

Expected: FAIL because the profile-neutral API/processor wrappers and complete AWS runtime fields are not yet wired.

- [ ] **Step 3: Verify CND-025 dependency and emulator exports**

Run:

```bash
uv lock --check
uv run python -c "import boto3; print(boto3.__version__)"
docker compose --profile aws-test config --services
```

Expected: the lock is current, boto3 imports from the CND-025 production dependency, and services include exactly `dynamodb-local` and `aws-emulator`. CND-025 already owns dependency manifests, lockfile, emulator markers, base Compose definitions, and adapter CI; this task does not recreate them.

- [ ] **Step 4: Wire the API and processor composition roots**

```python
@dataclass(frozen=True, slots=True)
class AwsApiServices:
    membership_authorizer: MembershipAuthorizer
    serving_access: S3SignedServingAccess


@dataclass(frozen=True, slots=True)
class RuntimeComponents:
    control_plane: ControlPlanePort
    object_store: ObjectStorePort
    executor: ProcessorExecutorPort
    audit_sink: AuditSinkPort
    raw_ingestion: RawIngestionService
    source_catalog: SourceCatalog
    run_planning: RunPlanningService
    services: AwsApiServices | None

    @classmethod
    def from_local(
        cls, runtime: LocalRuntime,
    ) -> "RuntimeComponents":
        return cls(
            control_plane=runtime.control_plane,
            object_store=runtime.object_store,
            executor=runtime.executor,
            audit_sink=runtime.audit_sink,
            raw_ingestion=runtime.raw_ingestion,
            source_catalog=runtime.source_catalog,
            run_planning=runtime.run_planning,
            services=None,
        )


def build_runtime(
    profile: str, values: Mapping[str, str], session: Session,
    execution_started: ExecutionStarted = noop_execution_started,
) -> RuntimeComponents:
    if profile == "local":
        local = build_local_runtime(parse_profile(values), utc_now)
        return RuntimeComponents.from_local(local)
    if profile != "aws":
        raise RuntimeConfigurationError("profile_unknown")
    settings = AwsRuntimeSettings.from_mapping(values)
    clients = create_aws_clients(settings, session)
    core = build_aws_runtime(settings, clients, clock=utc_now)
    return _build_aws_api_runtime(settings, clients, core, execution_started)


def _build_aws_api_runtime(
    settings: AwsRuntimeSettings, clients: AwsClients,
    core: AwsRuntimeComponents, execution_started: ExecutionStarted,
) -> RuntimeComponents:
    _validate_runtime(settings, clients)
    candidates = DynamoDBMembershipCandidates(
        clients.dynamodb, settings.control_plane_table,
    )
    authorizer = MembershipAuthorizer(core.control_plane, candidates)
    executor = StepFunctionsExecutor(
        clients.step_functions, settings.state_machine_arn,
    )
    source_catalog = build_source_catalog()
    run_planning = RunPlanningService(
        RunPlanningDependencies(
            core.control_plane, core.object_store, executor, source_catalog,
        ),
        _execution_config(settings, execution_started),
        utc_now,
    )
    raw_ingestion = RawIngestionService(
        core.control_plane,
        core.object_store,
        DeltaPolicy(),
        accepted_manifest=lambda record: _notify_accepted(run_planning, record),
    )
    access_policy = LocalServingAccess(core.control_plane, core.object_store)
    serving = S3SignedServingAccess(
        access_policy,
        core.object_store,
        clients.s3,
        SignedServingSettings(
            settings.data_bucket, settings.serving_url_ttl_seconds,
        ),
    )
    return RuntimeComponents(
        control_plane=core.control_plane,
        object_store=core.object_store,
        executor=executor,
        audit_sink=core.audit_sink,
        raw_ingestion=raw_ingestion,
        source_catalog=source_catalog,
        run_planning=run_planning,
        services=AwsApiServices(authorizer, serving),
    )


def _notify_accepted(
    run_planning: RunPlanningService, record: RawManifestRecord,
) -> None:
    run_planning.on_raw_manifest_accepted(record)
    return None


def _validate_runtime(settings: AwsRuntimeSettings, clients: AwsClients) -> None:
    validate_state_machine(
        clients.step_functions, settings.state_machine_arn,
        settings.processor_container_name, settings.processor_lease_seconds,
    )


def _execution_config(
    settings: AwsRuntimeSettings, execution_started: ExecutionStarted,
) -> ExecutionPolicyConfig:
    callbacks = ExecutionCallbacks(allow_execution, execution_started)
    return ExecutionPolicyConfig(
        settings.processor_max_concurrency,
        settings.processor_lease_seconds,
        callbacks,
    )


@dataclass(frozen=True, slots=True)
class AwsProcessorServices:
    recovery: ProcessorRecovery
    recovery_batch_size: int


@dataclass(frozen=True, slots=True)
class ProcessorRuntimeComponents:
    control_plane: ControlPlanePort
    object_store: ObjectStorePort
    executor: ProcessorExecutorPort
    publisher: DatasetPublisher
    source_registry: SourceRegistry
    stage_processor: StageProcessor
    coordinator: PipelineCoordinator
    unit_worker: UnitWorker
    unit_handler: RunUnitCommandHandler
    services: AwsProcessorServices | None

    @classmethod
    def from_local(
        cls, runtime: LocalProcessorRuntime,
    ) -> "ProcessorRuntimeComponents":
        return cls(
            control_plane=runtime.control_plane,
            object_store=runtime.object_store,
            executor=runtime.executor,
            publisher=runtime.publisher,
            source_registry=runtime.source_registry,
            stage_processor=runtime.stage_processor,
            coordinator=runtime.coordinator,
            unit_worker=runtime.unit_worker,
            unit_handler=runtime.unit_handler,
            services=None,
        )


def build_processor_runtime(
    profile: str, values: Mapping[str, str], session: Session,
    execution_started: ExecutionStarted = noop_execution_started,
) -> ProcessorRuntimeComponents:
    if profile == "local":
        local = build_local_processor_runtime(parse_profile(values), utc_now)
        return ProcessorRuntimeComponents.from_local(local)
    if profile != "aws":
        raise RuntimeConfigurationError("profile_unknown")
    settings = AwsRuntimeSettings.from_mapping(values)
    clients = create_aws_clients(settings, session)
    core = build_aws_runtime(settings, clients, clock=utc_now)
    return _build_aws_processor_runtime(
        settings, clients, core, execution_started,
    )


def _build_aws_processor_runtime(
    settings: AwsRuntimeSettings, clients: AwsClients,
    core: AwsRuntimeComponents, execution_started: ExecutionStarted,
) -> ProcessorRuntimeComponents:
    _validate_runtime(settings, clients)
    executor = StepFunctionsExecutor(
        clients.step_functions, settings.state_machine_arn,
    )
    publisher = DatasetPublisher(
        store=core.object_store, control_plane=core.control_plane,
    )
    source_catalog = build_source_catalog()
    source_registry = build_source_registry(source_catalog)
    stage_processor = StageProcessor(
        core.control_plane, core.object_store, source_registry, utc_now,
    )
    coordinator = PipelineCoordinator(
        CoordinatorDependencies(
            core.control_plane, executor, publisher, utc_now,
        ),
        _execution_config(settings, execution_started),
    )
    unit_worker = UnitWorker(
        UnitWorkerDependencies(
            core.control_plane, core.object_store, stage_processor, utc_now,
        ),
        UnitWorkerPolicy(
            after_persist=lambda unit: coordinator.resume(unit.tenant_id, unit.run_id),
        ),
    )
    unit_handler = RunUnitCommandHandler(unit_worker)
    services = AwsProcessorServices(
        recovery=ProcessorRecovery(
            core.control_plane, coordinator, utc_now,
        ),
        recovery_batch_size=settings.processor_recovery_batch_size,
    )
    return ProcessorRuntimeComponents(
        control_plane=core.control_plane,
        object_store=core.object_store,
        executor=executor,
        publisher=publisher,
        source_registry=source_registry,
        stage_processor=stage_processor,
        coordinator=coordinator,
        unit_worker=unit_worker,
        unit_handler=unit_handler,
        services=services,
    )
```

Implement the ECS boundary in `data_processor.aws_entrypoint` without storage, planner, or stage
selection logic:

```python
@dataclass(frozen=True, slots=True)
class EcsUnitEnvelope:
    tenant_id: str
    run_id: str
    wave_id: str
    dispatch_id: str
    unit_id: str
    execution_owner: str
    lease_seconds: int

    @classmethod
    def from_mapping(cls, values: Mapping[str, str]) -> "EcsUnitEnvelope":
        envelope = cls(
            tenant_id=_required(values, "TENANT_ID"),
            run_id=_required(values, "RUN_ID"),
            wave_id=_hex_id(values, "WAVE_ID"),
            dispatch_id=_hex_id(values, "DISPATCH_ID"),
            unit_id=_required(values, "UNIT_ID"),
            execution_owner=_required(values, "EXECUTION_OWNER"),
            lease_seconds=_lease_seconds(values),
        )
        if not envelope.execution_owner.startswith("arn:aws:states:"):
            raise EntrypointConfigurationError("execution_owner_invalid")
        return envelope

    def message(self, now: datetime) -> RunUnitMessage:
        return RunUnitMessage(
            tenant_id=self.tenant_id,
            run_id=self.run_id,
            wave_id=self.wave_id,
            dispatch_id=self.dispatch_id,
            unit_id=self.unit_id,
            owner=self.execution_owner,
            now=now,
            lease_seconds=self.lease_seconds,
        )


def run_aws_entrypoint(
    runtime: ProcessorRuntimeComponents,
    values: Mapping[str, str],
    argv: Sequence[str],
) -> int:
    services = _required_aws_services(runtime)
    if values.get("UNIT_ID"):
        if argv:
            raise EntrypointConfigurationError("unit_mode_rejects_command")
        envelope = EcsUnitEnvelope.from_mapping(values)
        runtime.unit_handler.handle(envelope.message(utc_now()))
        return 0
    if _has_partial_unit_envelope(values):
        raise EntrypointConfigurationError("partial_unit_envelope")
    if tuple(argv) != ("recover-once",):
        raise EntrypointConfigurationError("command_must_be_recover_once")
    services.recovery.run_once(limit=services.recovery_batch_size)
    return 0
```

`_hex_id` accepts exactly 16 lowercase hexadecimal characters; `_lease_seconds` accepts
`30..3600`; `_has_partial_unit_envelope` checks all seven ECS variable names. Add parameterized
tests for uppercase/short IDs, missing owner, invalid lease, a partial envelope, arguments in unit
mode, and local runtime passed to this AWS-only boundary. `EcsUnitEnvelope.message(NOW)` must equal
the exact `RunUnitMessage`; the canonical handler validates the logical `wave_id`, converts it to
`ClaimRunUnit` preserving tenant/run/dispatch/unit/owner/time/lease, and executes that claim.
The canonical DynamoDB claim condition requires `Run.state == PROCESSING`, the exact active
`dispatch_id`, an unexpired dispatch lease, and a fresh unit fence, so an old ECS task cannot claim
work after recovery advances the dispatch generation.

Implement the bounded operational wrapper in `data_processor.recovery`:

```python
@dataclass(frozen=True, slots=True)
class RecoveryResult:
    scanned: int
    recovered: int


class ProcessorRecovery:
    def __init__(
        self, control_plane: ControlPlanePort, coordinator: PipelineCoordinator,
        clock: Callable[[], datetime],
    ) -> None:
        self._control_plane = control_plane
        self._coordinator = coordinator
        self._clock = clock

    def run_once(self, limit: int) -> RecoveryResult:
        if not 1 <= limit <= 1000:
            raise ValueError("recovery_limit_invalid")
        runs = self._control_plane.list_recoverable_runs(
            now=self._clock(), limit=limit,
        )
        recovered = self._coordinator.recover(limit=limit)
        return RecoveryResult(len(runs), len(recovered))
```

The wrapper never calls `executor.status`, `reserve_run_dispatch`, or any mutation itself.
`PipelineCoordinator.recover(limit)` owns the authoritative second strong read and uses its
canonical `ProcessorExecutorPort.status` plus dispatch CAS path. An unexpired `RESERVED` dispatch
is replayed byte-for-byte; a `STARTED` dispatch is observed through the canonical executor; a
terminal dispatch, or an expired dispatch lease with no live unit lease, is finished and replaced
by generation+1 before `executor.start`. `PUBLISHING` recovery re-enters the single publisher CAS.
A control-plane, executor-status, or coordinator error propagates so the scheduled task exits
nonzero and can be retried. Candidate state may improve between the telemetry read and coordinator
revalidation; that race is expected and cannot bypass CAS/fencing.

Place `RuntimeComponents`, `AwsApiServices`, and `build_runtime` in `central_api.composition`; this
preserves the exact API factory consumed later by Billing Task 13. Place
`ProcessorRuntimeComponents` and `build_processor_runtime` in `data_processor.composition`. The AWS
branch composes the same stage controller/coordinator/worker graph as local over AWS ports; it does
not create another orchestration loop. Do not import app code into infra, make either runtime inherit
the other, or add a second `SourceRegistry`, source-stage bundle, publisher, or local adapter
constructor. Build one canonical `ExecutionPolicyConfig` and pass the supplied
`execution_started` directly as `ExecutionCallbacks.started`; do not wrap it in an AWS-specific
cancel/bind guard. `RunPlanningService` and `PipelineCoordinator` own start → bind → callback and
the compensating cancel plus `FinishRunDispatch(CANCELED)` on any bind/callback failure.

In `central_api.deps`, lifespan calls `central_api.composition.build_runtime` once with `PROFILE`,
`os.environ`, and one boto3 `Session`, installs `OidcVerifier` plus the stable outbox dispatcher, and
stores the immutable `RuntimeComponents` in `app.state.runtime`. `PROFILE=local` delegates to
`build_local_runtime` before adapting all seven CND fields; unknown profiles fail startup. Existing
API dependencies read `control_plane`, `object_store`, `executor`, `audit_sink`, `raw_ingestion`,
`source_catalog`, and `run_planning` from the same object, so Billing may inject its callbacks
without replacing or narrowing `RuntimeComponents`.

In `central_api.middleware`, replace the current dashboard repository upsert path for `PROFILE=aws`: verify the bearer token into `OidcPrincipal`, require `X-Tenant-Id` on tenant-scoped routes, authorize through `MembershipAuthorizer`, then set only the authorized tenant into the domain request context. `/api/v1/system/health` remains identity-free; agent mTLS/device routes preserve their separate authentication surface.

In `central_api.routes.serving`, preserve CND-062 streaming for `PROFILE=local`. For `PROFILE=aws`, derive `ServingRequest.user_id` only from the verified principal and `tenant_id` only from the authorized request context, append the route-owned `.json` document suffix, call `S3SignedServingAccess.grant`, and return `307` with the signed URL in `Location`, `X-Dataset-Version=<grant.version_id>`, and `Cache-Control: private, no-store`. Do not accept tenant IDs or S3 keys in query/body parameters, and do not log the `Location` value.

In `data_processor.main`, call only `data_processor.composition.build_processor_runtime(PROFILE,
os.environ, session)`. The local branch delegates unchanged to CND-060 and passes the adapted
runtime to canonical `run_processor`. The AWS branch calls
`configure_json_stdout("data-processor")` and returns `run_aws_entrypoint(runtime, os.environ,
sys.argv[1:])`: a task with `UNIT_ID` executes exactly one canonical handler call; a task without
any unit-envelope variable accepts only `recover-once`. Both paths receive the same executor,
publisher, registry, stage processor, coordinator, unit worker/handler, S3, and DynamoDB; neither
rebuilds a component. Remove the current catch-all MinIO-to-null fallback from the AWS branch;
misconfiguration or AWS unavailability fails startup/operation explicitly.

- [ ] **Step 5: Add exact non-secret environment documentation**

Document these AWS application settings in `.env.example`: `PROFILE=aws`, `AUTH_MODE=oidc`,
`AWS_REGION`, `AWS_CONTROL_PLANE_TABLE`, `AWS_DATA_BUCKET`, `AWS_AUDIT_BUCKET`,
`AWS_STATE_MACHINE_ARN`, `AWS_PROCESSOR_CONTAINER_NAME`,
`AWS_PROCESSOR_MAX_CONCURRENCY=8`, `AWS_PROCESSOR_LEASE_SECONDS=300`,
`AWS_PROCESSOR_RECOVERY_BATCH_SIZE=100`, `AWS_SERVING_URL_TTL_SECONDS=300`,
`AWS_AUDIT_RETENTION_DAYS`, `OIDC_ISSUER`, and `OIDC_AUDIENCE`. Document the two independent
CND-025 endpoints, `DYNAMODB_ENDPOINT_URL=http://127.0.0.1:18000` for `dynamodb-local` and
`AWS_ENDPOINT_URL=http://127.0.0.1:4566` for `aws-emulator`, only under the `aws-test` emulator
section; production leaves both unset. State that credentials come from the AWS provider chain;
do not add access-key examples.

- [ ] **Step 6: Extend the CND-025 emulator service for workflow APIs**

Keep the CND-025 `dynamodb-local` service unchanged. Extend the existing `aws-emulator` service in `docker-compose.yml` so the same `aws-test` profile provides S3 plus Step Functions/ECS/Logs mocks:

```yaml
  aws-emulator:
    profiles: ["aws-test"]
    environment:
      SERVICES: s3,stepfunctions,ecs,logs
      AWS_DEFAULT_REGION: us-east-1
```

Preserve the digest-pinned image, loopback port, and healthcheck established by CND-025. Do not add production account IDs, IAM users, public bucket policies, static user credentials, VPCs, or deployment resources.

The test bootstrap creates disposable table/buckets/state machine in the emulator, enables versioning/Object Lock on the audit bucket, and removes them after the session. That bootstrap is test fixture behavior, not production IaC.
Before setup, the fixture polls DynamoDB `ListTables` and LocalStack's health endpoint for at most 60 seconds, so readiness does not depend on utilities inside the DynamoDB Local image.

The application-to-deployment contract has two invocations of the same processor image. The
Step Functions ECS task supplies exactly `TENANT_ID`, `RUN_ID`, `WAVE_ID`, `DISPATCH_ID`, `UNIT_ID`,
`EXECUTION_OWNER`, and `LEASE_SECONDS`, with no command arguments. A separately scheduled ECS task
supplies none of those seven variables and uses command `recover-once`, running at a cadence no
greater than half `AWS_PROCESSOR_LEASE_SECONDS`; it must allow overlap because dispatch CAS makes
concurrent recovery safe. Its role needs only the same control-plane reads/writes plus
`states:DescribeExecution`; the unit task keeps its data-plane permissions. EventBridge schedule,
ECS service/task resources, IAM documents, alarms, and rollout remain inputs to the separate
deployment specification and are not created here.

Both modes emit redacted JSON events. Recovery logs `processor_recovery_scanned`,
`processor_execution_observed`, `processor_execution_probe_failed`, and
`processor_recovery_completed`, including counts plus run state and dispatch generation but never
object keys, manifests, signed URLs, or bearer data. The deployment spec must derive metrics
`ProcessorRecoveryScanned`, `ProcessorRecoveryRecovered`, `ProcessorRecoveryFailed`,
`ExecutionBindingFailures`, and `ExecutionRedispatches`; dimensions are limited to `Environment`,
`Service`, and bounded `Reason`—tenant/run/wave/dispatch are log correlation fields, never metric
dimensions. Alert thresholds and CloudWatch resources are deployment-owned.

- [ ] **Step 7: Run composition tests to verify GREEN**

Run: `uv run pytest apps/central_api/tests/test_aws_composition.py apps/data_processor/tests/test_aws_composition.py apps/central_api/tests/test_app_wiring.py apps/central_api/tests/routes/test_serving.py apps/data_processor/tests/test_aws_entrypoint.py apps/data_processor/tests/test_recovery.py apps/data_processor/tests/test_data_processor_main.py -q`

Expected: PASS for exact local delegation, complete API/processor fields, AWS selection, missing
OIDC fail-closed behavior, canonical publisher/registry/handler reuse, exact ECS envelope parsing,
bounded recovery, signed serving, JSON stdout wiring, and all composition function bodies under 50
lines.

Run: `uv run ruff check apps/central_api apps/data_processor packages/cnes_infra/src/cnes_infra`

Expected: `All checks passed!`

- [ ] **Step 8: Commit the serial integration change**

```bash
git add apps/central_api/src/central_api/composition.py \
  apps/central_api/tests/test_aws_composition.py \
  apps/central_api/src/central_api/deps.py \
  apps/central_api/src/central_api/middleware.py \
  apps/central_api/src/central_api/routes/serving.py \
  apps/central_api/tests/test_app_wiring.py \
  apps/central_api/tests/routes/test_serving.py \
  apps/data_processor/src/data_processor/composition.py \
  apps/data_processor/tests/test_aws_composition.py \
  apps/data_processor/src/data_processor/main.py \
  apps/data_processor/src/data_processor/aws_entrypoint.py \
  apps/data_processor/src/data_processor/recovery.py \
  apps/data_processor/tests/test_aws_entrypoint.py \
  apps/data_processor/tests/test_recovery.py \
  apps/data_processor/tests/test_data_processor_main.py \
  packages/cnes_infra/src/cnes_infra/auth/__init__.py \
  packages/cnes_infra/src/cnes_infra/aws/__init__.py \
  packages/cnes_infra/src/cnes_infra/observability/__init__.py \
  apps/central_api/src/central_api/serving/__init__.py \
  .env.example docker-compose.yml
git commit -m "feat(aws): integrate managed runtime profile"
```

### Task 9: AWS-014 Multi-Tenant, Consistency, Fencing, and Recovery Suite

**Files:**
- Create: `tests/integration/aws/conftest.py`
- Create: `tests/integration/aws/test_cross_tenant.py`
- Create: `tests/integration/aws/test_stale_gsi.py`
- Create: `tests/integration/aws/test_fencing_idempotency.py`
- Create: `tests/integration/aws/test_three_wave_orchestration.py`
- Create: `tests/integration/aws/test_failure_recovery.py`

**Interfaces:**
- Consumes: the integrated AWS runtime plus stable control-plane and `RunDispatch` CAS fixtures from
  `CND-013`/`CND-025`, three-wave coordinator fixtures from `CND-060`, publication race/crash
  helpers from `CND-045`, and the `dynamodb-local`/`aws-emulator` endpoints from the CND-025
  `aws-test` Compose profile.
- Produces: an acceptance gate demonstrating `AWS-010` through `AWS-014` without a production AWS account, selected with the CND-025 markers `dynamodb_local` and `s3_integration`; no new marker is registered.

- [ ] **Step 1: Create isolated emulator fixtures and write failing cross-tenant tests**

```python
pytestmark = (pytest.mark.dynamodb_local, pytest.mark.s3_integration)


def test_tenant_b_nao_le_pointer_ou_serving_do_tenant_a(aws_runtime) -> None:
    _seed_membership(aws_runtime, "tenant-a", "user-a")
    _seed_membership(aws_runtime, "tenant-b", "user-b")
    _publish_serving(aws_runtime, "tenant-a", "run-a", "overview.json")
    response = _client(aws_runtime, subject="user-b").get(
        "/api/v1/dashboard/serving/cnes/overview",
        headers={"Authorization": "Bearer user-b", "X-Tenant-Id": "tenant-a"},
        follow_redirects=False,
    )
    assert response.status_code == 403
    aws_runtime.s3_client.generate_presigned_url.assert_not_called()


def test_request_nao_assina_raw_normalized_reconciliation_ou_audit(aws_runtime) -> None:
    for relative_name in (
        "../../raw/data.parquet",
        "../../normalized/data.parquet",
        "../../reconciliation/data.parquet",
        "../../audit/event.json",
    ):
        with pytest.raises(ServingKeyForbidden, match="serving_key_forbidden"):
            aws_runtime.serving_access.grant(
                _request("user-a", "tenant-a", relative_name), NOW,
            )
```

Run: `uv run pytest -m "dynamodb_local and s3_integration" tests/integration/aws/test_cross_tenant.py -q`

Expected: FAIL until the fixtures and integrated runtime create isolated tenant data and route all serving grants through membership authorization.

- [ ] **Step 2: Write stale-GSI tests against DynamoDB Local**

```python
def test_gsi_membership_revogada_nao_autoriza(aws_runtime) -> None:
    stale_query = _captured_membership_query_page("tenant-a", "user-1")
    _delete_base_membership(aws_runtime, "tenant-a", "user-1")
    authorizer = MembershipAuthorizer(
        aws_runtime.control_plane,
        DynamoDBMembershipCandidates(stale_query, TABLE_NAME),
    )
    assert authorizer.list_authorized(_principal("user-1")) == ()


def test_gsi_job_stale_nao_permite_claim(aws_runtime) -> None:
    stale_page, job = _capture_claimable_job_page_then_transition(
        aws_runtime, new_state=JobState.SUCCEEDED,
    )
    control_plane = DynamoDBControlPlane(
        _query_stale_delegate(aws_runtime.dynamodb, stale_page), TABLE_NAME,
        clock=lambda: NOW,
    )
    claimed = control_plane.claim_job(ClaimJob(
        tenant_id=job.tenant_id, job_id=job.job_id, owner="worker-new",
        now=NOW, lease_seconds=300,
    ))
    assert claimed is None
```

`_captured_membership_query_page` is a deterministic `BaseClient` query seam containing the exact low-level page previously returned by DynamoDB Local; it serves only `Query` and keeps that captured candidate after the real base membership is deleted. The control plane remains the real DynamoDB adapter. The job variant captures a claimable GSI page before changing the real base job to `SUCCEEDED`. Assertions consume the stale discovery page and prove the subsequent strongly consistent base read/conditional update denies it; they do not wait for, patch, or attempt to mutate an actual GSI independently.

- [ ] **Step 3: Write fencing and logical-expiry idempotency tests**

```python
def test_worker_com_fence_antigo_nao_commita_unit(aws_runtime) -> None:
    old_lease = _claim_unit(aws_runtime, owner="worker-old")
    _expire_lease_physically_presente(aws_runtime, old_lease, NOW)
    new_lease = _claim_unit(aws_runtime, owner="worker-new")
    assert new_lease.fencing_token == old_lease.fencing_token + 1
    with pytest.raises(FenceRejected):
        _commit_unit(
            aws_runtime, old_lease,
            output_manifests=(_manifest_ref("manifest-old"),),
        )
    committed = _commit_unit(
        aws_runtime, new_lease,
        output_manifests=(_manifest_ref("manifest-new"),),
    )
    assert tuple(ref.manifest_id for ref in committed.output_manifests) == (
        "manifest-new",
    )


def test_idempotencia_expirada_pode_ser_reclamada_com_item_ttl_presente(
    aws_runtime,
) -> None:
    _seed_expired_idempotency_item(
        aws_runtime, key="request-1", request_hash="hash-old", expires_at=NOW,
    )
    outcome = aws_runtime.control_plane.begin_idempotency(BeginIdempotency(
        tenant_id="tenant-a", scope="run", key="request-1",
        request_hash="hash-new", resource_id="run-new", now=NOW,
        expires_at=NOW + timedelta(hours=24),
    ))
    assert outcome.created is True
    assert outcome.record.resource_id == "run-new"


def test_task_ecs_de_dispatch_antigo_nao_reclama_unit(aws_runtime) -> None:
    first = _reserve_dispatch(aws_runtime)
    _expire_dispatch_without_live_unit_lease(aws_runtime, first)
    current = _reserve_dispatch(aws_runtime)
    assert current.generation == first.generation + 1
    assert aws_runtime.control_plane.claim_run_unit(ClaimRunUnit(
        tenant_id=first.tenant_id, run_id=first.run_id,
        dispatch_id=first.dispatch_id, unit_id=first.unit_ids[0],
        owner="arn:aws:states:us-east-1:000000000000:execution:old",
        now=NOW, lease_seconds=300,
    )) is None
```

Also assert same key/same hash returns the same resource, same key/different hash before logical
expiry raises the CND `Conflict`, duplicate Step Functions start for the same `dispatch_id` returns
the same execution reference, and unit retry reuses `unit_id` while incrementing `attempt` and
writing a new tmp prefix. Every assertion reads the tuple-valued `RunUnit.output_manifests`; no
test or fixture uses the removed singular manifest field.

- [ ] **Step 4: Write exact three-wave, dispatch-retry, failure, and recovery tests**

```python
def test_pipeline_inicia_exatamente_tres_ondas_em_ordem(aws_runtime) -> None:
    run = _launch_frozen_cnes_run(aws_runtime)
    normalize = _active_dispatch(aws_runtime, run)
    _succeed_dispatch_units(aws_runtime, normalize)
    reconcile = _resume_and_active_dispatch(aws_runtime, run)
    _succeed_dispatch_units(aws_runtime, reconcile)
    materialize = _resume_and_active_dispatch(aws_runtime, run)
    _succeed_dispatch_units(aws_runtime, materialize)
    result = aws_runtime.processor.coordinator.resume(run.tenant_id, run.run_id)

    dispatches = _recorded_start_requests(aws_runtime, run)
    assert tuple(_stages(aws_runtime, dispatch.unit_ids) for dispatch in dispatches) == (
        (RunStage.NORMALIZE,), (RunStage.RECONCILE,), (RunStage.MATERIALIZE,),
    )
    assert len({dispatch.wave_id for dispatch in dispatches}) == 3
    assert len({dispatch.dispatch_id for dispatch in dispatches}) == 3
    assert result.state in {RunState.PUBLISHED, RunState.PUBLISHED_DEGRADED}
    assert aws_runtime.publisher.publish_calls(run.tenant_id, run.run_id) == 1


def test_terminal_sem_claim_retry_mantem_wave_e_avanca_dispatch(aws_runtime) -> None:
    run = _launch_frozen_cnes_run(aws_runtime)
    first = _active_dispatch(aws_runtime, run)
    assert _leased_units(aws_runtime, run) == ()
    _set_execution_status(aws_runtime, first.execution_ref, "FAILED")
    _advance_clock(aws_runtime, past=first.lease_until)

    aws_runtime.processor.services.recovery.run_once(limit=10)
    retry = _active_dispatch(aws_runtime, run)
    assert retry.wave_id == first.wave_id
    assert retry.generation == first.generation + 1
    assert retry.dispatch_id != first.dispatch_id
    assert _execution_name(retry.execution_ref) == retry.dispatch_id
    assert aws_runtime.executor.start(_request_from(retry)) == retry.execution_ref


def test_bind_dispatch_falha_cancela_ref_e_recovery_cria_geracao_nova(
    aws_runtime,
) -> None:
    coordinator = _coordinator(aws_runtime)
    run = _planned_run(aws_runtime)
    with _fail_bind_run_dispatch_once(aws_runtime, "stale_generation"):
        with pytest.raises(Conflict, match="stale_generation"):
            coordinator.resume(run.tenant_id, run.run_id)
    failed = _latest_dispatch(aws_runtime, run)
    assert _stopped_execution_refs(aws_runtime) == (failed.execution_ref,)
    assert failed.state is DispatchState.FINISHED
    assert failed.terminal_outcome is DispatchOutcome.CANCELED

    aws_runtime.processor.services.recovery.run_once(limit=10)
    recovered = _active_dispatch(aws_runtime, run)
    assert recovered.wave_id == failed.wave_id
    assert recovered.generation == failed.generation + 1
    assert recovered.dispatch_id != failed.dispatch_id
    assert recovered.execution_ref != failed.execution_ref


def test_falha_s3_antes_do_cas_preserva_pointer_anterior(aws_runtime) -> None:
    old_pointer = _publish_version(aws_runtime, "run-old")
    with _fail_s3_after_writes(aws_runtime, successful_writes=1):
        with pytest.raises(ObjectStoreUnavailable):
            _publish_version(aws_runtime, "run-new")
    assert _pointer(aws_runtime).version_id == old_pointer.version_id


def test_falha_audit_apos_cas_reexecuta_outbox_sem_republicar(aws_runtime) -> None:
    with _fail_audit_sink_once(aws_runtime):
        _publish_version(aws_runtime, "run-new")
    pointer = _pointer(aws_runtime)
    version = aws_runtime.control_plane.get_dataset_version(
        "tenant-a", "cnes", pointer.version_id,
    )
    assert version is not None and version.run_id == "run-new"
    assert _pending_outbox(aws_runtime) == ("reconciliation.published",)
    aws_runtime.outbox_dispatcher.dispatch_once(limit=10)
    assert _pending_outbox(aws_runtime) == ()
    assert _audit_object_count(aws_runtime, event_type="reconciliation.published") == 1


def test_dynamodb_indisponivel_falha_fechado(aws_runtime) -> None:
    with _stop_dynamodb_local():
        with pytest.raises(ControlPlaneUnavailable):
            aws_runtime.membership_authorizer.authorize(_principal("user-1"), "tenant-a")
        with pytest.raises(ControlPlaneUnavailable):
            _claim_pending_unit(aws_runtime)
        with pytest.raises(ControlPlaneUnavailable):
            _publish_version(aws_runtime, "run-new")
```

Add failures for Step Functions throttling before start, ECS unit terminal failure, serving object missing, competing pointer CAS, and invalid Express/Distributed state-machine definition. Required-source failure ends the run as `FAILED`; optional-source failure publishes `PUBLISHED_DEGRADED` with explicit `missing_sources`.

- [ ] **Step 5: Run each AWS-014 slice to verify RED, then implement only fixture glue**

Run:

```bash
docker compose --profile aws-test up -d --wait dynamodb-local aws-emulator
AWS_ENDPOINT_URL=http://127.0.0.1:4566 \
DYNAMODB_ENDPOINT_URL=http://127.0.0.1:18000 \
uv run pytest -m "dynamodb_local and s3_integration" tests/integration/aws -q
```

Expected before fixture glue: FAIL on missing `aws_runtime`/fault-injection fixtures. Implement
deterministic table/bucket/state-machine setup and cleanup in `tests/integration/aws/conftest.py`.
Its `RecordingStepFunctionsClient` delegates emulator calls while retaining exact start/stop
requests and exposes a test-only `set_status(execution_ref, status)` response seam for
`DescribeExecution`; it cannot alter DynamoDB state. `_launch_frozen_cnes_run` seeds the declared
raw manifests then calls `RunPlanningService.launch`; `_active_dispatch` calls only
`get_active_run_dispatch`; `_succeed_dispatch_units` sends one `RunUnitMessage` per active unit
through the already-composed handler using deterministic valid stage fixtures; and
`_recorded_start_requests` decodes only recorded IDs-only start inputs. `_coordinator` constructs
the canonical `PipelineCoordinator(CoordinatorDependencies(...), ExecutionPolicyConfig(...))`
from the runtime's exact control plane, executor, publisher, deployment/lease values, callbacks,
and injected clock. `_fail_bind_run_dispatch_once` delegates every control-plane operation except
one injected canonical `bind_run_dispatch` conflict, then resumes delegation so compensation can
persist `FinishRunDispatch(CANCELED)`. Do not build a
second publisher/source registry, weaken production conditions, or catch expected service failures
in production code.

- [ ] **Step 6: Run the complete AWS-014 suite to verify GREEN**

Run:

```bash
AWS_ENDPOINT_URL=http://127.0.0.1:4566 \
DYNAMODB_ENDPOINT_URL=http://127.0.0.1:18000 \
uv run pytest -m "dynamodb_local and s3_integration" tests/integration/aws -q
```

Expected: PASS for cross-tenant denial, stale membership/job GSI revalidation, stale unit/dispatch
fence rejection, same/different/expired idempotency, exactly three sequential logical waves,
same-dispatch replay, generation+1 terminal/binding-failure redispatch, cancellation of only the
just-started reference, pointer preservation, outbox replay, degraded/failed runs, serving absence,
and fail-closed DynamoDB outage.

- [ ] **Step 7: Run race repetition and lint**

Run: `for iteration in $(seq 1 20); do uv run pytest -m "dynamodb_local and s3_integration" tests/integration/aws/test_fencing_idempotency.py -q || exit 1; done`

Expected: PASS in all 20 repetitions with one winning claim/commit and no stale publication.

Run: `uv run ruff check tests/integration/aws`

Expected: `All checks passed!`

- [ ] **Step 8: Commit the acceptance suite**

```bash
git add tests/integration/aws
git commit -m "test(aws): cover isolation consistency and recovery"
```

### Task 10: Serial AWS Profile Acceptance Gate

This is the integration controller's final serial gate after Task 9 is merged.

**Files:**
- Modify: `.github/workflows/python-quality.yml`
- Create: `tests/scripts/test_ci_aws_runtime.py`
- Create: `docs/runbooks/aws-runtime-validation.md`

**Interfaces:**
- Consumes: integrated `AWS-010` through `AWS-014` and the repository's existing lint/coverage commands.
- Produces: a reproducible CI gate and application validation runbook; it does not deploy or mutate production AWS resources.

- [ ] **Step 1: Add the failing CI contract check**

```python
def test_ci_executa_suite_aws_e_sempre_remove_emuladores() -> None:
    workflow = yaml.safe_load(
        Path(".github/workflows/python-quality.yml").read_text(),
    )
    steps = workflow["jobs"]["aws-runtime-integration"]["steps"]
    commands = "\n".join(step.get("run", "") for step in steps)
    assert "docker compose --profile aws-test up -d --wait" in commands
    assert 'pytest -m "dynamodb_local and s3_integration" tests/integration/aws' in commands
    teardown = next(step for step in steps if step.get("name") == "Stop AWS emulators")
    assert teardown["if"] == "always()"
    assert "docker compose --profile aws-test down -v" in teardown["run"]
```

Run: `uv run pytest tests/scripts/test_ci_aws_runtime.py -q`

Expected: FAIL because the job is not yet declared.

- [ ] **Step 2: Add the isolated CI job and runbook**

The job uses the CND-025 boto3 dependency, starts `dynamodb-local` and `aws-emulator` from the `aws-test` profile, waits for health, runs AWS-014, and executes `docker compose --profile aws-test down -v` under `if: always()`. It uses dummy SDK credentials only inside the emulator job because botocore requires a credential provider; application configuration and `.env.example` remain free of static credentials.

The runbook records required application environment names, health/readiness checks, expected
compatible state-machine properties, the seven-variable unit-task envelope, the separate
no-`UNIT_ID` scheduled `recover-once` command/cadence, required IAM actions by component, Object
Lock/versioning preconditions, CloudWatch stdout/metric contracts, recovery backlog and binding
failure symptoms, and rollback behavior. Its recovery procedure first inspects active
`RunDispatch` generation/lease and Standard execution status, then runs one bounded recovery pass;
it never edits DynamoDB records or restarts a named execution manually. It explicitly labels
resource creation and exact IAM/KMS/network/alarm definitions as blocked on the deployment
specification.

- [ ] **Step 3: Run all targeted and repository gates**

Run: `uv run ruff check .`

Expected: `All checks passed!`

Run:

```bash
uv run pytest packages/cnes_infra apps/central_api apps/data_processor \
  -m "not integration and not bigquery and not e2e and not stress and not soak and not spike and not windows_only" \
  -q
```

Expected: PASS with package branch and app line coverage gates unchanged.

Run:

```bash
docker compose --profile aws-test up -d --wait dynamodb-local aws-emulator
AWS_ENDPOINT_URL=http://127.0.0.1:4566 \
DYNAMODB_ENDPOINT_URL=http://127.0.0.1:18000 \
uv run pytest -m "dynamodb_local and s3_integration" tests/integration/aws -q
docker compose --profile aws-test down -v
```

Expected: PASS, followed by removal of the disposable emulator volumes.

- [ ] **Step 4: Verify forbidden dependencies and provisioning calls are absent**

Run:

```bash
rg -n "(postgres|psycopg|minio|keycloak|bigquery|create_table|create_bucket|create_state_machine|register_task_definition)" \
  packages/cnes_infra/src/cnes_infra/aws \
  packages/cnes_infra/src/cnes_infra/executor/step_functions.py \
  apps/central_api/src/central_api/serving \
  apps/central_api/src/central_api/auth \
  apps/central_api/src/central_api/composition.py \
  apps/data_processor/src/data_processor/composition.py \
  apps/data_processor/src/data_processor/main.py \
  apps/data_processor/src/data_processor/aws_entrypoint.py \
  apps/data_processor/src/data_processor/recovery.py
```

Expected: no matches. Emulator resource creation is confined to `tests/integration/aws/conftest.py`.

Run:

```bash
rg -n "(tenant_id|tenant_ids)" packages/cnes_infra/src/cnes_infra/auth/oidc.py
```

Expected: no matches, proving the generic OIDC verifier does not expose a tenant claim.

- [ ] **Step 5: Commit the final gate**

```bash
git add .github/workflows/python-quality.yml tests/scripts/test_ci_aws_runtime.py \
  docs/runbooks/aws-runtime-validation.md
git commit -m "ci(aws): gate managed runtime integration"
```

## Acceptance Traceability

| Backlog ID | Acceptance evidence |
|---|---|
| `AWS-010` | Tasks 1, 2, and 8 compose DynamoDB/S3 without legacy fallbacks or provisioning calls |
| `AWS-011` | Tasks 3, 4, and 8 validate generic OIDC and authorize by canonical membership |
| `AWS-012` | Tasks 5 and 8 require Standard Inline Map, explicit concurrency, the seven-field ECS Fargate envelope, dispatch-named idempotency, and bounded operational recovery |
| `AWS-013` | Tasks 6, 7, and 8 produce JSON stdout, locked/versioned audit writes, and short-lived tenant-safe serving URLs |
| `AWS-014` | Tasks 9 and 10 prove cross-tenant denial, stale-GSI safety, unit/dispatch fencing, exactly three waves, same-attempt replay, generation+1 redispatch, binding-failure cancellation, and recovery |

## External Implementation References

- AWS Step Functions Map state: <https://docs.aws.amazon.com/step-functions/latest/dg/state-map.html>
- AWS Step Functions ECS integration: <https://docs.aws.amazon.com/step-functions/latest/dg/connect-ecs.html>
- DynamoDB transactions: <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html>
- DynamoDB read consistency: <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html>
- DynamoDB TTL: <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html>
- S3 Object Lock: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html>
- ECS `awslogs`: <https://docs.aws.amazon.com/AmazonECS/latest/developerguide/using_awslogs.html>
