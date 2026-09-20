# CnesData Production Runtime Amendments Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
                           (recommended) or superpowers:executing-plans to implement this plan
                           task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Amend the completed CND/AWS runtime with the exact production browser, OIDC, CORS,
signed-serving, public-subnet Fargate, environment-gate, recovery and audit-dispatch contracts
required before CnesData can be provisioned or promoted.

**Architecture:** Extend stable ports/adapters instead of introducing deployment convenience
shortcuts. The dashboard uses one absolute authenticated API client and a second header-free signed
S3 fetch. The API remains provider-neutral, uses Cognito only through OIDC configuration, and
coordinates starts through a durable fence/semaphore/monthly counter. Processor entrypoints remain
one-shot and reuse the canonical coordinator/outbox services.

**Tech Stack:** Python 3.13, FastAPI, boto3/botocore, DynamoDB, S3, Step Functions Standard, ECS
Fargate, React 18, TypeScript, Vite, oidc-client-ts, Bun, pytest, Hypothesis, Vitest, Playwright,
Docker Compose emulators.

**Spec:** docs/superpowers/specs/2026-08-29-cnesdata-production-deployment-design.md and
docs/superpowers/specs/2026-08-29-cnesdata-production-operations-design.md

## Global Constraints

- This plan starts only after CND-020 through CND-025 and AWS-010 through AWS-014 are merged into a
  green develop. CND-020…025 are satisfied (#122-#127, closed 2026-09-01..2026-09-06). AWS-010…014
  remain entirely unimplemented and are gated behind EPIC #94's controller Task 8, which itself
  queues behind CND-064 (#222 / PR #227, in flight); no task may bypass the dependency gate.
- The inspected planning baseline is develop@f8160577f54aa17ba236f3d3f694a01ce80e3436 (2026-09-19,
  CND-063). Re-read the head and record the dependency-complete execution SHA before Task 1.
- The existing docs/superpowers/plans/2026-08-23-cnesdata-aws-runtime-profile-implementation-plan.md
  remains authoritative. This plan modifies its produced files in place and never recreates a
  parallel AWS adapter/composition.
- Integration base is develop. Never commit directly to main or develop.
- Production defaults AWS_REGION=us-east-2. Application examples/tests that intentionally emulate
  another region must be clearly fixture-local.
- PROFILE=aws and AUTH_MODE=oidc are mandatory. PostgreSQL, MinIO, Keycloak and BigQuery have no
  production fallback/import.
- Dashboard production API base is exactly https://api.cnesdata.vinisantana.com/api/v1. Relative
  /api is forbidden.
- Bearer is sent only to https://api.cnesdata.vinisantana.com. X-Tenant-Id is sent only after tenant
  selection and only for tenant-scoped calls.
- Activation exists only at /api/v1/activate/confirm. The origin-level legacy route is absent.
- FastAPI is the sole CORS authority; Nginx forwards OPTIONS unchanged and adds no CORS header.
- Signed serving returns 200 private/no-store with only url, version_id and expires_in=300. Browser
  fetch to S3 sends no credentials, cookies, Authorization, X-Tenant-Id or custom header.
- SigV4 URLs and object keys are bearer-sensitive and absent from logs, telemetry, evidence and
  persistence.
- Production AWS-012 requires AssignPublicIp=ENABLED, exact public subnets/security group, zero
  ingress, FARGATE, max concurrency one, no NAT and no ALB.
- Environment fence, semaphore and monthly execution counter are canonical durable control-plane
  items. TTL is garbage collection only.
- Every initial and recovery StartExecution attempt consumes the shared atomic monthly maximum of
  200. The 100 combined Fargate task-hour target is monitoring, not a start gate.
- Recovery and audit-dispatch entrypoints are separate modes with separate compositions and
  deadlines.
- Function body <=50 lines, complexity <=10, line width <=100, file <=500 lines, parameters <=4 and
  nesting <=3.
- Package coverage and application coverage gates remain unchanged; Portuguese behavior-oriented
  test names remain the convention.

## Dependency Interface Gate

Before Task 1 can pass, the exact symbols in the existing AWS runtime plan must exist and its serial
AWS-014 acceptance must be green. Additionally verify:

| Symbol | Required behavior |
|---|---|
| AwsRuntimeSettings | Existing immutable AWS/profile settings, no static credentials |
| StepFunctionsExecutor | Canonical start/cancel/status transport only |
| DynamoDBControlPlane | Existing strongly consistent/conditional adapter base |
| LocalServingAccess | Canonical membership/pointer authorization before signing |
| S3ObjectLockAuditSink | Existing COMPLIANCE append behavior |
| PipelineCoordinator.recover | Canonical bounded recovery semantics |
| dispatch_once | Existing outbox delivery behavior before cursor extension |

Baseline state (inspected on develop@f816057, see Global Constraints):

- AwsRuntimeSettings: Does not exist. Delivered by AWS-010; nearest analogue today is
                      cnes_domain.profiles.ProfileSettings, whose RuntimeProfile.AWS branch raises
                      ProfileNotImplemented("aws_runtime_plan_required").
- StepFunctionsExecutor: Exists — cnes_infra.executor.step_functions (delivered by CND-042).
- DynamoDBControlPlane: Exists — cnes_infra.control_plane.dynamodb_adapter.
- LocalServingAccess: Exists — central_api.services.serving_access. Its router is not yet registered
                      in central_api.app; wiring lands with CND-064 (#222 / PR #227).
- S3ObjectLockAuditSink: Exists — cnes_infra.audit.s3_object_lock_sink.
- PipelineCoordinator.recover: Exists —
                               data_processor.orchestration.coordinator.PipelineCoordinator.recover.
- dispatch_once: Exists — cnes_domain.outbox_dispatcher.

If any import/signature differs, integration first reconciles this plan against the merged
implementation in a focused docs-only PR. Do not rename the merged contract inside a production
feature branch.

## File Map

Paths marked "does not exist" are created by this plan or by the AWS-010…014
tasks it depends on; they are not renames of something already on develop.

| Path | Responsibility |
|---|---|
| apps/web_dashboard/src/api/client.ts | sole absolute authenticated API transport |
| apps/web_dashboard/src/auth/oidc.ts | resource/audience/scope request |
| apps/web_dashboard/src/api/hooks/useServingOverview.ts | header-free signed S3 second fetch |
| apps/central_api/src/central_api/app.py | exact production CORS |
| apps/central_api/src/central_api/routes/oauth.py | versioned activation path only |
| apps/central_api/src/central_api/routes/serving.py | 200 signed-serving envelope |
| apps/central_api/src/central_api/serving/aws_signed.py | production signed-URL issuance |
| packages/cnes_infra/src/cnes_infra/aws/settings.py | production OIDC/network/credential contract |
| packages/cnes_infra/src/cnes_infra/executor/step_functions.py | production network validator |
| packages/cnes_domain/src/cnes_domain/ports/environment_gate.py | typed fence/semaphore interface |
| packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_adapter.py | gate/counter/cursor ops |
| apps/data_processor/src/data_processor/aws_entrypoint.py | unit/recover/dispatch allowlist |
| apps/data_processor/src/data_processor/recovery.py | deadline-bounded recovery |
| apps/data_processor/src/data_processor/audit_dispatch.py | cursor-aware outbox pass |

State on the inspected baseline (develop@f816057, see Global Constraints):

- apps/web_dashboard/src/api/client.ts: Exists (apiFetch/ApiError).
- apps/web_dashboard/src/auth/oidc.ts: Exists (oidc-client-ts UserManager wrapper).
- apps/web_dashboard/src/api/hooks/useServingOverview.ts: Exists as a TanStack Query hook (CND-063),
                                                          not as api/serving.ts. This plan extends
                                                          that hook rather than creating a parallel
                                                          module.
- apps/central_api/src/central_api/app.py: Exists.
- apps/central_api/src/central_api/routes/oauth.py: Exists. POST /activate/confirm is mounted via
                                                    include_router(oauth.router) with no prefix, so
                                                    the origin-level route this task removes is
                                                    real.
- apps/central_api/src/central_api/routes/serving.py: Exists but returns 200 StreamingResponse of
                                                      raw JSON bytes (CND-062), not an envelope, and
                                                      its router is not yet registered in app.py.
                                                      AWS-013 adds the signed-URL envelope as the
                                                      PROFILE=aws response mode; PROFILE=local keeps
                                                      streaming.
- apps/central_api/src/central_api/serving/aws_signed.py: Does not exist; the central_api/serving/
                                                          package itself is absent. Created by
                                                          AWS-013.
- packages/cnes_infra/src/cnes_infra/aws/settings.py: Does not exist; the cnes_infra/aws/ directory
                                                      is absent. Created by AWS-010.
- packages/cnes_infra/src/cnes_infra/executor/step_functions.py: Exists (delivered by CND-042); this
                                                                 plan extends it.
- packages/cnes_domain/src/cnes_domain/ports/environment_gate.py: Does not exist. Created by this
                                                                  plan (Task 7).
- packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_adapter.py: Exists; this plan extends
                                                                        it.
- apps/data_processor/src/data_processor/aws_entrypoint.py: Does not exist. Current entrypoint is
                                                            data_processor.main + parse_profile.
                                                            Created by EPIC #94 controller Task 8.
- apps/data_processor/src/data_processor/recovery.py: Recovery already lives in
                                                      data_processor.orchestration.coordinator.PipelineCoordinator.recover
                                                      and
                                                      central_api.services.run_planning.RunPlanningService.recover;
                                                      this plan adapts those instead of creating a
                                                      new module.
- apps/data_processor/src/data_processor/audit_dispatch.py: dispatch_once already lives in
                                                            cnes_domain.outbox_dispatcher (domain,
                                                            not app); this plan extends that instead
                                                            of creating an app-level module.

---

### Task 1: Materialize the Dependency and Production-Profile Gate

**Branch:** test/prod-001-dependency-gate

**Files:**
- Create: tests/production/test_dependency_gate.py
- Create: docs/production-readiness.md

**Interfaces:**
- Produces no runtime change.
- Fails until all required CND/AWS imports, signatures and behavior probes are integrated.

- [ ] **Step 1: Write import/signature tests from the table above**

Use inspect.signature plus focused behavior fakes. Also run the existing CND-025 and AWS-014 test
entrypoints by exact path.

- [ ] **Step 2: Run on the current baseline**

    uv run pytest -q tests/production/test_dependency_gate.py

Expected on the inspected baseline: FAIL because Phase 2/AWS materialization is incomplete. Stop
Tasks 2-12.

- [ ] **Step 3: After dependencies merge, record exact develop SHA and green commands**

Document CND issue/PR completion, AWS plan task completion and test run IDs. This is evidence, not a
skip marker.

- [ ] **Step 4: Commit only on a genuinely green dependency head**

    git add tests/production/test_dependency_gate.py docs/production-readiness.md
    git commit -m "test(prod): require integrated CND and AWS runtime"

### Task 2: Make the Dashboard API Boundary Absolute and Origin-Safe

**Branch:** feat/prod-002-dashboard-api

**Files:**
- Modify: apps/web_dashboard/src/lib/env.ts
- Modify: apps/web_dashboard/src/api/client.ts
- Modify: apps/web_dashboard/src/api/hooks/useActivate.ts
- Modify: apps/web_dashboard/src/auth/AuthProvider.tsx
- Modify: apps/web_dashboard/.env.example
- Create: apps/web_dashboard/tests/unit/api/production-client.test.ts
- Modify: apps/web_dashboard/tests/unit/lib/env.test.ts

**Interfaces:**
- ProductionEnv requires VITE_API_BASE_URL exact absolute /api/v1.
- apiRequest(path, options, tenant?) is the only authenticated fetch.
- Activation path passed to the client is /activate/confirm, producing /api/v1/activate/confirm.

- [ ] **Step 1: Write failing environment/client tests**

Reject /api/v1, https://wrong.example/api/v1, missing /api/v1 and credentials=include. Assert
auth/me and activation use one mocked client and never call global fetch directly.

- [ ] **Step 2: Add origin-token tests**

    it("nao envia bearer fora da origem da API", async () => {
      await expect(apiRequestAbsolute("https://evil.test/x")).rejects.toThrow(
        "api_origin_mismatch",
      )
      expect(fetch).not.toHaveBeenCalled()
    })

- [ ] **Step 3: Implement strict URL joining**

Normalize one trailing slash, reject absolute endpoint arguments, query-bearing base and path
traversal. Set bearer only after URL.origin equals the configured API origin. Add X-Tenant-Id only
when tenant is explicitly required.

- [ ] **Step 4: Move activation to the versioned client**

Remove any origin-level /activate/confirm or relative /api use.

- [ ] **Step 5: Run frontend gates and commit**

    cd apps/web_dashboard
    bun run lint
    bun run typecheck
    bun run test --run
    bun run build
    git add src .env.example tests
    git commit -m "feat(dashboard): enforce production api origin"

### Task 3: Configure OIDC Resource Audience and Standard Endpoints

**Branch:** feat/prod-003-oidc-resource

**Files:**
- Modify: apps/web_dashboard/src/auth/oidc.ts
- Modify: apps/web_dashboard/src/lib/env.ts
- Create: apps/web_dashboard/tests/unit/auth/oidc-production.test.ts
- Modify: packages/cnes_infra/src/cnes_infra/aws/settings.py
- Modify: packages/cnes_infra/tests/aws/test_settings.py

**Interfaces:**
- Dashboard requests openid/profile/email plus https://api.cnesdata.vinisantana.com/api.access.
- Authorization extraQueryParams.resource is https://api.cnesdata.vinisantana.com.
- Production settings expose Cognito domain, authorize/token/logout URLs and require OIDC_AUDIENCE
  equal to the API origin.

- [ ] **Step 1: Write exact OIDC config tests**

Require authorization URL /oauth2/authorize, token URL /oauth2/token, logout /logout, resource
parameter and API scope. Reject managed-login-v2/custom-domain assumptions.

- [ ] **Step 2: Write AWS settings consistency tests**

Issuer/user-pool region is us-east-2; audience and resource server identifier are the API origin.
Static key fields remain absent.

- [ ] **Step 3: Implement provider-neutral OIDC fields**

The verifier still consumes issuer/audience/JWKS; no Cognito-specific claim enters domain or
authorization logic.

- [ ] **Step 4: Run Python/dashboard tests**

    uv run pytest -q packages/cnes_infra/tests/aws/test_settings.py
    (cd apps/web_dashboard && bun run test --run tests/unit/auth/oidc-production.test.ts)

- [ ] **Step 5: Commit**

    git add apps/web_dashboard packages/cnes_infra
    git commit -m "feat(auth): request the production api audience"

### Task 4: Move Activation and Install Exact FastAPI CORS

On the inspected baseline, oauth.router also serves /oauth/device_authorization and /oauth/token
(RFC 8628 device flow) from the same APIRouter as /activate/confirm, mounted with no prefix. The Go
edge agent hardcodes both device-flow paths at root (dumpagent register), so a naive prefix change
on the whole router would move them under /api/v1 too and break agent registration. Split
/activate/confirm into its own router, or mount oauth.router twice with route-level excludes; the
two device-flow paths must stay exactly where they are.

**Branch:** feat/prod-004-api-cors

**Files:**
- Modify: apps/central_api/src/central_api/app.py
- Modify: apps/central_api/src/central_api/routes/oauth.py
- Modify: apps/central_api/tests/test_oauth_activate_confirm.py
- Create: apps/central_api/tests/test_production_cors.py
- Modify: apps/central_api/tests/test_app.py
- Create/Modify: a Go registration test asserting /oauth/device_authorization and /oauth/token stay
  root-level (apps/dump_agent_go/cmd/dumpagent/cmd_register_test.go already exercises these paths)

**Interfaces:**
- POST /api/v1/activate/confirm is the only activation route.
- POST /oauth/device_authorization and POST /oauth/token remain root-level, unprefixed and
  unchanged; only /activate/confirm moves.
- CORS exact origin/method/header contract and allow_credentials=false applies only to the
  production AWS profile.

- [ ] **Step 1: Write route absence/presence tests**

Assert /activate/confirm not in OpenAPI, /api/v1/activate/confirm present, bearer+tenant
authorization behavior unchanged, and /oauth/device_authorization plus /oauth/token still resolve
at root with no prefix.

- [ ] **Step 2: Write complete preflight matrix**

Allowed: origin cnesdata, methods GET/POST, headers Authorization/Content-Type/X-Tenant-Id. Denied:
another origin, PUT/PATCH/DELETE, cookie credentials, wildcard and any additional header.

- [ ] **Step 3: Implement versioned router mount**

Keep route function/service unchanged. Split /activate/confirm into its own router (or an
equivalent route-level mount) so only that path gets the /api/v1 prefix; /oauth/device_authorization
and /oauth/token keep mounting at root exactly as today. Do not create a redirect compatibility
route.

- [ ] **Step 4: Install CORSMiddleware as sole authority**

No Nginx header or OPTIONS short-circuit is application code. Error responses still follow the exact
origin policy.

- [ ] **Step 5: Run and commit**

    uv run pytest -q apps/central_api/tests/test_oauth_activate_confirm.py apps/central_api/tests/test_production_cors.py apps/central_api/tests/test_app.py
    (cd apps/dump_agent_go && go test ./cmd/dumpagent/... -run TestRegister)
    git add apps/central_api apps/dump_agent_go
    git commit -m "feat(api): version activation and enforce exact cors"

### Task 5: Replace Signed-Serving Redirect with a 200 Envelope

This task replaces the 307 that AWS-013 was planned to add to PROFILE=aws (per
the AWS runtime plan and the merged production deployment design, which states
"This production handoff replaces the AWS-014 307 route contract before
promotion"). It does not replace anything live: on the inspected baseline
routes/serving.py returns 200 StreamingResponse of raw JSON bytes for
PROFILE=local (CND-062), and that streaming behavior is unaffected. The 200
envelope below is the PROFILE=aws response mode only. aws_signed.py itself is
created by AWS-013, a dependency this plan requires merged before Task 1; by
the time this task runs it already exists. Preserve its canonical
authorization/signing behavior (LocalServingAccess check, single authorized
key/version) and change only the response envelope it returns.

**Branch:** feat/prod-005-serving-envelope

**Files:**
- Modify: apps/central_api/src/central_api/serving/aws_signed.py (created by AWS-013; change the
  response envelope only, keep its authorization/signing behavior)
- Modify: apps/central_api/src/central_api/routes/serving.py
- Create: apps/central_api/tests/serving/test_aws_signed.py
- Create: apps/central_api/tests/routes/test_production_serving.py
- Modify: apps/web_dashboard/src/api/hooks/useServingOverview.ts (extends the existing CND-063 hook;
  do not create a parallel src/api/serving.ts)
- Modify: apps/web_dashboard/tests/unit/api/hooks/useServingOverview.test.tsx
- Modify: tests/integration/aws/test_signed_serving.py

**Interfaces:**
- ServingResponse(url: SecretStr-like redacted value, version_id: str, expires_in: Literal[300]).
- First response status 200 and Cache-Control private, no-store.
- Second fetch is direct S3 credentials=omit with no headers.

- [ ] **Step 1: Write API response-schema tests**

Assert exact keys only, status/cache header, 300 TTL, prior authorization/stat and denial of
raw/normalized/reconciliation/tmp/audit prefixes.

- [ ] **Step 2: Write logging/redaction tests**

Exercise success/error and assert signed query/key never appears in caplog, tracing attributes or
exception repr.

- [ ] **Step 3: Implement the 200 envelope**

Preserve canonical LocalServingAccess authorization. Generate GET only for the single authorized
serving key/version; attach ResponseCacheControl private,no-store where supported.

- [ ] **Step 4: Implement the browser second fetch**

Do not reuse apiRequest. Build fetch(url, {method:"GET", credentials:"omit", redirect:"error",
headers:{}}). Reject non-HTTPS or non-S3 signed URL origin patterns configured by the runtime
contract.

- [ ] **Step 5: Run backend/frontend/integration tests**

    uv run pytest -q apps/central_api/tests/serving apps/central_api/tests/routes/test_production_serving.py tests/integration/aws/test_signed_serving.py
    (cd apps/web_dashboard && bun run test --run tests/unit/api/hooks/useServingOverview.test.tsx)

- [ ] **Step 6: Commit**

    git add apps/central_api apps/web_dashboard tests/integration/aws
    git commit -m "feat(serving): return a private signed-url envelope"

### Task 6: Apply the Production AWS-012 Network Override

**Branch:** feat/prod-006-fargate-network

**Files:**
- Modify: packages/cnes_infra/src/cnes_infra/aws/settings.py
- Modify: packages/cnes_infra/src/cnes_infra/executor/step_functions.py
- Modify: packages/cnes_infra/tests/executor/test_step_functions.py
- Create: packages/cnes_infra/tests/executor/test_production_network.py
- Modify: tests/integration/aws/test_step_functions_ecs.py

**Interfaces:**
- Production settings contain exact tuple public_subnet_ids and security_group_id,
  max_concurrency=1.
- validate_state_machine compares the generated ASL ECS Parameters against those exact values and an
  inspected zero-ingress network contract.

- [ ] **Step 1: Write the acceptance/rejection matrix**

Accept only ENABLED+exact subnets+exact SG+FARGATE+1+no NAT/ALB. Reject DISABLED,
missing/extra/reordered normalized subnet set, SG drift, ingress, EC2 launch, concurrency >1 and
forbidden resource markers.

- [ ] **Step 2: Preserve emulator/generic fixture behavior explicitly**

Use a profile/network policy argument; do not weaken the production validator to accept both modes
silently.

- [ ] **Step 3: Implement exact comparison**

Normalize subnet tuple deterministically, but do not accept a superset. Validate Inline Map remains
non-distributed and no service/desired count is introduced.

- [ ] **Step 4: Run AWS-012/AWS-014 focused suites**

    uv run pytest -q packages/cnes_infra/tests/executor tests/integration/aws/test_step_functions_ecs.py

- [ ] **Step 5: Commit**

    git add packages/cnes_infra tests/integration/aws
    git commit -m "feat(processing): enforce public-ip fargate profile"

### Task 7: Add Typed Environment Fence and Semaphore Operations

Both PipelineCoordinator.recover
(apps/data_processor/src/data_processor/orchestration/coordinator.py) and RunPlanningService
(apps/central_api/src/central_api/services/run_planning.py) reach executor.start() through their own
_dispatch_protocol. Adding the permit type without wiring it into those two call sites means closing
the promotion fence would not stop API or recovery starts, and the one-execution semaphore this task
promises would not be enforced. This task must wire both call sites, not just add the primitive.

**Branch:** feat/prod-007-environment-gate

**Files:**
- Create: packages/cnes_domain/src/cnes_domain/ports/environment_gate.py
- Create: packages/cnes_domain/src/cnes_domain/control_plane/environment_gate.py
- Create: packages/cnes_domain/tests/ports/test_environment_gate.py
- Modify: packages/cnes_domain/src/cnes_domain/ports/control_plane.py
- Modify: packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_adapter.py
- Create: packages/cnes_infra/tests/control_plane/test_environment_gate.py
- Modify: apps/central_api/src/central_api/services/run_planning.py (wire acquire/bind around
  _dispatch_protocol and release around _settle_started)
- Modify: apps/data_processor/src/data_processor/orchestration/coordinator.py (wire acquire/bind
  around _dispatch_protocol and release around _settle_started)
- Modify: apps/central_api/tests/services/test_run_planning.py
- Modify: apps/data_processor/tests/test_coordinator.py (or the coordinator's current test module)

**Interfaces:**
- observe_environment_gate() is strongly consistent.
- Typed commands: AcquireUnitPermit, BindUnitPermit, RenewUnitPermit, ReleaseUnitPermit,
  ClosePromotionFence and ReopenPromotionFence.
- Fence and semaphore are separate items; acquire transaction requires open fence.
- Both dispatch call sites (run_planning and coordinator) acquire a permit before executor.start()
  and bind it to the returned execution reference immediately after; a closed fence or a held permit
  rejects the dispatch before any Step Functions call.
- Both settlement call sites (_settle_started in run_planning.py and coordinator.py) release the
  bound permit the moment an execution reaches a terminal status (succeeded, failed or cancelled),
  not only on a rejected/failed dispatch attempt; a successful wave must free the semaphore for the
  next start.
- Every terminal transition of a bound run releases the permit, not only the ones reached through
  _settle_started. On the inspected baseline, coordinator.py's resume()/_resume_processing() reach
  _cancel() and _publish_now() (both directly and via a FAILED fan-in decision) without going
  through _settle_started; TTL is explicitly non-authoritative, so a permit left bound on any of
  these paths blocks subsequent starts and promotion drain indefinitely.

- [ ] **Step 1: Write pure transition tests**

Cover close-first, acquire/bind/renew/release, only bound canary while fenced, token mismatch, stale
generation, TTL non-authority and reopen only matching promotion.

- [ ] **Step 2: Write adapter race tests**

Concurrent acquire winners <=1. A fence close racing acquire either closes before and rejects, or
observes the live permit and drain waits; never both closed+new unbound start.

- [ ] **Step 3: Implement immutable entities/commands first**

No AWS imports in cnes_domain. Use aware UTC datetimes and explicit version/fencing tokens.

- [ ] **Step 4: Implement conditional DynamoDB transactions**

Use consistent reads and TransactWriteItems conditions. Expired takeover requires a supplied
liveness proof adapter result; never time alone.

- [ ] **Step 5: Wire the permit into both dispatch call sites**

In run_planning.py and coordinator.py, acquire the unit permit and persist the deterministic
execution reference (the client-supplied StartExecution idempotency name) into the permit record
itself, before calling executor.start() -- not only after a successful bind. The process can die
between StartExecution being accepted and BindUnitPermit running; if the deterministic reference
lives only in local memory at that point, the crash loses it and the next recovery attempt finds a
held-but-unbound permit with no way to tell whether it belongs to a real in-flight execution or an
orphan. With the reference persisted at acquisition, recovery can always reconcile: probe Step
Functions by that name and bind if found, or release if not, regardless of when the process died.
Release the dispatch-time record only on a provably failed dispatch attempt, never on an ambiguous
one -- a timeout or connection error from executor.start() does not prove StartExecution was not
accepted server-side; on an ambiguous error, probe by the persisted deterministic reference before
releasing. Write a test per call site proving a closed fence or held permit blocks executor.start()
from being called at all (no Step Functions call on rejection), and a separate test proving an
ambiguous-error path probes before releasing.

- [ ] **Step 6: Reconcile held-but-unbound permits at acquisition, not only on dispatch**

TTL is explicitly non-authoritative, so a held-but-unbound permit must never simply be rejected --
that would block all later starts and promotion drain indefinitely, since nothing else ever clears
it, and recover() reaches this same acquire call through _dispatch_protocol (see this task's opening
note), so a crash between StartExecution and BindUnitPermit is exactly what the next recover() call
encounters. Before AcquireUnitPermit rejects on an already-held permit, check whether that permit
carries a persisted deterministic execution reference (Step 5) with no bound execution: if so, probe
Step Functions by that reference the same way Step 5's ambiguous-error path does, then bind on found
or release on proven-absent, before falling back to the ordinary reject-on-held behavior. Write a
test proving this reconciliation recovers a permit orphaned by a process death between
StartExecution and BindUnitPermit, and a test proving a permit that is both held and bound (a
genuinely live execution) is left untouched and still rejects a second acquire.

- [ ] **Step 7: Release the permit on every terminal transition, not only settlement**

In both _settle_started implementations, release the bound permit as soon as an execution's status
transitions to succeeded, failed or cancelled. Then grep both modules for every function that can
return a terminal CoordinatorResult/RunLaunchResult for a bound run -- on the inspected baseline
this includes _cancel(), _publish_now() and the FAILED branch inside _resume_processing() in
coordinator.py, reached through resume() without ever calling _settle_started -- and release there
too. Do not assume _settle_started is the only terminal path; treat this as an exhaustive audit, not
a fixed list. Write a test per non-settlement terminal path proving the permit is released and the
next AcquireUnitPermit succeeds; an execution left running or in an unrecognized status must not
release.

- [ ] **Step 8: Run package 100% branch gates and commit**

    uv run pytest packages/cnes_domain/tests/ports/test_environment_gate.py --cov --cov-branch
    uv run pytest -q packages/cnes_infra/tests/control_plane/test_environment_gate.py apps/central_api/tests apps/data_processor/tests
    git add packages/cnes_domain packages/cnes_infra apps/central_api apps/data_processor
    git commit -m "feat(control-plane): add promotion fence and unit semaphore"

### Task 8: Add the Shared Monthly Execution-Attempt Counter

**Branch:** feat/prod-008-execution-quota

**Files:**
- Create: packages/cnes_domain/src/cnes_domain/control_plane/execution_quota.py
- Create: packages/cnes_domain/tests/control_plane/test_execution_quota.py
- Modify: packages/cnes_domain/src/cnes_domain/ports/control_plane.py
- Modify: packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_adapter.py
- Create: packages/cnes_infra/tests/control_plane/test_execution_quota.py
- Modify: apps/central_api/src/central_api/services/run_planning.py
- Modify: apps/data_processor/src/data_processor/orchestration/coordinator.py (recover path; the
  plan-doc name recovery.py does not exist on develop)

**Interfaces:**
- consume_execution_attempt(environment, period, now, limit=200) atomically increments after Task
  7's unit permit is acquired and before StartExecution, never before acquisition -- checking quota
  first would spend a scarce monthly attempt on a request a closed fence or an already-held permit
  was always going to reject.
- Same monthly item/limit is used by both callers.
- Quota rejection performs no Step Functions call, releases the just-acquired unit permit (Task 7)
  so it does not sit occupied indefinitely, and maps to the documented 429/quota result.

- [ ] **Step 1: Write boundary and race tests**

199->200 succeeds; 200->201 rejects. Initial/recovery races total exactly 200. Failed StartExecution
still counts as an attempt. TTL deletion is irrelevant. Add a test proving quota rejection releases
the caller's unit permit (Task 7) instead of leaving it held.

- [ ] **Step 2: Add service-level no-call tests**

Fake executor records zero calls when quota rejects. The 100 task-hour metric is absent from the
decision path.

- [ ] **Step 3: Implement conditional counter and wire both callers**

UTC month key; atomic ADD/condition; no read-then-write race. In both run_planning.py and
coordinator.py the call order is: acquire Task 7's unit permit, consume the monthly attempt, then
StartExecution. If quota consumption rejects, release the permit before returning the 429/quota
result -- the permit must never remain held after this function returns without a call to
StartExecution having been attempted.

- [ ] **Step 4: Run and commit**

    uv run pytest -q packages/cnes_domain/tests/control_plane/test_execution_quota.py packages/cnes_infra/tests/control_plane/test_execution_quota.py apps/central_api/tests apps/data_processor/tests
    git add packages/cnes_domain packages/cnes_infra apps/central_api apps/data_processor
    git commit -m "feat(processing): cap monthly execution attempts atomically"

### Task 9: Extend Outbox Delivery with Cursor Paging

pending_outbox(limit) lives on the shared ControlPlanePort and is implemented by both
dynamodb_publication.py and sqlite_adapter.py; the SQLite adapter backs PROFILE=local and its
replay/chaos integration suites. Replacing the port method without an equivalent SQLite
implementation breaks the local profile the moment this port change merges, well before any AWS
profile exists. sqlite_adapter.py must gain read_outbox_page/advance_outbox_cursor in the same
task, not later.

**Branch:** feat/prod-009-outbox-cursor

**Files:**
- Modify: packages/cnes_domain/src/cnes_domain/ports/control_plane.py
- Modify: packages/cnes_domain/src/cnes_domain/outbox_dispatcher.py
- Modify: packages/cnes_domain/tests/test_outbox_dispatcher.py
- Modify: packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_adapter.py
- Modify: packages/cnes_infra/src/cnes_infra/control_plane/sqlite_adapter.py
- Modify: packages/cnes_infra/tests/control_plane/test_sqlite_adapter.py
- Modify: packages/cnes_infra/tests/contracts/control_plane_contract.py (shared cursor contract
  exercised by both adapters)
- Create: packages/cnes_infra/tests/control_plane/test_outbox_cursor.py

**Interfaces:**
- Replaces pending_outbox(limit) with read_outbox_page(cursor, limit) and
  advance_outbox_cursor(expected, next) on the shared port; both DynamoDB and SQLite adapters
  implement it.
- A pass advances after evaluating each page, wraps, and retries poison without starving later
  pages, on either backend.
- Delivery marker changes only after S3 COMPLIANCE append succeeds (DynamoDB/AWS profile) or after
  the equivalent local audit sink append succeeds (SQLite/local profile).

- [ ] **Step 1: Write 100-poison plus second-page test**

First page remains pending/retrying; second page still delivers in the same/bounded subsequent pass;
cursor wraps and replay is idempotent. Run against both adapters via the shared contract suite.

- [ ] **Step 2: Write CAS conflict and crash tests**

Concurrent cursor advancement has one winner. Crash after append/before marker replays the same
object idempotently and then marks. Cover both DynamoDB conditional writes and SQLite's transaction
boundary.

- [ ] **Step 3: Change the port and all fake implementations serially**

This is a shared interface hotspot; no parallel task edits control_plane.py.

- [ ] **Step 4: Implement DynamoDB Query/GetItem/UpdateItem only**

No Scan, DeleteItem or PutItem for the dispatch role path. Strongly revalidate each event.

- [ ] **Step 5: Implement the SQLite cursor equivalent**

Same read_outbox_page/advance_outbox_cursor contract, backed by SQLite's own transaction/CAS
primitives instead of DynamoDB conditionals. The local profile must keep passing every outbox test
that passed before this task.

- [ ] **Step 6: Run full contract harness and commit**

    uv run pytest -q packages/cnes_domain/tests/test_outbox_dispatcher.py packages/cnes_infra/tests/control_plane/test_outbox_cursor.py packages/cnes_infra/tests/control_plane/test_sqlite_adapter.py packages/cnes_infra/tests/contracts
    git add packages/cnes_domain packages/cnes_infra
    git commit -m "feat(audit): page outbox without poison starvation"

### Task 10: Add Separate Recovery and Audit-Dispatch Entrypoints

**Branch:** feat/prod-010-processor-modes

**Files:**
- Modify: apps/data_processor/src/data_processor/aws_entrypoint.py
- Modify: apps/data_processor/src/data_processor/orchestration/coordinator.py (recover path;
  recovery.py does not exist on develop, see File Map)
- Create: apps/data_processor/src/data_processor/audit_dispatch.py
- Create: apps/data_processor/src/data_processor/deadline.py
- Modify: apps/data_processor/tests/test_aws_entrypoint.py
- Create: apps/data_processor/tests/test_recovery_mode.py
- Create: apps/data_processor/tests/test_audit_dispatch_mode.py
- Create: apps/data_processor/tests/test_deadline.py

**Interfaces:**
- Allowed modes: run-unit, recover-once, dispatch-outbox-once.
- recover-once builds coordinator only, batch 100, lease/deadline 3600, no unit env or audit sink.
- dispatch-outbox-once builds control plane+audit sink+UTC clock, limit 100, deadline 60, no unit
  env.

- [ ] **Step 1: Write mode/env isolation tests**

Each mode rejects another mode's required/forbidden variables and dependency construction. Unknown
mode exits terminal configuration code.

- [ ] **Step 2: Write deadline behavior**

PID 1 deadline cancels work, logs bounded event, exits nonzero and never continues past lease. Audit
deadline is 60 seconds.

- [ ] **Step 3: Implement small composition factories**

Reuse canonical AwsRuntime client/adapters. Do not instantiate API runtime, normal unit registry in
recover, or audit sink in recover.

- [ ] **Step 4: Test overlapping recovery passes**

No global recovery lock, but environment semaphore ensures at most one unit task; losing recoveries
do not StartExecution and same-run dispatch CAS remains stable.

- [ ] **Step 5: Run and commit**

    uv run pytest -q apps/data_processor/tests/test_aws_entrypoint.py apps/data_processor/tests/test_recovery_mode.py apps/data_processor/tests/test_audit_dispatch_mode.py apps/data_processor/tests/test_deadline.py
    git add apps/data_processor
    git commit -m "feat(processor): separate recovery and audit passes"

### Task 11: Add the Production API Container Credential Contract

deploy/ already exists on develop (deploy/dev/ and deploy/prod/, both
Compose+Caddy driven by shell scripts). This task's branch (feat/prod-011-api-container) can merge
to develop right after Tasks 1 and 3, long before any AWS resource exists (infrastructure plan Tasks
1-12) and long before the delivery/operations plan's actual promotion cutover (Phase C, Task 9).
Removing deploy/prod/docker-compose.prod.yml's data-processor service here would take down live VPS
processing with no AWS replacement running yet. This task therefore only builds the new AWS-profile
container and credential contract; it does not touch the VPS Compose file. Actual retirement of the
VPS data-processor service belongs to the delivery/operations plan's promotion cutover, after AWS
unit/recovery/audit tasks are verified green and routing is accepted -- see that plan's Task 9.

**Branch:** feat/prod-011-api-container

**Files:**
- Modify: apps/central_api/Dockerfile
- Create: apps/central_api/docker/production-entrypoint.sh
- Create: deploy/compose.production.yaml
- Create: deploy/aws/config
- Create: tests/production/test_api_container.py
- Create: tests/production/test_compose_contract.py
- Create: tests/production/test_credential_process.py

**Interfaces:**
- AWS_PROFILE=cnesdata-production and AWS_CONFIG_FILE point to a read-only config whose
  credential_process calls aws_signing_helper credential-process --session-duration 3600.
- Container sees only required helper/config/leaf key material, key readable by fixed UID; no shared
  static credentials.
- API binds one loopback port. The new deploy/compose.production.yaml never defines a
  data-processor service; the existing VPS data-processor service is retired later, during
  promotion cutover, not by this task.

- [ ] **Step 1: Write static-image/Compose tests**

Require non-root/read-only/cap-drop/no-new-privileges/limits/health. Reject docker.sock, host
network, public port, LimnoPulse path/network, AWS_ACCESS_KEY_ID/SECRET/SESSION and a data-processor
service definition in the new deploy/compose.production.yaml (the existing VPS
docker-compose.prod.yml keeps its data-processor service until cutover retires it).

- [ ] **Step 2: Write botocore advisory-refresh test**

First AWS call invokes helper, calls before advisory window do not, first call inside 15-minute
window refreshes once; helper failure fails closed. Use fake clock/process provider, no real
certificate.

- [ ] **Step 3: Implement credential_process config**

Exact helper path, certificate/private-key/trust-anchor/profile/role ARNs from mounted
public/root-owned files; session duration 3600. No shell interpolation.

- [ ] **Step 4: Build and smoke**

    docker build --tag cnesdata-api:prod-test -f apps/central_api/Dockerfile .
    docker compose -f deploy/compose.production.yaml config --quiet
    uv run pytest -q tests/production/test_api_container.py tests/production/test_compose_contract.py tests/production/test_credential_process.py

- [ ] **Step 5: Commit**

    git add apps/central_api deploy tests/production
    git commit -m "feat(api): add roles-anywhere production container"

### Task 12: Build the Runtime Amendment Acceptance Gate

**Branch:** test/prod-012-runtime-acceptance

**Files:**
- Create: tests/production/test_runtime_acceptance.py
- Create: docs/runbooks/runtime-acceptance.md
- Modify: .github/workflows/python-quality.yml
- Modify: .github/workflows/web-dashboard.yml

**Interfaces:**
- Extends existing credential-free CI only; no production secret/OIDC on pull requests.
- Produces a single required runtime-amendments check.

- [ ] **Step 1: Aggregate all production deltas**

Dependency gate, dashboard absolute client, OIDC resource, route/CORS, 200 serving, AWS-012
override, fence/semaphore, quota, cursor outbox, processor modes and credential process.

- [ ] **Step 2: Add negative source checks**

Reject relative production /api, origin-level activation, API log formatting of url, 307 serving
response, AssignPublicIp DISABLED, NAT/ALB, static AWS key fields, Scan in new adapters and
task-hour start gate.

- [ ] **Step 3: Extend existing hosted-runner jobs**

Run focused suites and emulator-backed integration with unconditional teardown. Keep root
permissions contents:read and no secrets/id-token.

- [ ] **Step 4: Run complete quality gates**

    uv run ruff check .
    uv run pytest -m "not integration and not postgres and not bigquery and not e2e and not stress and not soak and not spike and not windows_only" -q
    uv run pytest -q tests/production
    (cd apps/web_dashboard && bun run lint && bun run typecheck && bun run test --run && bun run build)
    git diff --check

- [ ] **Step 5: Commit**

    git add tests/production docs/runbooks/runtime-acceptance.md .github/workflows
    git commit -m "test(prod): gate production runtime amendments"

## Execution Order

Task 1 is a hard gate. Tasks 2 and 6 may start after it. Task 3 waits for Task 2; Tasks 4 and 5 wait
for Tasks 2-3. Task 7 is the serial control-plane foundation. Task 8 waits for Task 7. Task 9 is
another serial control-plane hotspot and starts after Task 8 merges. Task 10 waits for Tasks 8-9.
Task 11 can proceed after Tasks 1 and 3. Task 12 waits for all prior tasks.

## Plan Self-Review Record

- Existing-plan boundary: AWS-010…014 files are extended in place; no duplicate
  adapter/composition exists.
- Browser contract: exact API origin, versioned activation, bearer/tenant scoping and header-free S3
  fetch align across dashboard/API tests.
- Processing contract: public-IP Fargate is explicit and does not loosen the generic test profile.
- Correctness: fence, semaphore, counter and outbox cursor are typed domain operations with
  conditional adapters.
- Operations: recovery and audit share the image but not configuration/composition/deadline.
- Credentials: production uses credential_process; no static keys or shared credential file
  assumption.
- Completeness scan: no unresolved marker, skipped dependency or undefined mode.

## Execution Handoff

Do not dispatch any implementation task until the dependency gate can pass on develop. After this
plan is merged and green, the production infrastructure plan may consume these exact settings, IAM
and task-mode contracts.

