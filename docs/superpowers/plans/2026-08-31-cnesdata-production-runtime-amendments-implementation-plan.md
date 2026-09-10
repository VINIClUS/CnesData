# CnesData Production Runtime Amendments Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Amend the completed CND/AWS runtime with the exact production browser, OIDC, CORS, signed-serving, public-subnet Fargate, environment-gate, recovery and audit-dispatch contracts required before CnesData can be provisioned or promoted.

**Architecture:** Extend stable ports/adapters instead of introducing deployment convenience shortcuts. The dashboard uses one absolute authenticated API client and a second header-free signed S3 fetch. The API remains provider-neutral, uses Cognito only through OIDC configuration, and coordinates starts through a durable fence/semaphore/monthly counter. Processor entrypoints remain one-shot and reuse the canonical coordinator/outbox services.

**Tech Stack:** Python 3.13, FastAPI, boto3/botocore, DynamoDB, S3, Step Functions Standard, ECS Fargate, React 18, TypeScript, Vite, oidc-client-ts, Bun, pytest, Hypothesis, Vitest, Playwright, Docker Compose emulators.

**Spec:** docs/superpowers/specs/2026-08-29-cnesdata-production-deployment-design.md and docs/superpowers/specs/2026-08-29-cnesdata-production-operations-design.md

## Global Constraints

- This plan starts only after CND-020 through CND-025 and AWS-010 through AWS-014 are merged into a green develop. As of the planning review, CND-020…025 remain open; no task may bypass the dependency gate.
- The inspected planning baseline is develop@976c042f6c3454bb9ca3760d708a84e7d23187e1. Re-read the head and record the dependency-complete execution SHA before Task 1.
- The existing docs/superpowers/plans/2026-08-23-cnesdata-aws-runtime-profile-implementation-plan.md remains authoritative. This plan modifies its produced files in place and never recreates a parallel AWS adapter/composition.
- Integration base is develop. Never commit directly to main or develop.
- Production defaults AWS_REGION=us-east-2. Application examples/tests that intentionally emulate another region must be clearly fixture-local.
- PROFILE=aws and AUTH_MODE=oidc are mandatory. PostgreSQL, MinIO, Keycloak and BigQuery have no production fallback/import.
- Dashboard production API base is exactly https://api.cnesdata.vinisantana.com/api/v1. Relative /api is forbidden.
- Bearer is sent only to https://api.cnesdata.vinisantana.com. X-Tenant-Id is sent only after tenant selection and only for tenant-scoped calls.
- Activation exists only at /api/v1/activate/confirm. The origin-level legacy route is absent.
- FastAPI is the sole CORS authority; Nginx forwards OPTIONS unchanged and adds no CORS header.
- Signed serving returns 200 private/no-store with only url, version_id and expires_in=300. Browser fetch to S3 sends no credentials, cookies, Authorization, X-Tenant-Id or custom header.
- SigV4 URLs and object keys are bearer-sensitive and absent from logs, telemetry, evidence and persistence.
- Production AWS-012 requires AssignPublicIp=ENABLED, exact public subnets/security group, zero ingress, FARGATE, max concurrency one, no NAT and no ALB.
- Environment fence, semaphore and monthly execution counter are canonical durable control-plane items. TTL is garbage collection only.
- Every initial and recovery StartExecution attempt consumes the shared atomic monthly maximum of 200. The 100 combined Fargate task-hour target is monitoring, not a start gate.
- Recovery and audit-dispatch entrypoints are separate modes with separate compositions and deadlines.
- Function body <=50 lines, complexity <=10, line width <=100, file <=500 lines, parameters <=4 and nesting <=3.
- Package coverage and application coverage gates remain unchanged; Portuguese behavior-oriented test names remain the convention.

## Dependency Interface Gate

Before Task 1 can pass, the exact symbols in the existing AWS runtime plan must exist and its serial AWS-014 acceptance must be green. Additionally verify:

| Symbol | Required behavior |
|---|---|
| AwsRuntimeSettings | Existing immutable AWS/profile settings, no static credentials |
| StepFunctionsExecutor | Canonical start/cancel/status transport only |
| DynamoDBControlPlane | Existing strongly consistent/conditional adapter base |
| LocalServingAccess | Canonical membership/pointer authorization before signing |
| S3ObjectLockAuditSink | Existing COMPLIANCE append behavior |
| PipelineCoordinator.recover | Canonical bounded recovery semantics |
| dispatch_once | Existing outbox delivery behavior before cursor extension |

If any import/signature differs, integration first reconciles this plan against the merged implementation in a focused docs-only PR. Do not rename the merged contract inside a production feature branch.

## File Map

| Path | Responsibility |
|---|---|
| apps/web_dashboard/src/api/client.ts | sole absolute authenticated API transport |
| apps/web_dashboard/src/auth/oidc.ts | resource/audience/scope request |
| apps/web_dashboard/src/api/serving.ts | header-free signed S3 second fetch |
| apps/central_api/src/central_api/app.py | exact production CORS |
| apps/central_api/src/central_api/routes/oauth.py | versioned activation path only |
| apps/central_api/src/central_api/routes/serving.py | 200 signed-serving envelope |
| packages/cnes_infra/src/cnes_infra/aws/settings.py | production OIDC/network/credential constraints |
| packages/cnes_infra/src/cnes_infra/executor/step_functions.py | exact production network validator |
| packages/cnes_domain/src/cnes_domain/ports/environment_gate.py | typed fence/semaphore interface |
| packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_adapter.py | conditional gate/counter/cursor operations |
| apps/data_processor/src/data_processor/aws_entrypoint.py | unit/recover/dispatch allowlist |
| apps/data_processor/src/data_processor/recovery.py | deadline-bounded recovery |
| apps/data_processor/src/data_processor/audit_dispatch.py | cursor-aware outbox pass |

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

Use inspect.signature plus focused behavior fakes. Also run the existing CND-025 and AWS-014 test entrypoints by exact path.

- [ ] **Step 2: Run on the current baseline**

    uv run pytest -q tests/production/test_dependency_gate.py

Expected on the inspected baseline: FAIL because Phase 2/AWS materialization is incomplete. Stop Tasks 2-12.

- [ ] **Step 3: After dependencies merge, record exact develop SHA and green commands**

Document CND issue/PR completion, AWS plan task completion and test run IDs. This is evidence, not a skip marker.

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

Reject /api/v1, https://wrong.example/api/v1, missing /api/v1 and credentials=include. Assert auth/me and activation use one mocked client and never call global fetch directly.

- [ ] **Step 2: Add origin-token tests**

    it("nao envia bearer fora da origem da API", async () => {
      await expect(apiRequestAbsolute("https://evil.test/x")).rejects.toThrow(
        "api_origin_mismatch",
      )
      expect(fetch).not.toHaveBeenCalled()
    })

- [ ] **Step 3: Implement strict URL joining**

Normalize one trailing slash, reject absolute endpoint arguments, query-bearing base and path traversal. Set bearer only after URL.origin equals the configured API origin. Add X-Tenant-Id only when tenant is explicitly required.

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
- Production settings expose Cognito domain, authorize/token/logout URLs and require OIDC_AUDIENCE equal to the API origin.

- [ ] **Step 1: Write exact OIDC config tests**

Require authorization URL /oauth2/authorize, token URL /oauth2/token, logout /logout, resource parameter and API scope. Reject managed-login-v2/custom-domain assumptions.

- [ ] **Step 2: Write AWS settings consistency tests**

Issuer/user-pool region is us-east-2; audience and resource server identifier are the API origin. Static key fields remain absent.

- [ ] **Step 3: Implement provider-neutral OIDC fields**

The verifier still consumes issuer/audience/JWKS; no Cognito-specific claim enters domain or authorization logic.

- [ ] **Step 4: Run Python/dashboard tests**

    uv run pytest -q packages/cnes_infra/tests/aws/test_settings.py
    cd apps/web_dashboard && bun run test --run tests/unit/auth/oidc-production.test.ts

- [ ] **Step 5: Commit**

    git add apps/web_dashboard packages/cnes_infra
    git commit -m "feat(auth): request the production api audience"

### Task 4: Move Activation and Install Exact FastAPI CORS

**Branch:** feat/prod-004-api-cors

**Files:**
- Modify: apps/central_api/src/central_api/app.py
- Modify: apps/central_api/src/central_api/routes/oauth.py
- Modify: apps/central_api/tests/test_oauth_activate_confirm.py
- Create: apps/central_api/tests/test_production_cors.py
- Modify: apps/central_api/tests/test_app.py

**Interfaces:**
- POST /api/v1/activate/confirm is the only activation route.
- CORS exact origin/method/header contract and allow_credentials=false applies only to the production AWS profile.

- [ ] **Step 1: Write route absence/presence tests**

Assert /activate/confirm not in OpenAPI, /api/v1/activate/confirm present and bearer+tenant authorization behavior unchanged.

- [ ] **Step 2: Write complete preflight matrix**

Allowed: origin cnesdata, methods GET/POST, headers Authorization/Content-Type/X-Tenant-Id. Denied: another origin, PUT/PATCH/DELETE, cookie credentials, wildcard and any additional header.

- [ ] **Step 3: Implement versioned router mount**

Keep route function/service unchanged; change only prefix ownership. Do not create a redirect compatibility route.

- [ ] **Step 4: Install CORSMiddleware as sole authority**

No Nginx header or OPTIONS short-circuit is application code. Error responses still follow the exact origin policy.

- [ ] **Step 5: Run and commit**

    uv run pytest -q apps/central_api/tests/test_oauth_activate_confirm.py apps/central_api/tests/test_production_cors.py apps/central_api/tests/test_app.py
    git add apps/central_api
    git commit -m "feat(api): version activation and enforce exact cors"

### Task 5: Replace Signed-Serving Redirect with a 200 Envelope

**Branch:** feat/prod-005-serving-envelope

**Files:**
- Modify: apps/central_api/src/central_api/serving/aws_signed.py
- Modify: apps/central_api/src/central_api/routes/serving.py
- Modify: apps/central_api/tests/serving/test_aws_signed.py
- Create: apps/central_api/tests/routes/test_production_serving.py
- Create: apps/web_dashboard/src/api/serving.ts
- Create: apps/web_dashboard/tests/unit/api/serving.test.ts
- Modify: tests/integration/aws/test_signed_serving.py

**Interfaces:**
- ServingResponse(url: SecretStr-like redacted value, version_id: str, expires_in: Literal[300]).
- First response status 200 and Cache-Control private, no-store.
- Second fetch is direct S3 credentials=omit with no headers.

- [ ] **Step 1: Write API response-schema tests**

Assert exact keys only, status/cache header, 300 TTL, prior authorization/stat and denial of raw/normalized/reconciliation/tmp/audit prefixes.

- [ ] **Step 2: Write logging/redaction tests**

Exercise success/error and assert signed query/key never appears in caplog, tracing attributes or exception repr.

- [ ] **Step 3: Implement the 200 envelope**

Preserve canonical LocalServingAccess authorization. Generate GET only for the single authorized serving key/version; attach ResponseCacheControl private,no-store where supported.

- [ ] **Step 4: Implement the browser second fetch**

Do not reuse apiRequest. Build fetch(url, {method:"GET", credentials:"omit", redirect:"error", headers:{}}). Reject non-HTTPS or non-S3 signed URL origin patterns configured by the runtime contract.

- [ ] **Step 5: Run backend/frontend/integration tests**

    uv run pytest -q apps/central_api/tests/serving apps/central_api/tests/routes/test_production_serving.py tests/integration/aws/test_signed_serving.py
    cd apps/web_dashboard && bun run test --run tests/unit/api/serving.test.ts

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
- Production settings contain exact tuple public_subnet_ids and security_group_id, max_concurrency=1.
- validate_state_machine compares the generated ASL ECS Parameters against those exact values and an inspected zero-ingress network contract.

- [ ] **Step 1: Write the acceptance/rejection matrix**

Accept only ENABLED+exact subnets+exact SG+FARGATE+1+no NAT/ALB. Reject DISABLED, missing/extra/reordered normalized subnet set, SG drift, ingress, EC2 launch, concurrency >1 and forbidden resource markers.

- [ ] **Step 2: Preserve emulator/generic fixture behavior explicitly**

Use a profile/network policy argument; do not weaken the production validator to accept both modes silently.

- [ ] **Step 3: Implement exact comparison**

Normalize subnet tuple deterministically, but do not accept a superset. Validate Inline Map remains non-distributed and no service/desired count is introduced.

- [ ] **Step 4: Run AWS-012/AWS-014 focused suites**

    uv run pytest -q packages/cnes_infra/tests/executor tests/integration/aws/test_step_functions_ecs.py

- [ ] **Step 5: Commit**

    git add packages/cnes_infra tests/integration/aws
    git commit -m "feat(processing): enforce public-ip fargate profile"

### Task 7: Add Typed Environment Fence and Semaphore Operations

**Branch:** feat/prod-007-environment-gate

**Files:**
- Create: packages/cnes_domain/src/cnes_domain/ports/environment_gate.py
- Create: packages/cnes_domain/src/cnes_domain/control_plane/environment_gate.py
- Create: packages/cnes_domain/tests/ports/test_environment_gate.py
- Modify: packages/cnes_domain/src/cnes_domain/ports/control_plane.py
- Modify: packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_adapter.py
- Create: packages/cnes_infra/tests/control_plane/test_environment_gate.py

**Interfaces:**
- observe_environment_gate() is strongly consistent.
- Typed commands: AcquireUnitPermit, BindUnitPermit, RenewUnitPermit, ReleaseUnitPermit, ClosePromotionFence and ReopenPromotionFence.
- Fence and semaphore are separate items; acquire transaction requires open fence.

- [ ] **Step 1: Write pure transition tests**

Cover close-first, acquire/bind/renew/release, only bound canary while fenced, token mismatch, stale generation, TTL non-authority and reopen only matching promotion.

- [ ] **Step 2: Write adapter race tests**

Concurrent acquire winners <=1. A fence close racing acquire either closes before and rejects, or observes the live permit and drain waits; never both closed+new unbound start.

- [ ] **Step 3: Implement immutable entities/commands first**

No AWS imports in cnes_domain. Use aware UTC datetimes and explicit version/fencing tokens.

- [ ] **Step 4: Implement conditional DynamoDB transactions**

Use consistent reads and TransactWriteItems conditions. Expired takeover requires a supplied liveness proof adapter result; never time alone.

- [ ] **Step 5: Run package 100% branch gates and commit**

    uv run pytest packages/cnes_domain/tests/ports/test_environment_gate.py --cov --cov-branch
    uv run pytest -q packages/cnes_infra/tests/control_plane/test_environment_gate.py
    git add packages/cnes_domain packages/cnes_infra
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
- Modify: apps/data_processor/src/data_processor/recovery.py

**Interfaces:**
- consume_execution_attempt(environment, period, now, limit=200) atomically increments before each initial/recovery StartExecution.
- Same monthly item/limit is used by both callers.
- Quota rejection performs no Step Functions call and maps to the documented 429/quota result.

- [ ] **Step 1: Write boundary and race tests**

199->200 succeeds; 200->201 rejects. Initial/recovery races total exactly 200. Failed StartExecution still counts as an attempt. TTL deletion is irrelevant.

- [ ] **Step 2: Add service-level no-call tests**

Fake executor records zero calls when quota rejects. The 100 task-hour metric is absent from the decision path.

- [ ] **Step 3: Implement conditional counter and wire both callers**

UTC month key; atomic ADD/condition; no read-then-write race.

- [ ] **Step 4: Run and commit**

    uv run pytest -q packages/cnes_domain/tests/control_plane/test_execution_quota.py packages/cnes_infra/tests/control_plane/test_execution_quota.py apps/central_api/tests apps/data_processor/tests
    git add packages/cnes_domain packages/cnes_infra apps/central_api apps/data_processor
    git commit -m "feat(processing): cap monthly execution attempts atomically"

### Task 9: Extend Outbox Delivery with Cursor Paging

**Branch:** feat/prod-009-outbox-cursor

**Files:**
- Modify: packages/cnes_domain/src/cnes_domain/ports/control_plane.py
- Modify: packages/cnes_domain/src/cnes_domain/outbox_dispatcher.py
- Modify: packages/cnes_domain/tests/test_outbox_dispatcher.py
- Modify: packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_adapter.py
- Create: packages/cnes_infra/tests/control_plane/test_outbox_cursor.py

**Interfaces:**
- Replaces pending_outbox(limit) with read_outbox_page(cursor, limit) and advance_outbox_cursor(expected, next).
- A pass advances after evaluating each page, wraps, and retries poison without starving later pages.
- Delivery marker changes only after S3 COMPLIANCE append succeeds.

- [ ] **Step 1: Write 100-poison plus second-page test**

First page remains pending/retrying; second page still delivers in the same/bounded subsequent pass; cursor wraps and replay is idempotent.

- [ ] **Step 2: Write CAS conflict and crash tests**

Concurrent cursor advancement has one winner. Crash after S3 append/before marker replays the same object idempotently and then marks.

- [ ] **Step 3: Change the port and all fake implementations serially**

This is a shared interface hotspot; no parallel task edits control_plane.py.

- [ ] **Step 4: Implement DynamoDB Query/GetItem/UpdateItem only**

No Scan, DeleteItem or PutItem for the dispatch role path. Strongly revalidate each event.

- [ ] **Step 5: Run full contract harness and commit**

    uv run pytest -q packages/cnes_domain/tests/test_outbox_dispatcher.py packages/cnes_infra/tests/control_plane/test_outbox_cursor.py packages/cnes_infra/tests/contracts
    git add packages/cnes_domain packages/cnes_infra
    git commit -m "feat(audit): page outbox without poison starvation"

### Task 10: Add Separate Recovery and Audit-Dispatch Entrypoints

**Branch:** feat/prod-010-processor-modes

**Files:**
- Modify: apps/data_processor/src/data_processor/aws_entrypoint.py
- Modify: apps/data_processor/src/data_processor/recovery.py
- Create: apps/data_processor/src/data_processor/audit_dispatch.py
- Create: apps/data_processor/src/data_processor/deadline.py
- Modify: apps/data_processor/tests/test_aws_entrypoint.py
- Create: apps/data_processor/tests/test_recovery_mode.py
- Create: apps/data_processor/tests/test_audit_dispatch_mode.py
- Create: apps/data_processor/tests/test_deadline.py

**Interfaces:**
- Allowed modes: run-unit, recover-once, dispatch-outbox-once.
- recover-once builds coordinator only, batch 100, lease/deadline 3600, no unit env or audit sink.
- dispatch-outbox-once builds control plane+audit sink+UTC clock, limit 100, deadline 60, no unit env.

- [ ] **Step 1: Write mode/env isolation tests**

Each mode rejects another mode's required/forbidden variables and dependency construction. Unknown mode exits terminal configuration code.

- [ ] **Step 2: Write deadline behavior**

PID 1 deadline cancels work, logs bounded event, exits nonzero and never continues past lease. Audit deadline is 60 seconds.

- [ ] **Step 3: Implement small composition factories**

Reuse canonical AwsRuntime client/adapters. Do not instantiate API runtime, normal unit registry in recover, or audit sink in recover.

- [ ] **Step 4: Test overlapping recovery passes**

No global recovery lock, but environment semaphore ensures at most one unit task; losing recoveries do not StartExecution and same-run dispatch CAS remains stable.

- [ ] **Step 5: Run and commit**

    uv run pytest -q apps/data_processor/tests/test_aws_entrypoint.py apps/data_processor/tests/test_recovery_mode.py apps/data_processor/tests/test_audit_dispatch_mode.py apps/data_processor/tests/test_deadline.py
    git add apps/data_processor
    git commit -m "feat(processor): separate recovery and audit passes"

### Task 11: Add the Production API Container Credential Contract

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
- AWS_PROFILE=cnesdata-production and AWS_CONFIG_FILE point to a read-only config whose credential_process calls aws_signing_helper credential-process --session-duration 3600.
- Container sees only required helper/config/leaf key material, key readable by fixed UID; no shared static credentials.
- API binds one loopback port and no CnesData processor runs on the VPS.

- [ ] **Step 1: Write static-image/Compose tests**

Require non-root/read-only/cap-drop/no-new-privileges/limits/health. Reject docker.sock, host network, public port, LimnoPulse path/network and AWS_ACCESS_KEY_ID/SECRET/SESSION.

- [ ] **Step 2: Write botocore advisory-refresh test**

First AWS call invokes helper, calls before advisory window do not, first call inside 15-minute window refreshes once; helper failure fails closed. Use fake clock/process provider, no real certificate.

- [ ] **Step 3: Implement credential_process config**

Exact helper path, certificate/private-key/trust-anchor/profile/role ARNs from mounted public/root-owned files; session duration 3600. No shell interpolation.

- [ ] **Step 4: Build and smoke**

    docker build --tag cnesdata-api:prod-test apps/central_api
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

Dependency gate, dashboard absolute client, OIDC resource, route/CORS, 200 serving, AWS-012 override, fence/semaphore, quota, cursor outbox, processor modes and credential process.

- [ ] **Step 2: Add negative source checks**

Reject relative production /api, origin-level activation, API log formatting of url, 307 serving response, AssignPublicIp DISABLED, NAT/ALB, static AWS key fields, Scan in new adapters and task-hour start gate.

- [ ] **Step 3: Extend existing hosted-runner jobs**

Run focused suites and emulator-backed integration with unconditional teardown. Keep root permissions contents:read and no secrets/id-token.

- [ ] **Step 4: Run complete quality gates**

    uv run ruff check .
    uv run pytest -m "not integration and not postgres and not bigquery and not e2e and not stress and not soak and not spike and not windows_only" -q
    uv run pytest -q tests/production
    cd apps/web_dashboard && bun run lint && bun run typecheck && bun run test --run && bun run build
    git diff --check

- [ ] **Step 5: Commit**

    git add tests/production docs/runbooks/runtime-acceptance.md .github/workflows
    git commit -m "test(prod): gate production runtime amendments"

## Execution Order

Task 1 is a hard gate. Tasks 2 and 6 may start after it. Task 3 waits for Task 2; Tasks 4 and 5 wait for Tasks 2-3. Task 7 is the serial control-plane foundation. Task 8 waits for Task 7. Task 9 is another serial control-plane hotspot and starts after Task 8 merges. Task 10 waits for Tasks 8-9. Task 11 can proceed after Tasks 1 and 3. Task 12 waits for all prior tasks.

## Plan Self-Review Record

- Existing-plan boundary: AWS-010…014 files are extended in place; no duplicate adapter/composition exists.
- Browser contract: exact API origin, versioned activation, bearer/tenant scoping and header-free S3 fetch align across dashboard/API tests.
- Processing contract: public-IP Fargate is explicit and does not loosen the generic test profile.
- Correctness: fence, semaphore, counter and outbox cursor are typed domain operations with conditional adapters.
- Operations: recovery and audit share the image but not configuration/composition/deadline.
- Credentials: production uses credential_process; no static keys or shared credential file assumption.
- Completeness scan: no unresolved marker, skipped dependency or undefined mode.

## Execution Handoff

Do not dispatch any implementation task until the dependency gate can pass on develop. After this plan is merged and green, the production infrastructure plan may consume these exact settings, IAM and task-mode contracts.
