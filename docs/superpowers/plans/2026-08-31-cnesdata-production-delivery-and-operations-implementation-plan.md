# CnesData Production Delivery and Operations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create credential-free CI, immutable candidate/activation evidence, manual OpenTofu plan/apply, phased processor routing, health-guarded API/static promotion, canary/audit verification, rollback/drain and production acceptance for CnesData.

**Architecture:** A candidate manifest binds one green develop commit to tested artifacts but contains no ECS revisions. Phase A promotes the processor bytes to ECR, registers immutable unit/recovery/audit revisions and dual-authorizes them without routing changes; it then signs an activation manifest. Phase B switches only unit routing while the environment fence is closed. After unit/API/frontend canaries, separate Phase C applies switch recovery then audit. Only complete accepted routing reopens the fence.

**Tech Stack:** GitHub Actions hosted runners, GHCR, ECR, OpenTofu binary plans, GitHub OIDC through personal-infra-live reusable gates, Python release/deployment tooling, S3/CloudFront, Nginx shared dispatcher, ECS/Fargate, Step Functions, EventBridge Scheduler, Cognito PKCE, Playwright, pytest, OPA/Conftest, Syft, Trivy.

**Spec:** docs/superpowers/specs/2026-08-29-cnesdata-production-operations-design.md with resource contracts from docs/superpowers/specs/2026-08-29-cnesdata-production-deployment-design.md

## Global Constraints

- Start only after runtime amendments and infrastructure Tasks 1-12 are merged and green on develop.
- Infrastructure may be planned after CND-020…025; application promotion additionally requires AWS-010…014 and the serial AWS gate.
- Untrusted pull requests use GitHub-hosted runners with no AWS, Cloudflare, SSH, id-token or production secret.
- Candidate release is one green develop SHA. Mutable tags are never authority.
- Candidate manifest contains processor GHCR digest and expected ECR repository but no ECR promoted digest or ECS revision ARN.
- Activation manifest is signed only after Phase A and binds candidate SHA-256, release/source IDs, verified ECR_URI@sha256, exact unit/recovery/audit revisions and Phase A evidence.
- Plan/apply are separate workflow_dispatch operations; apply verifies one unexpired binary plan and never replans.
- Promotion is manual. Merge never deploys.
- Phase A registers revisions once and dual-authorizes old/new RunTask without routing changes.
- Fence closes atomically before Phase B, then drain verifies semaphore idle/no dispatch. Only bound canary is admitted.
- Phase B changes only Step Functions unit TaskDefinition. Both Schedulers stay prior.
- Phase C has two separate reviewed applies: recovery first, audit second. Reopen only after both accepted.
- Partial apply or mixed routing remains fenced until a new reviewed convergence plan completes.
- Successful fence window through both Phase C acceptances is capped at 30 minutes; routine cutoff restores prior routes while remaining safe.
- Old revisions/grants persist until old Standard/scheduled work drains and rollback retention ends.
- Static release uploads immutable unit first and root entrypoint last. Rollback restores one prior entrypoint version.
- CloudFront Free exact distribution/WAF subscription is ACTIVE before DNS; no paid fallback.
- Logs/evidence exclude signed URLs, object keys, synthetic bodies, credentials, emails and tenant/run/user identifiers.
- Product cost remains <=USD 8 and aggregate automation freeze is USD 15.

## File Map

| Path | Responsibility |
|---|---|
| release/candidate.schema.json | pre-infrastructure immutable artifact binding |
| release/activation.schema.json | post-Phase-A exact routing binding |
| release/*.py | canonical build/verify/sign tooling |
| .github/workflows/verify-production.yml | credential-free PR/main gates |
| .github/workflows/release-candidate.yml | main/develop candidate creation |
| .github/workflows/infra-plan.yml | manual exact plan |
| .github/workflows/infra-apply.yml | manual exact binary apply |
| .github/workflows/promote-production.yml | orchestrated phased promotion |
| scripts/promotion/** | pure phase verification/transition helpers |
| tests/production/delivery/** | manifest/workflow/phase/failure contracts |
| docs/runbooks/** | operator evidence, rollback, recovery and acceptance |

---

### Task 1: Extend Credential-Free Production CI

**Branch:** ci/prod-delivery-001-validation

**Files:**
- Create: .github/workflows/verify-production.yml
- Modify: .github/workflows/python-quality.yml
- Modify: .github/workflows/web-dashboard.yml
- Create: tests/production/delivery/test_ci_workflows.py

**Interfaces:**
- Required jobs: CND/AWS, dashboard, API image, processor image, OCI/SBOM/scan, OpenTofu/policy/cost and manifest/secret scan.
- Root permissions contents:read; no self-hosted runner or credential.

- [ ] **Step 1: Write workflow contract tests**

Require full-SHA pinned actions, hosted runners, locked dependencies, exact dashboard production env, emulator teardown and no secrets/id-token/packages-write on pull_request.

- [ ] **Step 2: Add dashboard route/build gates**

Production build asserts exact VITE_API_BASE_URL, one API client, versioned activation, bearer origin, tenant header scope and no relative /api.

- [ ] **Step 3: Add OCI and OpenTofu gates**

Build without push, generate SBOM/checksum, scan reviewed severity, validate Compose, tofu/provider lock, OPA cost/secret/ownership.

- [ ] **Step 4: Run local equivalents and actionlint**

    uv run pytest -q tests/production/delivery/test_ci_workflows.py
    actionlint .github/workflows
    uv run ruff check .
    git diff --check

- [ ] **Step 5: Commit**

    git add .github/workflows tests/production/delivery/test_ci_workflows.py
    git commit -m "ci(prod): validate cnesdata production artifacts"

### Task 2: Define the Candidate Release Manifest

**Branch:** feat/prod-delivery-002-candidate-manifest

**Files:**
- Create: release/candidate.schema.json
- Create: release/build_candidate.py
- Create: release/verify_candidate.py
- Create: tests/production/delivery/test_candidate_manifest.py
- Create: .github/workflows/release-candidate.yml

**Interfaces:**
- Candidate records source SHA/run IDs, release ID, API GHCR digest, processor GHCR digest+expected ECR repo, dashboard checksum, Compose/config checksum, OpenTofu/provider lock and SBOM checksums.
- No task-definition ARN or promoted ECR digest.

- [ ] **Step 1: Write schema and forbidden-field tests**

Reject taskDefinitionArn, ECR_URI@digest, mutable tag, wrong source/run, missing SBOM/checksum, expired manifest, secret-like keys and noncanonical JSON.

- [ ] **Step 2: Implement deterministic builder/verifier**

Canonical UTF-8 sorted JSON and SHA-256 sidecar. Every artifact is produced by the same successful run/SHA.

- [ ] **Step 3: Implement candidate workflow**

Trigger manually from a green develop commit contained in develop; verify required checks; build/push API+processor GHCR once; upload bounded artifact. packages:write only for its own images.

- [ ] **Step 4: Run tests/actionlint and commit**

    uv run pytest -q tests/production/delivery/test_candidate_manifest.py
    actionlint .github/workflows/release-candidate.yml
    git add release .github/workflows/release-candidate.yml tests/production/delivery/test_candidate_manifest.py
    git commit -m "feat(release): create revision-free candidate manifest"

### Task 3: Add Exact Product Plan and Apply Workflows

**Branch:** ci/prod-delivery-003-plan-apply

**Files:**
- Create: release/infra-plan.schema.json
- Create: .github/workflows/infra-plan.yml
- Create: .github/workflows/infra-apply.yml
- Create: scripts/promotion/verify_plan_artifact.py
- Create: tests/production/delivery/test_plan_apply_workflows.py

**Interfaces:**
- Plan artifact contains binary/redacted plan, source/dependency/provider/backend/policy/cost/drift/expiry data and aggregate digest.
- Apply inputs plan_run_id, artifact_id, artifact_digest and expected source SHA.
- Calls exact pinned CnesData plan/deploy reusable gates from personal-infra-live.

- [ ] **Step 1: Write wrong/mixed/expired artifact tests**

All fail before OIDC. Reject cost >8/product or >15 aggregate, protected replacement/deletion, secret in plan, backend mismatch and insufficient validity margin.

- [ ] **Step 2: Write workflow-order/permission tests**

Validation precedes reusable gate and id-token. No apply-on-merge, replan, destroy or -lock=false.

- [ ] **Step 3: Implement plan caller**

Use read-only product plan role, native .tflock and separate coordination lease through reusable workflow. Upload exact artifact.

- [ ] **Step 4: Implement apply caller**

Verify exact artifact/run/SHA/ref/digest/expiry, preflight drift, apply exact binary with native locking, verify state/drift, record redacted evidence.

- [ ] **Step 5: Validate and commit**

    uv run pytest -q tests/production/delivery/test_plan_apply_workflows.py
    actionlint .github/workflows/infra-plan.yml .github/workflows/infra-apply.yml
    git add release .github/workflows scripts/promotion tests/production/delivery
    git commit -m "ci(infra): join exact cnesdata plan and apply"

### Task 4: Implement Byte-Identical Processor Promotion and Phase A

**Branch:** feat/prod-delivery-004-phase-a

**Files:**
- Create: scripts/promotion/promote_ecr.py
- Create: scripts/promotion/phase_a.py
- Create: scripts/promotion/verify_task_revisions.py
- Create: release/phase-a-evidence.schema.json
- Create: tests/production/delivery/test_phase_a.py

**Interfaces:**
- Copies processor GHCR bytes to expected private ECR and verifies exact image digest.
- Phase A exact plan/apply registers unit/recovery/audit revisions once and dual-authorizes old/new ecs:RunTask without changing state-machine/Schedulers.
- Evidence records revision ARNs, digest, IAM propagation/readback and clean-state hash.

- [ ] **Step 1: Write byte identity and repository tests**

Reject wrong repo, architecture/media manifest, digest drift, mutable destination and unreviewed vulnerability severity.

- [ ] **Step 2: Write phase state-delta tests**

Permitted addresses/fields are new task revisions and dual grants only. Any state-machine/Scheduler target change fails.

- [ ] **Step 3: Implement idempotent registration**

If exact family/revision definition already exists from this activation, read/verify it; never register another revision on retry.

- [ ] **Step 4: Wait/revalidate IAM propagation**

Prove old and candidate RunTask authorizations with exact roles/network before emitting evidence.

- [ ] **Step 5: Run and commit**

    uv run pytest -q tests/production/delivery/test_phase_a.py
    git add scripts/promotion release tests/production/delivery/test_phase_a.py
    git commit -m "feat(promotion): register and dual-authorize phase-a tasks"

### Task 5: Build and Sign the Activation Manifest

**Branch:** feat/prod-delivery-005-activation-manifest

**Files:**
- Create: release/activation.schema.json
- Create: release/build_activation.py
- Create: release/verify_activation.py
- Create: tests/production/delivery/test_activation_manifest.py

**Interfaces:**
- Binds candidate manifest SHA-256, release ID, source SHA, ECR_URI@sha256, unit/recovery/audit revision ARNs and Phase A evidence digest.
- Signed through the approved KMS/evidence gate; contains no secret.

- [ ] **Step 1: Write artifact-mixing tests**

Reject any candidate/revision/digest/evidence from another release/run, re-registered revision, mutable ARN field, wrong state hash and missing signature context.

- [ ] **Step 2: Implement canonical payload and detached signature**

Signature context binds project=cnesdata, environment=prod, schema and release ID. Verifier checks expiry and all referenced evidence before use.

- [ ] **Step 3: Add exact OpenTofu input renderer**

Renderer produces only approved variables and exact revision ARNs; no ignore_changes or out-of-band CLI target mutation.

- [ ] **Step 4: Run and commit**

    uv run pytest -q tests/production/delivery/test_activation_manifest.py
    git add release tests/production/delivery/test_activation_manifest.py
    git commit -m "feat(promotion): bind phase-a activation evidence"

### Task 6: Implement Canary Seed, Visibility and Retention Verification

**Branch:** feat/prod-delivery-006-canary

**Files:**
- Create: scripts/promotion/seed_canary.py
- Create: scripts/promotion/run_canary.py
- Create: scripts/promotion/verify_canary.py
- Create: tests/production/delivery/test_canary.py

**Interfaces:**
- Seeder PutItem exact release-bound pending event.
- Verifier Query exact GSI up to 60 seconds, then conditioned GetItem and exact audit GetObject/GetObjectRetention.
- Runs candidate with canary-only role/resource overrides and exact PassRole.

- [ ] **Step 1: Write IAM/identity denial tests**

Other release/table/index/bucket/object/role is rejected. Seeder cannot read/update. Verifier cannot Scan, list or mutate domain delivery.

- [ ] **Step 2: Write visibility timeout test**

Bounded poll with jitter <=60 seconds; timeout fails closed and no promotion proceeds.

- [ ] **Step 3: Implement canary run**

Pass only canary task/execution roles and resource names. Require COMPLIANCE object retention plus delivered marker matching event/release.

- [ ] **Step 4: Test replay**

Same release is idempotent; mixed/old event cannot satisfy current acceptance.

- [ ] **Step 5: Commit**

    uv run pytest -q tests/production/delivery/test_canary.py
    git add scripts/promotion tests/production/delivery/test_canary.py
    git commit -m "feat(promotion): verify release-bound audit canary"

### Task 7: Implement Fence Close, Drain and Phase B

**Branch:** feat/prod-delivery-007-phase-b

**Files:**
- Create: scripts/promotion/environment_gate.py
- Create: scripts/promotion/phase_b.py
- Create: scripts/promotion/verify_routes.py
- Create: tests/production/delivery/test_phase_b.py

**Interfaces:**
- Operator assumes release-tagged fence role, closes exact fence item and cannot write semaphore.
- Drain uses strong reads until semaphore idle/no dispatch, bounded by promotion deadline.
- Phase B reviewed apply switches only state-machine TaskDefinition to activation unit revision; Schedulers remain prior.

- [ ] **Step 1: Write close-first/race tests**

New initial/recovery acquires fail after close; existing permit drains. Only bound canary can acquire. Direct semaphore update denied.

- [ ] **Step 2: Write plan-delta and route readback tests**

Reject any Scheduler change, new revision registration, IAM grant removal, ignore_changes or out-of-band update. Verify clean state/no drift after apply.

- [ ] **Step 3: Implement bounded drain**

If not drained before safety margin, abort without Phase B and retain/restore prior open state only when no partial mutation occurred.

- [ ] **Step 4: Implement partial-apply fail-closed behavior**

Any uncertain/mixed route leaves fence closed and emits diagnostic evidence for a new reviewed convergence plan.

- [ ] **Step 5: Run and commit**

    uv run pytest -q tests/production/delivery/test_phase_b.py
    git add scripts/promotion tests/production/delivery/test_phase_b.py
    git commit -m "feat(promotion): fence and switch unit routing"

### Task 8: Implement API Candidate and Atomic Frontend Promotion

**Branch:** feat/prod-delivery-008-api-static

**Files:**
- Create: scripts/promotion/build_host_archive.py
- Create: scripts/promotion/promote_api.py
- Create: scripts/promotion/publish_dashboard.py
- Create: scripts/promotion/rollback_api_static.py
- Create: tests/production/delivery/test_api_static_promotion.py
- Create: .github/workflows/promote-production.yml

**Interfaces:**
- Product workflow verifies exact candidate artifact with its ephemeral GITHUB_TOKEN, then invokes immutable personal-infra-live CnesData host gate.
- API candidate starts on alternate loopback, passes local auth/AWS readiness, switches Nginx, then passes tunneled production hostname smoke.
- Dashboard immutable unit uploads/verifies before root entrypoint last.

- [ ] **Step 1: Write three-frame host-archive tests**

Envelope/archive/GITHUB_TOKEN bind exact source/run/artifact/digest, GHCR API digest, Compose/config checksums, platform-plan evidence and expiry. Archive has no secret.

- [ ] **Step 2: Write candidate failure injection**

Pull/start/readiness/auth/AWS/local smoke/Nginx validate/switch/tunnel smoke failures preserve or restore prior API; global lease is released.

- [ ] **Step 3: Write static atomicity tests**

Mixed release references, overwrite of immutable prefix and failed asset verification leave root unchanged. Successful rollback restores prior index version and invalidates entrypoints only.

- [ ] **Step 4: Implement workflow up through unit canary boundary**

Workflow coordinates Tasks 4-7, API/static steps and unit canary while fence remains closed. It does not reopen or switch schedulers yet.

- [ ] **Step 5: Validate and commit**

    uv run pytest -q tests/production/delivery/test_api_static_promotion.py
    actionlint .github/workflows/promote-production.yml
    git add scripts/promotion .github/workflows/promote-production.yml tests/production/delivery
    git commit -m "feat(promotion): switch api and static release atomically"

### Task 9: Implement Separate Phase C Recovery and Audit Acceptance

**Branch:** feat/prod-delivery-009-phase-c

**Files:**
- Create: scripts/promotion/phase_c_recovery.py
- Create: scripts/promotion/phase_c_audit.py
- Create: scripts/promotion/accept_routing.py
- Create: tests/production/delivery/test_phase_c.py
- Modify: .github/workflows/promote-production.yml

**Interfaces:**
- First reviewed apply changes only recovery Scheduler revision.
- After recovery canary, second reviewed apply changes only audit Scheduler revision.
- Reopen fence only when unit+recovery+audit exact activation revisions are accepted and state is clean.

- [ ] **Step 1: Write phase-specific plan-delta tests**

Recovery plan cannot change unit/audit; audit plan cannot change unit/recovery. Both consume/verify the same activation manifest.

- [ ] **Step 2: Add recovery canary tests**

Force one bounded recovery, verify exact mode/network/role/deadline and failure alarm. Overlap/duplicate passes never exceed one unit task.

- [ ] **Step 3: Add audit canary tests**

Use release-bound pending event, exact cursor behavior and COMPLIANCE proof. Audit remains functional under a simulated cost freeze.

- [ ] **Step 4: Implement route acceptance/reopen**

Strongly read state machine and both Schedulers plus OpenTofu state. Mixed/dirty state cannot reopen.

- [ ] **Step 5: Enforce 30-minute routine window**

Watchdog stops new phase activity before cutoff and initiates prior-route convergence while fence stays closed.

- [ ] **Step 6: Run and commit**

    uv run pytest -q tests/production/delivery/test_phase_c.py
    actionlint .github/workflows/promote-production.yml
    git add scripts/promotion .github/workflows/promote-production.yml tests/production/delivery/test_phase_c.py
    git commit -m "feat(promotion): accept recovery and audit routes separately"

### Task 10: Implement Rollback, Drain and Revision Pruning

**Branch:** feat/prod-delivery-010-rollback-drain

**Files:**
- Create: scripts/promotion/rollback.py
- Create: scripts/promotion/drain.py
- Create: scripts/promotion/prune_revisions.py
- Create: tests/production/delivery/test_rollback_drain.py
- Create: docs/runbooks/rollback.md

**Interfaces:**
- Rollback consumes the prior retained activation manifest and a new reviewed exact OpenTofu plan/apply.
- Active Standard executions keep their started definition unless safe cancellation is explicitly approved.
- Prune only revisions older than current/prior and after active old Standard/recovery drain plus retention.

- [ ] **Step 1: Write active-execution and retention tests**

Current, prior, referenced-by-active and within-retention revisions/grants are protected. Only older unreferenced revisions qualify.

- [ ] **Step 2: Write full-route rollback plan tests**

Restore unit/recovery/audit and grants together via declarative plans; no manual state/Dynamo mutation.

- [ ] **Step 3: Implement API/static immediate restore**

Restore previous Nginx upstream/GHCR digest and dashboard entrypoint while processor convergence proceeds fenced.

- [ ] **Step 4: Implement failure-safe pruning**

Readback twice around decision; any uncertainty keeps resources.

- [ ] **Step 5: Run and commit**

    uv run pytest -q tests/production/delivery/test_rollback_drain.py
    git add scripts/promotion tests/production/delivery/test_rollback_drain.py docs/runbooks/rollback.md
    git commit -m "feat(ops): retain drain and rollback exact revisions"

### Task 11: Add CloudFront Free, Demo Seed and Real-AWS Smoke Tooling

**Branch:** feat/prod-delivery-011-cutover-smoke

**Files:**
- Create: scripts/promotion/reconcile_cloudfront_free.py
- Create: scripts/promotion/seed_demo.py
- Create: scripts/promotion/smoke_production.py
- Create: tests/production/delivery/test_cloudfront_free.py
- Create: tests/production/delivery/test_demo_seed.py
- Create: tests/production/delivery/test_smoke_redaction.py
- Create: docs/runbooks/cloudfront-free.md
- Create: docs/runbooks/demo-seed.md

**Interfaces:**
- Free reconciliation exact distribution+dedicated WAF+FREE at us-east-1 and ACTIVE; no replacement/cancel/paid action.
- Demo seed writes one synthetic tenant through a distinct seed role, cannot write semaphore and conditionally rejects non-demo collisions.
- Smoke output is redacted pass/fail evidence only.

- [ ] **Step 1: Write Free-plan eligibility/refusal tests**

Require account eligibility and phase-one slot availability. Reject any paid tier, ambiguous/conflicting association or inactive result.

- [ ] **Step 2: Write seed-role/idempotency tests**

No GitHub general data-plane write. Exact reviewed seed role; conditional known keys; no bulk delete/Scan; semaphore denied.

- [ ] **Step 3: Implement real-AWS smoke probes**

Cognito standard endpoints+PKCE+audience, membership/cross-tenant denial, CORS, exact Step Functions trust/task, signed-serving metadata->200 envelope->header-free S3 body, Object Lock and Roles Anywhere/CRL evidence.

- [ ] **Step 4: Redact sensitive values**

Never print/store access tokens, SigV4 URLs, object paths, record bodies, emails or IDs. Evidence contains named assertion+timestamp+release hash only.

- [ ] **Step 5: Run fixture tests and commit**

    uv run pytest -q tests/production/delivery/test_cloudfront_free.py tests/production/delivery/test_demo_seed.py tests/production/delivery/test_smoke_redaction.py
    git add scripts/promotion tests/production/delivery docs/runbooks
    git commit -m "feat(acceptance): add exact cutover and smoke tooling"

### Task 12: Build the Final Production Operations Gate

**Branch:** test/prod-delivery-012-operations-acceptance

**Files:**
- Create: tests/production/delivery/test_operations_acceptance.py
- Create: docs/runbooks/production-promotion.md
- Create: docs/runbooks/recovery-drill.md
- Create: docs/runbooks/roles-anywhere-incident.md
- Create: docs/runbooks/cost-freeze.md
- Create: docs/runbooks/quarterly-restore.md

**Interfaces:**
- Produces final preproduction/production checklists and evidence schema.
- Does not deploy automatically.

- [ ] **Step 1: Encode the pre-production matrix**

All CND/AWS gates; real Dynamo/Object Lock; OIDC rotation/revocation/cross-tenant; fence/dispatch replay; source behavior; outbox cursor/poison; browser contract; credential refresh; VPS IAM; external CA/CRL; Step Functions IAM; manifest/Phase A/B/C; canary; drain/prune; recovery/audit modes/deadlines; gate races; task-hour/counter budget; publishing crash IAM; AWS-012 override; CDN/Free/Cognito/rate/cost policy.

- [ ] **Step 2: Encode the production smoke matrix**

Static/TLS/private origin; Tunnel-only API; standard Cognito endpoints/PKCE/audience; exact Roles Anywhere and CRL revocation; phase evidence/routes/fence; recovery/audit failure behavior; hung recovery stop; overlapping recoveries; one synthetic publication/pointer; 200 serving envelope and second S3 fetch; no leakage; previous release restore.

- [ ] **Step 3: Write incident/recovery drills**

Roles Anywhere: disable profile/trust, update CRL, revoke prior sessions/TokenIssueTime Deny, prove Dynamo/S3/Step Functions denial, retain deny for max session+propagation, rotate leaf, re-enable and test.

- [ ] **Step 4: Write cost-freeze and quarterly restore**

Freeze aborts promotion/new cost while audit/read/backup continue. Quarterly recovery reconstructs API and one synthetic pointer/serving object without modifying production.

- [ ] **Step 5: Run every non-mutating repository gate**

    uv run ruff check .
    uv run pytest -q tests/production
    uv run pytest -m "not integration and not postgres and not bigquery and not e2e and not stress and not soak and not spike and not windows_only" -q
    cd apps/web_dashboard && bun run lint && bun run typecheck && bun run test --run && bun run build
    tofu -chdir=infra/opentofu fmt -check -recursive
    tofu -chdir=infra/opentofu init -backend=false -input=false
    tofu -chdir=infra/opentofu validate -no-color
    tofu -chdir=infra/opentofu test
    conftest test tests/fixtures/plans --policy policies
    actionlint .github/workflows
    git diff --check

- [ ] **Step 6: Commit**

    git add tests/production/delivery/test_operations_acceptance.py docs/runbooks
    git commit -m "test(prod): gate cnesdata promotion and recovery"

## Execution Order

Tasks 1-3 are the release/delivery foundation. Task 4 waits for a candidate and infrastructure task definitions. Task 5 waits for Task 4. Task 6 may proceed after Task 5. Task 7 waits for Tasks 5-6. Task 8 waits for Task 7 and the infra-ansible dispatcher. Task 9 waits for Tasks 6-8. Task 10 waits for Task 9. Task 11 may proceed after Tasks 2-3 and joins before final cutover. Task 12 is final.

## Plan Self-Review Record

- Manifest separation: candidate is revision-free; activation exists only after Phase A and binds exact revisions/digest/evidence.
- Declarative routing: Phase A/B/C all use reviewed exact plans; no ignore_changes or CLI out-of-band update.
- Fence safety: close-first, strong drain, bound canary and complete-route reopen are consistent across scripts/tests/workflow.
- Failure safety: partial/mixed routes remain fenced and require reviewed convergence.
- Rollback: prior API/static can restore promptly; processor uses retained manifest and does not mutate data backward.
- Identity: product GITHUB_TOKEN validates its own CI evidence; OIDC/SSH credentials come only through pinned personal-infra-live gates.
- Cost: CloudFront exact FREE, no paid fallback, product <=USD 8 and aggregate freeze at USD 15.
- Completeness scan: no unresolved marker, automatic deployment, ambiguous evidence or destructive shortcut.

## Execution Handoff

Execute with isolated worktrees and review each PR. Completing these tasks creates delivery machinery and runbooks; the first production plan/apply/promotion is a distinct manual operation after LimnoPulse is accepted and all real-world preflight evidence is current.
