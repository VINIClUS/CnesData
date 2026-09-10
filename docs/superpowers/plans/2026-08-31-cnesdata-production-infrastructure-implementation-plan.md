# CnesData Production Infrastructure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Define the cost-bounded CnesData production AWS resources in OpenTofu: private static delivery, Cognito, DynamoDB, versioned/Object-Locked S3, ECR/ECS, Step Functions, scheduled recovery/audit, Athena, least-privilege IAM, alarms and product cost policy.

**Architecture:** One product state owns only CnesData resources and consumes narrow shared outputs for backend/KMS/OIDC/Roles Anywhere/Budget/edge coordination. Processing has no service or load balancer: Step Functions and EventBridge Scheduler run immutable Fargate task revisions in public subnets with ephemeral public IP and zero ingress. Promotion registers and routes exact revisions through reviewed phase plans; resource definitions remain declarative and protected.

**Tech Stack:** OpenTofu 1.8+, AWS provider, AWS us-east-2, ACM/WAF us-east-1, S3/CloudFront/OAC, Cognito, DynamoDB, ECR, ECS Fargate, Step Functions Standard, EventBridge Scheduler, Athena, CloudWatch, IAM, OPA/Conftest, pytest.

**Spec:** docs/superpowers/specs/2026-08-29-cnesdata-production-deployment-design.md and docs/superpowers/specs/2026-08-29-cnesdata-production-operations-design.md

## Global Constraints

- Start after the runtime-amendments plan is merged and green; infrastructure planning also requires CND-020…025 integrated.
- Runtime promotion remains blocked until AWS-010…014 and serial acceptance are green.
- State key is cnesdata/prod/opentofu.tfstate in the shared private S3 backend with native locking.
- Default provider is us-east-2. Alias global_edge in us-east-1 owns only CloudFront ACM and CLOUDFRONT-scope WAF.
- Shared state owns KMS/SSM hierarchy, GitHub OIDC role classes, Roles Anywhere trust anchor/profile/role/CRL, Budget action, backup/evidence storage, Cloudflare and VPS. Product state consumes reviewed outputs and never imports shared state with broad read access.
- Web, data and audit buckets are distinct. Web/data are versioned; audit enables Object Lock at creation and COMPLIANCE 365 days.
- No bucket is public. S3 Block Public Access and bucket-owner-enforced ownership are mandatory.
- DynamoDB is one PAY_PER_REQUEST control-plane table, PITR 35 days, deletion protection, TTL-GC only and max 100 reads/25 writes on table/GSIs.
- ECS has no service, desired count, autoscaling, Spot, NAT, ALB or inbound rule. Unit/recovery/audit use 0.25 vCPU, 0.5-1 GiB, included 20 GiB, public IP and exact zero-ingress groups.
- Unit semaphore maximum is one. Recovery invocations may overlap but each is bounded. 100 combined task-hours is an alarm/target only.
- Step Functions Standard and Inline Map only; payload logging is disabled. Maximum 200 start attempts is enforced by runtime counter.
- Processor ECR retains at most five production rollback digests/30 days, untagged seven days and current/prior protections.
- Cognito creates resource server identifier https://api.cnesdata.vinisantana.com and scope api.access; public PKCE SPA client; collision-safe managed prefix domain; no SMS/social/M2M/custom domain.
- Athena is operator-only with 5 GB/query and 100 GB/month cutoff; never an API dependency.
- Product envelope is USD 8 and aggregate base/max evidence remains below USD 15 freeze.
- OpenTofu has no provisioner/local-exec and no destroy workflow. Paid CloudFront subscription reconciliation is outside state and exact FREE only.

## Target Layout

    infra/opentofu/
      modules/
        control-plane/
        data-buckets/
        static-web/
        cognito/
        network/
        processor-registry/
        processing/
        schedulers/
        athena/
        runtime-iam/
        observability/
      env/prod/
      tests/
    policies/
    tests/production/infra/
    docs/runbooks/

---

### Task 1: Create Product Composition and Ownership Contracts

**Branch:** feat/prod-infra-001-composition

**Files:**
- Create: infra/opentofu/versions.tf
- Create: infra/opentofu/providers.tf
- Create: infra/opentofu/env/prod/main.tf
- Create: infra/opentofu/env/prod/variables.tf
- Create: infra/opentofu/env/prod/outputs.tf
- Create: infra/opentofu/env/prod/backend.hcl.example
- Create: infra/opentofu/env/prod/shared-outputs.schema.json
- Create: tests/production/infra/test_composition.py

**Interfaces:**
- Inputs from shared: account_id, shared KMS ARN, Roles Anywhere API role/profile/trust-anchor/CRL evidence, GitHub role ARNs, budget freeze policy/action contract and state/evidence bucket contract.
- Outputs to edge: CloudFront domain/distribution ID, ACM DNS validation records and API loopback contract only.

- [ ] **Step 1: Write ownership tests**

Reject aws_kms_key, aws_ssm_parameter, aws_rolesanywhere_*, aws_iam_openid_connect_provider, aws_budgets_*, cloudflare_* and Hostinger resources from the product graph.

- [ ] **Step 2: Write provider-region tests**

Every resource must use default us-east-2 except aws_acm_certificate and CLOUDFRONT WAF through aws.global_edge.

- [ ] **Step 3: Implement pinned provider/backend contract**

Use use_lockfile=true and no DynamoDB lock table. Public backend example contains placeholders only.

- [ ] **Step 4: Add required tags/local naming**

Project=cnesdata, Environment=prod, ManagedBy=opentofu, Owner=vinisantana.

- [ ] **Step 5: Validate and commit**

    tofu -chdir=infra/opentofu init -backend=false -input=false
    tofu -chdir=infra/opentofu fmt -check -recursive
    tofu -chdir=infra/opentofu validate -no-color
    uv run pytest -q tests/production/infra/test_composition.py
    git add infra/opentofu tests/production/infra
    git commit -m "feat(infra): define cnesdata production ownership"

### Task 2: Provision the DynamoDB Control Plane

**Branch:** feat/prod-infra-002-control-plane

**Files:**
- Create: infra/opentofu/modules/control-plane/main.tf
- Create: infra/opentofu/modules/control-plane/variables.tf
- Create: infra/opentofu/modules/control-plane/outputs.tf
- Create: infra/opentofu/modules/control-plane/checks.tf
- Modify: infra/opentofu/env/prod/main.tf
- Create: tests/production/infra/test_control_plane.py

**Interfaces:**
- Produces table ARN/name and exact index ARNs used by runtime policies.
- Schema/index definitions are generated from the merged DynamoDB adapter access-pattern contract; no convenience GSI is invented.

- [ ] **Step 1: Write plan tests for exact key/index inventory**

Compare to the merged adapter constants. Require PAY_PER_REQUEST, PITR 35, TTL, deletion protection, encryption, table/GSI max throughput and no global replicas/Contributor Insights/autoscaling.

- [ ] **Step 2: Add lifecycle protection tests**

prevent_destroy and replacement policy gate apply to the table; changing key schema fails policy.

- [ ] **Step 3: Implement module and alarms outputs**

Use AWS-owned encryption unless the approved shared KMS policy is proven simpler/cost-neutral; document the selected choice in the PR.

- [ ] **Step 4: Validate and commit**

    uv run pytest -q tests/production/infra/test_control_plane.py packages/cnes_infra/tests/control_plane
    tofu -chdir=infra/opentofu validate -no-color
    git add infra/opentofu/modules/control-plane infra/opentofu/env/prod tests/production/infra
    git commit -m "feat(infra): provision bounded control plane"

### Task 3: Provision Data and Object-Locked Audit Buckets

**Branch:** feat/prod-infra-003-data-buckets

**Files:**
- Create: infra/opentofu/modules/data-buckets/main.tf
- Create: infra/opentofu/modules/data-buckets/policies.tf
- Create: infra/opentofu/modules/data-buckets/variables.tf
- Create: infra/opentofu/modules/data-buckets/outputs.tf
- Modify: infra/opentofu/env/prod/main.tf
- Create: tests/production/infra/test_data_buckets.py

**Interfaces:**
- Data prefixes: raw, normalized, reconciliation, serving, tmp.
- Audit prefix: audit with Object Lock COMPLIANCE 365.
- Data CORS exact origin cnesdata, methods GET/HEAD, allowed headers empty, MaxAge 300.

- [ ] **Step 1: Write bucket isolation/public-access tests**

Require distinct buckets, account/bucket public block, TLS, owner-enforced ownership, versioning and no website/ACL.

- [ ] **Step 2: Write lifecycle and immutability tests**

Abort multipart; tmp expires after bounded recovery; published history not deployment-deleted. Audit Object Lock enabled on creation with prevent_destroy and no bypass/delete policy.

- [ ] **Step 3: Write exact S3 CORS tests**

No wildcard, credentials, custom header, PUT/POST or other origin.

- [ ] **Step 4: Implement module and prefix outputs**

Expose exact bucket/prefix ARNs needed by IAM, not broad bucket data sources.

- [ ] **Step 5: Validate and commit**

    uv run pytest -q tests/production/infra/test_data_buckets.py
    tofu -chdir=infra/opentofu validate -no-color
    git add infra/opentofu/modules/data-buckets infra/opentofu/env/prod tests/production/infra
    git commit -m "feat(storage): add private data and locked audit buckets"

### Task 4: Provision Cognito Resource Server and PKCE Client

**Branch:** feat/prod-infra-004-cognito

**Files:**
- Create: infra/opentofu/modules/cognito/main.tf
- Create: infra/opentofu/modules/cognito/variables.tf
- Create: infra/opentofu/modules/cognito/outputs.tf
- Modify: infra/opentofu/env/prod/main.tf
- Create: tests/production/infra/test_cognito.py

**Interfaces:**
- Resource server identifier is API origin and custom scope api.access.
- Outputs issuer, audience, pool/client IDs, managed prefix domain and absolute authorize/token/logout URLs.
- Managed prefix is collision-safe from environment plus approved unique suffix input.

- [ ] **Step 1: Write exact OAuth tests**

Require code flow, PKCE-compatible public client/no secret, exact callback/logout, openid/email/profile plus API scope, resource server and admin-created demo users only.

- [ ] **Step 2: Write forbidden feature tests**

No custom domain/certificate/DNS, managed-login-v2 dependency, SMS MFA, social IdP or client credentials.

- [ ] **Step 3: Implement deterministic collision-safe prefix**

Suffix is an explicit reviewed non-secret input bound in state; planning rejects empty/out-of-pattern values and reports unavailable domain as blocker.

- [ ] **Step 4: Validate app/output consistency**

Compare outputs to AwsRuntimeSettings/dashboard public-env schema from the runtime plan.

- [ ] **Step 5: Commit**

    uv run pytest -q tests/production/infra/test_cognito.py packages/cnes_infra/tests/aws/test_settings.py
    tofu -chdir=infra/opentofu validate -no-color
    git add infra/opentofu/modules/cognito infra/opentofu/env/prod tests/production/infra
    git commit -m "feat(auth): provision cnesdata cognito audience"

### Task 5: Provision Private Static Web Delivery

**Branch:** feat/prod-infra-005-static-web

**Files:**
- Create: infra/opentofu/modules/static-web/main.tf
- Create: infra/opentofu/modules/static-web/function.js
- Create: infra/opentofu/modules/static-web/variables.tf
- Create: infra/opentofu/modules/static-web/outputs.tf
- Modify: infra/opentofu/env/prod/main.tf
- Create: tests/production/infra/test_static_web.py
- Create: tests/production/infra/test_spa_rewrite.js

**Interfaces:**
- Private us-east-2 versioned bucket, OAC, CloudFront, dedicated global WAF and ACM.
- Public name cnesdata.vinisantana.com; Cloudflare DNS is external ownership.
- Runtime output maps VITE_API_BASE_URL exactly to https://api.cnesdata.vinisantana.com/api/v1.

- [ ] **Step 1: Write OAC/public policy tests**

Bucket reads limited to exact distribution SourceArn. No public ACL/website.

- [ ] **Step 2: Write rewrite/cache/header tests**

Extensionless SPA routes only; exclude assets/extensions/API/metadata. Immutable hashed assets; index/manifest/service-worker bounded no-cache. Security headers and CSP sources are exact.

- [ ] **Step 3: Add release lifecycle below 5 GB**

Retain current/prior units; expire unreferenced noncurrent only after rollback window.

- [ ] **Step 4: Validate and commit**

    node --test tests/production/infra/test_spa_rewrite.js
    uv run pytest -q tests/production/infra/test_static_web.py
    tofu -chdir=infra/opentofu validate -no-color
    git add infra/opentofu/modules/static-web infra/opentofu/env/prod tests/production/infra
    git commit -m "feat(web-infra): add private cnesdata distribution"

### Task 6: Provision Zero-Ingress Public-Subnet Networking

**Branch:** feat/prod-infra-006-network

**Files:**
- Create: infra/opentofu/modules/network/main.tf
- Create: infra/opentofu/modules/network/variables.tf
- Create: infra/opentofu/modules/network/outputs.tf
- Modify: infra/opentofu/env/prod/main.tf
- Create: tests/production/infra/test_network.py

**Interfaces:**
- Produces exact public subnet IDs, route/IGW, zero-ingress SG and optional free S3/DynamoDB gateway endpoint IDs.
- No NAT, ALB/NLB, public listener or service discovery.

- [ ] **Step 1: Write forbidden-cost/listener tests**

Reject NAT/egress-only internet gateway, load balancer/target group/listener, ECS service, VPC interface endpoint and any ingress rule.

- [ ] **Step 2: Write egress contract tests**

Allow DNS plus TLS/required AWS APIs without opening inbound. Document why ephemeral task public IP is accepted.

- [ ] **Step 3: Implement compact VPC/public subnets**

Use two AZs only if required by account/service validation without adding fixed-cost resources. Gateway endpoints are route-table scoped and free.

- [ ] **Step 4: Cross-check AWS-012 validator inputs**

Plan output exact subnet/SG tuple must pass the runtime production-network test.

- [ ] **Step 5: Commit**

    uv run pytest -q tests/production/infra/test_network.py packages/cnes_infra/tests/executor/test_production_network.py
    tofu -chdir=infra/opentofu validate -no-color
    git add infra/opentofu/modules/network infra/opentofu/env/prod tests/production/infra
    git commit -m "feat(network): add zero-ingress fargate egress"

### Task 7: Provision Processor ECR and Immutable ECS Task Families

**Branch:** feat/prod-infra-007-ecs

**Files:**
- Create: infra/opentofu/modules/processor-registry/**
- Create: infra/opentofu/modules/processing/ecs.tf
- Create: infra/opentofu/modules/processing/variables.tf
- Create: infra/opentofu/modules/processing/outputs.tf
- Modify: infra/opentofu/env/prod/main.tf
- Create: tests/production/infra/test_ecr_ecs.py

**Interfaces:**
- ECR receives byte-identical promoted processor content and exposes repository URI.
- Module registers exact unit, recovery and audit task definitions from a verified image_digest input.
- Task roles and execution roles are separate; log payloads disabled/redacted.

- [ ] **Step 1: Write ECR lifecycle/scan tests**

Scan on push; untagged seven days; at most five prod/30 days while retaining explicitly declared current/prior digests.

- [ ] **Step 2: Write task-definition tests**

Linux/x86_64, FARGATE, 0.25 vCPU, bounded memory, 20 GiB, read-only/non-root/no privilege, awslogs seven days, mode-specific command/deadline/env and no secret values.

- [ ] **Step 3: Implement revision inputs for promotion phases**

No mutable tag. image_uri must match ECR_URI@sha256. Candidate manifest does not contain task revision; phase A outputs exact revision ARNs.

- [ ] **Step 4: Validate and commit**

    uv run pytest -q tests/production/infra/test_ecr_ecs.py
    tofu -chdir=infra/opentofu validate -no-color
    git add infra/opentofu/modules/processor-registry infra/opentofu/modules/processing infra/opentofu/env/prod tests/production/infra
    git commit -m "feat(processing-infra): register immutable task families"

### Task 8: Provision Step Functions and Exact Service IAM

**Branch:** feat/prod-infra-008-step-functions

**Files:**
- Create: infra/opentofu/modules/processing/state_machine.asl.json
- Create: infra/opentofu/modules/processing/step_functions.tf
- Create: infra/opentofu/modules/runtime-iam/step-functions.tf
- Create: infra/opentofu/modules/runtime-iam/task-roles.tf
- Create: tests/production/infra/test_step_functions_iam.py

**Interfaces:**
- Standard state machine, Inline Map, canonical three waves, MaxConcurrency=1 and exact task revision ARN input.
- Step Functions role trusts states.amazonaws.com with exact SourceAccount/SourceArn.

- [ ] **Step 1: Write ASL contract tests**

IDs-only input, NORMALIZE/RECONCILE/MATERIALIZE order, bounded jitter retries/catches, no Distributed Map, no execution data logging and exact ECS network parameters.

- [ ] **Step 2: Write IAM positive/negative matrix**

ecs:RunTask exact current/candidate revisions; Describe/Stop Resource:* only where AWS requires; EventBridge managed-rule actions exact; iam:PassRole exact roles and PassedToService; required Logs delivery wildcard action set exact.

- [ ] **Step 3: Implement API/runtime task policies**

API exact machine start/describe, execution describe/stop, control-plane keys and serving GetObject/stat. Processor prefix actions follow the Spec; deny delete/list/raw/audit cross-access.

- [ ] **Step 4: Run runtime validator against rendered ASL**

    uv run pytest -q tests/production/infra/test_step_functions_iam.py packages/cnes_infra/tests/executor

- [ ] **Step 5: Commit**

    git add infra/opentofu/modules/processing infra/opentofu/modules/runtime-iam tests/production/infra
    git commit -m "feat(processing-infra): add exact step-functions iam"

### Task 9: Provision Recovery and Audit Schedulers

**Branch:** feat/prod-infra-009-schedulers

**Files:**
- Create: infra/opentofu/modules/schedulers/main.tf
- Create: infra/opentofu/modules/schedulers/iam.tf
- Create: infra/opentofu/modules/schedulers/variables.tf
- Create: infra/opentofu/modules/schedulers/outputs.tf
- Modify: infra/opentofu/env/prod/main.tf
- Create: tests/production/infra/test_schedulers.py

**Interfaces:**
- recover-once every 30 minutes, max retries 0, task deadline/lease 3600, batch 100.
- dispatch-outbox-once every 30 minutes, max retries 0, deadline 60, limit 100.
- Separate exact task revisions/roles; public subnets, public IP, zero-ingress SG.

- [ ] **Step 1: Write schedule and retry tests**

Reject flexible time windows that violate cadence, retry attempts >0, shared task role or wrong mode/definition.

- [ ] **Step 2: Write Scheduler trust/PassRole tests**

Trust scheduler.amazonaws.com with exact account/ARN. Pass only matching task/execution role.

- [ ] **Step 3: Implement alarms**

Failed/missed/timed-out invocation and outbox backlog. Audit remains allowed under cost freeze.

- [ ] **Step 4: Validate and commit**

    uv run pytest -q tests/production/infra/test_schedulers.py
    tofu -chdir=infra/opentofu validate -no-color
    git add infra/opentofu/modules/schedulers infra/opentofu/env/prod tests/production/infra
    git commit -m "feat(ops-infra): schedule recovery and audit passes"

### Task 10: Add Canary Resources and Promotion Fence IAM

**Branch:** feat/prod-infra-010-canary-fence

**Files:**
- Create: infra/opentofu/modules/processing/canary.tf
- Create: infra/opentofu/modules/runtime-iam/canary.tf
- Create: infra/opentofu/modules/runtime-iam/promotion.tf
- Create: tests/production/infra/test_canary_fence_iam.py

**Interfaces:**
- Persistent canary table/bucket owned by OpenTofu.
- Release-scoped short-session seeder/verifier task/execution roles.
- Seeder PutItem only exact canary table; verifier exact GSI Query, conditioned GetItem, bound GetObject/GetObjectRetention and exact PassRole.
- Promotion operator strongly reads gate/semaphore and mutates fence only.

- [ ] **Step 1: Write canary least-privilege matrix**

Reject other release IDs, tables/indexes/buckets, semaphore write, application data read and broad PassRole.

- [ ] **Step 2: Write audit dispatch IAM matrix**

Allow audit lock config/get/put/retention and outbox/cursor LeadingKeys; deny run/fence/semaphore, Scan, Delete, List and BypassGovernanceRetention.

- [ ] **Step 3: Implement resources and release conditions**

Canary event/object keys include release ID; roles have bounded max session and explicit activation input.

- [ ] **Step 4: Validate and commit**

    uv run pytest -q tests/production/infra/test_canary_fence_iam.py
    tofu -chdir=infra/opentofu validate -no-color
    git add infra/opentofu/modules/processing infra/opentofu/modules/runtime-iam tests/production/infra
    git commit -m "feat(promotion): add canary and fence boundaries"

### Task 11: Add Athena, Alarms and Cost/Freeze Contracts

**Branch:** feat/prod-infra-011-athena-cost

**Files:**
- Create: infra/opentofu/modules/athena/**
- Create: infra/opentofu/modules/observability/**
- Create: policies/cnesdata.rego
- Create: schemas/cost-manifest.schema.json
- Create: tests/production/infra/test_athena_observability_cost.py

**Interfaces:**
- Operator workgroup enforces 5 GB/query and monthly monitoring 100 GB.
- Alarms cover API/tunnel reference, DynamoDB, Step Functions, ECS, S3 denial, audit backlog, Athena cutoff, 100 task-hours and 200 attempts.
- Product outputs exact automation role ARNs eligible for shared freeze policy.

- [ ] **Step 1: Write Athena non-API and cutoff tests**

No API task role action. Workgroup enforce_work_group_configuration=true, bytes cutoff and private result location/lifecycle.

- [ ] **Step 2: Write cost manifest and forbidden-resource policy**

Reject NAT/LB/RDS/multi-region/Container Insights/paid plans. Product min/base/max <=8 and shared aggregate <=15.

- [ ] **Step 3: Write freeze survivability tests**

Freeze targets promotion/API/recovery/Step Functions/Athena new-cost actions; audit delivery, health/readback and backup writes remain. Shared Budget action remains external ownership.

- [ ] **Step 4: Validate and commit**

    conftest test tests/fixtures/plans --policy policies
    uv run pytest -q tests/production/infra/test_athena_observability_cost.py
    tofu -chdir=infra/opentofu validate -no-color
    git add infra/opentofu/modules/athena infra/opentofu/modules/observability policies schemas tests/production/infra
    git commit -m "feat(cost): bound analytics processing and freeze targets"

### Task 12: Build the Infrastructure Acceptance Gate

**Branch:** test/prod-infra-012-acceptance

**Files:**
- Create: infra/opentofu/tests/production.tftest.hcl
- Create: tests/production/infra/test_acceptance.py
- Create: docs/runbooks/infrastructure-plan.md
- Create: docs/runbooks/object-lock-capability.md
- Create: docs/runbooks/roles-anywhere-evidence.md
- Create: docs/runbooks/cloudfront-free.md

**Interfaces:**
- Produces no apply workflow.
- Local gate validates configuration/policy; real-AWS capability checks are separately approved and record redacted evidence.

- [ ] **Step 1: Aggregate resource and IAM contracts**

All Tasks 1-11 plus shared ownership, region, tags, protected resources and no secret/private-key/state content.

- [ ] **Step 2: Add real-AWS capability procedures**

DynamoDB condition/TTL behavior, audit Object Lock COMPLIANCE, external CA/CRL Roles Anywhere acceptance/revocation, Step Functions service IAM, exact ECR digest and CloudFront Free ACTIVE.

- [ ] **Step 3: Add plan rejection fixtures**

Protected deletion/replacement, NAT/ALB, public bucket/ingress, paid CloudFront, cost >8 or aggregate >15, wrong region, mutable image, ignore_changes on routing and secret-like values.

- [ ] **Step 4: Run the complete non-mutating gate**

    tofu -chdir=infra/opentofu fmt -check -recursive
    tofu -chdir=infra/opentofu init -backend=false -input=false
    tofu -chdir=infra/opentofu validate -no-color
    tofu -chdir=infra/opentofu test
    conftest test tests/fixtures/plans --policy policies
    uv run pytest -q tests/production/infra
    git diff --check

- [ ] **Step 5: Commit**

    git add infra/opentofu/tests tests/production/infra docs/runbooks
    git commit -m "test(infra): gate cnesdata production resources"

## Execution Order

Task 1 is serial. Tasks 2-5 may proceed after it with separate module ownership. Task 6 precedes Tasks 7-9. Task 7 precedes Task 8. Task 8 precedes Tasks 9-10. Task 11 may proceed after Tasks 2-10 have stable outputs. Task 12 is final. Promotion workflows are not implemented here.

## Plan Self-Review Record

- Ownership: shared/edge/host resources are referenced, never recreated.
- Region: us-east-2 is default and the global edge alias is restricted.
- Cost: architecture has zero fixed NAT/LB/database cost and product policy <=USD 8.
- Processing: no idle ECS service; exact revisions/roles and network match runtime validation.
- Data: browser CORS only on serving-capable data bucket; audit is distinct COMPLIANCE storage.
- IAM: AWS-required wildcard exceptions are enumerated; all other scope is exact and negative-tested.
- Promotion readiness: task families/canary/fence outputs support phases without ignore_changes or out-of-band routing.
- Completeness scan: no unresolved marker, unreviewed paid fallback or destructive automation.

## Execution Handoff

Implement with worktree PRs after runtime amendments are green. A reviewed plan from this module does not authorize apply; the companion operations/delivery plan supplies exact evidence and manual phase workflows.
