# CnesData Production Operations Design

**Date:** 2026-08-29  
**Status:** Draft for repository review; architecture approved in design discussion  
**Repository:** `VINIClUS/CnesData`  
**Integration base:** `develop`

## 1. Purpose

Define the operational contract for the production deployment architecture in
[the production deployment design](2026-08-29-cnesdata-production-deployment-design.md).
This document covers delivery, rollback, abuse containment, observability,
recovery, acceptance, costs and completion. The linked deployment design owns
the architecture, resources, identity, network and serving contracts.

## 2. CI/CD

### 2.1 Continuous integration

Pull requests to `develop` run the existing locked Python/CND/AWS gates plus:

- dashboard lint, typecheck, tests and deterministic production build;
- dashboard routing checks require
  `VITE_API_BASE_URL=https://api.cnesdata.vinisantana.com/api/v1`, mapped from
  the exact OpenTofu output into the dashboard build, one authenticated API
  client for `auth/me` and activation, no production relative `/api` request,
  bearer only to the API origin and `X-Tenant-Id` only for tenant-scoped calls;
- API and processor image build tests;
- OCI vulnerability scan and SBOM generation;
- OpenTofu format/validate/test and provider-lock verification;
- policy and cost-manifest checks;
- release-manifest schema and secret-scan tests.

Untrusted pull-request code runs only on GitHub-hosted runners and receives no
AWS, Cloudflare or SSH credential.

### 2.2 Release creation

A candidate release is bound to one green `develop` commit. Its candidate
manifest records:

- source SHA and test run IDs;
- release ID;
- API GHCR digest;
- processor GHCR digest and expected ECR destination/repository;
- dashboard artifact checksum;
- Compose/config checksum;
- OpenTofu/provider lock digest;
- SBOM checksums and schema version.

The candidate manifest contains no ECS task-definition revision ARN. An
activation manifest is finalized and signed only after phase A below.

Mutable tags may aid discovery but are never deployment authority.

### 2.3 Infrastructure plan/apply

Plan and apply are separate manual workflows. Apply downloads and verifies the
exact unexpired binary plan; it does not replan. GitHub OIDC supplies temporary
AWS credentials. The paid Actions spending limit is USD 0.

### 2.4 Application promotion

Production promotion is `workflow_dispatch` only. It verifies that the release
SHA is contained in `develop`, all required checks succeeded and the cost-freeze
marker is absent.

Order:

1. copy/promote and verify the processor digest in the expected ECR repository,
   yielding its exact `ECR_URI@sha256`;
2. under a reviewed exact OpenTofu plan/apply, phase A registers immutable unit
   and `recover-once` revisions pinned to that exact `ECR_URI@sha256` and
   dual-authorizes old and new `ecs:RunTask` ARNs without routing changes;
3. wait for and revalidate IAM propagation, prove both revisions authorized,
   then output their ARNs and phase-A evidence;
4. finalize and sign one post-registration activation manifest with the candidate
   manifest SHA-256, release ID, source SHA, exact revision ARNs, verified
   `ECR_URI@sha256` and phase-A evidence; never re-register those revisions;
5. atomically close the environment deployment fence. The API then rejects all
   tenant job admissions with `503` and `Retry-After`, recovery starts no new
   executions, and the workflow waits until no dispatch decision is in flight;
6. under a separate reviewed exact OpenTofu plan/apply, phase B consumes the
   signed activation manifest ARNs only after verifying that binding, then
   switches the still OpenTofu-owned state-machine `TaskDefinition` ARN and
   Scheduler recovery target. No out-of-band mutation or `ignore_changes` is
   allowed;
7. use `DescribeTaskDefinition`, state-machine and Scheduler evidence to prove
   both selected revisions resolve to the exact release digest and verify the
   manifest binding, then verify clean OpenTofu state and no drift;
8. verify that same signed binding, update the inactive API candidate and pass
   local health/authorization smoke tests;
9. switch Nginx to the candidate and run tunneled health/authorization through
   the normal production hostname, which now resolves to that candidate;
10. publish frontend assets and entrypoint;
11. while the fence rejects tenant jobs, admit only the release-bound synthetic
    canary, run its vertical slice and serving test, then reopen admissions;
12. record redacted evidence and retain the previous release. A failure in
    steps 6-11 keeps the fence closed and immediately restores the prior API,
    processor and Scheduler routes before reopening admissions.

Retain old revisions and their `ecs:RunTask` grants until all active old Standard
executions and recovery tasks drain and the rollback-retention period ends. Prune
only older non-rollback revisions after that point.

No production deployment occurs automatically after merge.

## 3. Rollback

- **API:** restore the previous Nginx upstream and already-pulled GHCR digest.
- **Frontend:** restore the prior versioned entrypoint and invalidate it.
- **Processor:** rollback uses the prior retained activation manifest and a
  reviewed exact OpenTofu plan/apply. Its prior routes and grants remain intact;
  restore both prior immutable unit and `recover-once` revisions, digests and
  state-machine/Scheduler routes for new dispatches. Active Standard executions
  continue with the definition they started unless cancellation is safe.
- **State machine:** revert only new-execution routing; do not edit an active
  execution or DynamoDB record manually.
- **DynamoDB/S3:** application releases never roll data backward or delete
  published versions. Compatibility changes are expand/contract and additive.
- **Infrastructure:** failed apply requires diagnosis and a new reviewed plan;
  protected-resource restore is not an automatic rollback step.

## 4. Rate limiting and abuse containment

- Cloudflare Free rule protects the most sensitive public job/auth surface.
- Nginx enforces IP-based general and expensive-mutation zones.
- The API enforces tenant/user/idempotency quotas, maximum 200 jobs and the
  semaphore's one concurrent unit task per environment. A separate atomic
  monthly counter permits at most 200 Step Functions execution attempts across
  initial and recovery starts; each caller increments it before `StartExecution`
  or rejects the start when exhausted. The monitored 100-hour
  monthly operating target counts every unit and recovery task-hour; it is not a
  pre-launch API limit.
- DynamoDB maximum throughput, Step Functions bounded retries and Athena bytes
  cutoffs contain downstream amplification.
- Requests beyond product quota fail with `429` or the documented quota error
  before starting Step Functions or Athena.
- A USD 15 aggregate budget action is a delayed cost-safety backstop. When it
  fires, it freezes new unit and recovery Fargate, Step Functions and Athena
  starts through automation roles while preserving reads and backups. Billing
  data can lag, so spend may exceed USD 15 before the action applies.

## 5. Observability and SLOs

Initial internal targets:

- static frontend availability 99.9%;
- API availability 99.5%;
- eligible processing-job success 99%;
- routine API p95 below two seconds;
- routine deployment interruption at most 60 seconds.

Grafana Alloy collects VPS/API/Nginx/Tunnel telemetry without Docker-socket
access. CloudWatch owns DynamoDB, Step Functions, ECS, S3 and Athena alarms.
Fargate logs retain seven days. High-cardinality tenant, run, object-key and
user labels are forbidden.

Required alarms include API/tunnel health, DynamoDB throttles/errors, failed or
timed-out Step Functions executions, ECS task failures, audit/outbox backlog,
S3 access denial, Athena cutoff, budget thresholds and anomaly detection.

## 6. Backup and recovery

- DynamoDB PITR: 35 days;
- S3 data/audit: versioning and prefix-specific lifecycle/Object Lock;
- static entrypoint: versioned rollback;
- infrastructure state: encrypted/versioned S3 backend;
- no cross-region replica in phase 1;
- quarterly recovery exercise reconstructs the API and proves one synthetic
  dataset pointer/serving object without altering production data;
- destructive PITR export/restore requires a separately approved runbook run.
- The security owner operates the external, offline CA; AWS Private CA is never
  enabled. Its trust-anchor CA has `CA:true`, `Certificate Sign` and `CRL Sign`
  key usage, and SHA-256 or stronger signatures. The CA private key and API/VPS
  leaf credentials never enter the repo or OpenTofu state.
- The owner issues a unique API/VPS X.509v3 leaf with `CA:false`, `Digital
  Signature` usage and SHA-256 or stronger signatures. Its certificate chain
  can be readable as needed; its host-owned private key remains outside the
  container and is bind-mounted read-only for the fixed runtime UID only, with
  no other user or process able to read it. Rotation occurs before expiry with
  overlap and a refresh test.
- Revocation imports or updates the CRL at the Roles Anywhere trust anchor; it
  does not depend on OCSP or CDP. Compromise revokes and rotates the leaf,
  confirms fail-closed behavior and alerts the security owner.
- The Roles Anywhere profile sets `durationSeconds=3600`; its signing helper
  uses `--session-duration 3600`; and the IAM role sets
  `MaxSessionDuration=3600`. This bounded lifetime exceeds botocore's default
  15-minute advisory refresh window. On incident, disable the profile or its
  trust, import/update the CRL and apply `AWSRevokeOlderSessions` or an explicit
  `aws:TokenIssueTime` Deny to the role.
- Before declaring fail-closed, verify pre-incident credentials are denied by
  DynamoDB, S3 and Step Functions. Retain the deny for at least the maximum
  session duration plus propagation, issue and install a new leaf, then
  re-enable the profile/trust and test a new session.
- The normal unit task is launched by Step Functions. `recover-once` is an
  EventBridge Scheduler task using the same processor image but a separate task
  definition and mode, with cadence no greater than half
  `AWS_PROCESSOR_LEASE_SECONDS`; it requires none of the seven normal processor
  environment variables. Scheduler `MaximumRetryAttempts=0`; at-least-once
  delivery or an external re-invocation can still create overlapping, retried or
  duplicate passes, with no global recovery-concurrency ceiling. Each is one
  bounded pass with
  `AWS_PROCESSOR_RECOVERY_BATCH_SIZE=100`. Its entrypoint/PID 1 enforces a
  hard wall-clock deadline equal to `AWS_PROCESSOR_LEASE_SECONDS`; at deadline,
  it emits timeout logs and alarm, cancels work and exits nonzero so the ECS task
  stops.
  Every recovery task-hour is counted in the 100-hour monitored monthly
  operating target. The environment semaphore arbitrates cross-run unit starts,
  while dispatch CAS handles only same-run recovery and idempotency.
- The recovery task role has control-plane read/write,
  `states:StartExecution` on the exact production state machine and
  `states:DescribeExecution` on its executions, plus minimum liveness
  `ecs:ListTasks`/`ecs:DescribeTasks`. The Scheduler role trusts
  `scheduler.amazonaws.com` with exact `aws:SourceAccount` and
  `aws:SourceArn`, scopes `ecs:RunTask` to recovery revision ARNs and, during
  activation, both old and candidate revisions; it passes only its task/execution
  roles. Recovery uses the unit task's public
  subnets, zero-ingress security group and public IP, with logs and alarm. The
  recovery task, API and any takeover actor use ECS liveness reads only for the
  configured cluster/task family with supported conditions; where ECS requires
  `Resource: *`, those same narrow conditions apply.
- For `PUBLISHING`, `s3:GetObject`/`s3:GetObjectVersion` read only the canonical
  data bucket's `tmp/`, `normalized/`, `reconciliation/` and `serving/` prefixes.
  `s3:PutObject` writes only final `normalized/`, `reconciliation/` and
  `serving/` objects used by immutable manifests/final objects, never `tmp/`.
  Copy reads the source and writes the destination only. It has no
  raw/audit/other-prefix access and no `s3:DeleteObject` or `s3:ListBucket`
  unless separately demonstrated necessary.
- One environment-wide control-plane DynamoDB semaphore/lease/fence item gates
  all unit work. Initial API starts and recovery conditionally acquire it before
  `StartExecution`, bind owner to dispatch/execution, renew it and release it
  terminally. An expired item cannot authorize takeover until
  `DescribeExecution` and ECS prove no work active; TTL is garbage collection
  only. The semaphore is the cross-run arbiter: losers do not call
  `StartExecution`; dispatch CAS applies only to same-run recovery/idempotency.
  A concurrent request receives the documented quota error or `429`.
- The deployment fence shares this control plane but is independent of the unit
  semaphore. It blocks initial and recovery dispatch, except for one
  release-bound synthetic canary during promotion, until rollback or acceptance
  explicitly reopens admissions.

## 7. Test and acceptance matrix

### 7.1 Pre-production tests

- all CND adapter and AWS-014 matrices on the exact release SHA;
- real-AWS DynamoDB transaction/TTL and S3 Object Lock capability checks;
- OIDC/JWKS rotation, revoked membership and cross-tenant denial;
- old fence/dispatch rejection and duplicate execution replay;
- required-source failure and optional-source degraded publication;
- pointer CAS race, S3 failure before publication and outbox replay;
- no presigned URL for non-serving prefixes;
- dashboard artifact and browser checks reject a relative `/api` request or an
  API call that bypasses the configured absolute `/api/v1` client;
- long-lived boto3/botocore clients complete an AWS call without process or
  container restart: the initial credential-using call invokes the IAM Roles
  Anywhere helper; calls before the 15-minute advisory window do not re-invoke
  it; and the first credential access inside that window invokes one lazy refresh
  that succeeds. Helper or refresh failure fails closed;
- VPS config tests require `AWS_CONFIG_FILE` and matching `AWS_PROFILE`; a
  missing or mismatched profile fails before readiness;
- OpenTofu creates the `us-east-2` trust anchor from external offline public
  CA PEM with the required CA, key-usage and SHA-256 constraints, and the Roles
  Anywhere profile, with AWS Private CA prohibited. The trust policy permits
  `sts:AssumeRole`, `sts:TagSession` and
  `sts:SetSourceIdentity` only to `rolesanywhere.amazonaws.com`, conditioned on
  the trust-anchor `SourceArn` and API/VPS X.509 identity; the profile remains
  separately referenced by `credential_process`; no private CA key or leaf
  appears in repository or state;
- the separate Step Functions role is trusted only by `states.amazonaws.com`
  with the exact `aws:SourceAccount` and the exact production state-machine
  `aws:SourceArn`. `ecs:RunTask` is scoped to the current/candidate unit
  revision ARNs during activation;
  `ecs:DescribeTasks` and `ecs:StopTask` use `Resource: *`; `events:PutTargets`,
  `events:PutRule` and `events:DescribeRule` are scoped to
  `StepFunctionsGetEventsForECSTaskRule`; `iam:PassRole` permits only the task
  and execution roles with `iam:PassedToService=ecs-tasks.amazonaws.com`;
- candidate-manifest tests require release ID/source SHA, processor GHCR digest
  and expected ECR destination/repository while rejecting ECS revision ARNs and
  a promoted ECR digest. Phase-A exact OpenTofu plan/apply registers each
  revision once from verified `ECR_URI@sha256`, dual-authorizes old/new
  `ecs:RunTask` ARNs without routing changes, waits for/revalidates IAM
  propagation and proves both authorized. One signed activation manifest binds
  candidate manifest SHA-256, release ID, source SHA, revision ARNs, verified
  digest and phase-A evidence;
- phase-B exact OpenTofu plan/apply consumes that signed manifest and switches
  only the OpenTofu-owned state-machine/Scheduler targets after verifying the
  binding. API/frontend steps verify the same binding. Tests reject artifact
  mixing, out-of-band mutation and `ignore_changes`, then prove selected exact
  digests, clean state and no drift;
- promotion tests close the deployment fence before phase B, prove no dispatch
  decision remains in flight, reject tenant jobs and recovery starts, and allow
  only the bound canary. Local smoke precedes the Nginx switch; tunneled smoke
  uses the production hostname after that switch. Any failure restores prior
  routes before admissions reopen;
- drain/prune tests retain old revisions and `ecs:RunTask` grants through active
  old Standard/recovery drain and rollback retention, then prune only older
  non-rollback revisions; rollback consumes the prior retained manifest through
  a reviewed exact OpenTofu plan/apply;
- recovery validation enforces the separate `recover-once` definition/mode,
  same image and network shape, Scheduler cadence at most half
  `AWS_PROCESSOR_LEASE_SECONDS`, `MaximumRetryAttempts=0`, no seven normal
  processor environment variables, batch 100 and a PID 1 hard wall-clock
  deadline equal to the lease that logs/alarms, cancels work, exits nonzero and
  stops the ECS task; overlapping, externally retried or at-least-once duplicate
  invocations have no global concurrency ceiling;
- profile `durationSeconds=3600`, helper `--session-duration 3600` and role
  `MaxSessionDuration=3600` are asserted;
  the incident drill disables new sessions, updates CRL, revokes old sessions
  and proves the timed Deny before re-enabling a rotated leaf;
- semaphore tests require conditional acquisition before initial or recovery
  `StartExecution`, dispatch/execution ownership, terminal release, no expired
  takeover before Step Functions/ECS proof, no losing cross-run `StartExecution`
  and `429` or quota on contention. Overlapping, externally retried and
  at-least-once duplicate recovery passes remain individually bounded, while
  dispatch CAS covers same-run recovery only;
- cost-contract tests count every unit and recovery task-hour toward the
  monitored 100-hour monthly operating target, never as a pre-launch API gate;
  the delayed USD 15 budget action freezes new unit and recovery Fargate, Step
  Functions and Athena starts only after it fires; tests do not claim a
  synchronous spend cap and explicitly allow billing-lag overshoot;
- execution-quota tests atomically count every initial and recovery
  `StartExecution` attempt against one 200-attempt monthly maximum and reject
  both callers when exhausted;
- recovery role tests scope `states:StartExecution` to the production machine,
  `states:DescribeExecution` to its executions and ECS liveness reads to the
  configured cluster/task family or required narrow `Resource: *` conditions;
- a crash during `PUBLISHING` proves the recovery role reads manifest sidecars,
  promotes/verifies source-to-destination copies, writes reconciliation/serving
  manifests and completes pointer CAS without `AccessDenied`. Negative tests
  deny `s3:PutObject` on `tmp/`, all raw/audit/other prefixes and delete/list
  operations;
- the production AWS-012 override accepts `AssignPublicIp=ENABLED` only with
  exact public subnet IDs, the configured zero-ingress security group,
  `FARGATE`, maximum concurrency one, and no NAT Gateway or ALB; it rejects
  `DISABLED`, drift, inbound rules and incompatible launch/network config;
- CDN private-origin, SPA rewrite, CSP and cache rollback;
- CloudFront Free eligibility plus exact `ACTIVE` distribution/WAF binding;
- Nginx and application rate-limit behavior;
- plan policy, cost envelope and protected-resource replacement denial.

### 7.2 Production smoke

- static site returns the expected release ID over TLS;
- S3 origin is not publicly readable;
- API is unreachable at the VPS address/ports and healthy through Tunnel;
- the collision-safe AWS-managed Cognito prefix domain serves real
  `/oauth2/authorize`, `/oauth2/token` and `/logout` endpoints; the PKCE code
  exchange and configured logout URL succeed, with no custom Cognito domain or
  managed login v2 dependency;
- demo user authenticates with PKCE and resolves only the demo tenant;
- a real constrained API/VPS leaf from the external CA assumes the Roles
  Anywhere profile; a CRL-revoked leaf is denied, alerts and remains failed
  closed;
- a Cognito bearer `access_token` with
  `aud=https://api.cnesdata.vinisantana.com` is accepted in the real
  environment;
- an API preflight from `https://cnesdata.vinisantana.com` for an allowed
  method/header is accepted by FastAPI;
- API preflights from another origin or for a disallowed method/header are
  denied by FastAPI;
- browser network evidence proves `auth/me` and activation use the configured
  `VITE_API_BASE_URL=https://api.cnesdata.vinisantana.com/api/v1`, never
  relative `/api`; bearer is sent only to the API origin and `X-Tenant-Id` only
  to tenant-scoped calls;
- a Step Functions execution starts only the approved Fargate task definition,
  uses only the exact source account/state-machine trust, event rule and pass
  roles above, and fails when any scope drifts;
- phase-A promotion evidence proves propagated old/new `ecs:RunTask`
  authorization without routing changes, then the signed activation manifest
  binds candidate manifest SHA-256, release ID, source SHA, exact ARNs/digest
  and phase-A evidence. Phase-B OpenTofu evidence verifies that binding and
  proves the state machine and Scheduler selected those revisions, exact ECR
  digest, clean state and no drift; API/frontend evidence verifies the same
  binding before switching. Active Standard executions remain on their original
  revision;
- promotion evidence proves the fence closed before phase B, no dispatch was in
  flight, local smoke passed before switching, and tunneled smoke reached the
  candidate through the switched production hostname. Only the bound canary is
  admitted before acceptance; failure restores prior routes while fenced;
- the `recover-once` Scheduler run uses the same image/network, emits its log
  and alarm evidence, uses `MaximumRetryAttempts=0`, and cannot start without
  its narrow roles;
- a deliberately hung recovery reaches the PID 1 deadline equal to
  `AWS_PROCESSOR_LEASE_SECONDS`, cancels, exits nonzero and stops in ECS by that
  deadline, with log and alarm evidence;
- two distinct runs plus overlapping, externally retried or at-least-once
  duplicate recovery passes leave at most one unit Fargate task active;
  semaphore losers make no
  `StartExecution`, while dispatch CAS remains limited to same-run recovery and
  each recovery pass is individually bounded with no global recovery-concurrency
  ceiling. Every recorded unit and recovery task-hour counts toward the monitored
  100-hour monthly operating target, not a pre-launch API limit;
- one synthetic run publishes exactly one new immutable version/pointer;
- the authenticated, tenant-authorized `X-Tenant-Id` API call returns `200`
  with `Cache-Control: private, no-store` and only `url`, `version_id` and
  `expires_in=300`, with no separate tenant or object-key field;
- the bearer-sensitive SigV4 `url` contains the expected authorized `serving/`
  key, appears in neither logs nor acceptance evidence, is never persisted,
  sent to telemetry, included in a referrer or cached, and no other prefix is
  signed;
- the dashboard makes a second direct S3 `fetch` with credentials omitted and
  without `Authorization`, `X-Tenant-Id`, cookies or other custom headers; it
  reads the expected JSON body from the 300-second signed URL and asserts the
  S3 response has `Cache-Control: private, no-store` from object metadata;
- this production handoff replaces the AWS-014 `307` route contract before
  promotion;
- logs, traces, state and artifacts contain no secret or synthetic record body;
- previous API/frontend release can be restored within the documented window.

## 8. Cost contract

CnesData receives a USD 8 monthly operational envelope. Expected initial use is
approximately USD 2-6 depending on Fargate jobs and stored data.

Cost controls:

- voluntary 5 GB operational cap for the web bucket, aligned with the
  CloudFront Free plan's account-wide 5 GB S3 Standard credit;
- one of the account's maximum three CloudFront Free-plan subscriptions, with
  eligibility and `ACTIVE` status checked before cutover;
- ECR generally below 2 GB;
- semaphore-enforced maximum one concurrent unit Fargate task per environment;
  recovery passes are individually bounded but have no global concurrency
  ceiling; zero idle desired count; and a monitored 100 combined unit and
  recovery task-hours monthly operating target, including every invocation;
- Step Functions Standard and 200 execution attempts maximum, atomically shared
  by initial and recovery starts;
- Athena 5 GB/query and 100 GB/month;
- DynamoDB on-demand maximum throughput;
- short CloudWatch retention and no Container Insights;
- no NAT Gateway, load balancer, RDS or multi-region resource;
- monthly review for the first three months and re-baseline if the project
  exceeds its envelope or aggregate forecast reaches USD 15.

## 9. Definition of done

- This specification is reviewed and its implementation plan is approved.
- Required CND and AWS logical gates are green on integrated `develop`.
- OpenTofu plan contains only approved resources and costs.
- All production artifacts are immutable and traceable to one source SHA.
- Static origin, API origin and AWS data resources are not publicly writable.
- No long-lived AWS or GitHub credential exists on the VPS or in GitHub.
- Synthetic portfolio data is clearly identified; municipal/local data paths
  are absent.
- Deploy, rollback, rate-limit, cost-freeze and recovery drills pass.
- `cnesdata.vinisantana.com` and `api.cnesdata.vinisantana.com` satisfy their
  health, TLS and isolation contracts.
- The deployment makes no claim that DATASUS ingestion or legacy cutover is
  complete before their governing gates pass.

## 10. References

- Amazon Cognito resource-server configuration:
  <https://docs.aws.amazon.com/cli/latest/reference/cognito-idp/create-resource-server.html>
- Amazon Cognito authorization endpoint resource binding:
  <https://docs.aws.amazon.com/cognito/latest/developerguide/authorization-endpoint.html>
- Amazon Cognito user pool domain:
  <https://docs.aws.amazon.com/cognito/latest/developerguide/cognito-user-pools-assign-domain.html>
- IAM Roles Anywhere credential helper:
  <https://docs.aws.amazon.com/rolesanywhere/latest/userguide/credential-helper.html>
- IAM Roles Anywhere getting started:
  <https://docs.aws.amazon.com/rolesanywhere/latest/userguide/getting-started.html>
- IAM Roles Anywhere trust model:
  <https://docs.aws.amazon.com/rolesanywhere/latest/userguide/trust-model.html>
- IAM Roles Anywhere CreateSession:
  <https://docs.aws.amazon.com/rolesanywhere/latest/userguide/authentication-create-session.html>
- IAM revoke role sessions:
  <https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_revoke-sessions.html>
- AWS Budgets update-delay considerations:
  <https://docs.aws.amazon.com/cost-management/latest/userguide/bcm-lite-use-budget.html>
- AWS SDK process credentials:
  <https://docs.aws.amazon.com/sdkref/latest/guide/feature-process-credentials.html>
- FastAPI CORS:
  <https://fastapi.tiangolo.com/tutorial/cors/>
- Amazon S3 CORS configuration:
  <https://docs.aws.amazon.com/AmazonS3/latest/userguide/ManageCorsUsing.html>
- Amazon ECR with Amazon ECS:
  <https://docs.aws.amazon.com/AmazonECR/latest/userguide/ECR_on_ECS.html>
- AWS Step Functions ECS integration:
  <https://docs.aws.amazon.com/step-functions/latest/dg/connect-ecs.html>
- Amazon DynamoDB on-demand capacity mode:
  <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/on-demand-capacity-mode.html>
- Amazon S3 Object Lock:
  <https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html>
- Amazon CloudFront Origin Access Control:
  <https://docs.aws.amazon.com/cloudfront/latest/APIReference/API_CreateOriginAccessControl.html>
- Amazon CloudFront flat-rate plan quotas:
  <https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/flat-rate-pricing-plan.html>
- AWS Pricing Plan Manager API:
  <https://docs.aws.amazon.com/pricingplanmanager/latest/APIReference/Welcome.html>
