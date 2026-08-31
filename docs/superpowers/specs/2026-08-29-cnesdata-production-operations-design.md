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
  client for `auth/me` and `/api/v1/activate/confirm`, no legacy origin-level
  activation or production relative `/api` request,
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
2. under a reviewed exact OpenTofu plan/apply, phase A registers immutable unit,
   recovery and audit-dispatch revisions pinned to exact `ECR_URI@sha256` and
   dual-authorizes old and new `ecs:RunTask` ARNs without routing changes;
   it creates canary task/execution/seeder/verifier roles plus a persistent canary
   table and bucket owned by OpenTofu;
3. wait for and revalidate IAM propagation, prove all revisions authorized,
   then output their ARNs and phase-A evidence;
4. sign one activation manifest with candidate SHA-256, release ID, source SHA,
   exact revisions, ECR digest and phase-A evidence; never re-register them. A seeder
   restricted to `PutItem` on the exact canary table writes one bound pending event.
   The verifier polls the exact canary GSI until that event is visible, bounded to
   60 seconds and fail-closed; then run promotion `RunTask`, overriding roles/resource
   variables to canary-only values. It passes only canary task/execution roles with
   ECS PassedToService. Verifier gets exact-GSI `Query`, conditioned `GetItem` and
   bound `GetObject`/`GetObjectRetention`; require retention/delivered proof;
5. promotion assumes the release-tagged fence operator and atomically closes the
   separate fence item first. New acquire transactions fail; use strongly consistent
   gate observation to await semaphore/decision drain. API returns `503`; recovery waits;
6. under a separate reviewed exact OpenTofu plan/apply, phase B consumes the
   signed activation manifest ARNs only after verifying that binding, then
   switches only the OpenTofu-owned state-machine `TaskDefinition` ARN; Schedulers
   stay on prior revisions. Out-of-band mutation and `ignore_changes` are prohibited;
7. use `DescribeTaskDefinition`, state-machine and Scheduler evidence to prove
   selected unit revision matches the release while both Schedulers match the
   prior manifest; then verify binding, clean state and no drift;
8. verify that same signed binding, update the inactive API candidate and pass
   local health/authorization smoke tests;
9. switch Nginx to the candidate and run tunneled health/authorization through
   the normal production hostname, which now resolves to that candidate;
10. publish frontend assets and entrypoint;
11. while fenced, run the bound application canary;
12. phase C uses separate reviewed applies for recovery and audit, accepting each only
    after binding/digest/no-drift proof. Partial applies remain fenced while a reviewed
    plan converges that route to candidate or prior. Reopen only after both accepted;
    else restore prior routes/evidence and leave deployment incomplete.

Retain old revisions/grants until active old Standard/scheduled work drains and
rollback retention ends. Then prune older non-rollback revisions.

No production deployment occurs automatically after merge.

## 3. Rollback

- **API:** restore the previous Nginx upstream and already-pulled GHCR digest.
- **Frontend:** restore the prior versioned entrypoint and invalidate it.
- **Processor:** rollback uses the prior retained activation manifest and a
  reviewed exact OpenTofu plan/apply. Its prior routes and grants remain intact;
  restore prior unit, recovery and audit-dispatch revisions/digests plus all
  state-machine/Scheduler routes. Active Standard executions
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
  or rejects the start when exhausted. The monitored 100-hour monthly target
  counts every unit, recovery and audit-dispatch task-hour; it is not a gate.
- DynamoDB maximum throughput, Step Functions bounded retries and Athena bytes
  cutoffs contain downstream amplification.
- Requests beyond product quota fail with `429` or the documented quota error
  before starting Step Functions or Athena.
- At USD 15, OpenTofu creates a dedicated same-account `ExecutionRoleArn` trusted
  only by `budgets.amazonaws.com` for the exact Budget ARN/source account. It gets
  `iam:AttachRolePolicy` on named promotion/API, recovery task/Scheduler, Step Functions
  execution and Athena roles, conditioned to `cnesdata-cost-freeze`. Audit stays on;
  only cost-clear detaches after reviewed recovery; history remains and billing may lag.

## 5. Observability and SLOs

Initial internal targets:

- static frontend availability 99.9%;
- API availability 99.5%;
- eligible processing-job success 99%;
- routine API p95 below two seconds;
- routine deployment interruption at most 30 minutes.

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
- The security owner operates the external offline CA; AWS Private CA is disabled.
  Trust-anchor CA has `CA:true`, certificate/CRL signing usage and SHA-256 or
  stronger signatures; CA private key/leaf credentials never enter repo/state.
- The unique API/VPS X.509v3 leaf has `CA:false`, `Digital Signature` and SHA-256
  or stronger. Its chain may be readable; its host key stays outside the container,
  read-only to the fixed UID and no other process. Rotate before expiry with overlap.
- Revocation updates the trust-anchor CRL, never OCSP/CDP; compromise revokes and
  rotates the leaf, proves fail-closed behavior and alerts the security owner.
- Roles Anywhere profile/helper/role session limits are all 3600 seconds, beyond
  botocore's 15-minute advisory refresh window. On incident, disable profile/trust,
  update CRL and apply `AWSRevokeOlderSessions` or an explicit
  `aws:TokenIssueTime` Deny to the role.
- Before declaring fail-closed, verify pre-incident credentials are denied by
  DynamoDB, S3 and Step Functions. Retain the deny for at least the maximum
  session duration plus propagation, issue and install a new leaf, then
  re-enable the profile/trust and test a new session.
- Step Functions launches unit tasks with production `AWS_PROCESSOR_LEASE_SECONDS=3600`.
  `recover-once` runs every 30 minutes with the same image, mode, batch 100 and coordinator
  composition without normal variables/audit sink. PID 1 enforces the lease deadline;
  the semaphore is cross-run and dispatch CAS same-run.
- Scheduler runs independent `dispatch-outbox-once` every 30 minutes with the same
  image, own definition/mode/role and 60-second PID deadline. `run_aws_entrypoint`
  allowlists it; composition builds DynamoDB, S3 audit sink and UTC clock, then calls
  `dispatch_once(control_plane, sink, now, limit=100)`.
  `ControlPlanePort` replaces `pending_outbox(limit)` with table-backed cursor page/read
  and CAS-advance operations, no hidden persistence. Each evaluated page advances and
  wraps; poison stays pending/retries without starvation. Alarm/exit is nonzero.
- Recovery role has control-plane read/write; `states:StartExecution` and
  `states:DescribeStateMachine` on the exact machine, `states:DescribeExecution`
  and `states:StopExecution` plus ECS liveness. API gets exact-machine start/describe,
  execution describe/stop and data-bucket `serving/*` `s3:GetObject` for stat/browser reads.
  Scheduler trusts only `scheduler.amazonaws.com` with exact source account/ARN, runs
  revisions and passes its task/execution roles. Dispatch Scheduler scope is equivalent
  only for dispatch revisions. Both use public subnets, zero-ingress groups,
  public IP, logs and alarms. Recovery/API/takeover ECS liveness reads use the
  configured cluster/family conditions, including required narrow `Resource: *`.
- Dispatch audit IAM allows `s3:GetBucketObjectLockConfiguration` on the bucket;
  `s3:GetObject`/`s3:PutObject`/`s3:PutObjectRetention` on `audit/*`; no delete/list/bypass.
  It permits exact-GSI `Query` and table `GetItem`/`UpdateItem` only with
  `dynamodb:LeadingKeys` matching outbox/event or cursor namespaces. Scan, delete, put,
  other indexes/tables and run/fence/semaphore keys are denied.
- For `PUBLISHING`, `s3:GetObject`/`s3:GetObjectVersion` read only the canonical
  data bucket's `tmp/`, `normalized/`, `reconciliation/` and `serving/` prefixes.
  `s3:PutObject` writes only final `normalized/`, `reconciliation/` and
  `serving/` objects used by immutable manifests/final objects, never `tmp/`.
  Copy reads the source and writes the destination only. It has no
  raw/audit/other-prefix access and no `s3:DeleteObject` or `s3:ListBucket`
  unless separately demonstrated necessary.
- `ControlPlanePort` exposes strongly consistent `observe_environment_gate()` and typed
  acquire, bind, renew, release, close-fence and reopen-fence mutation commands.
  Fence and semaphore are separate items. Runtime acquire transactionally requires an
  open fence while updating semaphore; close happens first, then promotion waits drain.
  API/recovery use runtime variants; operator reads fence/semaphore but updates only
  fence. Expired takeover needs Step Functions/ECS proof; TTL only collects garbage.
  Losers never `StartExecution` and receive quota/`429`; dispatch CAS stays same-run.
- The fence blocks initial/recovery dispatch except the bound synthetic canary.
  Acceptance or rollback reopens it through the port.

## 7. Test and acceptance matrix

### 7.1 Pre-production tests

- all CND adapter and AWS-014 matrices on the exact release SHA;
- real-AWS DynamoDB transaction/TTL and S3 Object Lock capability checks;
- OIDC/JWKS rotation, revoked membership and cross-tenant denial;
- old fence/dispatch rejection and duplicate execution replay;
- required-source failure and optional-source degraded publication;
- dispatcher boot tests accept only `dispatch-outbox-once` without unit variables
  and inject control plane, audit sink and UTC clock. Port tests require cursor-aware
  page/read plus cursor-advance CAS. A pass writes COMPLIANCE and marks after success;
  100 poison events do not starve a second page; wrap retries and replay is idempotent;
- no presigned URL for non-serving prefixes;
- dashboard artifact and browser checks reject a relative `/api` request or an
  API call that bypasses the configured absolute `/api/v1` client;
- long-lived boto3/botocore clients call AWS without restart: first use invokes
  Roles Anywhere helper, pre-advisory-window calls do not, and first access inside
  the 15-minute window refreshes once. Helper/refresh failure fails closed;
- VPS boot requires matching AWS config/profile, exact-machine start/describe and
  execution describe/stop; invalid scope fails. API omits audit sink/bucket requests;
- OpenTofu creates the `us-east-2` trust anchor from external offline public
  CA PEM with the required CA, key-usage and SHA-256 constraints, and the Roles
  Anywhere profile, with AWS Private CA prohibited. The trust policy permits
  `sts:AssumeRole`, `sts:TagSession` and
  `sts:SetSourceIdentity` only to `rolesanywhere.amazonaws.com`, conditioned on
  the trust-anchor `SourceArn` and API/VPS X.509 identity; the profile remains
  separately referenced by `credential_process`; no private CA key or leaf
  appears in repository or state;
- the separate Step Functions role is trusted only by `states.amazonaws.com`
  with exact `aws:SourceAccount` and production state-machine `aws:SourceArn`.
  `ecs:RunTask` is scoped to current/candidate unit revision ARNs in activation;
  `ecs:DescribeTasks` and `ecs:StopTask` use `Resource: *`; `events:PutTargets`,
  `events:PutRule` and `events:DescribeRule` are scoped to
  `StepFunctionsGetEventsForECSTaskRule`; `iam:PassRole` permits only the task
  and execution roles with `iam:PassedToService=ecs-tasks.amazonaws.com`. Logs
  delivery permits `logs:CreateLogDelivery`, `logs:GetLogDelivery`,
  `logs:UpdateLogDelivery`, `logs:DeleteLogDelivery`, `logs:ListLogDeliveries`,
  `logs:CreateLogStream`, `logs:PutLogEvents`, `logs:PutResourcePolicy`,
  `logs:DescribeResourcePolicies` and `logs:DescribeLogGroups`, all with required
  `Resource: *`. Acceptance observes payload-free logs and denies any omitted action;
- candidate-manifest tests require release ID/source SHA, processor GHCR digest
  and expected ECR destination/repository while rejecting ECS revision ARNs and
  a promoted ECR digest. Phase-A exact OpenTofu plan/apply registers each
  revision once from verified `ECR_URI@sha256`, dual-authorizes old/new
  `ecs:RunTask` ARNs without routing changes, waits for/revalidates IAM
  propagation and proves both authorized. One signed activation manifest binds
  candidate manifest SHA-256, release ID, source SHA, revision ARNs, verified
  digest and phase-A evidence;
- phase-B exact OpenTofu plan/apply consumes that signed manifest and switches
  only the state-machine target while both Schedulers remain prior.
  API/frontend verify the binding. Tests reject artifact
  mixing, out-of-band mutation and `ignore_changes`, then prove selected exact
  digests, clean state and no drift. Fault injection leaves a partial apply fenced
  until a diagnosed, reviewed plan converges all routes; mixed routes never reopen;
- phase C follows both canaries; separate reviewed applies accept recovery, then audit,
  while fenced. Faults converge that route; reopening requires both accepted;
- canary tests seed a release-bound pending event and require exact revision/overrides,
  then boundedly poll its exact GSI before dispatch; timeout fails closed. They require
  its COMPLIANCE object, retention and delivered marker. Seeder `PutItem` is table-only.
  Verifier IAM allows exact-GSI `Query`, conditioned `GetItem`, bound `GetObject` plus
  `GetObjectRetention`; `PassRole` is exact canary/ECS; other releases denied; roles expire;
- promotion tests atomically close the fence before phase B only when the unit
  semaphore is idle and no dispatch is in flight, then reject tenant/recovery
  starts and allow only the bound canary. Local smoke precedes Nginx; tunneled smoke
  uses the production hostname after that switch. The entire successful fence through
  both phase-C acceptances is capped at 30 minutes; routine cutoff restores routes
  within budget. Partial phase-B/C applies remain fail-closed until reviewed convergence;
- drain/prune tests retain old revisions and `ecs:RunTask` grants through active
  old Standard/recovery drain and rollback retention, then prune only older
  non-rollback revisions; rollback consumes the prior retained manifest through
  a reviewed exact OpenTofu plan/apply;
- recovery validation enforces the separate `recover-once` definition/mode,
  same image/network, coordinator-only composition without normal variables or
  audit sink, production lease 3600, 30-minute cadence, batch 100 and
  `MaximumRetryAttempts=0`. Its PID 1 hard wall-clock
  deadline equal to the lease that logs/alarms, cancels work, exits nonzero and
  stops the ECS task; overlapping, externally retried or at-least-once duplicate
  invocations have no global concurrency ceiling;
- unit-task tests enforce hard termination no later than the non-renewable lease;
- profile `durationSeconds=3600`, helper `--session-duration 3600` and role
  `MaxSessionDuration=3600` are asserted;
  the incident drill disables new sessions, updates CRL, revokes old sessions
  and proves the timed Deny before re-enabling a rotated leaf;
- gate-port tests cover typed acquire/bind/renew/release/close/reopen and conditional
  adapter transactions. API/recovery use runtime variants; promotion assumes only the
  operator with strong reads of fence/semaphore and close/reopen only on fence. Direct
  semaphore writes are denied. Races prove close-first blocks acquire before drain;
  expired takeover needs liveness proof; losers never start and get `429`/quota;
- cost tests count billed pull/start/run/stop. Two minutes per 30-minute recovery and
  audit budget 48+48=96 hours/30 days leaves 4 for units; excess is alarmed. Tests assert
  same-account `ExecutionRoleArn`, exact Budget trust and `iam:AttachRolePolicy` limited
  by target role/`iam:PolicyARN`; freeze, audit continuity and cost-clear remain proven;
- execution-quota tests atomically count every initial and recovery
  `StartExecution` attempt against one 200-attempt monthly maximum and reject
  both callers when exhausted;
- recovery-role tests scope start/describe-machine to the exact production
  machine, describe/stop-execution to its executions and ECS liveness to the
  configured cluster/task family or required narrow `Resource: *` conditions.
  Cancellation/failed binding proves stop succeeds; another machine is denied;
- audit IAM tests allow outbox/cursor `LeadingKeys` and audit lock/get/put/retention;
  deny run/fence/semaphore keys, other tables/indexes, scan/delete/list/bypass;
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
  base and exact `/api/v1/activate/confirm`; the legacy origin route and relative
  `/api` are absent. Bearer is API-origin only and tenant header tenant-call only;
- a Step Functions execution starts only the approved Fargate task definition,
  uses only the exact source account/state-machine trust, event rule and pass
  roles above, and fails when any scope drifts;
- phase-A promotion evidence proves propagated old/new `ecs:RunTask`
  authorization without routing changes, then the signed activation manifest
  binds candidate manifest SHA-256, release ID, source SHA, exact ARNs/digest
  and phase-A evidence. Phase-B OpenTofu evidence verifies that binding and
  proves phase B selected unit while both Schedulers remained prior. After both
  canaries, separate phase-C applies accept recovery then audit while fenced. Partial
  applies converge that route; both accepted to reopen. Executions keep revisions;
- promotion evidence proves the fence closed before phase B, no dispatch was in
  flight, local smoke passed before switching, and tunneled smoke reached the
  candidate through the switched production hostname. Only the bound canary is
  admitted before acceptance; failure restores prior routes while fenced;
- independent `recover-once` and `dispatch-outbox-once` runs use their exact
  images/networks/roles and zero Scheduler retries. A forced recovery failure and
  budget freeze do not stop bounded audit delivery to an Object-Locked object;
- a deliberately hung recovery reaches the PID 1 deadline equal to
  `AWS_PROCESSOR_LEASE_SECONDS`, cancels, exits nonzero and stops in ECS by that
  deadline, with log and alarm evidence;
- two distinct runs plus overlapping, externally retried or at-least-once
  duplicate recovery passes leave at most one unit Fargate task active;
  semaphore losers make no
  `StartExecution`, while dispatch CAS remains limited to same-run recovery and
  each recovery pass is individually bounded with no global recovery-concurrency
  ceiling. Every unit, recovery and audit-dispatch task-hour counts toward the
  monitored 100-hour target, not a pre-launch API limit;
- one synthetic run publishes exactly one new immutable version/pointer;
- the authenticated tenant-authorized call first completes the serving-key metadata stat;
  with `X-Tenant-Id` it returns `200`, `Cache-Control: private, no-store` and only
  `url`, `version_id` and `expires_in=300`, with no tenant or object-key field;
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
  ceiling; zero idle desired count; and a monitored 100 combined unit, recovery
  and audit-dispatch task-hours monthly target, including every invocation;
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
