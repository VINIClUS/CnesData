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

A candidate release is bound to one green `develop` commit and records:

- source SHA and test run IDs;
- API GHCR digest;
- processor GHCR and promoted ECR digests;
- dashboard artifact checksum;
- Compose/config checksum;
- OpenTofu/provider lock digest;
- SBOM checksums and schema version.

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

1. promote/verify the processor digest in ECR;
2. update the inactive API candidate on the VPS;
3. pass local and tunneled health/authorization smoke tests;
4. switch Nginx to the candidate;
5. publish frontend assets and entrypoint;
6. run the synthetic vertical slice and serving test;
7. record redacted evidence and retain the previous release.

No production deployment occurs automatically after merge.

## 3. Rollback

- **API:** restore the previous Nginx upstream and already-pulled GHCR digest.
- **Frontend:** restore the prior versioned entrypoint and invalidate it.
- **Processor:** register/select the previous task definition/digest for new
  dispatches; active Standard executions continue with the definition they
  started unless the runbook determines cancellation is safe.
- **State machine:** revert only new-execution routing; do not edit an active
  execution or DynamoDB record manually.
- **DynamoDB/S3:** application releases never roll data backward or delete
  published versions. Compatibility changes are expand/contract and additive.
- **Infrastructure:** failed apply requires diagnosis and a new reviewed plan;
  protected-resource restore is not an automatic rollback step.

## 4. Rate limiting and abuse containment

- Cloudflare Free rule protects the most sensitive public job/auth surface.
- Nginx enforces IP-based general and expensive-mutation zones.
- The API enforces tenant/user/idempotency quotas and maximum 200 jobs,
  100 task-hours and one concurrent processor task per month/environment.
- DynamoDB maximum throughput, Step Functions bounded retries and Athena bytes
  cutoffs contain downstream amplification.
- Requests beyond product quota fail with `429` or the documented quota error
  before starting Step Functions or Athena.
- A USD 15 aggregate budget action freezes new Fargate, Step Functions and
  Athena starts through automation roles while preserving reads and backups.

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
- long-lived boto3/botocore clients cross at least one IAM Roles Anywhere
  `credential_process` expiration/refresh and complete an AWS call without a
  process or container restart; helper or refresh failure fails closed;
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
  reads the expected JSON body from the 300-second signed URL;
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
- one Fargate task, zero idle desired count, 100 task-hours maximum;
- Step Functions Standard and 200 executions maximum;
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
