# CnesData audited backlog execution plan

> For agentic workers: use subagent-driven development, tests before implementation,
> independent test-utility review, then independent code and behavior review.

**Goal:** Deliver the executable open backlog, preserving evidence and the production write fence.
**Architecture:** Integrate serially into develop; parallelize independent investigations and
operational evidence. Preserve live features before retiring PostgreSQL/MinIO/BigQuery/Keycloak.
**Tech stack:** Python 3.13, uv, Go, Bun/React, SQLite/DynamoDB, AWS, OpenTofu and Ansible.
**Spec:** User-approved plan in this conversation, 2026-10-25; existing production and migration
specifications remain binding except where the explicit decisions below amend them.

## Global constraints

- Baseline: origin/develop f87d42bdb56ff10e755c67d7ad26b46528889379; revalidate before integration.
- Rebase active work only. Preserve existing worktrees, commits and untracked files.
- Branches use codex/. Never rewrite main/develop or bypass required checks.
- Every issue: plan, RED tests, independent utility audit, implementation, independent code audit.
- Evidence records author/reviewer identity, immutable SHAs, commands and RED/GREEN outcomes.
- Changes after review invalidate the corresponding review and checks.
- AWS account 836651842853; us-east-2 primary, us-east-1 global edge.
- Product monthly cost envelope USD 8; aggregate-account forecast freeze USD 15; paid Actions USD 0.
- BILLING_MODE=disabled. Do not reopen the completed billing epic.
- Cognito ESSENTIALS, managed login v2 and branding; preserve API-origin aud verification.
- No real production agent certificates until official IANA PEN allocation; issue 60 stays open.
- Python packages 100% branch, apps 90% line, Go filtered coverage 65%; unexpected skips fail.
- Use repository CLAUDE.md rules; Portuguese behavioral test names; boundary mocks only.
- Merges and deployments are authorized after independent audit and green checks.

## Review focus

- Late dispatch binding must not accept wrong dispatches, partial completion or foreign units.
- Membership discovery must not broaden authorization or leak another principal/tenant's cache.
- Freeze must preserve audit, backup and health and never affect another project's roles.
- Ephemeral AWS resources need cleanup after abrupt interruption as well as ordinary failures.
- Legacy deletion requires fresh write-fence, active-data inventory and restorable snapshot evidence.

## Execution tasks

### Task 1: Fix deterministic late-dispatch binding race (issue 413)

- Reproduce synchronous completion before bind using real SQLite control-plane behavior.
- Cover terminal success/failure, partial completion, wrong dispatch and incompatible units.
- Audit tests before production edits; implement the minimal safe late-binding behavior.
- Verify planner, control-plane and executor suites, negative controls and historical shadow.

### Task 2: Complete migration evidence (issues 278 and 279)

- Obtain a 15-minute non-root STS federated session restricted to ListBucket on cnesdata-landing.
- Keep credentials in memory; use bounded complete pagination and fail on expiration/incompletion.
- Record count, bytes, max LastModified and redacted identity; verify retained snapshot evidence.
- Independent auditor validates evidence before closing either issue as completed.

### Task 3: Complete real-AWS capability harness (issue 398)

- Use CLI access, unique execution prefixes/tags, synthetic data, bounded runtime and janitor.
- Test Cognito/DynamoDB/S3/IAM semantics; harness Object Lock uses GOVERNANCE only.
- No production apply. Require zero residual resources after success or failure.

### Task 4: Correct Cognito and finish PR 415 (issue 381)

- Replace Lite/classic UI with Essentials/managed-login v2 plus branding.
- Prove authorization-code PKCE, API aud and preserved aud after refresh on an ephemeral pool.
- Retain pool/client protections; update specs and cost assumptions before independent review.

### Task 5: Implement tenant discovery and AWS session identity (issue 410)

- GET /api/v1/dashboard/memberships returns tenant_id, municipality_name and role.
- Query paginated GSI candidates internally, deduplicate/sort, strongly revalidate with the
  existing MembershipAuthorizer; no Scan and no expansion of existing access rules.
- AWS /api/v1/dashboard/auth/me returns subject, issuer, nullable email and display_name.
- Declare the AWS/legacy response alternatives in OpenAPI; preserve legacy/local contracts.
- Exempt only these two GET paths from tenant-header requirements; valid Bearer remains required.
- No-store responses; no grants is 200/empty, invalid token 401, persistence unavailable 503.
- Update production guard, tenant selection/cache isolation and generated Go/TypeScript clients.

### Task 6: Cursor outbox delivery (issue 374)

- Implement the governing cursor/CAS contract in SQLite and DynamoDB, including poison retry,
  strong revalidation, paging and crash idempotency; dispatch uses Query/GetItem/UpdateItem.

### Task 7: Separate worker entrypoints (issue 375)

- Compose run-unit, recover-once and dispatch-outbox-once independently with separate settings,
  deadlines and terminal exit codes; AWS API does not own an outbox dispatch loop.

### Task 8: Runtime amendment acceptance gate (issue 377)

- Aggregate integrated runtime, Floci and discovery contracts; no secrets, apply or silent skips.
- Add production-runtime-acceptance to required checks after its verified introduction.

### Task 9: Private static delivery (issue 382)

- Implement OAC, exact CSP/rewrite, edge ACM/WAF and approved CloudFront Free contract.

### Task 10: ECR, runtime IAM and immutable ECS families (issue 384)

- Separate runtime roles/families, enforce the public-IP zero-ingress profile, immutable images
  and read-only root filesystem with explicit writable volumes.

### Task 11: Step Functions and service IAM (issue 385)

- Implement the approved ASL and IAM contracts with both validators and negative permission tests.

### Task 12: Recovery and audit schedulers (issue 386)

- Separate schedules/roles, retry zero, bounded deadlines/leases and maintenance cost accounting.

### Task 13: Canary and promotion fence IAM (issue 387)

- Enforce exact keys/resources, least privilege and the canary/fence/dispatch permission matrix.

### Task 14: Athena, alarms and costs (issue 388)

- Implement executable SNS/Lambda Athena cutoff, OPA, dated min/base/max cost manifest and role
  outputs. Include public IPv4, state, KMS, Budget Actions and every auxiliary cost.

### Task 15: Infrastructure acceptance gate (issue 389)

- Validate integrated product graph and shared-output ownership with negative fixtures and CI=true.
- Add production-infrastructure-acceptance to required checks; CI never applies infrastructure.

### Task 16: Minimal shared bootstrap, budget/freeze and delivery

- Create private VINIClUS/personal-infra-live for Budget/freeze and its dedicated state backend.
- Import existing cnesdata-monthly-cap and reconcile forecast to USD 15; do not import/change
  My Monthly Cost Budget. Explicit CnesData role allowlist excludes audit/backup/health.
- infra-ansible/private inventory owns host/deployer/bootstrap. Bootstrap locally with authorized
  credentials, then use temporary narrowly scoped identities; never put root credentials in CI.
- Create separate owned backends with locks/backups, required KMS/OIDC/Roles Anywhere and outputs;
  never use LimnoPulse state. Check tags/digests before adopting resources and resume partial work.
- Break bootstrap dependencies in stages: shared identities/backend, budget action targeting
  existing promotion roles, disabled product infrastructure, full freeze-role reconciliation,
  policy simulation, then enable runtime only after all readiness checks pass.
- Replace stale shared-host-gate references with explicit CnesData/infra-ansible contracts.
- Publish exact scanned images, signed manifests/binary plans, locks/expiry/checksums and rollback
  evidence according to production delivery specifications. Deploy through OIDC and trusted SSH.

### Task 17: Replace live legacy features before deletion (issue 280 prerequisites)

- Record feature/adapter/contract/test/evidence/removal matrix as subtasks in issue 280.
- Preserve certificates by serial/fingerprint, overlap rotation and revocation; hashed refresh
  tokens with TTL/CAS; user/admin authorization; access requests; public leads and audit behavior.
- Add SQLite/DynamoDB ports/adapters, independent synthetic legacy-oracle fixtures and contract
  tests before rewiring routes. Migrate needed records; unexpected active data blocks deletion.

### Task 18: Serial legacy retirement (issue 280)

- Require completed target promotion, evidence 278/279, fresh zero-write/active-data inventory,
  quiesce and restorable snapshots; abort on any unexplained delta.
- First create failing absence scanner/acceptance tests and audit the concrete removal inventory.
- Remove legacy dependencies only after equivalent features pass; preserve approved specs,
  snapshots, Firebird/DBF extraction, generic OIDC and mTLS identity.
- Regenerate lockfile; coordinate migrator check removal with its replacement absence gate.

### Task 19: Final local/AWS matrix (issue 281)

- Verify target-only manifests/hashes, exact equivalence, no SQL/MinIO fallback and no serving PII.

### Task 20: Reconcile trackers and close completed work (issues 365, 96 and 93)

- Update roadmap/readiness/current SHAs and linked evidence. Close only complete checklists with
  state_reason=completed; keep externally blocked items honestly open.

### Task 21: Official PEN dependency (issue 60)

- Document missing IANA allocation and certificate rotation requirements. Do not invent a PEN,
  send an external application without explicit instructions, or issue production certificates.
