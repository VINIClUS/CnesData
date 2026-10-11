# Production readiness dependency gate

## Tested dependency head

The dependency gate was materialized against the exact integrated `develop` SHA
`468b855ab1de5cae2b94420dc8a62c2b3e706c41` on 2026-10-11. The gate confirms that the
CND Phase 2 and AWS runtime contracts consumed by the production amendments are integrated. It
does not claim that the AWS infrastructure described by the later production plans is deployed.

## Integrated dependency lineage

The CND Phase 2 chain is closed and merged:

| Logical ID | Issue | Implementation PR |
|---|---|---|
| CND-020 | [#122][issue-122] | [#131][pr-131] |
| CND-021 | [#123][issue-123] | [#132][pr-132] |
| CND-022 | [#124][issue-124] | [#130][pr-130] |
| CND-023 | [#125][issue-125] | [#135][pr-135] |
| CND-024 | [#126][issue-126] | [#136][pr-136] |
| CND-025 | [#127][issue-127] | [#138][pr-138] |

The AWS runtime chain and serial acceptance gate are also closed and merged:

| Logical ID | Issue | Implementation PR |
|---|---|---|
| AWS-010 | [#302][issue-302] | [#311][pr-311] |
| AWS-011 | [#303][issue-303] | [#312][pr-312] |
| AWS-012 | [#304][issue-304] | [#309][pr-309] |
| AWS-013 | [#305][issue-305] | [#310][pr-310] |
| AWS composition | [#306][issue-306] | [#313][pr-313] |
| AWS-014 | [#307][issue-307] | [#315][pr-315] |
| AWS Gate | [#308][issue-308] | [#316][pr-316] |

[issue-122]: https://github.com/VINIClUS/CnesData/issues/122
[issue-123]: https://github.com/VINIClUS/CnesData/issues/123
[issue-124]: https://github.com/VINIClUS/CnesData/issues/124
[issue-125]: https://github.com/VINIClUS/CnesData/issues/125
[issue-126]: https://github.com/VINIClUS/CnesData/issues/126
[issue-127]: https://github.com/VINIClUS/CnesData/issues/127
[issue-302]: https://github.com/VINIClUS/CnesData/issues/302
[issue-303]: https://github.com/VINIClUS/CnesData/issues/303
[issue-304]: https://github.com/VINIClUS/CnesData/issues/304
[issue-305]: https://github.com/VINIClUS/CnesData/issues/305
[issue-306]: https://github.com/VINIClUS/CnesData/issues/306
[issue-307]: https://github.com/VINIClUS/CnesData/issues/307
[issue-308]: https://github.com/VINIClUS/CnesData/issues/308
[pr-130]: https://github.com/VINIClUS/CnesData/pull/130
[pr-131]: https://github.com/VINIClUS/CnesData/pull/131
[pr-132]: https://github.com/VINIClUS/CnesData/pull/132
[pr-135]: https://github.com/VINIClUS/CnesData/pull/135
[pr-136]: https://github.com/VINIClUS/CnesData/pull/136
[pr-138]: https://github.com/VINIClUS/CnesData/pull/138
[pr-309]: https://github.com/VINIClUS/CnesData/pull/309
[pr-310]: https://github.com/VINIClUS/CnesData/pull/310
[pr-311]: https://github.com/VINIClUS/CnesData/pull/311
[pr-312]: https://github.com/VINIClUS/CnesData/pull/312
[pr-313]: https://github.com/VINIClUS/CnesData/pull/313
[pr-315]: https://github.com/VINIClUS/CnesData/pull/315
[pr-316]: https://github.com/VINIClUS/CnesData/pull/316

Historical checks prove the dependency changes at their own reviewed or integrated heads, not at
the current SHA:

- CND-025 PR #138 run
  [34031786531](https://github.com/VINIClUS/CnesData/actions/runs/34031786531) covered PR head SHA
  `7e04e27776053b568819e64c5f2e2b909a8c00bf`.
- The integrated `develop` conformance run
  [34031957092](https://github.com/VINIClUS/CnesData/actions/runs/34031957092) covered merge SHA
  `80e59f218031891c9aedd5d338bd69c3b57c9f09`.
- AWS-014 PR CI run
  [36350827087](https://github.com/VINIClUS/CnesData/actions/runs/36350827087) covered PR #315.
- AWS Gate PR run
  [36445090387](https://github.com/VINIClUS/CnesData/actions/runs/36445090387) included the
  `aws-runtime-integration` job for PR #316.

## Current hosted evidence

Both hosted runs completed successfully on
`468b855ab1de5cae2b94420dc8a62c2b3e706c41`:

- `python-quality` run
  [38136816128](https://github.com/VINIClUS/CnesData/actions/runs/38136816128):
  `COMPLETED/SUCCESS`; both `phase2-adapter-conformance` and
  `aws-runtime-integration` succeeded.
- `Deploy develop` run
  [38137112387](https://github.com/VINIClUS/CnesData/actions/runs/38137112387):
  `COMPLETED/SUCCESS`; `gate / lint-test-coverage` succeeded in 7m04s, all four image builds
  succeeded, and the `deploy` and `smoke` jobs succeeded.

The deploy run validates the active VPS profile. It is evidence that the exact dependency head is
green through the current deployment gate, not evidence that the AWS target profile is deployed.

## Current local evidence

The worktree had no `.env`. Commands that import `cnes_infra.config` therefore received the public
`DB_URL` placeholder used by `python-quality.yml`; the AWS SDK still used only test provider-chain
credentials and emulator endpoints.

```bash
uv run pytest -q tests/production/test_dependency_gate.py

docker compose --profile aws-test up -d --wait dynamodb-local localstack
DB_URL=postgresql+psycopg://cnesdata:cnesdata_test@localhost:5433/cnesdata_test \
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
AWS_ENDPOINT_URL=http://127.0.0.1:4566 \
DYNAMODB_ENDPOINT_URL=http://127.0.0.1:18000 \
  uv run --no-build pytest -m "dynamodb_local and s3_integration" tests/integration/aws
docker compose --profile aws-test down -v

DB_URL=postgresql+psycopg://cnesdata:cnesdata_test@localhost:5433/cnesdata_test \
  bash scripts/ci_phase2_adapters.sh

uv run ruff check tests/production
uv run ruff check .
uv run pyright
git diff --check

DB_URL=postgresql+psycopg://cnesdata:cnesdata_test@localhost:5433/cnesdata_test \
COD_MUN_IBGE=354130 ID_MUNICIPIO_IBGE7=3541307 CNPJ_MANTENEDORA=55293427000117 \
  uv run pytest \
  -m "not integration and not postgres and not bigquery and not e2e and not stress and not soak \
  and not spike and not windows_only and not s3_integration and not dynamodb_local" -q
```

Green results:

- dependency gate: `10 passed`;
- AWS-014 exact entrypoint `tests/integration/aws`: `21 passed`;
- CND-025 script: local adapter matrix `2 passed`, AWS adapter matrix `1 passed`, S3 presign
  matrix `6 passed`;
- complete fast suite: `6892 passed, 20 skipped, 203 deselected`;
- Ruff and Pyright: no findings; `git diff --check`: clean.

## CI collection boundary

`tests/production` remains a local dependency gate until issue
[#377](https://github.com/VINIClUS/CnesData/issues/377) collects it in CI. The current hosted runs
validate the integrated dependencies through their existing jobs; they do not collect the new
local gate file yet.
