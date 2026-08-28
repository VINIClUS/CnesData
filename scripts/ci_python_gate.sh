#!/usr/bin/env bash
set -euo pipefail

gate_openapi="$(mktemp)"
gate_contracts="$(mktemp -d)"
trap 'rm -f "$gate_openapi"; rm -rf "$gate_contracts"' EXIT

uv run python scripts/gen_openapi.py --output "$gate_openapi"
diff -u docs/contracts/openapi.json "$gate_openapi"
uv run python scripts/gen_contracts.py --output "$gate_contracts/"
diff -ru docs/contracts/schemas/ "$gate_contracts/"
uv run ruff check .
(
  cd packages/cnes_infra
  uv run alembic -c alembic.ini upgrade head
)
uv run pytest packages/cnes_domain packages/cnes_infra \
  -m "not bigquery and not e2e and not stress and not soak and not spike" \
  --cov --cov-config=pyproject.toml --cov-report=term-missing
uv run pytest apps/ \
  -m "not integration and not bigquery and not e2e and not stress and not soak and not spike \
and not windows_only" \
  --cov --cov-config=.coveragerc --cov-report=term-missing
