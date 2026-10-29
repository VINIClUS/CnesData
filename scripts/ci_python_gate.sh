#!/usr/bin/env bash
set -euo pipefail
export PYTHONUNBUFFERED=1

gate_openapi="$(mktemp)"
gate_contracts="$(mktemp -d)"
trap 'rm -f "$gate_openapi"; rm -rf "$gate_contracts"' EXIT

run_phase() {
  local phase="$1" started ended external_rc
  shift
  started="$(date +%s)"
  printf 'phase=%s event=start timestamp=%s\n' "$phase" "$started"
  if "$@"; then
    external_rc=0
  else
    external_rc=$?
  fi
  ended="$(date +%s)"
  printf 'phase=%s event=end timestamp=%s elapsed=%s external_rc=%s\n' \
    "$phase" "$ended" "$((ended - started))" "$external_rc"
  return "$external_rc"
}

uv run --no-build python scripts/gen_openapi.py --output "$gate_openapi"
diff -u docs/contracts/openapi.json "$gate_openapi"
uv run --no-build python scripts/gen_contracts.py --output "$gate_contracts/"
diff -ru docs/contracts/schemas/ "$gate_contracts/"
uv run --no-build ruff check .
uv run --no-build pyright
(
  cd packages/cnes_infra
  uv run alembic -c alembic.ini upgrade head
)
run_phase packages timeout --signal=INT 900 uv run --no-build pytest \
  packages/cnes_domain packages/cnes_infra \
  -m "not bigquery and not e2e and not stress and not soak and not spike" \
  --cov --cov-config=pyproject.toml --cov-report=term-missing \
  --cov-report=xml:coverage-packages.xml -rs
run_phase apps timeout --signal=INT 300 uv run --no-build pytest apps/ \
  -m "not integration and not bigquery and not e2e and not stress and not soak and not spike \
and not windows_only" \
  --cov --cov-config=.coveragerc --cov-report=term-missing \
  --cov-report=xml:coverage-apps.xml -rs
