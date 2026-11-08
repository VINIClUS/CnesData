#!/usr/bin/env bash
set -euo pipefail

artifact_dir="$(mktemp -d)"
trap 'rm -rf "$artifact_dir"' EXIT

for manifest in packages/*/pyproject.toml; do
  package="${manifest%/pyproject.toml}"
  uv build --wheel --out-dir "$artifact_dir/wheels" "$package"
done
uv pip install --no-deps --target "$artifact_dir/installed" "$artifact_dir"/wheels/*.whl
uv run --no-build python scripts/verify_package_wheels.py "$artifact_dir"
