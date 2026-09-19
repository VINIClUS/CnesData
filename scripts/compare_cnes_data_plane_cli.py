"""CLI do gate golden/shadow — driver de execução, relatório e códigos de saída.

Uso: python scripts/compare_cnes_data_plane_cli.py --fixtures docs/fixtures/data-plane \
    --output docs/baselines/cnes-local-shadow-report.json --mode {golden,shadow}
"""

from __future__ import annotations

import argparse
import logging
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any
from uuid import uuid4

if __package__ in (None, ""):  # `python scripts/compare_cnes_data_plane_cli.py` direto
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cnes_infra import config
from cnes_infra.object_store.filesystem import FilesystemObjectStore
from scripts.compare_cnes_data_plane import (
    APPROVED_RULES,
    ComparisonDifference,
    UnexplainedDifference,
    _read_manifest,
    _write_json,
    compare_outputs,
    materialize_actual_directory,
    materialize_expected_directory,
    require_explained,
    run_vertical_slice,
)

if TYPE_CHECKING:
    from cnes_domain.ports.object_store import ObjectStorePort

LOGGER = logging.getLogger(__name__)


def _shadow_store() -> ObjectStorePort:
    import boto3
    from botocore.exceptions import ClientError

    from cnes_infra.object_store.s3 import S3ObjectStore

    scheme = "https" if config.MINIO_SECURE else "http"
    client = boto3.client(
        "s3", endpoint_url=f"{scheme}://{config.MINIO_ENDPOINT}",
        aws_access_key_id=config.MINIO_ACCESS_KEY, aws_secret_access_key=config.MINIO_SECRET_KEY,
        region_name="us-east-1",
    )
    try:
        client.create_bucket(Bucket=config.MINIO_BUCKET)
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code", "")
        if code not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
            raise
    return S3ObjectStore(client, config.MINIO_BUCKET, prefix="compare-cnes-data-plane")


def _build_store(mode: str, tmp_dir: Path) -> ObjectStorePort:
    if mode == "golden":
        return FilesystemObjectStore(tmp_dir / "store")
    return _shadow_store()


def _current_commit_sha() -> str:
    git = shutil.which("git")
    if git is None:
        return "unknown"
    args = (git, "rev-parse", "HEAD")
    completed = subprocess.run(args, capture_output=True, text=True, check=False)
    return completed.stdout.strip() or "unknown"


def _build_report(
    fixtures: Path, mode: str, differences: tuple[ComparisonDifference, ...]
) -> dict[str, Any]:
    manifest = _read_manifest(fixtures)
    layers = sorted({difference.layer for difference in differences})
    return {
        "mode": mode,
        "commit_sha": _current_commit_sha(),
        "recorded_at": datetime.now(UTC).isoformat(),
        "fixture_sha256": {n: m["sha256"] for n, m in manifest.get("files", {}).items()},
        "difference_count": len(differences),
        "differences_by_layer": {
            layer: sum(1 for d in differences if d.layer == layer) for layer in layers
        },
        "approved_rules_used": sorted({d.rule for d in differences if d.rule}),
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixtures", type=Path, default=Path("docs/fixtures/data-plane"))
    default_output = Path("docs/baselines/cnes-local-shadow-report.json")
    parser.add_argument("--output", type=Path, default=default_output)
    parser.add_argument("--mode", choices=("golden", "shadow"), default="golden")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Roda o vertical slice sobre a fixture e falha caso haja diferença não aprovada."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _parse_args(argv)
    with TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        store = _build_store(args.mode, tmp_dir)
        run_id = f"compare-{args.mode}-{uuid4().hex[:8]}"
        artifacts = run_vertical_slice(args.fixtures, store, run_id)
        expected_dir, actual_dir = tmp_dir / "expected", tmp_dir / "actual"
        materialize_expected_directory(args.fixtures, expected_dir)
        materialize_actual_directory(store, artifacts, actual_dir)
        differences = compare_outputs(expected_dir, actual_dir)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        _write_json(args.output, _build_report(args.fixtures, args.mode, differences))
        try:
            require_explained(differences, APPROVED_RULES)
        except UnexplainedDifference as error:
            LOGGER.error("compare_cnes status=unexplained mode=%s error=%s", args.mode, error)
            return 1
    LOGGER.info("compare_cnes status=ok mode=%s differences=%d", args.mode, len(differences))
    return 0


if __name__ == "__main__":
    sys.exit(main())
