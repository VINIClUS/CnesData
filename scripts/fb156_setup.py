"""Setup Firebird 1.5.6 embedded client from LFS zip fixture."""
from __future__ import annotations

import argparse
import hashlib
import logging
import os
import sys
import zipfile
from pathlib import Path

logger = logging.getLogger(__name__)

FIXTURE_ZIP = Path("docs/fixtures/firebird/Firebird-1.5.6.5026-0_embed_win32.zip")
CACHE_DIR = Path(".cache/firebird-1.5.6")
EXPECTED_SHA256 = "540f6ede8ee625afdb547bb6515ed7328a3fa87cb6769a4ecbfec41c54032be7"


def compute_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def verify_zip(zip_path: Path, expected_sha: str) -> None:
    if not zip_path.exists():
        logger.error("fb156_setup: zip_missing path=%s", zip_path)
        sys.exit(1)
    size = zip_path.stat().st_size
    if size < 1_000_000:
        logger.error("fb156_setup: lfs_pull_required size=%d", size)
        sys.exit(1)
    if expected_sha:
        actual = compute_sha256(zip_path)
        if actual != expected_sha:
            logger.error(
                "fb156_setup: sha_mismatch expected=%s got=%s",
                expected_sha, actual,
            )
            sys.exit(1)


def is_cache_valid(cache_dir: Path, marker: str) -> bool:
    marker_file = cache_dir / ".sha256"
    if not marker_file.exists():
        return False
    return marker_file.read_text().strip() == marker


def extract_zip(zip_path: Path, dest: Path) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(dest)


def create_fbclient_alias(cache_dir: Path) -> None:
    fbembed = cache_dir / "fbembed.dll"
    fbclient = cache_dir / "fbclient.dll"
    if not fbembed.exists():
        candidates = list(cache_dir.rglob("fbembed.dll"))
        if candidates:
            fbembed = candidates[0]
        else:
            logger.error("fb156_setup: fbembed_not_found in=%s", cache_dir)
            sys.exit(1)
    if fbclient.exists() or fbclient.is_symlink():
        fbclient.unlink()
    try:
        os.symlink(fbembed.name, fbclient)
    except OSError:
        import shutil
        shutil.copy2(fbembed, fbclient)


def write_marker(cache_dir: Path, sha: str) -> None:
    (cache_dir / ".sha256").write_text(sha + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true", help="re-extract even if cache valid")
    parser.add_argument("--verify-only", action="store_true", help="only verify SHA, skip extract")
    parser.add_argument(
        "--expected-sha",
        default=EXPECTED_SHA256,
        help="expected SHA256 of the zip (default: embedded constant)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    verify_zip(FIXTURE_ZIP, args.expected_sha)
    if args.verify_only:
        logger.info("fb156_setup: verify_ok sha=%s", args.expected_sha)
        return

    actual_sha = compute_sha256(FIXTURE_ZIP)
    if not args.force and is_cache_valid(CACHE_DIR, actual_sha):
        logger.info("fb156_setup: cache_valid path=%s", CACHE_DIR)
        return

    if CACHE_DIR.exists():
        import shutil
        shutil.rmtree(CACHE_DIR)
    try:
        extract_zip(FIXTURE_ZIP, CACHE_DIR)
        create_fbclient_alias(CACHE_DIR)
        write_marker(CACHE_DIR, actual_sha)
    except Exception:
        if CACHE_DIR.exists():
            import shutil
            shutil.rmtree(CACHE_DIR)
        raise

    logger.info("fb156_setup: ready path=%s", CACHE_DIR)


if __name__ == "__main__":
    main()
