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

SERVER_ZIP = Path("docs/fixtures/firebird/Firebird-1.5.6.5026-0_win32.zip")
SERVER_CACHE_DIR = Path(".cache/firebird-1.5.6-server")
SERVER_EXPECTED_SHA256 = (
    "4b718a918af7c65ff08a69b38a720200397221b856b5c2517d509b08a62ad16f"
)


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


def _setup_variant(
    *, zip_path: Path, cache_dir: Path, expected_sha: str,
    force: bool, verify_only: bool, alias_client: bool,
) -> None:
    verify_zip(zip_path, expected_sha)
    if verify_only:
        logger.info("fb156_setup: verify_ok sha=%s", expected_sha)
        return

    actual_sha = compute_sha256(zip_path)
    if not force and is_cache_valid(cache_dir, actual_sha):
        logger.info("fb156_setup: cache_valid path=%s", cache_dir)
        return

    if cache_dir.exists():
        import shutil
        shutil.rmtree(cache_dir)
    try:
        extract_zip(zip_path, cache_dir)
        if alias_client:
            create_fbclient_alias(cache_dir)
        write_marker(cache_dir, actual_sha)
    except Exception:
        if cache_dir.exists():
            import shutil
            shutil.rmtree(cache_dir)
        raise

    logger.info("fb156_setup: ready path=%s", cache_dir)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true",
                        help="re-extract even if cache valid")
    parser.add_argument("--verify-only", action="store_true",
                        help="only verify SHA, skip extract")
    parser.add_argument("--server", action="store_true",
                        help="setup server edition (fbserver.exe) instead of embedded")
    parser.add_argument("--expected-sha", default=None,
                        help="override expected SHA (default: constant for chosen variant)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if args.server:
        _setup_variant(
            zip_path=SERVER_ZIP,
            cache_dir=SERVER_CACHE_DIR,
            expected_sha=args.expected_sha or SERVER_EXPECTED_SHA256,
            force=args.force,
            verify_only=args.verify_only,
            alias_client=False,
        )
    else:
        _setup_variant(
            zip_path=FIXTURE_ZIP,
            cache_dir=CACHE_DIR,
            expected_sha=args.expected_sha or EXPECTED_SHA256,
            force=args.force,
            verify_only=args.verify_only,
            alias_client=True,
        )


if __name__ == "__main__":
    main()
