"""Manifesto canônico da árvore rastreada do produto em S para o gate de aplicação."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import subprocess
from pathlib import Path
from shutil import which
from typing import TYPE_CHECKING

from release.verify_candidate import require
from scripts.promotion._canonical import canonical_bytes

if TYPE_CHECKING:
    from collections.abc import Sequence

MEMBER = "product-source-manifest.json"
ADAPTER = "scripts/promotion/promote_api.py"
FILE_LIMIT = 8 * 1024**2
AGGREGATE_LIMIT = 128 * 1024**2
TREE_LIMIT = 4 * 1024**2
MANIFEST_LIMIT = 4 * 1024**2
MAX_FILES = 16384
MODES = frozenset({"100644", "100755"})
NAME = re.compile(r"[A-Za-z0-9_.@/-]+")
COMMIT = re.compile(r"[0-9a-f]{40}")
FAILURES = (ValueError, OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired)

logger = logging.getLogger(__name__)


def _git(checkout: Path, *args: str, stdin: bytes | None = None) -> bytes:
    return subprocess.run(
        [which("git") or "git", *args],
        cwd=checkout,
        input=stdin,
        capture_output=True,
        check=True,
        timeout=120,
    ).stdout


def _entry(record: bytes) -> tuple[str, str]:
    metadata, encoded = record.split(b"\t", 1)
    mode, kind, oid = metadata.decode("ascii").split(" ")
    name = encoded.decode("utf-8")
    require(mode in MODES and kind == "blob", f"product_source=entry path={name} mode={mode}")
    require(NAME.fullmatch(name) and ".." not in name.split("/"),
            f"product_source=path path={name!r}")
    return name, oid


def _inventory(checkout: Path, source_sha: str) -> list[tuple[str, str]]:
    tree = _git(checkout, "ls-tree", "-r", "-z", source_sha)
    require(tree.endswith(b"\0") and len(tree) <= TREE_LIMIT, "product_source=tree_size")
    entries = [_entry(record) for record in tree[:-1].split(b"\0")]
    require(len(entries) <= MAX_FILES, f"product_source=file_count count={len(entries)}")
    require(any(name == ADAPTER for name, _ in entries), "product_source=adapter_missing")
    return entries


def _batch(oids: list[str]) -> bytes:
    return "".join(f"{oid}\n" for oid in oids).encode("ascii")


def _sizes(checkout: Path, entries: list[tuple[str, str]]) -> list[int]:
    output = _git(checkout, "cat-file", "--batch-check=%(objectsize)",
                  stdin=_batch([oid for _, oid in entries]))
    sizes = [int(token) for token in output.split()]
    require(len(sizes) == len(entries), "product_source=size_inventory")
    for (name, _), size in zip(entries, sizes, strict=True):
        require(size <= FILE_LIMIT, f"product_source=file_size path={name} size={size}")
    require(sum(sizes) <= AGGREGATE_LIMIT, f"product_source=aggregate_size size={sum(sizes)}")
    return sizes


def _digests(checkout: Path, entries: list[tuple[str, str]], sizes: list[int]) -> dict[str, str]:
    output = _git(checkout, "cat-file", "--batch=%(objectsize)",
                  stdin=_batch([oid for _, oid in entries]))
    files, offset = {}, 0
    for (name, _), size in zip(entries, sizes, strict=True):
        start = output.index(b"\n", offset) + 1
        require(output[offset:start - 1] == str(size).encode("ascii")
                and output[start + size:start + size + 1] == b"\n", "product_source=blob_stream")
        files[name] = hashlib.sha256(output[start:start + size]).hexdigest()
        offset = start + size + 1
    require(offset == len(output), "product_source=blob_stream")
    return files


def build_manifest(checkout: Path, source_sha: str) -> bytes:
    """Args: checkout git em S e o SHA completo S.

    Returns: JSON canônico `{version, source_sha, files}` com o sha256 de cada blob rastreado.
    Raises: ValueError para árvore que authenticate_adapter_closure recusaria.
    """
    require(type(source_sha) is str and COMMIT.fullmatch(source_sha), "product_source=source_sha")
    head = _git(checkout, "rev-parse", "--verify", "HEAD^{commit}").decode("ascii").strip()
    require(head == source_sha, "product_source=checkout_head")
    entries = _inventory(checkout, source_sha)
    files = _digests(checkout, entries, _sizes(checkout, entries))
    return canonical_bytes({"version": 1, "source_sha": source_sha, "files": files}, MANIFEST_LIMIT)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Gera o manifesto de fonte do produto em S.")
    parser.add_argument("--checkout", type=Path, default=Path.cwd())
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--output", required=True, type=Path)
    return parser


def _write(output: Path, data: bytes) -> None:
    require(output.name == MEMBER, f"output=member_name name={output.name}")
    require(not output.exists(), "output=already_exists")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(data)


def main(argv: Sequence[str] | None = None) -> int:
    """Returns: 0 com o membro gravado; 2 em qualquer recusa, sem gravar a saída."""
    arguments = _parser().parse_args(argv)
    try:
        data = build_manifest(arguments.checkout, arguments.source_sha)
        _write(arguments.output, data)
    except FAILURES as error:
        logger.error("product_source_manifest=failed reason=%s", error)
        return 2
    logger.info("product_source_manifest=written sha256=%s files=%d bytes=%d",
                hashlib.sha256(data).hexdigest(), len(json.loads(data)["files"]), len(data))
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    raise SystemExit(main())
