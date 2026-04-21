"""Anonimiza fontes CNES/SIHD para gerar seed fixtures reproduzíveis.

CPF → SHA256(cpf + salt)[:11]; nome → faker.name() com seed = sha256(orig)[:8].
Datas/IDs/valores numéricos preservados (relevantes a bug tests).

Uso:
    python scripts/seed_anonymize.py --salt <secret> --seed 42 \\
        --output docs/fixtures/shadow-seed/
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import sys
from pathlib import Path

from faker import Faker

logger = logging.getLogger(__name__)


def anonymize_cpf(cpf: str, *, salt: str) -> str:
    """SHA256(cpf+salt) truncado a 11 dígitos numéricos."""
    digest = hashlib.sha256((cpf + salt).encode()).hexdigest()
    digits = "".join(c for c in digest if c.isdigit())
    return digits[:11].ljust(11, "0")


def anonymize_nome(nome: str, *, seed: int) -> str:
    """Nome via faker reproduzível usando seed composto."""
    local_seed = seed + int(hashlib.sha256(nome.encode()).hexdigest()[:8], 16)
    fake = Faker("pt_BR")
    Faker.seed(local_seed)
    return fake.name()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def build_manifest(files: list[Path], *, salt: str, seed: int) -> dict:
    salt_hash = hashlib.sha256(salt.encode()).hexdigest()[:16]
    return {
        "salt_hash": salt_hash,
        "faker_seed": seed,
        "files": {
            p.name: {"sha256": _sha256_file(p), "bytes": p.stat().st_size}
            for p in files
        },
    }


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--salt", required=True)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--sources",
        nargs="+",
        default=["E:/CNES_local/CNES.GDB", "E:/SIHD2/BDSIHD2.GDB"],
    )
    args = parser.parse_args()

    args.output.mkdir(parents=True, exist_ok=True)
    logger.info(
        "seed_anonymize_starting output=%s sources=%d",
        args.output, len(args.sources),
    )
    logger.warning("extract_step_not_implemented — run Task 3 manually")
    return 0


if __name__ == "__main__":
    sys.exit(main())
