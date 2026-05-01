"""Regenerate docs/contracts/schemas/ from pydantic models."""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from cnes_contracts.export import export_all

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        default="docs/contracts/schemas/",
        help="target directory for JSON Schema files",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    target = Path(args.output)
    paths = export_all(target)
    logger.info("contracts_exported count=%d target=%s", len(paths), target)


if __name__ == "__main__":
    main()
