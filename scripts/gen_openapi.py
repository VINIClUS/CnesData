"""Gera docs/contracts/openapi.json a partir do central_api FastAPI app."""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


def _bootstrap_env() -> None:
    os.environ.setdefault("DB_URL", "postgresql+psycopg://user:pw@localhost/placeholder")
    os.environ.setdefault("MINIO_ENDPOINT", "localhost:9000")
    os.environ.setdefault("MINIO_ACCESS_KEY", "placeholder")
    os.environ.setdefault("MINIO_SECRET_KEY", "placeholder")
    os.environ.setdefault("MINIO_BUCKET", "placeholder")


def generate(output_path: Path) -> int:
    """Gera OpenAPI schema e escreve em output_path.

    Args:
        output_path: caminho do arquivo JSON de saída.

    Returns:
        Código de saída (0 em sucesso).
    """
    _bootstrap_env()
    from central_api.app import create_app

    app = create_app()
    schema = app.openapi()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(schema, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    logger.info(
        "openapi_written path=%s paths=%d", output_path, len(schema.get("paths", {})),
    )
    return 0


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("docs/contracts/openapi.json"))
    args = parser.parse_args()
    return generate(args.output)


if __name__ == "__main__":
    sys.exit(main())
