"""Minimal migration runner — init-container / CI step."""

import logging
import os
import sys

from alembic import command
from alembic.config import Config

logger = logging.getLogger(__name__)


def main() -> None:
    db_url = os.environ.get("DB_URL")
    if not db_url:
        logger.error("DB_URL not set")
        sys.exit(1)

    cfg = Config()
    cfg.set_main_option(
        "script_location", "cnes_infra:alembic",
    )
    cfg.set_main_option("sqlalchemy.url", db_url)
    command.upgrade(cfg, "head")
    logger.info("migration_complete target=head")


if __name__ == "__main__":
    main()
