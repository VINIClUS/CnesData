from logging.config import fileConfig

from alembic import context
from sqlalchemy import MetaData, create_engine, pool

from cnes_infra import config as app_config
from cnes_infra.storage.job_queue import queue_metadata
from cnes_infra.storage.landing import landing_metadata
from cnes_infra.storage.schema import gold_metadata

alembic_config = context.config

if alembic_config.config_file_name is not None:  # pragma: no cover - alembic CLI only
    fileConfig(alembic_config.config_file_name)  # pragma: no cover

target_metadata = MetaData()
for md in (gold_metadata, landing_metadata, queue_metadata):
    for table in md.tables.values():
        table.tometadata(target_metadata)


def run_migrations_offline() -> None:  # pragma: no cover - alembic CLI only
    context.configure(
        url=app_config.DB_URL,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:  # pragma: no cover - alembic CLI only
    connectable = create_engine(app_config.DB_URL, poolclass=pool.NullPool)
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():  # pragma: no cover - alembic CLI only
    run_migrations_offline()
else:
    run_migrations_online()
