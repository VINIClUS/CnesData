from logging.config import fileConfig

from alembic import context
from sqlalchemy import MetaData, create_engine, pool

from cnes_infra import config as app_config
from cnes_infra.storage.schema_v2 import metadata as v2_metadata

alembic_config = context.config

if alembic_config.config_file_name is not None:  # pragma: no cover - alembic CLI only
    fileConfig(alembic_config.config_file_name)  # pragma: no cover

target_metadata = MetaData()
for table in v2_metadata.tables.values():
    table.tometadata(target_metadata)


def _resolver_db_url() -> str:  # pragma: no cover - alembic CLI only
    override = alembic_config.get_main_option("sqlalchemy.url")
    if override:
        return override
    return app_config.DB_URL


def run_migrations_offline() -> None:  # pragma: no cover - alembic CLI only
    context.configure(
        url=_resolver_db_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:  # pragma: no cover - alembic CLI only
    connectable = create_engine(_resolver_db_url(), poolclass=pool.NullPool)
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
