from __future__ import annotations

import os
import sys
from logging.config import fileConfig
from urllib.parse import quote_plus

from alembic import context
from sqlalchemy import engine_from_config, pool

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils.settings import Settings  # noqa: E402

target_metadata = None


def _build_connection_url() -> str:
    settings = Settings.from_env()
    missing = []
    if not settings.db_user:
        missing.append("DATABASE_USER")
    if not settings.db_password:
        missing.append("DATABASE_PASSWORD")
    if not settings.db_host:
        missing.append("DATABASE_HOST")
    if not settings.db_name:
        missing.append("DATABASE_NAME")
    if missing:
        raise RuntimeError(
            "Missing database environment variables for Alembic: "
            + ", ".join(missing)
        )
    password = quote_plus(settings.db_password)
    return (
        f"postgresql+psycopg://{settings.db_user}:{password}"
        f"@{settings.db_host}:{settings.db_port}/{settings.db_name}"
    )


def _configure_sqlalchemy_url() -> str:
    url = config.get_main_option("sqlalchemy.url")
    if not url or url.endswith("://"):
        url = _build_connection_url()
        config.set_main_option("sqlalchemy.url", url)
    return url


def run_migrations_offline() -> None:
    url = _configure_sqlalchemy_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    _configure_sqlalchemy_url()
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
