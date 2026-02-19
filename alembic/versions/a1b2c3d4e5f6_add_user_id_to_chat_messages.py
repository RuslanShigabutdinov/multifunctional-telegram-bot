"""Add user_id column to chat_messages for sender identification

Revision ID: a1b2c3d4e5f6
Revises: 8d7f9b7f4e32
Create Date: 2025-02-19 00:00:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "a1b2c3d4e5f6"
down_revision = "8d7f9b7f4e32"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "chat_messages",
        sa.Column("user_id", sa.BigInteger(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("chat_messages", "user_id")
