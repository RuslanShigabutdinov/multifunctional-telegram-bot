"""Add chat_messages table for storing short conversation history per chat

Revision ID: 8d7f9b7f4e32
Revises: 73b0710677f4
Create Date: 2024-09-09 00:00:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "8d7f9b7f4e32"
down_revision = "73b0710677f4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "chat_messages",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("chat_id", sa.BigInteger(), nullable=False),
        sa.Column("is_bot", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column("telegram_message_id", sa.BigInteger(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.create_index(
        "idx_chat_messages_chat_id_id",
        "chat_messages",
        ["chat_id", "id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_chat_messages_chat_id_id", table_name="chat_messages")
    op.drop_table("chat_messages")
