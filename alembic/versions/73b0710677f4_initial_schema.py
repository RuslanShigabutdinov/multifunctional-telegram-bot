"""Initial database schema

Revision ID: 73b0710677f4
Revises: 
Create Date: 2024-02-15 00:00:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "73b0710677f4"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "group_chats",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("title", sa.String(length=255), nullable=True),
        sa.Column("type", sa.String(length=30), nullable=True),
    )

    op.create_table(
        "users",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("first_name", sa.String(length=255), nullable=True),
        sa.Column("username", sa.String(length=255), nullable=True),
    )
    op.create_index("idx_users_username", "users", ["username"], unique=False)

    op.create_table(
        "user_group_chats",
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("group_chat_id", sa.BigInteger(), nullable=False),
        sa.ForeignKeyConstraint(
            ["group_chat_id"],
            ["group_chats.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("user_id", "group_chat_id"),
    )
    op.create_index(
        "idx_user_group_chats_user",
        "user_group_chats",
        ["user_id"],
        unique=False,
    )

    op.create_table(
        "groups",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(length=255), nullable=True),
        sa.Column("group_chat_id", sa.BigInteger(), nullable=True),
        sa.ForeignKeyConstraint(
            ["group_chat_id"],
            ["group_chats.id"],
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("group_chat_id", "name", name="uq_groups_chat_name"),
    )

    op.create_table(
        "user_groups",
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("group_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["group_id"],
            ["groups.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("user_id", "group_id"),
    )
    op.create_index(
        "idx_user_groups_group",
        "user_groups",
        ["group_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_user_groups_group", table_name="user_groups")
    op.drop_table("user_groups")
    op.drop_table("groups")
    op.drop_index("idx_user_group_chats_user", table_name="user_group_chats")
    op.drop_table("user_group_chats")
    op.drop_index("idx_users_username", table_name="users")
    op.drop_table("users")
    op.drop_table("group_chats")
