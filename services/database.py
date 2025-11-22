from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import List, Optional

from psycopg_pool import AsyncConnectionPool

from utils.settings import get_settings

logger = logging.getLogger(__name__)

GROUP_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_]{1,255}$")
USERNAME_PATTERN = re.compile(r"^[A-Za-z0-9_]{1,255}$")


@dataclass
class GroupCommand:
    name: str
    users: List[str]


def _sanitize_username(username: Optional[str]) -> Optional[str]:
    if not username:
        return None
    sanitized = username.strip().lstrip("@")
    return sanitized or None


def parse_group_command(command: str) -> Optional[GroupCommand]:
    cleaned = command.strip()
    if not cleaned.lower().startswith("name:"):
        return None

    users: List[str] = []
    name_section, _, users_section = cleaned.partition("users:")
    name = name_section.split("name:", 1)[1].strip()
    if not GROUP_NAME_PATTERN.fullmatch(name):
        return None

    if users_section:
        for raw_username in users_section.split(","):
            username = _sanitize_username(raw_username)
            if not username:
                continue
            if not USERNAME_PATTERN.fullmatch(username):
                return None
            users.append(username)

    return GroupCommand(name=name, users=users)


class DataBase:
    def __init__(self, pool: AsyncConnectionPool) -> None:
        self.pool = pool

    async def get_user(self, user_id: int):
        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT id, first_name, username FROM users WHERE id = %s LIMIT 1",
                        (user_id,),
                    )
                    row = await cur.fetchone()
        except Exception as exc:  # pragma: no cover
            logger.exception("Failed to fetch user %s: %s", user_id, exc)
            return None
        return {"id": row[0], "first_name": row[1], "username": row[2]} if row else None

    async def get_user_by_username(self, username: Optional[str]):
        username = _sanitize_username(username)
        if not username:
            return None
        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT id, first_name, username FROM users WHERE username = %s LIMIT 1",
                        (username,),
                    )
                    row = await cur.fetchone()
        except Exception as exc:  # pragma: no cover
            logger.exception("Failed to fetch user by username %s: %s", username, exc)
            return None
        return {"id": row[0], "first_name": row[1], "username": row[2]} if row else None

    async def create_user(
        self, user_id: int, first_name: str, username: Optional[str]
    ) -> bool:
        username = _sanitize_username(username)
        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        INSERT INTO users(id, first_name, username)
                        VALUES(%s,%s,%s)
                        ON CONFLICT (id) DO NOTHING
                        """,
                        (user_id, first_name, username),
                    )
                    return cur.rowcount > 0
        except Exception as exc:  # pragma: no cover
            logger.exception("Failed to create user %s: %s", user_id, exc)
            return False

    async def add_group_chat_to_user(self, user_id: int, chat_id: int) -> bool:
        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        INSERT INTO user_group_chats(user_id, group_chat_id)
                        VALUES(%s,%s)
                        ON CONFLICT DO NOTHING
                        """,
                        (user_id, chat_id),
                    )
                    return cur.rowcount > 0
        except Exception as exc:  # pragma: no cover
            logger.exception(
                "Failed to link user %s to chat %s: %s", user_id, chat_id, exc
            )
            return False

    async def create_group_chat(self, chat_id: int, title: str, chat_type: str) -> bool:
        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        INSERT INTO group_chats(id, title, type)
                        VALUES(%s,%s,%s)
                        ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, type = EXCLUDED.type
                        """,
                        (chat_id, title, chat_type),
                    )
                    return cur.rowcount > 0
        except Exception as exc:  # pragma: no cover
            logger.exception("Failed to upsert group chat %s: %s", chat_id, exc)
            return False

    async def get_group_chat(self, chat_id: int):
        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT id, title, type FROM group_chats WHERE id = %s LIMIT 1",
                        (chat_id,),
                    )
                    row = await cur.fetchone()
        except Exception as exc:  # pragma: no cover
            logger.exception("Failed to fetch group chat %s: %s", chat_id, exc)
            return None
        return {"id": row[0], "title": row[1], "type": row[2]} if row else None

    async def get_all_usernames(self, chat_id: int) -> str:
        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        SELECT users.username
                        FROM users
                        JOIN user_group_chats ON user_group_chats.user_id = users.id
                        WHERE user_group_chats.group_chat_id = %s
                        ORDER BY users.username
                        """,
                        (chat_id,),
                    )
                    rows = await cur.fetchall()
        except Exception as exc:  # pragma: no cover
            logger.exception("Failed to fetch usernames for chat %s: %s", chat_id, exc)
            return "Something went wrong"
        usernames = ["@" + row[0] for row in rows]
        return ", ".join(usernames) if usernames else "Не нашёл пользователей"

    async def update_user(
        self, user_id: int, first_name: str, username: Optional[str]
    ) -> bool:
        username = _sanitize_username(username)
        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "UPDATE users SET first_name = %s, username = %s WHERE id = %s",
                        (first_name, username, user_id),
                    )
                    return cur.rowcount > 0
        except Exception as exc:  # pragma: no cover
            logger.exception("Failed to update user %s: %s", user_id, exc)
            return False

    async def get_group_by_chat_and_name(self, group_chat_id: int, name: str):
        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        SELECT id, name, group_chat_id
                        FROM groups
                        WHERE group_chat_id = %s AND name = %s
                        LIMIT 1
                        """,
                        (group_chat_id, name),
                    )
                    row = await cur.fetchone()
        except Exception as exc:  # pragma: no cover
            logger.exception(
                "Failed to fetch group %s in chat %s: %s", name, group_chat_id, exc
            )
            return None
        return {"id": row[0], "name": row[1], "group_chat_id": row[2]} if row else None

    def _extract_group_command(self, command: str, prefix: str) -> Optional[GroupCommand]:
        cleaned = command.replace("@", "").strip()
        if not cleaned.lower().startswith(prefix):
            return None
        payload = cleaned[len(prefix):].strip()
        return parse_group_command(payload)

    def _extract_group_name(self, command: str, prefix: str) -> Optional[str]:
        cleaned = command.replace("@", "").strip()
        if not cleaned.lower().startswith(prefix):
            return None
        payload = cleaned[len(prefix):].strip()
        name = payload.replace("name:", "", 1).strip()
        if not GROUP_NAME_PATTERN.fullmatch(name):
            return None
        return name

    async def create_group(self, command: str, group_chat_id: int) -> str:
        parsed = self._extract_group_command(command, "/create group")
        if not parsed:
            return "Неверный формат. Используйте /create group name:{name} users:{username}"

        message = ""
        try:
            async with self.pool.connection() as conn:
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        await cur.execute(
                            """
                            INSERT INTO groups(name, group_chat_id)
                            VALUES(%s,%s)
                            ON CONFLICT (group_chat_id, name) DO NOTHING
                            RETURNING id
                            """,
                            (parsed.name, group_chat_id),
                        )
                        row = await cur.fetchone()
                        if row:
                            group_id = row[0]
                            message = f"Group @{parsed.name} has been created"
                        else:
                            await cur.execute(
                                "SELECT id FROM groups WHERE name = %s AND group_chat_id = %s",
                                (parsed.name, group_chat_id),
                            )
                            existing = await cur.fetchone()
                            if not existing:
                                return "Не удалось создать группу"
                            group_id = existing[0]
                            message = f"Group @{parsed.name} already exists"

                        for username in parsed.users:
                            await cur.execute(
                                "SELECT id FROM users WHERE username = %s", (username,)
                            )
                            user_row = await cur.fetchone()
                            if not user_row:
                                message += f"\nUser @{username} was not found"
                                continue
                            await cur.execute(
                                """
                                INSERT INTO user_groups(user_id, group_id)
                                VALUES(%s,%s)
                                ON CONFLICT DO NOTHING
                                """,
                                (user_row[0], group_id),
                            )
                            if cur.rowcount:
                                message += f"\nUser @{username} has been added"
                            else:
                                message += f"\nUser @{username} is already in group"
        except Exception as exc:  # pragma: no cover
            logger.exception(
                "Failed to create group %s in chat %s: %s", parsed.name, group_chat_id, exc
            )
            return "Не удалось создать группу"

        return message

    async def delete_group(self, command: str, group_chat_id: int) -> bool:
        group_name = self._extract_group_name(command, "/delete group")
        if not group_name:
            return False

        try:
            async with self.pool.connection() as conn:
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        await cur.execute(
                            "SELECT id FROM groups WHERE name = %s AND group_chat_id = %s",
                            (group_name, group_chat_id),
                        )
                        group = await cur.fetchone()
                        if not group:
                            return False
                        await cur.execute("DELETE FROM groups WHERE id = %s", (group[0],))
                        return cur.rowcount > 0
        except Exception as exc:  # pragma: no cover
            logger.exception(
                "Failed to delete group %s in chat %s: %s", group_name, group_chat_id, exc
            )
            return False

    async def add_users_to_group(self, command: str, group_chat_id: int) -> str:
        parsed = self._extract_group_command(command, "/add to group")
        if not parsed:
            return "Неверный формат. Используйте /add to group name:{name} users:{username}"

        try:
            async with self.pool.connection() as conn:
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        await cur.execute(
                            "SELECT id FROM groups WHERE name = %s AND group_chat_id = %s",
                            (parsed.name, group_chat_id),
                        )
                        group = await cur.fetchone()
                        if not group:
                            return f"Group @{parsed.name} was not found"

                        group_id = group[0]
                        message = ""
                        for username in parsed.users:
                            await cur.execute(
                                "SELECT id FROM users WHERE username = %s", (username,)
                            )
                            user_row = await cur.fetchone()
                            if not user_row:
                                message += f"\nUser @{username} was not found"
                                continue
                            await cur.execute(
                                """
                                INSERT INTO user_groups(user_id, group_id)
                                VALUES(%s,%s)
                                ON CONFLICT DO NOTHING
                                """,
                                (user_row[0], group_id),
                            )
                            if cur.rowcount:
                                message += f"\nUser @{username} has been added"
                            else:
                                message += f"\nUser @{username} is already in group"
        except Exception as exc:  # pragma: no cover
            logger.exception(
                "Failed to add users to group %s in chat %s: %s",
                parsed.name,
                group_chat_id,
                exc,
            )
            return "Не удалось добавить пользователей"

        return message.strip() or "No users were provided"

    async def delete_users_from_group(self, command: str, group_chat_id: int) -> str:
        parsed = self._extract_group_command(command, "/delete users group")
        if not parsed:
            return "Неверный формат. Используйте /delete users group name:{name} users:{username}"

        try:
            async with self.pool.connection() as conn:
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        await cur.execute(
                            "SELECT id FROM groups WHERE name = %s AND group_chat_id = %s",
                            (parsed.name, group_chat_id),
                        )
                        group = await cur.fetchone()
                        if not group:
                            return f"Group @{parsed.name} was not found"

                        group_id = group[0]
                        message = ""
                        for username in parsed.users:
                            await cur.execute(
                                "SELECT id FROM users WHERE username = %s", (username,)
                            )
                            user_row = await cur.fetchone()
                            if not user_row:
                                message += f"\nUser @{username} was not found"
                                continue
                            await cur.execute(
                                "DELETE FROM user_groups WHERE user_id = %s AND group_id = %s",
                                (user_row[0], group_id),
                            )
                            if cur.rowcount:
                                message += f"\nUser @{username} has been deleted"
                            else:
                                message += f"\nUser @{username} was not in group"
        except Exception as exc:  # pragma: no cover
            logger.exception(
                "Failed to delete users from group %s in chat %s: %s",
                parsed.name,
                group_chat_id,
                exc,
            )
            return "Не удалось удалить пользователей"

        return message.strip() or "No users were provided"

    async def get_groups_for_chat(self, group_chat_id: int):
        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT id, name, group_chat_id FROM groups WHERE group_chat_id = %s",
                        (group_chat_id,),
                    )
                    rows = await cur.fetchall()
        except Exception as exc:  # pragma: no cover
            logger.exception("Failed to fetch groups for chat %s: %s", group_chat_id, exc)
            return []
        return [
            {"id": row[0], "name": row[1], "group_chat_id": row[2]}
            for row in rows
        ]

    async def get_group_members_by_names(
        self, group_chat_id: int, names: list[str]
    ) -> dict[str, list[str]]:
        sanitized_names = []
        for name in names:
            cleaned = _sanitize_username(name)
            if cleaned and GROUP_NAME_PATTERN.fullmatch(cleaned):
                sanitized_names.append(cleaned)
        if not sanitized_names:
            return {}

        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        SELECT g.name,
                               COALESCE(
                                   array_agg(u.username ORDER BY u.username)
                                   FILTER (WHERE u.username IS NOT NULL),
                                   '{}'
                               ) AS usernames
                        FROM groups g
                        LEFT JOIN user_groups ug ON ug.group_id = g.id
                        LEFT JOIN users u ON u.id = ug.user_id
                        WHERE g.group_chat_id = %s AND g.name = ANY(%s)
                        GROUP BY g.name
                        """,
                        (group_chat_id, sanitized_names),
                    )
                    rows = await cur.fetchall()
        except Exception as exc:  # pragma: no cover
            logger.exception(
                "Failed to fetch group members for chat %s and names %s: %s",
                group_chat_id,
                sanitized_names,
                exc,
            )
            return {}

        return {row[0]: row[1] for row in rows}

    async def get_usernames_by_group(self, group_id: int) -> str:
        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        SELECT users.username
                        FROM user_groups
                        JOIN users ON user_groups.user_id = users.id
                        WHERE user_groups.group_id = %s
                        ORDER BY users.username
                        """,
                        (group_id,),
                    )
                    rows = await cur.fetchall()
        except Exception as exc:  # pragma: no cover
            logger.exception(
                "Failed to fetch usernames for group %s: %s", group_id, exc
            )
            return "Something went wrong"
        usernames = ["@" + name[0] for name in rows]
        return ", ".join(usernames) if rows else "Something went wrong"

    async def get_group_chats_for_user(self, user_id: int):
        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        SELECT gc.id, gc.title, gc.type
                        FROM group_chats gc
                        JOIN user_group_chats ugc ON ugc.group_chat_id = gc.id
                        WHERE ugc.user_id = %s
                        ORDER BY gc.title
                        """,
                        (user_id,),
                    )
                    rows = await cur.fetchall()
        except Exception as exc:  # pragma: no cover
            logger.exception(
                "Failed to fetch group chats for user %s: %s", user_id, exc
            )
            return []
        return [{"id": row[0], "title": row[1], "type": row[2]} for row in rows]


_pool: AsyncConnectionPool | None = None
_db_instance: DataBase | None = None


def get_database() -> DataBase:
    if _db_instance is None:
        raise RuntimeError("Database is not initialized. Call init_database() first.")
    return _db_instance


async def init_database() -> None:
    global _pool, _db_instance
    if _db_instance is not None:
        return

    settings = get_settings().require()
    conninfo = (
        f"host={settings.db_host} "
        f"port={settings.db_port} "
        f"dbname={settings.db_name} "
        f"user={settings.db_user} "
        f"password={settings.db_password}"
    )
    _pool = AsyncConnectionPool(conninfo, min_size=1, max_size=10, open=False)
    await _pool.open()
    _db_instance = DataBase(_pool)


async def close_database() -> None:
    global _pool, _db_instance
    pool, _pool = _pool, None
    _db_instance = None
    if pool is None:
        return
    try:
        await pool.close()
    except Exception as exc:  # pragma: no cover
        logger.exception("Failed to close database pool: %s", exc)
        raise
