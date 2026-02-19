from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import List, Optional, Tuple

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

    async def migrate_chat(self, old_id: int, new_id: int) -> bool:
        # Обновляет chat_id во всех таблицах при миграции группы в супергруппу.
        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    # Копируем group_chats запись с новым id.
                    await cur.execute(
                        """
                        INSERT INTO group_chats(id, title, type)
                        SELECT %s, title, 'supergroup'
                        FROM group_chats WHERE id = %s
                        ON CONFLICT (id) DO NOTHING
                        """,
                        (new_id, old_id),
                    )
                    # Обновляем дочерние таблицы.
                    await cur.execute(
                        "UPDATE user_group_chats SET group_chat_id = %s WHERE group_chat_id = %s",
                        (new_id, old_id),
                    )
                    await cur.execute(
                        "UPDATE groups SET group_chat_id = %s WHERE group_chat_id = %s",
                        (new_id, old_id),
                    )
                    await cur.execute(
                        "UPDATE chat_messages SET chat_id = %s WHERE chat_id = %s",
                        (new_id, old_id),
                    )
                    # Удаляем старую запись.
                    await cur.execute(
                        "DELETE FROM group_chats WHERE id = %s",
                        (old_id,),
                    )
            logger.info("Migrated chat %s -> %s", old_id, new_id)
            return True
        except Exception as exc:  # pragma: no cover
            logger.exception("Failed to migrate chat %s -> %s: %s", old_id, new_id, exc)
            return False

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

    async def get_chat_users_paginated(
        self, chat_id: int, limit: int, offset: int
    ) -> Tuple[list[dict], int]:
        """Возвращает пользователей чата с пагинацией.

        :param chat_id: идентификатор чата
        :param limit: количество записей на страницу
        :param offset: смещение
        :return: (список пользователей, всего пользователей)
        """
        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        SELECT u.id, u.first_name, u.username
                        FROM users u
                        JOIN user_group_chats ugc ON ugc.user_id = u.id
                        WHERE ugc.group_chat_id = %s
                        ORDER BY COALESCE(u.username, '') ASC, u.id ASC
                        LIMIT %s OFFSET %s
                        """,
                        (chat_id, limit, offset),
                    )
                    rows = await cur.fetchall()

                    await cur.execute(
                        "SELECT COUNT(*) FROM user_group_chats WHERE group_chat_id = %s",
                        (chat_id,),
                    )
                    total_row = await cur.fetchone()
        except Exception as exc:  # pragma: no cover
            logger.exception(
                "Failed to fetch paginated users for chat %s (limit %s offset %s): %s",
                chat_id,
                limit,
                offset,
                exc,
            )
            return [], 0

        users = [
            {"id": row[0], "first_name": row[1], "username": row[2]} for row in rows
        ]
        total = total_row[0] if total_row else 0
        return users, total

    async def get_groups_paginated(
        self, chat_id: int, limit: int, offset: int
    ) -> Tuple[list[dict], int]:
        """Возвращает группы чата с пагинацией.

        :param chat_id: идентификатор чата
        :param limit: количество записей на страницу
        :param offset: смещение
        :return: (список групп, всего групп)
        """
        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        SELECT id, name, group_chat_id
                        FROM groups
                        WHERE group_chat_id = %s
                        ORDER BY name ASC, id ASC
                        LIMIT %s OFFSET %s
                        """,
                        (chat_id, limit, offset),
                    )
                    rows = await cur.fetchall()
                    await cur.execute(
                        "SELECT COUNT(*) FROM groups WHERE group_chat_id = %s",
                        (chat_id,),
                    )
                    total_row = await cur.fetchone()
        except Exception as exc:  # pragma: no cover
            logger.exception(
                "Failed to fetch paginated groups for chat %s (limit %s offset %s): %s",
                chat_id,
                limit,
                offset,
                exc,
            )
            return [], 0
        groups = [{"id": row[0], "name": row[1], "group_chat_id": row[2]} for row in rows]
        total = total_row[0] if total_row else 0
        return groups, total

    async def get_group_user_ids(self, group_id: int) -> list[int]:
        """Возвращает список user_id участников группы.

        :param group_id: идентификатор группы
        :return: список user_id
        """
        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT user_id FROM user_groups WHERE group_id = %s ORDER BY user_id",
                        (group_id,),
                    )
                    rows = await cur.fetchall()
        except Exception as exc:  # pragma: no cover
            logger.exception("Failed to fetch user ids for group %s: %s", group_id, exc)
            return []
        return [row[0] for row in rows]

    async def rename_group(
        self, group_id: int, group_chat_id: int, new_name: str
    ) -> tuple[bool, str]:
        """Переименовывает группу.

        :param group_id: идентификатор группы
        :param group_chat_id: идентификатор чата
        :param new_name: новое имя
        :return: (успех, сообщение)
        """
        if not GROUP_NAME_PATTERN.fullmatch(new_name):
            return False, "Неверное имя группы. Используйте только буквы, цифры и _"

        try:
            async with self.pool.connection() as conn:
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        await cur.execute(
                            """
                            SELECT 1 FROM groups
                            WHERE group_chat_id = %s AND name = %s AND id <> %s
                            """,
                            (group_chat_id, new_name, group_id),
                        )
                        exists = await cur.fetchone()
                        if exists:
                            return False, "Группа с таким именем уже существует"

                        await cur.execute(
                            """
                            UPDATE groups
                            SET name = %s
                            WHERE id = %s AND group_chat_id = %s
                            """,
                            (new_name, group_id, group_chat_id),
                        )
                        if cur.rowcount == 0:
                            return False, "Группа не найдена"
        except Exception as exc:  # pragma: no cover
            logger.exception(
                "Failed to rename group %s in chat %s to %s: %s",
                group_id,
                group_chat_id,
                new_name,
                exc,
            )
            return False, "Не удалось переименовать группу"

        return True, f"Группа переименована в @{new_name}"

    async def delete_group_by_id(self, group_id: int, group_chat_id: int) -> tuple[bool, str]:
        """Удаляет группу по id.

        :param group_id: идентификатор группы
        :param group_chat_id: идентификатор чата
        :return: (успех, сообщение)
        """
        try:
            async with self.pool.connection() as conn:
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        await cur.execute(
                            "DELETE FROM groups WHERE id = %s AND group_chat_id = %s",
                            (group_id, group_chat_id),
                        )
                        if cur.rowcount == 0:
                            return False, "Группа не найдена"
        except Exception as exc:  # pragma: no cover
            logger.exception(
                "Failed to delete group %s in chat %s: %s", group_id, group_chat_id, exc
            )
            return False, "Не удалось удалить группу"
        return True, "Группа удалена"

    async def create_group_with_users(
        self, group_chat_id: int, name: str, user_ids: list[int]
    ) -> tuple[bool, str, Optional[int], int]:
        """Создаёт или обновляет группу и пересохраняет участников.

        :param group_chat_id: идентификатор чата группы
        :param name: имя группы
        :param user_ids: выбранные user_id
        :return: (успех, сообщение для пользователя, id группы, всего участников)
        """
        if not GROUP_NAME_PATTERN.fullmatch(name):
            return False, "Неверное имя группы. Используйте только буквы, цифры и _", None, 0

        unique_user_ids = list(dict.fromkeys(user_ids or []))

        group_id: Optional[int] = None
        total_members = 0

        try:
            async with self.pool.connection() as conn:
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        await cur.execute(
                            """
                            SELECT id FROM groups WHERE group_chat_id = %s AND name = %s
                            """,
                            (group_chat_id, name),
                        )
                        existing = await cur.fetchone()

                        if existing:
                            group_id = existing[0]
                            created = False
                        else:
                            await cur.execute(
                                """
                                INSERT INTO groups(name, group_chat_id)
                                VALUES(%s,%s)
                                RETURNING id
                                """,
                                (name, group_chat_id),
                            )
                            row = await cur.fetchone()
                            if not row:
                                return False, "Не удалось создать группу", None, 0
                            group_id = row[0]
                            created = True

                        await cur.execute(
                            "DELETE FROM user_groups WHERE group_id = %s", (group_id,)
                        )

                        inserted = 0
                        skipped = 0
                        if unique_user_ids:
                            await cur.execute(
                                """
                                SELECT u.id
                                FROM users u
                                JOIN user_group_chats ugc ON ugc.user_id = u.id
                                WHERE ugc.group_chat_id = %s AND u.id = ANY(%s)
                                """,
                                (group_chat_id, unique_user_ids),
                            )
                            allowed_ids = [row[0] for row in await cur.fetchall()]

                            for user_id in allowed_ids:
                                await cur.execute(
                                    """
                                    INSERT INTO user_groups(user_id, group_id)
                                    VALUES(%s,%s)
                                    ON CONFLICT DO NOTHING
                                    """,
                                    (user_id, group_id),
                                )
                                if cur.rowcount:
                                    inserted += 1

                            skipped = len(unique_user_ids) - len(allowed_ids)

        except Exception as exc:  # pragma: no cover
            logger.exception(
                "Failed to create/update group %s in chat %s with users %s: %s",
                name,
                group_chat_id,
                unique_user_ids,
                exc,
            )
            return False, "Не удалось сохранить группу", group_id, total_members

        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT COUNT(*) FROM user_groups WHERE group_id = %s", (group_id,)
                    )
                    row = await cur.fetchone()
                    total_members = row[0] if row else 0
        except Exception as exc:  # pragma: no cover
            logger.exception("Failed to count members for group %s: %s", group_id, exc)

        action = "создана" if created else "обновлена"
        parts = [f"Группа @{name} {action}."]
        parts.append(f"Добавлено участников: {inserted}.")
        if skipped > 0:
            parts.append(f"Пропущено: {skipped} (нет в чате).")
        return True, " ".join(parts), group_id, total_members

    async def get_group_by_id(self, group_id: int):
        """Возвращает группу по id.

        :param group_id: идентификатор группы
        :return: dict или None
        """
        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT id, name, group_chat_id FROM groups WHERE id = %s LIMIT 1",
                        (group_id,),
                    )
                    row = await cur.fetchone()
        except Exception as exc:  # pragma: no cover
            logger.exception("Failed to fetch group by id %s: %s", group_id, exc)
            return None
        return {"id": row[0], "name": row[1], "group_chat_id": row[2]} if row else None


    async def add_chat_message(
        self,
        chat_id: int,
        is_bot: bool,
        text: str,
        telegram_message_id: Optional[int] = None,
        user_id: Optional[int] = None,
        history_limit: Optional[int] = None,
    ) -> bool:
        """Adds a chat message to history and prunes old rows beyond the limit."""
        limit = history_limit
        if limit is None:
            limit = max(get_settings().chat_history_limit, 0)
        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        INSERT INTO chat_messages (chat_id, is_bot, text, telegram_message_id, user_id)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (chat_id, is_bot, text, telegram_message_id, user_id),
                    )
                    if limit > 0:
                        await cur.execute(
                            """
                            DELETE FROM chat_messages
                            WHERE chat_id = %s
                              AND id NOT IN (
                                SELECT id
                                FROM chat_messages
                                WHERE chat_id = %s
                                ORDER BY id DESC
                                LIMIT %s
                              )
                            """,
                            (chat_id, chat_id, limit),
                        )
                    return True
        except Exception as exc:  # pragma: no cover
            logger.exception(
                "Failed to add chat message for chat %s: %s", chat_id, exc
            )
            return False

    async def get_chat_history(
        self, chat_id: int, limit: Optional[int] = None
    ) -> list[dict]:
        """Fetches latest chat history rows for a chat ordered newest-first."""
        max_rows = limit
        if max_rows is None:
            max_rows = max(get_settings().chat_history_limit, 0)
        if max_rows <= 0:
            return []
        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        SELECT cm.id, cm.chat_id, cm.is_bot, cm.text,
                               cm.telegram_message_id, cm.created_at, u.first_name
                        FROM chat_messages cm
                        LEFT JOIN users u ON cm.user_id = u.id
                        WHERE cm.chat_id = %s
                          AND cm.created_at >= NOW() - INTERVAL '12 hours'
                        ORDER BY cm.id DESC
                        LIMIT %s
                        """,
                        (chat_id, max_rows),
                    )
                    rows = await cur.fetchall()
        except Exception as exc:  # pragma: no cover
            logger.exception(
                "Failed to fetch chat history for chat %s: %s", chat_id, exc
            )
            return []
        return [
            {
                "id": row[0],
                "chat_id": row[1],
                "is_bot": row[2],
                "text": row[3],
                "telegram_message_id": row[4],
                "created_at": row[5],
                "first_name": row[6],
            }
            for row in rows
        ]


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
