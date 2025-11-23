import logging
import math
import re
from secrets import choice

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from app.responds import respondsOld
from services.database import GROUP_NAME_PATTERN, get_database
from services.media.instagram import downloadInstagram
from services.media.tiktok import downloadTikTok, findLink

logger = logging.getLogger(__name__)

(
    SAY_SELECT_CHAT,
    SAY_ENTER_MESSAGE,
    GROUP_SELECT_CHAT,
    GROUP_ENTER_NAME,
    GROUP_SELECT_USERS,
) = range(5)

USERS_PAGE_SIZE = 10
GC_SELECTED_USERS = "gc_selected_user_ids"
GC_TARGET_CHAT = "gc_target_chat"
GC_GROUP_NAME = "gc_group_name"
GC_PAGE = "gc_page"
GC_TOTAL = "gc_total"
GC_AVAILABLE_CHATS = "gc_available_chats"
GC_STAGE = "gc_stage"
GC_PROMPT_MSG = "gc_prompt_msg"
GRP_STAGE = "grp_stage"
GRP_TARGET_CHAT = "grp_target_chat"
GRP_AVAILABLE_CHATS = "grp_available_chats"
GRP_PAGE = "grp_page"
GRP_TOTAL = "grp_total"
GRP_SELECTED_GROUP = "grp_selected_group"
GRP_PROMPT_MSG = "grp_prompt_msg"


async def _reply_db_error(update: Update) -> None:
    await update.message.reply_text("База данных недоступна. Попробуйте позже.")


def _reset_group_create_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Сбрасывает состояние мастера создания группы.

    :param context: контекст PTB с user_data
    :return: None
    """
    for key in (
        GC_SELECTED_USERS,
        GC_TARGET_CHAT,
        GC_GROUP_NAME,
        GC_PAGE,
        GC_TOTAL,
        GC_AVAILABLE_CHATS,
        GC_STAGE,
        GC_PROMPT_MSG,
    ):
        context.user_data.pop(key, None)


def _reset_group_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Сбрасывает состояние управления группами (/group).

    :param context: контекст PTB с user_data
    :return: None
    """
    for key in (
        GRP_STAGE,
        GRP_TARGET_CHAT,
        GRP_AVAILABLE_CHATS,
        GRP_PAGE,
        GRP_TOTAL,
        GRP_SELECTED_GROUP,
        GRP_PROMPT_MSG,
    ):
        context.user_data.pop(key, None)


async def _delete_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Пытается удалить сообщение пользователя (команда или ввод)."""
    if not update.message:
        return
    try:
        await context.bot.delete_message(
            chat_id=update.message.chat_id, message_id=update.message.message_id
        )
    except Exception:
        pass


def _store_prompt_message(message, context: ContextTypes.DEFAULT_TYPE, key: str) -> None:
    """Сохраняет данные сообщения-подсказки для последующего удаления.

    :param message: telegram.Message
    :param context: контекст PTB
    :param key: ключ для сохранения
    :return: None
    """
    if not message:
        return
    context.user_data[key] = {
        "chat_id": message.chat_id,
        "message_id": message.message_id,
    }


async def _delete_prompt_and_user_message(
    update: Update, context: ContextTypes.DEFAULT_TYPE, prompt_key: str
) -> None:
    """Пытается удалить подсказку и сообщение пользователя.

    :param update: Update с сообщением пользователя
    :param context: контекст PTB
    :param prompt_key: ключ сохранённой подсказки
    :return: None
    """
    bot = context.bot
    prompt = context.user_data.pop(prompt_key, None)
    tasks = []

    if prompt:
        tasks.append(
            bot.delete_message(chat_id=prompt["chat_id"], message_id=prompt["message_id"])
        )

    if update.message:
        tasks.append(
            bot.delete_message(
                chat_id=update.message.chat_id, message_id=update.message.message_id
            )
        )

    for task in tasks:
        try:
            await task
        except Exception:
            pass


def _format_user_label(user: dict, selected_ids: set[int]) -> str:
    """Формирует подпись кнопки пользователя.

    :param user: данные пользователя (id, username, first_name)
    :param selected_ids: выбранные user_id
    :return: текст кнопки
    """
    checked = user["id"] in selected_ids
    prefix = "[✓]" if checked else "[ ]"
    name = user.get("username") or user.get("first_name") or str(user["id"])
    if user.get("username"):
        name = f"@{name}"
    return f"{prefix} {name}"


def _build_user_keyboard(
    users: list[dict], selected_ids: set[int], page: int, total: int
) -> InlineKeyboardMarkup:
    """Создаёт инлайн-клавиатуру с пользователями и пагинацией.

    :param users: список пользователей страницы
    :param selected_ids: выбранные user_id
    :param page: номер страницы (0-based)
    :param total: всего пользователей
    :return: InlineKeyboardMarkup
    """
    user_buttons = [
        InlineKeyboardButton(
            _format_user_label(user, selected_ids), callback_data=f"gc_user:{user['id']}"
        )
        for user in users
    ]
    rows = [user_buttons[i : i + 2] for i in range(0, len(user_buttons), 2)]

    total_pages = max(math.ceil(total / USERS_PAGE_SIZE), 1)
    nav_row = [InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data="gc_ignore")]
    if page > 0:
        nav_row.insert(0, InlineKeyboardButton("◀️ Prev", callback_data="gc_nav:prev"))
    if (page + 1) * USERS_PAGE_SIZE < total:
        nav_row.append(InlineKeyboardButton("Next ▶️", callback_data="gc_nav:next"))
    rows.append(nav_row)
    rows.append([InlineKeyboardButton("Submit ✅", callback_data="gc_submit")])
    rows.append([InlineKeyboardButton("Отмена", callback_data="gc_cancel")])
    return InlineKeyboardMarkup(rows)


async def _send_user_selection(
    update: Update, context: ContextTypes.DEFAULT_TYPE, page: int
) -> int:
    """Отправляет/обновляет сообщение выбора пользователей.

    :param update: Update с callback или сообщением
    :param context: контекст PTB
    :param page: номер страницы (0-based)
    :return: состояние ConversationHandler (GROUP_SELECT_USERS) либо END
    """
    target_chat = context.user_data.get(GC_TARGET_CHAT)
    group_name = context.user_data.get(GC_GROUP_NAME)
    if not target_chat or not group_name:
        if update.callback_query:
            await update.callback_query.answer("Данные устарели, начните заново.", show_alert=True)
            await update.callback_query.edit_message_text(
                "Данные не найдены. Запустите /group-create ещё раз."
            )
        elif update.message:
            await update.message.reply_text("Данные не найдены. Запустите /group-create ещё раз.")
        _reset_group_create_state(context)
        return ConversationHandler.END

    db = get_database()
    requested_page = max(page, 0)
    users, total = await db.get_chat_users_paginated(
        target_chat["id"], USERS_PAGE_SIZE, requested_page * USERS_PAGE_SIZE
    )
    total_pages = max(math.ceil(total / USERS_PAGE_SIZE), 1)
    max_page = max(total_pages - 1, 0)
    if requested_page > max_page:
        requested_page = max_page
        users, total = await db.get_chat_users_paginated(
            target_chat["id"], USERS_PAGE_SIZE, requested_page * USERS_PAGE_SIZE
        )
        total_pages = max(math.ceil(total / USERS_PAGE_SIZE), 1)

    context.user_data[GC_PAGE] = requested_page
    context.user_data[GC_TOTAL] = total
    selected_ids = context.user_data.get(GC_SELECTED_USERS)
    if selected_ids is None:
        selected_ids = set()
        context.user_data[GC_SELECTED_USERS] = selected_ids

    chat_label = target_chat.get("title") or target_chat["id"]
    text_lines = [
        f"Группа @{group_name} для чата: {chat_label}",
        f"Отметьте участников (страница {requested_page + 1}/{total_pages}) и нажмите Submit.",
    ]
    if total == 0:
        text_lines.append(
            "В базе нет пользователей из этого чата. Можно создать пустую группу или /cancel."
        )

    markup = _build_user_keyboard(users, selected_ids, requested_page, total)
    if update.callback_query:
        await update.callback_query.answer()
        try:
            await update.callback_query.edit_message_text(
                "\n".join(text_lines), reply_markup=markup
            )
        except Exception as exc:  # pragma: no cover
            logger.exception("Failed to edit selection message: %s", exc)
            await update.callback_query.message.reply_text(
                "\n".join(text_lines), reply_markup=markup
            )
    elif update.message:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="\n".join(text_lines),
            reply_markup=markup,
        )
    return GROUP_SELECT_USERS


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Общий текстовый обработчик: загрузка медиа и команды групп/пользователей.

    :param update: Update с текстом
    :param context: контекст PTB
    :return: None или ConversationHandler state
    """
    if update.edited_message:
        return

    text: str = update.message.text
    if text.startswith("/group_create") or text.startswith("/group-create"):
        return
    grp_stage = context.user_data.get(GRP_STAGE)
    if grp_stage == "rename_wait" and not text.startswith("/"):
        await group_receive_new_name(update, context)
        return

    gc_stage = context.user_data.get(GC_STAGE)
    if gc_stage and text == "/cancel":
        await group_create_cancel(update, context)
        return
    if gc_stage == "enter_name" and not text.startswith("/"):
        await group_receive_name(update, context)
        return

    db = get_database()

    link = findLink(text)
    if link is not None:
        if "tiktok.com" in link:
            media = await downloadTikTok(link)
            if media:
                await update.message.reply_video(video=media.url)
            else:
                await update.message.reply_text("Failed to download video.")
        elif "instagram.com/reel/" in link or "instagram.com/p/" in link:
            media = await downloadInstagram(link)
            if media:
                if media.extension == ".jpeg":
                    await update.message.reply_photo(photo=media.url)
                else:
                    await update.message.reply_video(video=media.url)
            else:
                await update.message.reply_text("Failed to download media.")

    try:
        if text == "/create chat":
            chatId = update.message.chat["id"]
            title = update.message.chat["title"]
            type = update.message.chat["type"]
            status = await db.create_group_chat(chatId, title, type)
            await update.message.reply_text(
                "Чат был успешно создан" if status else "Что-то пошло не так"
            )

        elif text == "/create me":
            chatId = update.message.chat["id"]
            userId = update.message.from_user["id"]
            firstName = update.message.from_user["first_name"]
            username = update.message.from_user["username"]
            chat = await db.get_group_chat(chatId)
            if chat is None:
                await update.message.reply_text("Сначала выполните /create chat в этом чате.")
                return
            status1 = await db.create_user(userId, firstName, username)
            status2 = await db.add_group_chat_to_user(userId, chatId)
            await update.message.reply_text(
                "Пользователь был успешно добавлен"
                if status1 and status2
                else "Что-то пошло не так"
            )

        elif text == "/update me":
            userId = update.message.from_user["id"]
            firstName = update.message.from_user["first_name"]
            username = update.message.from_user["username"]
            status = await db.update_user(userId, firstName, username)
            await update.message.reply_text(
                "Пользователь был успешно изменен" if status else "Что-то пошло не так"
            )

        elif text.startswith("/create group"):
            chatId = update.message.chat["id"]
            message = await db.create_group(text, chatId)
            await update.message.reply_text(message)

        elif text.startswith("/delete group name:"):
            chatId = update.message.chat["id"]
            status = await db.delete_group(text, chatId)
            await update.message.reply_text(
                "Group was succesfully delited" if status else "Something went wrong"
            )

        elif text.startswith("/add to group "):
            chatId = update.message.chat["id"]
            message = await db.add_users_to_group(text, chatId)
            await update.message.reply_text(message)

        elif text.startswith("/delete users group "):
            chatId = update.message.chat["id"]
            message = await db.delete_users_from_group(text, chatId)
            await update.message.reply_text(message)

        if "@TikTokDownloaderRusBot" in text:
            await update.message.reply_text(choice(respondsOld))

        if text == "/get_commands@TikTokDownloaderRusBot":
            command_list = """/create chat - Add current chat to bot DB
/create me - Add current user to bot DB
/update me - Update user info in bot DB
/group - Меню управления группами
/create group name:{name} users:{username},{username} - Add group to chat
/add to group name:{name} users:{username},{username} - Add users to group
/delete group name:{name} - Delete group from chat
/delete users group name:{name} users:{username},{username}- delete users from group"""
            await update.message.reply_text(command_list)

        if "@all" in text:
            usernames = await db.get_all_usernames(update.message.chat["id"])
            await update.message.reply_text(usernames)
        elif "@" in text:
            chatId = update.message.chat["id"]
            mentions = set(re.findall(r"@(\w+)", text))
            mentions.discard("all")
            if mentions:
                members_by_group = await db.get_group_members_by_names(
                    chatId, list(mentions)
                )
                for group_name, usernames in members_by_group.items():
                    editedText = text.replace(f"@{group_name}", "").strip()
                    mention_list = (
                        ", ".join(f"@{username}" for username in usernames)
                        if usernames
                        else "Не нашёл пользователей"
                    )
                    respondText = (
                        f"{mention_list}\n{editedText}" if editedText else mention_list
                    )
                    await update.message.reply_text(respondText)
    except Exception as exc:  # pragma: no cover
        logger.exception("Database operation failed: %s", exc)
        await _reply_db_error(update)


def _set_target_chat_from_current(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сохраняет текущий чат как целевой для управления группами.

    :param update: Update
    :param context: контекст PTB
    :return: dict с данными чата
    """
    chat = update.effective_chat
    target = {"id": chat.id, "title": chat.title, "type": chat.type}
    context.user_data[GRP_TARGET_CHAT] = target
    return target


async def _show_group_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показывает главное меню /group (создать / список).

    :param update: Update (callback или сообщение)
    :param context: контекст PTB
    :return: None
    """
    target = context.user_data.get(GRP_TARGET_CHAT)
    if not target:
        await update.effective_message.reply_text("Чат не выбран. Запустите /group заново.")
        return
    context.user_data[GRP_STAGE] = "menu"
    chat_label = target.get("title") or target["id"]
    buttons = [
        [InlineKeyboardButton("➕ Создать группу", callback_data="grp_action:create")],
        [InlineKeyboardButton("📜 Список групп", callback_data="grp_action:list:0")],
    ]
    text = f"Управление группами для чата: {chat_label}"
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(buttons))
    else:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=text,
            reply_markup=InlineKeyboardMarkup(buttons),
        )


async def _send_group_list(
    update: Update, context: ContextTypes.DEFAULT_TYPE, page: int
) -> None:
    """Отправляет список групп с пагинацией.

    :param update: Update (callback или сообщение)
    :param context: контекст PTB
    :param page: номер страницы (0-based)
    :return: None
    """
    target = context.user_data.get(GRP_TARGET_CHAT)
    if not target:
        await update.effective_message.reply_text("Чат не выбран. Запустите /group заново.")
        return

    db = get_database()
    requested_page = max(page, 0)
    groups, total = await db.get_groups_paginated(
        target["id"], USERS_PAGE_SIZE, requested_page * USERS_PAGE_SIZE
    )
    total_pages = max(math.ceil(total / USERS_PAGE_SIZE), 1)
    max_page = max(total_pages - 1, 0)
    if requested_page > max_page:
        requested_page = max_page
        groups, total = await db.get_groups_paginated(
            target["id"], USERS_PAGE_SIZE, requested_page * USERS_PAGE_SIZE
        )
        total_pages = max(math.ceil(total / USERS_PAGE_SIZE), 1)

    context.user_data[GRP_STAGE] = "list"
    context.user_data[GRP_PAGE] = requested_page
    context.user_data[GRP_TOTAL] = total

    buttons = [
        [InlineKeyboardButton(f"@{grp['name']}", callback_data=f"grp_open:{grp['id']}")]
        for grp in groups
    ]
    nav_row = [InlineKeyboardButton(f"{requested_page + 1}/{total_pages}", callback_data="grp_ignore")]
    if requested_page > 0:
        nav_row.insert(0, InlineKeyboardButton("◀️ Prev", callback_data="grp_list:prev"))
    if (requested_page + 1) * USERS_PAGE_SIZE < total:
        nav_row.append(InlineKeyboardButton("Next ▶️", callback_data="grp_list:next"))
    buttons.append(nav_row)
    buttons.append([InlineKeyboardButton("⬅️ Меню", callback_data="grp_back_menu")])
    text = f"Группы ({total}): страница {requested_page + 1}/{total_pages}"
    if total == 0:
        text = "Группы не найдены. Создайте новую группу."

    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(
            text, reply_markup=InlineKeyboardMarkup(buttons)
        )
    else:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=text,
            reply_markup=InlineKeyboardMarkup(buttons),
        )


async def group_command_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик /group: выбор чата (в ЛС) и показ меню."""
    _reset_group_state(context)
    _reset_group_create_state(context)
    chat = update.effective_chat
    if chat.type == "private":
        db = get_database()
        user_id = update.effective_user.id
        try:
            chats = await db.get_group_chats_for_user(user_id)
        except Exception as exc:  # pragma: no cover
            logger.exception("Failed to fetch chats for /group: %s", exc)
            await _reply_db_error(update)
            return ConversationHandler.END

        if not chats:
            await update.message.reply_text(
                "Не нашёл чатов с вашим участием. Добавьте меня в чат и выполните /create chat и /create me."
            )
            return ConversationHandler.END

        context.user_data[GRP_AVAILABLE_CHATS] = chats
        context.user_data[GRP_STAGE] = "choose_chat"
        buttons = [
            [
                InlineKeyboardButton(
                    chat_item.get("title") or str(chat_item["id"]),
                    callback_data=f"grpchat:{chat_item['id']}",
                )
            ]
            for chat_item in chats
        ]
        await update.message.reply_text(
            "Выберите чат для управления группами:", reply_markup=InlineKeyboardMarkup(buttons)
        )
        await _delete_user_message(update, context)
        return ConversationHandler.END

    _set_target_chat_from_current(update, context)
    await _show_group_menu(update, context)
    await _delete_user_message(update, context)
    return ConversationHandler.END


async def _load_group_and_check_chat(group_id: int, target_chat_id: int):
    """Возвращает группу, убеждаясь что она принадлежит чату."""
    db = get_database()
    group = await db.get_group_by_id(group_id)
    if not group or group["group_chat_id"] != target_chat_id:
        return None
    return group


async def group_choose_chat_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выбор чата в ЛС."""
    query = update.callback_query
    await query.answer()
    data = query.data or ""
    chat_id_text = data.split(":", 1)[1] if ":" in data else None
    chats = context.user_data.get(GRP_AVAILABLE_CHATS, [])
    try:
        chat_id = int(chat_id_text)
    except (TypeError, ValueError):
        await query.edit_message_text("Не понял выбранный чат. Запустите /group заново.")
        _reset_group_state(context)
        return ConversationHandler.END

    selected = next((item for item in chats if item["id"] == chat_id), None)
    if not selected:
        await query.edit_message_text("Чат не найден. Запустите /group заново.")
        _reset_group_state(context)
        return ConversationHandler.END

    context.user_data[GRP_TARGET_CHAT] = selected
    _store_prompt_message(query.message, context, GRP_PROMPT_MSG)
    await _show_group_menu(update, context)
    return ConversationHandler.END


async def group_open(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    """Показывает карточку группы и действия."""
    target = context.user_data.get(GRP_TARGET_CHAT)
    if not target:
        await update.effective_message.reply_text("Чат не выбран. Запустите /group заново.")
        return

    group = await _load_group_and_check_chat(group_id, target["id"])
    if not group:
        await update.effective_message.reply_text("Группа не найдена.")
        return

    db = get_database()
    members = await db.get_usernames_by_group(group_id)
    context.user_data[GRP_SELECTED_GROUP] = group_id
    context.user_data[GRP_STAGE] = "group_view"
    text_lines = [
        f"Группа @{group['name']}",
        f"Участники: {members}",
    ]
    buttons = [
        [InlineKeyboardButton("✏️ Переименовать", callback_data=f"grp_rename:{group_id}")],
        [InlineKeyboardButton("👥 Изменить участников", callback_data=f"grp_edit:{group_id}")],
        [InlineKeyboardButton("🗑 Удалить", callback_data=f"grp_delete:{group_id}")],
        [InlineKeyboardButton("⬅️ К списку", callback_data="grp_back_list")],
        [InlineKeyboardButton("🏠 Меню", callback_data="grp_back_menu")],
    ]
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(
            "\n".join(text_lines), reply_markup=InlineKeyboardMarkup(buttons)
        )
    else:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="\n".join(text_lines),
            reply_markup=InlineKeyboardMarkup(buttons),
        )


async def group_receive_new_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Принимает новое имя выбранной группы."""
    target = context.user_data.get(GRP_TARGET_CHAT)
    group_id = context.user_data.get(GRP_SELECTED_GROUP)
    if not target or not group_id:
        await update.message.reply_text("Группа не выбрана. Запустите /group заново.")
        _reset_group_state(context)
        return

    new_name = update.message.text.strip().lstrip("@")
    db = get_database()
    success, message = await db.rename_group(group_id, target["id"], new_name)
    await update.message.reply_text(message)
    await _delete_prompt_and_user_message(update, context, GRP_PROMPT_MSG)
    context.user_data[GRP_STAGE] = "menu"
    if success:
        await _show_group_menu(update, context)


async def group_edit_users(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    """Открывает выбор участников для существующей группы."""
    target = context.user_data.get(GRP_TARGET_CHAT)
    if not target:
        await update.effective_message.reply_text("Чат не выбран. Запустите /group заново.")
        return

    group = await _load_group_and_check_chat(group_id, target["id"])
    if not group:
        await update.effective_message.reply_text("Группа не найдена.")
        return

    db = get_database()
    member_ids = await db.get_group_user_ids(group_id)
    _reset_group_create_state(context)
    context.user_data[GC_TARGET_CHAT] = target
    context.user_data[GC_GROUP_NAME] = group["name"]
    context.user_data[GC_SELECTED_USERS] = set(member_ids)
    context.user_data[GC_STAGE] = "select_users"
    context.user_data[GRP_SELECTED_GROUP] = group_id
    await _send_user_selection(update, context, 0)


async def group_delete_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    """Показывает подтверждение удаления группы."""
    target = context.user_data.get(GRP_TARGET_CHAT)
    if not target:
        await update.effective_message.reply_text("Чат не выбран. Запустите /group заново.")
        return

    group = await _load_group_and_check_chat(group_id, target["id"])
    if not group:
        await update.effective_message.reply_text("Группа не найдена.")
        return

    context.user_data[GRP_SELECTED_GROUP] = group_id
    buttons = [
        [
            InlineKeyboardButton("✅ Да, удалить", callback_data=f"grp_delete_yes:{group_id}"),
            InlineKeyboardButton("⬅️ Назад", callback_data=f"grp_open:{group_id}"),
        ]
    ]
    buttons.append([InlineKeyboardButton("🏠 Меню", callback_data="grp_back_menu")])
    text = f"Удалить группу @{group['name']}? Это действие необратимо."
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(
            text, reply_markup=InlineKeyboardMarkup(buttons)
        )
    else:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=text,
            reply_markup=InlineKeyboardMarkup(buttons),
        )


async def group_delete_execute(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    """Удаляет группу после подтверждения."""
    target = context.user_data.get(GRP_TARGET_CHAT)
    if not target:
        await update.effective_message.reply_text("Чат не выбран. Запустите /group заново.")
        return

    db = get_database()
    success, message = await db.delete_group_by_id(group_id, target["id"])
    await update.effective_message.reply_text(message)
    if success:
        context.user_data[GRP_SELECTED_GROUP] = None
        await _show_group_menu(update, context)


async def group_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает callback-поток /group."""
    query = update.callback_query
    data = query.data or ""
    logger.info("Group menu callback: %s", data)
    if data.startswith("grpchat:"):
        return await group_choose_chat_menu(update, context)
    if data == "grp_back_menu":
        return await _show_group_menu(update, context)
    if data.startswith("grp_action:create"):
        target = context.user_data.get(GRP_TARGET_CHAT)
        if not target:
            await query.answer("Чат не выбран", show_alert=True)
            return
        _reset_group_create_state(context)
        context.user_data[GC_TARGET_CHAT] = target
        context.user_data[GC_STAGE] = "enter_name"
        context.user_data[GRP_STAGE] = "create_name"
        await query.answer()
        await query.edit_message_text("Введите имя новой группы (буквы, цифры, _ ).")
        _store_prompt_message(query.message, context, GRP_PROMPT_MSG)
        return
    if data.startswith("grp_action:list"):
        await _send_group_list(update, context, 0)
        return
    if data == "grp_list:prev":
        page = max(0, (context.user_data.get(GRP_PAGE) or 0) - 1)
        await _send_group_list(update, context, page)
        return
    if data == "grp_list:next":
        page = (context.user_data.get(GRP_PAGE) or 0) + 1
        await _send_group_list(update, context, page)
        return
    if data == "grp_back_list":
        await _send_group_list(update, context, context.user_data.get(GRP_PAGE, 0))
        return
    if data.startswith("grp_open:"):
        try:
            group_id = int(data.split(":", 1)[1])
        except ValueError:
            await query.answer("Не понял группу", show_alert=True)
            return
        await group_open(update, context, group_id)
        return
    if data.startswith("grp_rename:"):
        try:
            group_id = int(data.split(":", 1)[1])
        except ValueError:
            await query.answer("Не понял группу", show_alert=True)
            return
        context.user_data[GRP_SELECTED_GROUP] = group_id
        context.user_data[GRP_STAGE] = "rename_wait"
        await query.answer()
        await query.edit_message_text("Введите новое имя группы (буквы, цифры, _ ).")
        return
    if data.startswith("grp_edit:"):
        try:
            group_id = int(data.split(":", 1)[1])
        except ValueError:
            await query.answer("Не понял группу", show_alert=True)
            return
        await group_edit_users(update, context, group_id)
        return
    if data.startswith("grp_delete:"):
        try:
            group_id = int(data.split(":", 1)[1])
        except ValueError:
            await query.answer("Не понял группу", show_alert=True)
            return
        await group_delete_confirm(update, context, group_id)
        return
    if data.startswith("grp_delete_yes:"):
        try:
            group_id = int(data.split(":", 1)[1])
        except ValueError:
            await query.answer("Не понял группу", show_alert=True)
            return
        await query.answer()
        await group_delete_execute(update, context, group_id)
        return
    if data == "grp_ignore":
        await query.answer()
        return
    await query.answer("Неизвестное действие")


async def group_create_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Старт сценария /group-create: выбирает чат (в ЛС) или запрашивает имя в группе.

    :param update: Update команды
    :param context: контекст PTB
    :return: состояние выбора чата/имени или END
    """
    _reset_group_create_state(context)

    chat = update.effective_chat
    if chat.type == "private":
        db = get_database()
        user_id = update.effective_user.id
        try:
            chats = await db.get_group_chats_for_user(user_id)
        except Exception as exc:  # pragma: no cover
            logger.exception("Failed to fetch chats for /group-create: %s", exc)
            await _reply_db_error(update)
            return ConversationHandler.END

        if not chats:
            await update.message.reply_text(
                "Не нашёл чатов с вашим участием. Добавьте меня в чат и выполните /create chat и /create me."
            )
            return ConversationHandler.END

        context.user_data[GC_STAGE] = "choose_chat"
        context.user_data[GC_AVAILABLE_CHATS] = chats
        buttons = [
            [
                InlineKeyboardButton(
                    chat_item.get("title") or str(chat_item["id"]),
                    callback_data=f"gcchat:{chat_item['id']}",
                )
            ]
            for chat_item in chats
        ]
        await update.message.reply_text(
            "Выберите чат, для которого создаём группу:",
            reply_markup=InlineKeyboardMarkup(buttons),
        )
        return GROUP_SELECT_CHAT

    context.user_data[GC_TARGET_CHAT] = {
        "id": chat.id,
        "title": chat.title,
        "type": chat.type,
    }
    context.user_data[GC_STAGE] = "enter_name"
    prompt = await update.message.reply_text("Введите имя группы (буквы, цифры, _ ).")
    _store_prompt_message(prompt, context, GC_PROMPT_MSG)
    return GROUP_ENTER_NAME


async def group_choose_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает выбор чата в ЛС.

    :param update: callback с gcchat:<id>
    :param context: контекст PTB
    :return: состояние ввода имени или END
    """
    query = update.callback_query
    await query.answer()
    data = query.data or ""
    chat_id_text = data.split(":", 1)[1] if ":" in data else None
    chats = context.user_data.get(GC_AVAILABLE_CHATS, [])
    try:
        chat_id = int(chat_id_text)
    except (TypeError, ValueError):
        await query.edit_message_text("Не понял выбранный чат. Запустите /group-create заново.")
        _reset_group_create_state(context)
        return ConversationHandler.END

    selected = next((item for item in chats if item["id"] == chat_id), None)
    if not selected:
        await query.edit_message_text("Чат не найден. Запустите /group-create заново.")
        _reset_group_create_state(context)
        return ConversationHandler.END

    context.user_data[GC_TARGET_CHAT] = selected
    context.user_data[GC_STAGE] = "enter_name"
    await query.edit_message_text(
        f"Чат выбран: {selected.get('title') or selected['id']}.\n"
        "Введите имя группы (буквы, цифры, _ )."
    )
    _store_prompt_message(query.message, context, GC_PROMPT_MSG)
    return GROUP_ENTER_NAME


async def group_receive_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Принимает имя группы и открывает выбор пользователей.

    :param update: сообщение с именем группы
    :param context: контекст PTB
    :return: состояние выбора пользователей или END
    """
    target_chat = context.user_data.get(GC_TARGET_CHAT)
    if not target_chat:
        await update.message.reply_text("Чат не выбран. Запустите /group-create заново.")
        _reset_group_create_state(context)
        return ConversationHandler.END

    name = update.message.text.strip().lstrip("@")
    if not GROUP_NAME_PATTERN.fullmatch(name):
        await update.message.reply_text("Неверное имя группы. Используйте буквы, цифры и _")
        context.user_data[GC_STAGE] = "enter_name"
        return GROUP_ENTER_NAME

    context.user_data[GC_GROUP_NAME] = name
    context.user_data[GC_SELECTED_USERS] = set()
    context.user_data[GC_STAGE] = "select_users"
    await _delete_prompt_and_user_message(update, context, GRP_PROMPT_MSG)
    await _delete_prompt_and_user_message(update, context, GC_PROMPT_MSG)
    return await _send_user_selection(update, context, 0)


async def _finalize_group_creation(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    """Сохраняет группу и выбранных пользователей в БД.

    :param update: callback Submit
    :param context: контекст PTB
    :return: END
    """
    query = update.callback_query
    target_chat = context.user_data.get(GC_TARGET_CHAT)
    group_name = context.user_data.get(GC_GROUP_NAME)
    selected_ids = context.user_data.get(GC_SELECTED_USERS) or set()
    if not target_chat or not group_name:
        await query.answer("Данные устарели, начните заново.", show_alert=True)
        await query.edit_message_text("Данные не найдены. Запустите /group-create ещё раз.")
        _reset_group_create_state(context)
        return ConversationHandler.END

    db = get_database()
    user_ids = [int(user_id) for user_id in selected_ids]
    success, message, group_id, total_members = await db.create_group_with_users(
        target_chat["id"], group_name, user_ids
    )
    await query.answer()
    actor_username = update.effective_user.username if update.effective_user else None
    actor_first = update.effective_user.first_name if update.effective_user else None
    actor_label = f"@{actor_username}" if actor_username else (actor_first or "пользователем")
    if success:
        final_msg = (
            f"Группа @{group_name} обновлена участником {actor_label}. "
            f"Участников группы: {total_members}"
        )
    else:
        final_msg = message
    await query.edit_message_text(final_msg)
    _reset_group_create_state(context)
    return ConversationHandler.END


async def group_handle_user_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает все gc*-callback: навигация, выбор пользователей, submit/cancel.

    :param update: callback Update
    :param context: контекст PTB
    :return: состояние выбора пользователей или END
    """
    query = update.callback_query
    data = query.data or ""
    logger.info(
        "Group create callback: data=%s chat=%s user=%s state=%s",
        data,
        getattr(query.message, "chat_id", None),
        query.from_user.id if query.from_user else None,
        dict(context.user_data),
    )
    try:
        if data.startswith("gcchat:"):
            return await group_choose_chat(update, context)

        if data == "gc_ignore":
            await query.answer()
            return GROUP_SELECT_USERS

        if data == "gc_cancel":
            await query.answer("Отменено")
            await query.edit_message_text("Создание группы отменено.")
            _reset_group_create_state(context)
            return ConversationHandler.END

        if data == "gc_submit":
            return await _finalize_group_creation(update, context)

        if data.startswith("gc_nav:"):
            direction = data.split(":", 1)[1]
            current_page = context.user_data.get(GC_PAGE, 0)
            if direction == "next":
                current_page += 1
            elif direction == "prev":
                current_page = max(0, current_page - 1)
            return await _send_user_selection(update, context, current_page)

        if data.startswith("gc_user:"):
            try:
                user_id = int(data.split(":", 1)[1])
            except ValueError:
                await query.answer("Не удалось обработать пользователя.", show_alert=True)
                return GROUP_SELECT_USERS
            selected = context.user_data.get(GC_SELECTED_USERS)
            if selected is None:
                selected = set()
                context.user_data[GC_SELECTED_USERS] = selected
            if user_id in selected:
                selected.remove(user_id)
            else:
                selected.add(user_id)
            await query.answer("Обновлено")
            return await _send_user_selection(update, context, context.user_data.get(GC_PAGE, 0))

        await query.answer()
        return GROUP_SELECT_USERS
    except Exception as exc:  # pragma: no cover
        logger.exception("Failed to handle group callback %s: %s", data, exc)
        try:
            await query.answer("Произошла ошибка, попробуйте ещё раз.", show_alert=True)
        except Exception:
            pass
        return ConversationHandler.END


async def log_unknown_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Логирует любые неожиданные callback и отвечает пользователю.

    :param update: callback Update
    :param context: контекст PTB
    :return: None
    """
    query = update.callback_query
    if not query:
        return
    logger.warning(
        "Unknown callback data=%s chat=%s user=%s",
        query.data,
        getattr(query.message, "chat_id", None),
        query.from_user.id if query.from_user else None,
    )
    try:
        await query.answer("Неизвестное действие.", show_alert=False)
    except Exception:
        pass


async def group_create_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена сценария создания группы.

    :param update: команда /cancel или callback gc_cancel
    :param context: контекст PTB
    :return: END
    """
    if update.callback_query:
        await update.callback_query.answer("Отменено")
        await update.callback_query.edit_message_text("Создание группы отменено.")
    elif update.message:
        await update.message.reply_text("Создание группы отменено.")
    _reset_group_create_state(context)
    return ConversationHandler.END


async def say_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        await update.message.reply_text("Команда /say доступна только в личных сообщениях.")
        return ConversationHandler.END

    user_id = update.effective_user.id
    db = get_database()
    try:
        chats = await db.get_group_chats_for_user(user_id)
    except Exception as exc:  # pragma: no cover
        logger.exception("Failed to fetch chats for /say: %s", exc)
        await _reply_db_error(update)
        return ConversationHandler.END

    if not chats:
        await update.message.reply_text(
            "Я не знаю ни одного чата с вашим участием. Добавьте меня в чат и выполните /create chat и /create me."
        )
        return ConversationHandler.END

    context.user_data["say_chats"] = chats
    chat_lines = [
        "Выберите чат, ответив его номером:",
        *[
            f"{idx}. {chat.get('title') or chat['id']} (ID: {chat['id']})"
            for idx, chat in enumerate(chats, start=1)
        ],
        "Введите номер или /cancel для отмены.",
    ]
    await update.message.reply_text("\n".join(chat_lines))
    return SAY_SELECT_CHAT


async def say_choose_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return ConversationHandler.END

    chats = context.user_data.get("say_chats")
    if not chats:
        await update.message.reply_text("Чаты не найдены. Вызовите /say заново.")
        return ConversationHandler.END

    choice_text = update.message.text.strip()
    if not choice_text.isdigit():
        await update.message.reply_text("Введите номер из списка.")
        return SAY_SELECT_CHAT

    index = int(choice_text) - 1
    if index < 0 or index >= len(chats):
        await update.message.reply_text("Такого номера нет. Попробуйте ещё раз.")
        return SAY_SELECT_CHAT

    context.user_data["say_selected_chat"] = chats[index]
    await update.message.reply_text(
        "Отправьте сообщение, которое нужно переслать, или /cancel для отмены."
    )
    return SAY_ENTER_MESSAGE


async def say_receive_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return ConversationHandler.END

    selected = context.user_data.get("say_selected_chat")
    if not selected:
        await update.message.reply_text("Чат не выбран. Запустите /say заново.")
        return ConversationHandler.END

    text_to_send = update.message.text
    try:
        await context.bot.send_message(chat_id=selected["id"], text=text_to_send)
        await update.message.reply_text(
            f"Сообщение отправлено в {selected.get('title') or selected['id']}."
        )
    except Exception as exc:  # pragma: no cover
        logger.exception("Failed to send /say message: %s", exc)
        await update.message.reply_text("Не удалось отправить сообщение.")

    context.user_data.pop("say_selected_chat", None)
    context.user_data.pop("say_chats", None)
    return ConversationHandler.END


async def say_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message:
        await update.message.reply_text("Отменено.")
    context.user_data.pop("say_selected_chat", None)
    context.user_data.pop("say_chats", None)
    return ConversationHandler.END


def build_say_conversation_handler():
    return ConversationHandler(
        entry_points=[CommandHandler("say", say_start)],
        states={
            SAY_SELECT_CHAT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, say_choose_chat)
            ],
            SAY_ENTER_MESSAGE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, say_receive_message)
            ],
        },
        fallbacks=[CommandHandler("cancel", say_cancel)],
        allow_reentry=True,
    )
