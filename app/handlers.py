from random import randint
from secrets import choice
import logging

from telegram import Update
from telegram.ext import (
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from app.responds import responds, respondsOld
from services.database import DataBase
from services.media.instagram import downloadInstagram
from services.media.tiktok import downloadTikTok, findLink

logger = logging.getLogger(__name__)

SELECT_CHAT, ENTER_MESSAGE = range(2)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.edited_message:
        return

    text: str = update.message.text
    user = update.message.from_user
    username = user["username"]

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

    if username != "amialmighty":
        chance = randint(0, 100)
        if chance == 69:
            await update.message.reply_text(choice(responds))

    if text == "/create chat":
        chatId = update.message.chat["id"]
        title = update.message.chat["title"]
        type = update.message.chat["type"]
        db = DataBase()
        status = db.createGroupChat(chatId, title, type)
        await update.message.reply_text(
            "Чат был успешно создан" if status else "Что-то пошло не так"
        )

    elif text == "/create me":
        chatId = update.message.chat["id"]
        userId = update.message.from_user["id"]
        firstName = update.message.from_user["first_name"]
        username = update.message.from_user["username"]
        db = DataBase()
        chat = db.getGroupChat(chatId)
        if chat is None:
            await update.message.reply_text("Сначала выполните /create chat в этом чате.")
            return
        status1 = db.createUser(userId, firstName, username)
        status2 = db.addGroupChatToUser(userId, chatId)
        await update.message.reply_text(
            "Пользователь был успешно добавлен"
            if status1 and status2
            else "Что-то пошло не так"
        )

    elif text == "/update me":
        userId = update.message.from_user["id"]
        firstName = update.message.from_user["first_name"]
        username = update.message.from_user["username"]
        status = DataBase().updateUser(userId, firstName, username)
        await update.message.reply_text(
            "Пользователь был успешно изменен" if status else "Что-то пошло не так"
        )

    elif text.startswith("/create group "):
        chatId = update.message.chat["id"]
        message = DataBase().createGroup(text, chatId)
        await update.message.reply_text(message)

    elif text.startswith("/delete group name:"):
        chatId = update.message.chat["id"]
        status = DataBase().deleteGroup(text, chatId)
        await update.message.reply_text(
            "Group was succesfully delited" if status else "Something went wrong"
        )

    elif text.startswith("/add to group "):
        chatId = update.message.chat["id"]
        message = DataBase().addUsersToGroup(text, chatId)
        await update.message.reply_text(message)

    elif text.startswith("/delete users group "):
        chatId = update.message.chat["id"]
        message = DataBase().deleteUsersFromGroup(text, chatId)
        await update.message.reply_text(message)

    if "@TikTokDownloaderRusBot" in text:
        await update.message.reply_text(choice(respondsOld))

    if text == "/get_commands@TikTokDownloaderRusBot":
        command_list = """/create chat - Add current chat to bot DB
/create me - Add current user to bot DB
/update me - Update user info in bot DB
/create group name:{name} users:{username},{username} - Add group to chat
/add to group name:{name} users:{username},{username} - Add users to group
/delete group name:{name} - Delete group from chat
/delete users group name:{name} users:{username},{username}- delete users from group"""
        await update.message.reply_text(command_list)

    if "@all" in text:
        usernames = DataBase().getAllUsernames(update.message.chat["id"])
        await update.message.reply_text(usernames)
    elif "@" in text:
        groups = DataBase().getAllGroups(update.message.chat["id"])
        for group in groups:
            if "@" + group["name"] in text:
                groupName = group["name"].replace("@", "")
                chatId = update.message.chat["id"]
                currentGroup = DataBase().getGroupByGroupChatIdAndName(
                    chatId, groupName
                )
                editedText = text.replace("@" + groupName, "")
                respondText = (
                    DataBase().getUsernamesByGroup(currentGroup["id"]) + "\n" + editedText
                )
                await update.message.reply_text(respondText)


async def say_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        await update.message.reply_text("Команда /say доступна только в личных сообщениях.")
        return ConversationHandler.END

    user_id = update.effective_user.id
    db = DataBase()
    chats = db.getGroupChatsForUser(user_id)
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
    return SELECT_CHAT


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
        return SELECT_CHAT

    index = int(choice_text) - 1
    if index < 0 or index >= len(chats):
        await update.message.reply_text("Такого номера нет. Попробуйте ещё раз.")
        return SELECT_CHAT

    context.user_data["say_selected_chat"] = chats[index]
    await update.message.reply_text(
        "Отправьте сообщение, которое нужно переслать, или /cancel для отмены."
    )
    return ENTER_MESSAGE


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
            SELECT_CHAT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, say_choose_chat)
            ],
            ENTER_MESSAGE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, say_receive_message)
            ],
        },
        fallbacks=[CommandHandler("cancel", say_cancel)],
        allow_reentry=True,
    )
