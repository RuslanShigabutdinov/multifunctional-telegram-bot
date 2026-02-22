import logging

from telegram import Update
from telegram import (
    BotCommand,
    BotCommandScopeAllGroupChats,
    BotCommandScopeAllPrivateChats,
    BotCommandScopeDefault,
)
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    MessageHandler,
    filters,
)

from app.handlers import (
    build_say_conversation_handler,
    group_command_start,
    group_handle_user_callback,
    group_menu_callback,
    handle_bug_command,
    handle_bug_media,
    handle_chat_migration,
    handle_feature_command,
    handle_feature_media,
    handle_message,
    log_unknown_callback,
)
from services.database import close_database, init_database
from utils.settings import get_settings

logging.basicConfig(level=logging.INFO)


async def _post_init(application: Application) -> None:
    """Инициализация БД и установка списка команд (только /group).

    :param application: экземпляр приложения PTB (не используется напрямую)
    :return: None
    """
    await init_database()
    try:
        commands = [
            BotCommand("group", "Меню управления группами"),
            BotCommand("bug", "Сообщить о баге"),
            BotCommand("feature", "Предложить фичу"),
        ]
        # Сначала очищаем любые старые команды.
        await application.bot.delete_my_commands(scope=BotCommandScopeDefault())
        await application.bot.delete_my_commands(scope=BotCommandScopeAllGroupChats())
        await application.bot.delete_my_commands(scope=BotCommandScopeAllPrivateChats())
        # Задаём только /group для всех скоупов.
        await application.bot.set_my_commands(commands, scope=BotCommandScopeDefault())
        await application.bot.set_my_commands(commands, scope=BotCommandScopeAllGroupChats())
        await application.bot.set_my_commands(commands, scope=BotCommandScopeAllPrivateChats())
    except Exception as exc:  # pragma: no cover
        logging.exception("Failed to set bot commands: %s", exc)


async def _post_shutdown(application: Application) -> None:
    """Закрытие подключения к БД при остановке бота.

    :param application: экземпляр приложения PTB (не используется напрямую)
    :return: None
    """
    del application
    await close_database()


def build_application() -> Application:
    """Создаёт и настраивает Application.

    :return: готовое приложение Telegram.
    """
    settings = get_settings().require()
    application = (
        Application.builder()
        .token(settings.telegram_bot_token)
        .post_init(_post_init)
        .post_shutdown(_post_shutdown)
        .build()
    )
    application.add_handler(MessageHandler(filters.StatusUpdate.MIGRATE, handle_chat_migration))
    application.add_handler(build_say_conversation_handler())
    application.add_handler(CommandHandler("bug", handle_bug_command))
    application.add_handler(CommandHandler("feature", handle_feature_command))
    application.add_handler(MessageHandler(
        filters.CaptionRegex(r"^/bug") & (filters.PHOTO | filters.VIDEO),
        handle_bug_media,
    ))
    application.add_handler(MessageHandler(
        filters.CaptionRegex(r"^/feature") & (filters.PHOTO | filters.VIDEO),
        handle_feature_media,
    ))
    application.add_handler(CommandHandler("group", group_command_start))
    application.add_handler(CallbackQueryHandler(group_menu_callback, pattern=r"^grp"))
    application.add_handler(CallbackQueryHandler(group_handle_user_callback, pattern=r"^gc"))
    application.add_handler(CallbackQueryHandler(log_unknown_callback))
    application.add_handler(MessageHandler(filters.TEXT, handle_message))
    return application


def run() -> None:
    """Точка входа для запуска бота."""
    application = build_application()
    application.run_polling(poll_interval=3, allowed_updates=Update.ALL_TYPES)
