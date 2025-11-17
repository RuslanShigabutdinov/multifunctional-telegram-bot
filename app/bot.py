from telegram.ext import Application, MessageHandler, filters

from app.handlers import build_say_conversation_handler, handle_message
from services.database import close_database, init_database
from utils.settings import get_settings


async def _post_init(application: Application) -> None:
    del application
    await init_database()


async def _post_shutdown(application: Application) -> None:
    del application
    await close_database()


def build_application() -> Application:
    settings = get_settings().require()
    application = (
        Application.builder()
        .token(settings.telegram_bot_token)
        .post_init(_post_init)
        .post_shutdown(_post_shutdown)
        .build()
    )
    application.add_handler(build_say_conversation_handler())
    application.add_handler(MessageHandler(filters.TEXT, handle_message))
    return application


def run() -> None:
    application = build_application()
    application.run_polling(poll_interval=3)
