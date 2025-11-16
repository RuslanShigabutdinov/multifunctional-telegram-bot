from telegram.ext import Application, MessageHandler, filters

from app.handlers import build_say_conversation_handler, handle_message
from utils.settings import get_settings


def build_application() -> Application:
    settings = get_settings().require()
    application = Application.builder().token(settings.telegram_bot_token).build()
    application.add_handler(build_say_conversation_handler())
    application.add_handler(MessageHandler(filters.TEXT, handle_message))
    return application


def run() -> None:
    application = build_application()
    application.run_polling(poll_interval=3)
