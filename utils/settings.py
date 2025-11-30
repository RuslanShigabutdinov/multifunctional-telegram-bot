from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Settings:
    telegram_bot_token: str
    tiktok_api_key: str
    instagram_api_key: str
    gemini_api_key: str
    gemini_model: str
    bot_name: str
    bot_username: str
    chat_history_limit: int
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()
        return cls(
            telegram_bot_token=os.getenv("TOKEN", ""),
            tiktok_api_key=os.getenv("TIKTOK_KEY", ""),
            instagram_api_key=os.getenv("INSTAGRAM_KEY", ""),
            gemini_api_key=os.getenv("GEMINI_API_KEY", ""),
            gemini_model=os.getenv("GEMINI_MODEL", ""),
            bot_name=os.getenv("BOT_NAME", ""),
            bot_username=os.getenv("BOT_USERNAME", ""),
            chat_history_limit=int(os.getenv("CHAT_HISTORY_LIMIT", "100")),
            db_host=os.getenv("DATABASE_HOST", ""),
            db_port=int(os.getenv("DATABASE_PORT", "5432")),
            db_name=os.getenv("DATABASE_NAME", ""),
            db_user=os.getenv("DATABASE_USER", ""),
            db_password=os.getenv("DATABASE_PASSWORD", ""),
        )

    def require(self) -> "Settings":
        missing = []
        if not self.telegram_bot_token:
            missing.append("TOKEN")
        if not self.tiktok_api_key:
            missing.append("TIKTOK_KEY")
        if not self.instagram_api_key:
            missing.append("INSTAGRAM_KEY")
        if not self.gemini_api_key:
            missing.append("GEMINI_API_KEY")
        if not self.bot_name:
            missing.append("BOT_NAME")
        if not self.bot_username:
            missing.append("BOT_USERNAME")
        if not self.db_host:
            missing.append("DATABASE_HOST")
        if not self.db_name:
            missing.append("DATABASE_NAME")
        if not self.db_user:
            missing.append("DATABASE_USER")
        if not self.db_password:
            missing.append("DATABASE_PASSWORD")
        if missing:
            raise RuntimeError(
                f"Missing required environment variables: {', '.join(missing)}"
            )
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings.from_env()
