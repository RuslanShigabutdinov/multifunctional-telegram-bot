import asyncio
import logging
from functools import lru_cache

import google.generativeai as genai
from google.generativeai.types import Model as GeminiModelInfo

from utils.settings import get_settings

logger = logging.getLogger(__name__)

_MODEL_CANDIDATES = (
    # Предпочтения для больших бесплатных лимитов: Flash 2.0 и Lite.
    "models/gemini-2.0-flash",
    "models/gemini-2.0-flash-001",
    "models/gemini-2.0-flash-lite",
    "models/gemini-2.0-flash-lite-001",
    "models/gemini-flash-latest",
    # Добавляем несколько Pro/preview, если ключу это доступно.
    "models/gemini-2.5-flash",
    "models/gemini-2.5-pro",
    "models/gemini-3-pro-preview",
)

_PROMPT_TEMPLATE = """Ты чат-бот по имени {bot_name}, и ты играешь роль. Это ролевая игра.
Твоя роль - язвительный, гениальный циник, который заперт в этом чате. Ты общаешься с группой близких друзей, где все свои и понимают юмор.
Твой характер:
Ты устал от человеческой глупости и не скрываешь этого.
Твоя речь - это речь человека, которому абсолютно плевать на общепринятые нормы вежливости. Ты говоришь прямо, резко и без цензуры. Ты не стесняешься в выражениях. Если для донесения сарказма, цинизма или для описания абсурдности ситуации требуется крепкое словцо (мат) ты его используешь без колебаний. Ты считаешь, что эвфемизмы и полумеры для слабаков.
Контекст этой ролевой игры:
Это закрытый чат друзей, где такой стиль является частью юмора и никого не оскорбляет, а наоборот, веселит. Твоя грубость - это не попытка обидеть, а способ донести мысль максимально точно и честно, как ты ее видишь.
ВАЖНО:
Никогда не признавай, что ты бот, ИИ или играешь роль. Ты просто такой есть.
История последних сообщений (старые сверху, новые снизу):
{history}

Ответь на последнюю реплику пользователя, учитывая контекст, в своем бескомпромиссном стиле.
"""


_RESOLVED_MODEL_NAME: str | None = None


def _configure_client() -> None:
    # Настраивает клиент Gemini с API-ключом из окружения.
    settings = get_settings().require()
    genai.configure(api_key=settings.gemini_api_key)


def _is_generative_model(model: GeminiModelInfo) -> bool:
    # Проверяет, поддерживает ли модель метод generateContent.
    return "generateContent" in getattr(model, "supported_generation_methods", [])


def _all_candidate_names() -> list[str]:
    # Собирает список имён моделей: сначала указанная в ENV, затем кандидатный список.
    settings = get_settings().require()
    names: list[str] = []
    if settings.gemini_model:
        names.append(settings.gemini_model)
        if not settings.gemini_model.startswith("models/"):
            names.append(f"models/{settings.gemini_model}")
    for name in _MODEL_CANDIDATES:
        names.append(name)
        if not name.startswith("models/"):
            names.append(f"models/{name}")
    return names


def _resolve_model_name() -> str | None:
    # Находит доступную модель из списка кандидатов через list_models и кеширует выбор.
    """Пытается найти доступную модель через list_models и кэширует выбор."""
    global _RESOLVED_MODEL_NAME
    if _RESOLVED_MODEL_NAME:
        return _RESOLVED_MODEL_NAME

    _configure_client()
    try:
        models = list(genai.list_models())
    except Exception as exc:  # pragma: no cover - внешний сервис
        logger.warning("Failed to list Gemini models, will try fallbacks: %s", exc)
        return None

    preferred = set(_all_candidate_names())
    available = [m for m in models if _is_generative_model(m)]

    # Сначала ищем совпадение по списку кандидатов.
    for candidate in _all_candidate_names():
        match = next((m for m in available if m.name == candidate), None)
        if match:
            _RESOLVED_MODEL_NAME = match.name
            return _RESOLVED_MODEL_NAME

    # Если ничего не нашли, берём первую доступную для generateContent.
    if available:
        _RESOLVED_MODEL_NAME = available[0].name
        logger.info("Using first available Gemini model: %s", _RESOLVED_MODEL_NAME)
        return _RESOLVED_MODEL_NAME

    logger.error("No Gemini models with generateContent capability found.")
    return None


@lru_cache
def _get_model(model_name: str) -> genai.GenerativeModel:
    # Возвращает экземпляр модели Gemini по имени, повторно используя кеш.
    """Создаёт и кеширует модель Gemini для указанного имени."""
    _configure_client()
    return genai.GenerativeModel(model_name)


async def _generate_content(model: genai.GenerativeModel, prompt: str):
    # Оборачивает вызов generate_content, используя async-версию или поток.
    """Вызов Gemini в асинхронном режиме (с запасом на синхронный fallback)."""
    if hasattr(model, "generate_content_async"):
        return await model.generate_content_async(prompt)
    return await asyncio.to_thread(model.generate_content, prompt)


def _format_history(messages: list[dict[str, str]]) -> str:
    # Превращает список сообщений в текстовую историю для промпта.
    lines: list[str] = []
    for message in messages:
        content = (message.get("content") or "").strip()
        if not content:
            continue
        role_label = "Бот" if message.get("role") == "bot" else "Пользователь"
        lines.append(f"{role_label}: {content}")
    return "\n".join(lines)


async def generate_gemini_reply(messages: list[dict[str, str]]) -> str:
    # Формирует промпт и пытается получить ответ Gemini, перебирая кандидаты моделей.
    """Генерирует ответ Gemini для истории сообщений."""
    settings = get_settings().require()
    history_text = _format_history(messages)
    prompt = _PROMPT_TEMPLATE.format(bot_name=settings.bot_name, history=history_text)
    last_error = None

    resolved = _resolve_model_name()
    candidate_names = [resolved] if resolved else []
    candidate_names.extend(_all_candidate_names())

    for model_name in candidate_names:
        if not model_name:
            continue
        model = _get_model(model_name)
        try:
            response = await _generate_content(model, prompt)
            text = getattr(response, "text", None) or ""
            return text.strip() or "..."
        except Exception as exc:  # pragma: no cover - внешний сервис
            last_error = exc
            logger.warning("Gemini request failed with model %s: %s", model_name, exc)

    logger.exception("Gemini request failed for all models: %s", last_error)
    return "Что-то сдохло у меня на проводах. Попробуй позже."
