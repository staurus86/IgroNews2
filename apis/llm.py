import json
import logging
import os
from openai import OpenAI
import config

logger = logging.getLogger(__name__)

# Primary: OpenRouter key 1, Fallback: OpenRouter key 2
_API_KEYS = [k for k in [
    config.OPENAI_API_KEY,
    os.getenv("OPENAI_API_KEY_2", ""),
] if k]

client = OpenAI(
    api_key=_API_KEYS[0] if _API_KEYS else "",
    base_url=config.OPENAI_BASE_URL,
)

PROMPT_TREND_FORECAST = """
Ты — аналитик игровых медиа. Оцени вероятность того, что эта новость
станет трендовой в ближайшие 24 часа.

Новость: {title}
Текст: {text}
Топ биграммы: {bigrams}
Частота в поиске: {keyso_freq}
Google Trends сейчас: {trends}

Ответь в JSON:
{{
  "trend_score": 0-100,
  "reasoning": "почему",
  "peak_window": "когда ожидать пик (часы)",
  "recommendation": "публиковать немедленно / подождать / пропустить"
}}
"""

PROMPT_MERGE_ANALYSIS = """
Перед тобой список новостей из разных источников об одном событии.
Объедини их в одну финальную новость.

Новости:
{news_list}

Верни JSON:
{{
  "merged_title": "лучший заголовок",
  "merged_text": "объединённый текст",
  "unique_facts": ["факт 1", "факт 2"],
  "best_source": "самый полный источник"
}}
"""

PROMPT_KEYSO_QUERIES = """
На основе биграмм и темы новости предложи 10 поисковых запросов
для проверки в Keys.so. Запросы должны быть близки к теме,
но охватывать смежные интенты (информационные, навигационные).

Тема: {title}
Биграммы: {bigrams}
Регион: {region}

Верни JSON: {{"queries": ["запрос 1", ...]}}
"""


def _call_llm(prompt: str) -> dict | None:
    """Вызывает LLM через OpenRouter с fallback на второй ключ."""
    global client
    for i, key in enumerate(_API_KEYS):
        try:
            c = OpenAI(api_key=key, base_url=config.OPENAI_BASE_URL)
            response = c.chat.completions.create(
                model=config.LLM_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
            )
            text = response.choices[0].message.content
            logger.info("LLM response (key %d): %s", i + 1, text[:200])
            # Try to parse as JSON (strip markdown fences if present)
            cleaned = text.strip()
            if cleaned.startswith("```"):
                cleaned = "\n".join(cleaned.split("\n")[1:])
                if cleaned.endswith("```"):
                    cleaned = cleaned[:-3]
                cleaned = cleaned.strip()
            return json.loads(cleaned)
        except Exception as e:
            logger.warning("LLM key %d failed: %s", i + 1, e)
            continue
    logger.error("All LLM keys failed")
    return None


def forecast_trend(title: str, text: str, bigrams: list,
                   keyso_freq: int, trends: dict) -> dict | None:
    prompt = PROMPT_TREND_FORECAST.format(
        title=title,
        text=text[:2000],
        bigrams=bigrams,
        keyso_freq=keyso_freq,
        trends=trends,
    )
    return _call_llm(prompt)


def merge_news(news_list: list[dict]) -> dict | None:
    formatted = "\n\n".join(
        f"Источник: {n['source']}\nЗаголовок: {n['title']}\nТекст: {n.get('plain_text', '')[:1000]}"
        for n in news_list
    )
    prompt = PROMPT_MERGE_ANALYSIS.format(news_list=formatted)
    return _call_llm(prompt)


REWRITE_STYLES = {
    "news": {
        "desc": "Информационная заметка",
        "instructions": """Стиль: информационная новостная заметка.
Правила:
- Заголовок: чёткий, фактологический, без оценок, до 80 символов
- Первый абзац (лид): кто, что, когда, где — вся суть за 2 предложения
- Тело: 2-3 абзаца, только проверенные факты, хронология событий
- Тон: нейтральный, деловой, без эмоций и мнений автора
- НЕ используй: восклицательные знаки, оценочные прилагательные, слова «удивительный», «невероятный»
- Укажи источник информации если он есть в тексте""",
    },
    "seo": {
        "desc": "SEO-оптимизированная статья",
        "instructions": """Стиль: SEO-оптимизированная статья для поисковых систем.
Правила:
- Заголовок: содержит главное ключевое слово, до 70 символов, привлекательный для клика
- Структура: используй подзаголовки (## H2), списки, выделение ключевых слов
- Первый абзац: обязательно содержит основное ключевое слово в первом предложении
- Текст: 4-5 абзацев, плотность ключевых слов 2-3%, естественная речь
- Включи LSI-ключевые слова (синонимы, связанные термины)
- seo_title: точно до 60 символов, с ключевым словом в начале
- seo_description: точно до 155 символов, с призывом к действию
- Теги: 5-7 тегов включая название игры, жанр, платформу""",
    },
    "review": {
        "desc": "Обзорная статья с мнением",
        "instructions": """Стиль: авторский обзор / аналитика.
Правила:
- Заголовок: содержит мнение или оценку, может быть провокационным вопросом
- Вступление: зацепи читателя контекстом — почему это важно
- Тело: 4-5 абзацев, анализ ситуации, плюсы и минусы, сравнение с аналогами
- Включи авторское мнение, но подкрепи его аргументами
- Заключение: вывод и прогноз — что это значит для индустрии/игроков
- Тон: экспертный, вдумчивый, допускается субъективность""",
    },
    "clickbait": {
        "desc": "Кликбейтный заголовок и интрига",
        "instructions": """Стиль: кликбейт для максимального CTR.
Правила:
- Заголовок: интригующий, с эмоциональным крючком, вопросом или числом. Примеры паттернов: «Вот почему...», «Это изменит всё», «5 причин...», «Никто не ожидал...»
- Первый абзац: усиль интригу, но НЕ раскрывай всё сразу
- Тело: 3-4 абзаца, раскрывай информацию постепенно, держи внимание
- Используй: короткие предложения, восклицательные знаки, эмоциональные слова
- Финал: неожиданный поворот или сильный вывод
- НЕ ври и не искажай факты — привлекай внимание подачей, а не ложью""",
    },
    "short": {
        "desc": "Короткая заметка",
        "instructions": """Стиль: ультра-короткая заметка / брифинг.
Правила:
- Заголовок: максимально лаконичный, до 60 символов
- Текст: ровно 2-3 предложения, только самая суть
- Первое предложение — главный факт
- Второе — ключевая деталь или контекст
- Третье (опционально) — дата/платформа/цена если релевантно
- НЕ используй: вводные слова, лишние прилагательные, повторы
- Идеально для дайджестов и push-уведомлений""",
    },
    "social": {
        "desc": "Пост для соцсетей",
        "instructions": """Стиль: пост для Telegram / VK / Twitter.
Правила:
- Заголовок: цепляющий, с эмодзи в начале, до 60 символов
- Текст: 3-5 коротких строк, между ними пустые строки для читаемости
- Используй 2-4 эмодзи к месту (🎮 ⚡ 🔥 💰 📅 🏆 и т.д.)
- Тон: неформальный, дружеский, как будто рассказываешь другу
- Добавь призыв: вопрос к аудитории или «Что думаете?»
- В конце: 3-5 хештегов через пробел (#игры #gaming и т.д.)
- Общая длина: до 280 символов для Twitter, до 500 для Telegram""",
    },
}

PROMPT_REWRITE = """
Ты — профессиональный игровой журналист и редактор. Перепиши новость строго по инструкции.

Оригинальный заголовок: {title}
Оригинальный текст: {text}

{style_instructions}

Язык ответа: {language}

Верни строго JSON (без markdown):
{{
  "title": "новый заголовок по правилам стиля",
  "text": "переписанный текст",
  "seo_title": "SEO title до 60 символов с ключевым словом",
  "seo_description": "meta description до 155 символов",
  "tags": ["тег1", "тег2", "тег3", "тег4", "тег5"]
}}
"""


def rewrite_news(title: str, text: str, style: str = "news", language: str = "русский") -> dict | None:
    style_data = REWRITE_STYLES.get(style, REWRITE_STYLES["news"])
    prompt = PROMPT_REWRITE.format(
        title=title,
        text=text[:3000],
        style_instructions=style_data["instructions"],
        language=language,
    )
    return _call_llm(prompt)


def suggest_keyso_queries(title: str, bigrams: list, region: str = "RU") -> list[str]:
    prompt = PROMPT_KEYSO_QUERIES.format(
        title=title,
        bigrams=bigrams,
        region=region,
    )
    result = _call_llm(prompt)
    if result and "queries" in result:
        return result["queries"]
    return []
