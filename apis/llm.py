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
