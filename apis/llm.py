import json
import logging
from openai import OpenAI
import config

logger = logging.getLogger(__name__)

client = OpenAI(api_key=config.OPENAI_API_KEY)

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
    """Вызывает OpenAI GPT и парсит JSON-ответ."""
    try:
        response = client.chat.completions.create(
            model=config.LLM_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            response_format={"type": "json_object"},
        )
        text = response.choices[0].message.content
        return json.loads(text)
    except Exception as e:
        logger.error("LLM error: %s", e)
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
