import logging
import json
import time
from apscheduler.schedulers.blocking import BlockingScheduler

import config
from parsers.rss_parser import parse_rss_source
from parsers.html_parser import parse_html_source
from nlp.tfidf import extract_keywords
from apis.keyso import get_keyword_info, get_similar_keywords
from apis.google_trends import get_trends_for_keyword
from apis.llm import forecast_trend, suggest_keyso_queries
from storage.database import get_unprocessed_news, update_news_status, save_analysis
from storage.sheets import write_news_row

logger = logging.getLogger(__name__)


def parse_sources(interval_min: int):
    """Парсит все источники с указанным интервалом."""
    sources = [s for s in config.SOURCES if s["interval"] == interval_min]
    total = 0
    for source in sources:
        if source["type"] == "rss":
            total += parse_rss_source(source)
        elif source["type"] in ("html", "dtf"):
            total += parse_html_source(source)
        elif source["type"] == "sitemap":
            from parsers.html_parser import parse_sitemap_source
            total += parse_sitemap_source(source)
    logger.info("[%dmin] Total new articles: %d", interval_min, total)


def _process_single_news(news_id: str) -> dict:
    """Обрабатывает одну новость по ID. Возвращает результат."""
    from storage.database import get_connection, _is_postgres
    conn = get_connection()
    cur = conn.cursor()
    ph = "%s" if _is_postgres() else "?"
    cur.execute(f"SELECT * FROM news WHERE id = {ph}", (news_id,))
    if _is_postgres():
        columns = [desc[0] for desc in cur.description]
        news = dict(zip(columns, cur.fetchone()))
    else:
        news = dict(cur.fetchone())
    return _do_process(news)


def _do_process(news: dict) -> dict:
    """Выполняет полный цикл обработки одной новости."""
    news_id = news["id"]
    title = news.get("title", "")
    text = news.get("plain_text", "") or news.get("description", "")

    # 1. TF-IDF
    combined_text = f"{title} {news.get('h1', '')} {text}"
    keywords = extract_keywords(combined_text)
    bigrams = keywords.get("bigrams", [])
    trigrams = keywords.get("trigrams", [])

    # 2. Keys.so (with rate limit)
    top_bigram = bigrams[0][0] if bigrams else title
    try:
        keyso_info = get_keyword_info(top_bigram)
        time.sleep(2)
        similar = get_similar_keywords(top_bigram, limit=10)
        time.sleep(2)
    except Exception as e:
        logger.warning("Keys.so error: %s", e)
        keyso_info = {"ws": 0, "wsk": 0}
        similar = []

    # 3. Google Trends (with rate limit)
    try:
        trends = get_trends_for_keyword(top_bigram)
        time.sleep(3)
    except Exception as e:
        logger.warning("Trends error: %s", e)
        trends = {}

    # 4. LLM (with rate limit)
    try:
        fc = forecast_trend(
            title=title, text=text, bigrams=bigrams,
            keyso_freq=keyso_info.get("ws", 0), trends=trends,
        )
        time.sleep(2)
    except Exception as e:
        logger.warning("LLM error: %s", e)
        fc = None
    recommendation = fc.get("recommendation", "") if fc else ""
    trend_score = str(fc.get("trend_score", "")) if fc else ""

    # 5. Save
    analysis_data = {
        "bigrams": bigrams, "trigrams": trigrams,
        "trends_data": trends,
        "keyso_data": {"freq": keyso_info.get("ws", 0), "similar": similar},
        "llm_recommendation": recommendation,
        "llm_trend_forecast": trend_score,
    }
    save_analysis(news_id, **analysis_data)

    # 6. Sheets — отключено, экспорт только вручную через кнопку "В Sheets"
    # analysis_for_sheets = { ... }
    # row = write_news_row(news, analysis_for_sheets)

    update_news_status(news_id, "processed")
    logger.info("Processed: %s", title[:60])
    return {"trend_score": trend_score, "recommendation": recommendation, "bigrams": bigrams}


def process_news():
    """Обрабатывает новые новости: NLP, APIs, LLM, Sheets."""
    news_list = get_unprocessed_news(limit=10)
    if not news_list:
        logger.info("No unprocessed news")
        return

    for news in news_list:
        try:
            _do_process(news)
        except Exception as e:
            logger.error("Error processing news %s: %s", news.get("id"), e)


def start_scheduler():
    """Запускает планировщик задач."""
    scheduler = BlockingScheduler(timezone="Europe/Moscow")

    # Парсинг по интервалам
    intervals = sorted(set(s["interval"] for s in config.SOURCES))
    for mins in intervals:
        scheduler.add_job(parse_sources, "interval", minutes=mins, args=[mins], id=f"parse_{mins}min")

    # Обработка новостей каждые 10 минут
    scheduler.add_job(process_news, "interval", minutes=10, id="process_news")

    # Первый запуск сразу
    for mins in intervals:
        parse_sources(mins)
    process_news()

    logger.info("Scheduler started")
    scheduler.start()
