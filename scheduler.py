import logging
import json
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
        elif source["type"] == "html":
            total += parse_html_source(source)
    logger.info("[%dmin] Total new articles: %d", interval_min, total)


def process_news():
    """Обрабатывает новые новости: NLP, APIs, LLM, Sheets."""
    news_list = get_unprocessed_news(limit=10)
    if not news_list:
        logger.info("No unprocessed news")
        return

    for news in news_list:
        try:
            news_id = news["id"]
            title = news.get("title", "")
            text = news.get("plain_text", "") or news.get("description", "")

            # 1. TF-IDF
            combined_text = f"{title} {news.get('h1', '')} {text}"
            keywords = extract_keywords(combined_text)
            bigrams = keywords.get("bigrams", [])
            trigrams = keywords.get("trigrams", [])

            # 2. Keys.so — частота топ-биграммы
            top_bigram = bigrams[0][0] if bigrams else title
            keyso_info = get_keyword_info(top_bigram)
            similar = get_similar_keywords(top_bigram, limit=10)

            # 3. Google Trends
            trends = get_trends_for_keyword(top_bigram)

            # 4. LLM — прогноз трендовости
            forecast = forecast_trend(
                title=title,
                text=text,
                bigrams=bigrams,
                keyso_freq=keyso_info.get("ws", 0),
                trends=trends,
            )

            recommendation = ""
            trend_score = ""
            if forecast:
                recommendation = forecast.get("recommendation", "")
                trend_score = str(forecast.get("trend_score", ""))

            # 5. Сохраняем анализ в БД
            analysis_data = {
                "bigrams": bigrams,
                "trigrams": trigrams,
                "trends_data": trends,
                "keyso_data": {"freq": keyso_info.get("ws", 0), "similar": similar},
                "llm_recommendation": recommendation,
                "llm_trend_forecast": trend_score,
            }
            save_analysis(news_id, **analysis_data)

            # 6. Пишем в Google Sheets
            analysis_for_sheets = {
                "bigrams": json.dumps(bigrams, ensure_ascii=False),
                "trends_data": json.dumps(trends, ensure_ascii=False),
                "keyso_data": json.dumps({"freq": keyso_info.get("ws", 0), "similar": [s["word"] for s in similar]}, ensure_ascii=False),
                "llm_recommendation": recommendation,
                "llm_trend_forecast": trend_score,
                "llm_merged_with": "",
            }
            row = write_news_row(news, analysis_for_sheets)
            if row:
                save_analysis(news_id, sheets_row=row, **analysis_data)

            update_news_status(news_id, "processed")
            logger.info("Processed: %s", title[:60])

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
