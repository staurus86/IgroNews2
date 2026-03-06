import hashlib
import logging
import os
import sqlite3
from datetime import datetime, timezone
from urllib.parse import urlparse

import config

logger = logging.getLogger(__name__)

_conn = None


def _is_postgres():
    return config.DATABASE_URL.startswith("postgres")


def get_connection():
    global _conn
    if _conn is not None:
        return _conn

    if _is_postgres():
        import psycopg2
        url = config.DATABASE_URL
        # Railway иногда даёт postgres:// вместо postgresql://
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql://", 1)
        _conn = psycopg2.connect(url)
        _conn.autocommit = True
    else:
        db_path = config.DATABASE_URL.replace("sqlite:///", "")
        _conn = sqlite3.connect(db_path)
        _conn.row_factory = sqlite3.Row

    return _conn


def init_db():
    conn = get_connection()
    cur = conn.cursor()

    articles_sql = """
        CREATE TABLE IF NOT EXISTS articles (
            id TEXT PRIMARY KEY,
            news_id TEXT,
            title TEXT,
            text TEXT,
            seo_title TEXT,
            seo_description TEXT,
            tags TEXT,
            style TEXT,
            language TEXT DEFAULT 'русский',
            original_title TEXT,
            original_text TEXT,
            source_url TEXT,
            status TEXT DEFAULT 'draft',
            created_at TEXT,
            updated_at TEXT
        )
    """

    task_queue_sql = """
        CREATE TABLE IF NOT EXISTS task_queue (
            id TEXT PRIMARY KEY,
            task_type TEXT,
            news_id TEXT,
            news_title TEXT,
            style TEXT,
            status TEXT DEFAULT 'pending',
            result TEXT,
            created_at TEXT,
            updated_at TEXT
        )
    """

    feedback_sql = """
        CREATE TABLE IF NOT EXISTS feedback_stats (
            id TEXT PRIMARY KEY,
            stat_type TEXT,
            stat_key TEXT,
            approved INTEGER DEFAULT 0,
            rejected INTEGER DEFAULT 0,
            total INTEGER DEFAULT 0,
            weight_adjustment REAL DEFAULT 0.0,
            updated_at TEXT
        )
    """

    prompt_versions_sql = """
        CREATE TABLE IF NOT EXISTS prompt_versions (
            id TEXT PRIMARY KEY,
            prompt_name TEXT,
            version INTEGER,
            content TEXT,
            avg_score REAL DEFAULT 0.0,
            usage_count INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 0,
            created_at TEXT,
            notes TEXT
        )
    """

    if _is_postgres():
        cur.execute("""
            CREATE TABLE IF NOT EXISTS news (
                id TEXT PRIMARY KEY,
                source TEXT,
                url TEXT,
                title TEXT,
                h1 TEXT,
                description TEXT,
                plain_text TEXT,
                published_at TEXT,
                parsed_at TEXT,
                status TEXT DEFAULT 'new'
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS news_analysis (
                news_id TEXT PRIMARY KEY REFERENCES news(id),
                bigrams TEXT,
                trigrams TEXT,
                trends_data TEXT,
                keyso_data TEXT,
                llm_recommendation TEXT,
                llm_trend_forecast TEXT,
                llm_merged_with TEXT,
                sheets_row INTEGER,
                processed_at TEXT
            )
        """)
        cur.execute(articles_sql)
        cur.execute(task_queue_sql)
        cur.execute(feedback_sql)
        cur.execute(prompt_versions_sql)
    else:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS news (
                id TEXT PRIMARY KEY,
                source TEXT,
                url TEXT,
                title TEXT,
                h1 TEXT,
                description TEXT,
                plain_text TEXT,
                published_at TEXT,
                parsed_at TEXT,
                status TEXT DEFAULT 'new'
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS news_analysis (
                news_id TEXT PRIMARY KEY REFERENCES news(id),
                bigrams TEXT,
                trigrams TEXT,
                trends_data TEXT,
                keyso_data TEXT,
                llm_recommendation TEXT,
                llm_trend_forecast TEXT,
                llm_merged_with TEXT,
                sheets_row INTEGER,
                processed_at TEXT
            )
        """)
        cur.execute(articles_sql)
        cur.execute(task_queue_sql)
        cur.execute(feedback_sql)
        cur.execute(prompt_versions_sql)
        conn.commit()

    logger.info("Database initialized")


def news_exists(url: str) -> bool:
    news_id = hashlib.md5(url.encode()).hexdigest()
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM news WHERE id = %s" if _is_postgres() else "SELECT 1 FROM news WHERE id = ?", (news_id,))
    return cur.fetchone() is not None


def insert_news(source: str, url: str, title: str, h1: str = "",
                description: str = "", plain_text: str = "", published_at: str = ""):
    news_id = hashlib.md5(url.encode()).hexdigest()
    if news_exists(url):
        return None

    conn = get_connection()
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()

    if _is_postgres():
        cur.execute(
            """INSERT INTO news (id, source, url, title, h1, description, plain_text, published_at, parsed_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (id) DO NOTHING""",
            (news_id, source, url, title, h1, description, plain_text, published_at, now)
        )
    else:
        cur.execute(
            """INSERT OR IGNORE INTO news (id, source, url, title, h1, description, plain_text, published_at, parsed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (news_id, source, url, title, h1, description, plain_text, published_at, now)
        )
        conn.commit()

    logger.info("Inserted news: %s — %s", source, title[:60])
    return news_id


def get_unprocessed_news(limit: int = 20):
    conn = get_connection()
    cur = conn.cursor()
    q = "SELECT * FROM news WHERE status = 'approved' ORDER BY parsed_at DESC"
    if _is_postgres():
        cur.execute(q + " LIMIT %s", (limit,))
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]
    else:
        cur.execute(q + " LIMIT ?", (limit,))
        return [dict(row) for row in cur.fetchall()]


def update_news_status(news_id: str, status: str):
    conn = get_connection()
    cur = conn.cursor()
    if _is_postgres():
        cur.execute("UPDATE news SET status = %s WHERE id = %s", (status, news_id))
    else:
        cur.execute("UPDATE news SET status = ? WHERE id = ?", (status, news_id))
        conn.commit()


def save_analysis(news_id: str, **kwargs):
    conn = get_connection()
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()

    import json
    bigrams = json.dumps(kwargs.get("bigrams", []), ensure_ascii=False)
    trigrams = json.dumps(kwargs.get("trigrams", []), ensure_ascii=False)
    trends_data = json.dumps(kwargs.get("trends_data", {}), ensure_ascii=False)
    keyso_data = json.dumps(kwargs.get("keyso_data", {}), ensure_ascii=False)
    llm_recommendation = kwargs.get("llm_recommendation", "")
    llm_trend_forecast = kwargs.get("llm_trend_forecast", "")
    llm_merged_with = json.dumps(kwargs.get("llm_merged_with", []), ensure_ascii=False)
    sheets_row = kwargs.get("sheets_row")

    if _is_postgres():
        cur.execute(
            """INSERT INTO news_analysis
               (news_id, bigrams, trigrams, trends_data, keyso_data,
                llm_recommendation, llm_trend_forecast, llm_merged_with, sheets_row, processed_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (news_id) DO UPDATE SET
                bigrams=EXCLUDED.bigrams, trigrams=EXCLUDED.trigrams,
                trends_data=EXCLUDED.trends_data, keyso_data=EXCLUDED.keyso_data,
                llm_recommendation=EXCLUDED.llm_recommendation,
                llm_trend_forecast=EXCLUDED.llm_trend_forecast,
                llm_merged_with=EXCLUDED.llm_merged_with,
                sheets_row=EXCLUDED.sheets_row, processed_at=EXCLUDED.processed_at""",
            (news_id, bigrams, trigrams, trends_data, keyso_data,
             llm_recommendation, llm_trend_forecast, llm_merged_with, sheets_row, now)
        )
    else:
        cur.execute(
            """INSERT OR REPLACE INTO news_analysis
               (news_id, bigrams, trigrams, trends_data, keyso_data,
                llm_recommendation, llm_trend_forecast, llm_merged_with, sheets_row, processed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (news_id, bigrams, trigrams, trends_data, keyso_data,
             llm_recommendation, llm_trend_forecast, llm_merged_with, sheets_row, now)
        )
        conn.commit()
