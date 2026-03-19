import hashlib
import logging
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from urllib.parse import urlparse

import config

logger = logging.getLogger(__name__)

_conn = None
_pool = None  # PostgreSQL connection pool
_local = threading.local()  # Per-thread connections for SQLite
_conn_lock = threading.Lock()


def _is_postgres():
    return config.DATABASE_URL.startswith("postgres")


def get_connection():
    global _conn, _pool

    if _is_postgres():
        with _conn_lock:
            if _conn is not None:
                try:
                    cur = _conn.cursor()
                    cur.execute("SELECT 1")
                    cur.close()
                except Exception:
                    logger.warning("PostgreSQL connection lost, reconnecting...")
                    try:
                        _conn.close()
                    except Exception:
                        pass
                    _conn = None

            if _conn is None:
                import psycopg2
                url = config.DATABASE_URL
                if url.startswith("postgres://"):
                    url = url.replace("postgres://", "postgresql://", 1)
                _conn = psycopg2.connect(url)
                _conn.autocommit = True
            return _conn
    else:
        # Per-thread connections for SQLite (avoids "database is locked")
        conn = getattr(_local, 'conn', None)
        if conn is not None:
            return conn
        db_path = config.DATABASE_URL.replace("sqlite:///", "")
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        _local.conn = conn
        return conn


@contextmanager
def db_cursor():
    """Context manager for safe cursor lifecycle."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        yield cur
    finally:
        cur.close()


def ph():
    """Returns the correct placeholder for the current DB engine."""
    return "%s" if _is_postgres() else "?"


def rows_to_dicts(cur) -> list[dict]:
    """Converts cursor results to list of dicts."""
    if _is_postgres():
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]
    return [dict(row) for row in cur.fetchall()]


def init_db():
    conn = get_connection()
    cur = conn.cursor()
    try:
        _init_db_impl(conn, cur)
    finally:
        cur.close()
    logger.info("Database initialized")


def _init_db_impl(conn, cur):
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

    digests_sql = """
        CREATE TABLE IF NOT EXISTS digests (
            id TEXT PRIMARY KEY,
            digest_date TEXT,
            style TEXT,
            title TEXT,
            text TEXT,
            news_count INTEGER DEFAULT 0,
            created_at TEXT
        )
    """

    viral_triggers_sql = """
        CREATE TABLE IF NOT EXISTS viral_triggers_config (
            trigger_id TEXT PRIMARY KEY,
            label TEXT NOT NULL,
            weight INTEGER DEFAULT 0,
            keywords TEXT DEFAULT '[]',
            is_active INTEGER DEFAULT 1,
            is_custom INTEGER DEFAULT 0,
            updated_at TEXT
        )
    """

    news_sql = """
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
    """
    analysis_sql = """
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
    """

    if _is_postgres():
        cur.execute(news_sql)
        cur.execute(analysis_sql)
        cur.execute(articles_sql)
        cur.execute(task_queue_sql)
        cur.execute(feedback_sql)
        cur.execute(prompt_versions_sql)
        cur.execute(digests_sql)
        cur.execute(viral_triggers_sql)
    else:
        cur.execute(news_sql)
        cur.execute(analysis_sql)
        cur.execute(articles_sql)
        cur.execute(task_queue_sql)
        cur.execute(feedback_sql)
        cur.execute(prompt_versions_sql)
        cur.execute(digests_sql)
        cur.execute(viral_triggers_sql)
        conn.commit()

    # Add check_data columns if missing (stores viral, sentiment, freshness, tags as JSON)
    _add_column_if_missing(cur, "news_analysis", "viral_score", "INTEGER DEFAULT 0")
    _add_column_if_missing(cur, "news_analysis", "viral_level", "TEXT DEFAULT ''")
    _add_column_if_missing(cur, "news_analysis", "viral_data", "TEXT DEFAULT '{}'")
    _add_column_if_missing(cur, "news_analysis", "sentiment_label", "TEXT DEFAULT ''")
    _add_column_if_missing(cur, "news_analysis", "sentiment_score", "REAL DEFAULT 0")
    _add_column_if_missing(cur, "news_analysis", "freshness_status", "TEXT DEFAULT ''")
    _add_column_if_missing(cur, "news_analysis", "freshness_hours", "REAL DEFAULT -1")
    _add_column_if_missing(cur, "news_analysis", "tags_data", "TEXT DEFAULT '[]'")
    _add_column_if_missing(cur, "news_analysis", "momentum_score", "INTEGER DEFAULT 0")
    _add_column_if_missing(cur, "news_analysis", "headline_score", "INTEGER DEFAULT 0")
    _add_column_if_missing(cur, "news_analysis", "total_score", "INTEGER DEFAULT 0")
    # Этап 2: расширенные check results для единой таблицы
    _add_column_if_missing(cur, "news_analysis", "quality_score", "INTEGER DEFAULT 0")
    _add_column_if_missing(cur, "news_analysis", "relevance_score", "INTEGER DEFAULT 0")
    _add_column_if_missing(cur, "news_analysis", "all_checks_pass", "INTEGER DEFAULT 0")
    _add_column_if_missing(cur, "news_analysis", "entity_names", "TEXT DEFAULT '[]'")
    _add_column_if_missing(cur, "news_analysis", "entity_best_tier", "TEXT DEFAULT ''")
    _add_column_if_missing(cur, "news_analysis", "reviewed_at", "TEXT DEFAULT ''")

    # Articles: scheduled publication time
    _add_column_if_missing(cur, "articles", "scheduled_at", "TEXT")

    # Soft-delete support
    _add_column_if_missing(cur, "news", "is_deleted", "INTEGER DEFAULT 0")
    _add_column_if_missing(cur, "news", "deleted_at", "TEXT")
    _add_column_if_missing(cur, "articles", "is_deleted", "INTEGER DEFAULT 0")
    _add_column_if_missing(cur, "articles", "deleted_at", "TEXT")

    # Phase 0: new columns for explainability (nullable, safe)
    _add_column_if_missing(cur, "news_analysis", "decision_reason", "TEXT DEFAULT ''")
    _add_column_if_missing(cur, "news_analysis", "score_breakdown", "TEXT DEFAULT '{}'")

    # Phase 2: confidence and cluster
    _add_column_if_missing(cur, "news_analysis", "confidence_score", "INTEGER DEFAULT 0")
    _add_column_if_missing(cur, "news_analysis", "cluster_id", "TEXT DEFAULT ''")

    if not _is_postgres():
        conn.commit()

    # Phase 2: article_versions table for content versioning
    cur.execute("""
        CREATE TABLE IF NOT EXISTS article_versions (
            id TEXT PRIMARY KEY,
            article_id TEXT NOT NULL,
            version INTEGER DEFAULT 1,
            title TEXT DEFAULT '',
            text TEXT DEFAULT '',
            seo_title TEXT DEFAULT '',
            seo_description TEXT DEFAULT '',
            tags TEXT DEFAULT '[]',
            change_type TEXT DEFAULT 'manual',
            changed_by TEXT DEFAULT 'system',
            created_at TEXT NOT NULL
        )
    """)
    if not _is_postgres():
        conn.commit()

    # ─── Indexes for performance ───
    _create_indexes(cur)
    if not _is_postgres():
        conn.commit()

    # Initialize feature flags and observability tables
    try:
        from core.feature_flags import init_flags_table
        init_flags_table()
    except Exception as e:
        logger.warning("Feature flags init skipped: %s", e)
    try:
        from core.observability import init_observability_tables
        init_observability_tables()
    except Exception as e:
        logger.warning("Observability tables init skipped: %s", e)


def _create_indexes(cur):
    """Create indexes for frequently-queried columns (IF NOT EXISTS is safe to re-run)."""
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_news_status ON news(status)",
        "CREATE INDEX IF NOT EXISTS idx_news_parsed_at ON news(parsed_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_news_source ON news(source)",
        "CREATE INDEX IF NOT EXISTS idx_analysis_score ON news_analysis(total_score DESC)",
        "CREATE INDEX IF NOT EXISTS idx_analysis_newsid ON news_analysis(news_id)",
        "CREATE INDEX IF NOT EXISTS idx_task_queue_status ON task_queue(status)",
        "CREATE INDEX IF NOT EXISTS idx_task_queue_type ON task_queue(task_type)",
        "CREATE INDEX IF NOT EXISTS idx_task_queue_created ON task_queue(created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_articles_status ON articles(status)",
        "CREATE INDEX IF NOT EXISTS idx_articles_newsid ON articles(news_id)",
        "CREATE INDEX IF NOT EXISTS idx_news_deleted ON news(is_deleted)",
    ]
    for sql in indexes:
        try:
            cur.execute(sql)
        except Exception as e:
            logger.debug("Index creation skipped: %s", e)


def _add_column_if_missing(cur, table, column, col_type):
    """Безопасно добавляет столбец если его нет."""
    try:
        if _is_postgres():
            cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {col_type}")
        else:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
    except Exception:
        pass  # Column already exists


def news_exists(url: str) -> bool:
    news_id = hashlib.md5(url.encode()).hexdigest()
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM news WHERE id = %s" if _is_postgres() else "SELECT 1 FROM news WHERE id = ?", (news_id,))
        return cur.fetchone() is not None
    finally:
        cur.close()


def insert_news(source: str, url: str, title: str, h1: str = "",
                description: str = "", plain_text: str = "", published_at: str = ""):
    news_id = hashlib.md5(url.encode()).hexdigest()
    if news_exists(url):
        return None

    conn = get_connection()
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()

    try:
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
    finally:
        cur.close()

    logger.info("Inserted news: %s — %s", source, title[:60])
    return news_id


def get_unprocessed_news(limit: int = 20):
    conn = get_connection()
    cur = conn.cursor()
    q = "SELECT * FROM news WHERE status = 'approved' ORDER BY parsed_at DESC"
    try:
        if _is_postgres():
            cur.execute(q + " LIMIT %s", (limit,))
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
        else:
            cur.execute(q + " LIMIT ?", (limit,))
            return [dict(row) for row in cur.fetchall()]
    finally:
        cur.close()


def update_news_status(news_id: str, status: str):
    conn = get_connection()
    cur = conn.cursor()
    try:
        if _is_postgres():
            cur.execute("UPDATE news SET status = %s WHERE id = %s", (status, news_id))
        else:
            cur.execute("UPDATE news SET status = ? WHERE id = ?", (status, news_id))
            conn.commit()
    finally:
        cur.close()


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

    try:
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
    finally:
        cur.close()


def save_check_results(news_id: str, checks: dict, sentiment: dict = None,
                       tags: list = None, momentum: dict = None,
                       headline: dict = None, total_score: int = 0,
                       entities: list = None, score_breakdown: dict = None):
    """Сохраняет результаты проверок (viral, sentiment, freshness и др.) в news_analysis."""
    import json
    conn = get_connection()
    cur = conn.cursor()
    ph = "%s" if _is_postgres() else "?"

    viral = checks.get("viral", {})
    freshness = checks.get("freshness", {})
    quality = checks.get("quality", {})
    relevance = checks.get("relevance", {})

    all_pass = all(c.get("pass", False) for c in checks.values())

    # Entity data
    ent_list = entities or []
    ent_names = [e.get("name", "") for e in ent_list[:10]]
    ent_best_tier = ent_list[0].get("tier", "") if ent_list else ""

    vals = {
        "viral_score": viral.get("score", 0),
        "viral_level": viral.get("level", ""),
        "viral_data": json.dumps(viral.get("triggers", []), ensure_ascii=False),
        "sentiment_label": (sentiment or {}).get("label", ""),
        "sentiment_score": (sentiment or {}).get("score", 0),
        "freshness_status": freshness.get("status", ""),
        "freshness_hours": freshness.get("age_hours", -1),
        "tags_data": json.dumps([{"id": t["id"], "label": t["label"]} for t in (tags or [])[:5]], ensure_ascii=False),
        "momentum_score": (momentum or {}).get("score", 0),
        "headline_score": (headline or {}).get("score", 0),
        "total_score": total_score,
        "quality_score": quality.get("score", 0),
        "relevance_score": relevance.get("score", 0),
        "all_checks_pass": 1 if all_pass else 0,
        "entity_names": json.dumps(ent_names, ensure_ascii=False),
        "entity_best_tier": ent_best_tier,
        "reviewed_at": datetime.now(timezone.utc).isoformat(),
        "score_breakdown": json.dumps(score_breakdown or {}, ensure_ascii=False),
    }

    # Ensure row exists in news_analysis
    try:
        if _is_postgres():
            cur.execute(f"INSERT INTO news_analysis (news_id) VALUES ({ph}) ON CONFLICT DO NOTHING", (news_id,))
            set_clause = ", ".join(f"{k} = {ph}" for k in vals)
            cur.execute(f"UPDATE news_analysis SET {set_clause} WHERE news_id = {ph}",
                        list(vals.values()) + [news_id])
        else:
            cur.execute(f"INSERT OR IGNORE INTO news_analysis (news_id) VALUES ({ph})", (news_id,))
            set_clause = ", ".join(f"{k} = {ph}" for k in vals)
            cur.execute(f"UPDATE news_analysis SET {set_clause} WHERE news_id = {ph}",
                        list(vals.values()) + [news_id])
            conn.commit()
    finally:
        cur.close()


def cleanup_old_plaintext(days: int = 14):
    """Очищает plain_text для новостей старше N дней (экономия памяти)."""
    conn = get_connection()
    cur = conn.cursor()
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    _ph = "%s" if _is_postgres() else "?"
    try:
        cur.execute(f"""
            UPDATE news SET plain_text = ''
            WHERE parsed_at < {_ph} AND plain_text != '' AND status IN ('processed', 'ready', 'rejected', 'duplicate')
        """, (cutoff,))
        count = cur.rowcount
        if not _is_postgres():
            conn.commit()
        if count > 0:
            logger.info("Cleaned plain_text for %d old news items", count)
        return count
    finally:
        cur.close()


def cleanup_old_tasks(days: int = 7):
    """Удаляет завершённые/отменённые задачи старше N дней."""
    conn = get_connection()
    cur = conn.cursor()
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    ph = "%s" if _is_postgres() else "?"
    try:
        cur.execute(f"""
            DELETE FROM task_queue
            WHERE created_at < {ph} AND status IN ('done', 'error', 'cancelled', 'skipped')
        """, (cutoff,))
        if _is_postgres():
            count = cur.rowcount
        else:
            count = cur.rowcount
            conn.commit()
        if count > 0:
            logger.info("Cleaned %d old tasks from task_queue", count)
        return count
    finally:
        cur.close()


def save_digest(digest_id: str, digest_date: str, style: str,
                title: str, text: str, news_count: int):
    """Сохраняет дайджест в БД."""
    conn = get_connection()
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    ph = "%s" if _is_postgres() else "?"
    try:
        if _is_postgres():
            cur.execute(
                f"""INSERT INTO digests (id, digest_date, style, title, text, news_count, created_at)
                    VALUES ({','.join([ph]*7)})
                    ON CONFLICT (id) DO UPDATE SET title=EXCLUDED.title, text=EXCLUDED.text,
                    news_count=EXCLUDED.news_count, created_at=EXCLUDED.created_at""",
                (digest_id, digest_date, style, title, text, news_count, now)
            )
        else:
            cur.execute(
                f"""INSERT OR REPLACE INTO digests (id, digest_date, style, title, text, news_count, created_at)
                    VALUES ({','.join([ph]*7)})""",
                (digest_id, digest_date, style, title, text, news_count, now)
            )
            conn.commit()
    finally:
        cur.close()
    logger.info("Saved digest: %s (%s)", title[:60], style)


def get_digests(limit: int = 10) -> list[dict]:
    """Возвращает последние дайджесты."""
    conn = get_connection()
    cur = conn.cursor()
    ph = "%s" if _is_postgres() else "?"
    try:
        cur.execute(f"SELECT * FROM digests ORDER BY created_at DESC LIMIT {ph}", (limit,))
        if _is_postgres():
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
        else:
            return [dict(row) for row in cur.fetchall()]
    finally:
        cur.close()
