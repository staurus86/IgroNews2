"""Momentum scoring — скорость распространения новости."""

import logging
from datetime import datetime, timezone, timedelta
from checks.deduplication import tfidf_similarity, normalize
from storage.database import get_connection, _is_postgres

logger = logging.getLogger(__name__)


def get_momentum(news: dict) -> dict:
    """Проверяет сколько источников написали о похожей теме за последние часы."""
    conn = get_connection()
    cur = conn.cursor()
    ph = "%s" if _is_postgres() else "?"

    title = news.get("title", "")
    now = datetime.now(timezone.utc)

    # Берём новости за последние 24ч
    cutoff = (now - timedelta(hours=24)).isoformat()
    cur.execute(f"""
        SELECT id, source, title, parsed_at FROM news
        WHERE parsed_at > {ph}
        ORDER BY parsed_at DESC
        LIMIT 500
    """, (cutoff,))

    if _is_postgres():
        columns = [desc[0] for desc in cur.description]
        recent = [dict(zip(columns, row)) for row in cur.fetchall()]
    else:
        recent = [dict(row) for row in cur.fetchall()]

    if not recent:
        return {"sources_1h": 0, "sources_6h": 0, "sources_24h": 0, "level": "none", "score": 0}

    # Сравниваем заголовок с остальными
    titles = [title] + [r["title"] for r in recent]
    pairs = tfidf_similarity(titles)

    # Находим похожие (которые матчатся с индексом 0 — наш заголовок)
    similar_indices = set()
    for i, j, score in pairs:
        if i == 0:
            similar_indices.add(j - 1)  # offset by 1 because we prepended
        elif j == 0:
            similar_indices.add(i - 1)

    # Считаем по временным окнам
    sources_1h = set()
    sources_6h = set()
    sources_24h = set()

    for idx in similar_indices:
        if idx < 0 or idx >= len(recent):
            continue
        r = recent[idx]
        source = r["source"]
        parsed = r.get("parsed_at", "")
        if not parsed:
            continue

        try:
            pt = datetime.fromisoformat(parsed.replace("Z", "+00:00"))
            if pt.tzinfo is None:
                pt = pt.replace(tzinfo=timezone.utc)
            age = (now - pt).total_seconds() / 3600

            if age <= 1:
                sources_1h.add(source)
            if age <= 6:
                sources_6h.add(source)
            sources_24h.add(source)
        except Exception:
            sources_24h.add(source)

    s1 = len(sources_1h)
    s6 = len(sources_6h)
    s24 = len(sources_24h)

    if s1 >= 4:
        level = "viral"
        score = 100
    elif s6 >= 4:
        level = "growing"
        score = 70
    elif s24 >= 3:
        level = "spreading"
        score = 40
    elif s24 >= 2:
        level = "noticed"
        score = 20
    else:
        level = "none"
        score = 0

    return {
        "sources_1h": s1,
        "sources_6h": s6,
        "sources_24h": s24,
        "similar_sources": list(sources_24h),
        "level": level,
        "score": score,
    }
