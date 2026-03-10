"""Momentum scoring — скорость распространения новости."""

import logging
from datetime import datetime, timezone, timedelta
from checks.deduplication import normalize
from storage.database import get_connection, _is_postgres

logger = logging.getLogger(__name__)


def _word_overlap(title1: str, title2: str) -> float:
    """Быстрое сравнение заголовков по пересечению слов (без TF-IDF)."""
    words1 = set(normalize(title1).split())
    words2 = set(normalize(title2).split())
    # Убираем стоп-слова (короткие)
    words1 = {w for w in words1 if len(w) > 2}
    words2 = {w for w in words2 if len(w) > 2}
    if not words1 or not words2:
        return 0
    return len(words1 & words2) / min(len(words1), len(words2))


def get_momentum(news: dict) -> dict:
    """Проверяет сколько источников написали о похожей теме за последние часы.

    Использует быстрое сравнение по словам вместо TF-IDF (экономия CPU).
    """
    conn = get_connection()
    cur = conn.cursor()
    ph = "%s" if _is_postgres() else "?"

    title = news.get("title", "")
    now = datetime.now(timezone.utc)

    # Берём новости за последние 24ч (лимит 100 вместо 500)
    cutoff = (now - timedelta(hours=24)).isoformat()
    cur.execute(f"""
        SELECT id, source, title, parsed_at FROM news
        WHERE parsed_at > {ph}
        ORDER BY parsed_at DESC
        LIMIT 100
    """, (cutoff,))

    if _is_postgres():
        columns = [desc[0] for desc in cur.description]
        recent = [dict(zip(columns, row)) for row in cur.fetchall()]
    else:
        recent = [dict(row) for row in cur.fetchall()]

    if not recent:
        return {"sources_1h": 0, "sources_6h": 0, "sources_24h": 0, "level": "none", "score": 0}

    # Быстрое сравнение по пересечению слов (вместо TF-IDF)
    similar_indices = []
    for idx, r in enumerate(recent):
        overlap = _word_overlap(title, r["title"])
        if overlap >= 0.5:  # 50%+ общих слов
            similar_indices.append(idx)

    # Считаем по временным окнам
    sources_1h = set()
    sources_6h = set()
    sources_24h = set()

    for idx in similar_indices:
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
