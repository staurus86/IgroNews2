"""Health Monitor — проверка работоспособности источников."""

import logging
from datetime import datetime, timezone, timedelta
from storage.database import get_connection, _is_postgres

logger = logging.getLogger(__name__)


def get_sources_health() -> list[dict]:
    """Возвращает статус здоровья каждого источника за последние 24ч."""
    conn = get_connection()
    cur = conn.cursor()

    cutoff_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    cutoff_3h = (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat()

    ph = "%s" if _is_postgres() else "?"

    # Количество новостей за 24ч по источникам
    cur.execute(f"""
        SELECT source,
               COUNT(*) as count_24h,
               MAX(parsed_at) as last_parsed
        FROM news
        WHERE parsed_at > {ph}
        GROUP BY source
        ORDER BY count_24h DESC
    """, (cutoff_24h,))

    if _is_postgres():
        columns = [desc[0] for desc in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    else:
        rows = [dict(row) for row in cur.fetchall()]

    results = []
    for row in rows:
        last_parsed = row["last_parsed"] or ""
        count = row["count_24h"]

        # Determine health status
        if last_parsed > cutoff_3h:
            if count >= 10:
                status = "healthy"
            else:
                status = "low"
        elif last_parsed > cutoff_24h:
            status = "warning"
        else:
            status = "down"

        # Calculate minutes since last parse
        minutes_ago = -1
        if last_parsed:
            try:
                lp = datetime.fromisoformat(last_parsed.replace("Z", "+00:00"))
                if lp.tzinfo is None:
                    lp = lp.replace(tzinfo=timezone.utc)
                minutes_ago = int((datetime.now(timezone.utc) - lp).total_seconds() / 60)
            except Exception:
                pass

        results.append({
            "source": row["source"],
            "count_24h": count,
            "last_parsed": last_parsed,
            "minutes_ago": minutes_ago,
            "status": status,
        })

    return results
