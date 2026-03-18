"""
News and editorial API handlers extracted from web.py.
Each function is standalone (no self), returns a dict for JSON serialization.
"""

import json
import logging
import threading
import time as _time

from storage.database import get_connection, _is_postgres, update_news_status

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# GET endpoints
# ---------------------------------------------------------------------------

def get_news(query_params):
    """Combined _get_news + _get_news_impl. query_params is a parsed qs dict."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        qs = query_params
        limit = int(qs.get("limit", [100])[0])
        offset = int(qs.get("offset", [0])[0])
        status_filter = qs.get("status", [None])[0]
        source_filter = qs.get("source", [None])[0]
        date_from = qs.get("date_from", [None])[0]
        date_to = qs.get("date_to", [None])[0]
        llm_filter = qs.get("llm", [None])[0]

        ph = "%s" if _is_postgres() else "?"
        conditions = []
        params = []
        need_join_for_count = False
        if status_filter:
            conditions.append(f"n.status = {ph}")
            params.append(status_filter)
        else:
            conditions.append("n.status IN ('approved', 'processed', 'ready')")
        if source_filter:
            conditions.append(f"n.source = {ph}")
            params.append(source_filter)
        if date_from:
            conditions.append(f"n.parsed_at >= {ph}")
            params.append(date_from)
        if date_to:
            conditions.append(f"n.parsed_at <= {ph}")
            params.append(date_to + "T23:59:59")
        if llm_filter:
            need_join_for_count = True
            if llm_filter == "has_rec":
                conditions.append("a.llm_recommendation IS NOT NULL AND a.llm_recommendation != ''")
            elif llm_filter == "no_rec":
                conditions.append("(a.llm_recommendation IS NULL OR a.llm_recommendation = '')")
            else:
                conditions.append(f"LOWER(a.llm_recommendation) LIKE {ph}")
                params.append(f"%{llm_filter.lower()}%")

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        count_join = "LEFT JOIN news_analysis a ON n.id = a.news_id" if need_join_for_count else ""
        cur.execute(f"SELECT COUNT(*) FROM news n {count_join} {where}", params[:])
        total_count = cur.fetchone()[0]

        query = f"""
            SELECT n.id, n.source, n.title, n.url, n.h1, n.description,
                   n.published_at, n.parsed_at, n.status,
                   a.bigrams, a.trigrams, a.trends_data, a.keyso_data,
                   a.llm_recommendation, a.llm_trend_forecast, a.sheets_row, a.processed_at,
                   a.viral_score, a.viral_level, a.viral_data,
                   a.sentiment_label, a.sentiment_score,
                   a.freshness_status, a.freshness_hours,
                   a.tags_data, a.momentum_score, a.headline_score, a.total_score
            FROM news n
            LEFT JOIN news_analysis a ON n.id = a.news_id
            {where}
            ORDER BY n.parsed_at DESC LIMIT {ph} OFFSET {ph}
        """
        params.append(limit)
        params.append(offset)
        cur.execute(query, params)

        if _is_postgres():
            columns = [desc[0] for desc in cur.description]
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        else:
            rows = [dict(row) for row in cur.fetchall()]

        return {"news": rows, "total": total_count, "limit": limit, "offset": offset}
    finally:
        cur.close()


def get_editorial(query_params):
    """Combined _get_editorial + _get_editorial_impl."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        qs = query_params
        limit = int(qs.get("limit", [100])[0])
        offset = int(qs.get("offset", [0])[0])
        status_filter = qs.get("status", [None])[0]
        source_filter = qs.get("source", [None])[0]
        min_score = int(qs.get("min_score", [0])[0])
        max_score = int(qs.get("max_score", [0])[0])
        score_filter = qs.get("score_filter", [None])[0]
        viral_level = qs.get("viral_level", [None])[0]
        tier_filter = qs.get("tier", [None])[0]
        search = qs.get("q", [None])[0]

        ph = "%s" if _is_postgres() else "?"
        conditions = []
        params = []

        if status_filter:
            conditions.append(f"n.status = {ph}")
            params.append(status_filter)
        else:
            conditions.append(f"n.status NOT IN ('duplicate', 'rejected')")

        if source_filter:
            conditions.append(f"n.source = {ph}")
            params.append(source_filter)
        if min_score > 0:
            conditions.append(f"COALESCE(a.total_score, 0) >= {ph}")
            params.append(min_score)
        if max_score > 0:
            conditions.append(f"COALESCE(a.total_score, 0) <= {ph}")
            params.append(max_score)
        if score_filter == "zero":
            conditions.append("COALESCE(a.total_score, 0) = 0")
        elif score_filter == "nonzero":
            conditions.append("COALESCE(a.total_score, 0) > 0")
        if viral_level:
            conditions.append(f"a.viral_level = {ph}")
            params.append(viral_level)
        if tier_filter:
            conditions.append(f"a.entity_best_tier = {ph}")
            params.append(tier_filter)
        if search:
            conditions.append(f"LOWER(n.title) LIKE {ph}")
            params.append(f"%{search.lower()}%")

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        cur.execute(f"SELECT COUNT(*) FROM news n LEFT JOIN news_analysis a ON n.id = a.news_id {where}", params[:])
        total_count = cur.fetchone()[0]

        cur.execute("SELECT status, COUNT(*) FROM news GROUP BY status")
        status_counts = {}
        for row in cur.fetchall():
            if _is_postgres():
                status_counts[row[0]] = row[1]
            else:
                status_counts[row[0]] = row[1]

        query = f"""
            SELECT n.id, n.source, n.title, n.description, n.url, n.published_at, n.parsed_at, n.status,
                   COALESCE(a.total_score, 0) as total_score,
                   COALESCE(a.quality_score, 0) as quality_score,
                   COALESCE(a.relevance_score, 0) as relevance_score,
                   COALESCE(a.viral_score, 0) as viral_score,
                   COALESCE(a.viral_level, '') as viral_level,
                   COALESCE(a.viral_data, '[]') as viral_data,
                   COALESCE(a.sentiment_label, '') as sentiment_label,
                   COALESCE(a.sentiment_score, 0) as sentiment_score,
                   COALESCE(a.freshness_status, '') as freshness_status,
                   COALESCE(a.freshness_hours, -1) as freshness_hours,
                   COALESCE(a.tags_data, '[]') as tags_data,
                   COALESCE(a.momentum_score, 0) as momentum_score,
                   COALESCE(a.headline_score, 0) as headline_score,
                   COALESCE(a.all_checks_pass, 0) as all_checks_pass,
                   COALESCE(a.entity_names, '[]') as entity_names,
                   COALESCE(a.entity_best_tier, '') as entity_best_tier,
                   COALESCE(a.reviewed_at, '') as reviewed_at,
                   COALESCE(a.score_breakdown, '{{}}') as score_breakdown,
                   a.bigrams, a.llm_recommendation, a.llm_trend_forecast,
                   a.keyso_data, a.trends_data
            FROM news n
            LEFT JOIN news_analysis a ON n.id = a.news_id
            {where}
            ORDER BY n.parsed_at DESC
            LIMIT {ph} OFFSET {ph}
        """
        params.append(limit)
        params.append(offset)
        cur.execute(query, params)

        if _is_postgres():
            columns = [desc[0] for desc in cur.description]
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        else:
            rows = [dict(row) for row in cur.fetchall()]

        return {
            "news": rows,
            "total": total_count,
            "limit": limit,
            "offset": offset,
            "stats": status_counts,
        }
    finally:
        cur.close()


def get_final(query_params):
    """Combined _get_final + _get_final_impl."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        qs = query_params
        limit = int(qs.get("limit", [100])[0])
        offset = int(qs.get("offset", [0])[0])
        source_filter = qs.get("source", [None])[0]
        sort_field = qs.get("sort", ["final_score"])[0]
        sort_dir = qs.get("dir", ["desc"])[0]

        ph = "%s" if _is_postgres() else "?"
        conditions = [
            "n.status IN ('processed', 'ready')",
            "LOWER(a.llm_recommendation) = 'publish_now'",
        ]
        params = []

        if source_filter:
            conditions.append(f"n.source = {ph}")
            params.append(source_filter)

        where = "WHERE " + " AND ".join(conditions)

        cur.execute(f"SELECT COUNT(*) FROM news n JOIN news_analysis a ON n.id = a.news_id {where}", params[:])
        total_count = cur.fetchone()[0]

        allowed_sorts = {
            "final_score": "a.total_score",
            "total_score": "a.total_score",
            "viral_score": "a.viral_score",
            "freshness_hours": "a.freshness_hours",
            "source": "n.source",
            "parsed_at": "n.parsed_at",
        }
        order_col = allowed_sorts.get(sort_field, "a.total_score")
        order_dir = "ASC" if sort_dir == "asc" else "DESC"

        query = f"""
            SELECT n.id, n.source, n.title, n.url, n.h1,
                   n.published_at, n.parsed_at, n.status,
                   a.bigrams, a.trigrams, a.trends_data, a.keyso_data,
                   a.llm_recommendation, a.llm_trend_forecast,
                   a.viral_score, a.viral_level, a.viral_data,
                   a.sentiment_label, a.sentiment_score,
                   a.freshness_status, a.freshness_hours,
                   a.tags_data, a.momentum_score, a.headline_score,
                   a.total_score, a.quality_score, a.relevance_score,
                   a.entity_names, a.entity_best_tier, a.processed_at
            FROM news n
            JOIN news_analysis a ON n.id = a.news_id
            {where}
            ORDER BY {order_col} {order_dir} LIMIT {ph} OFFSET {ph}
        """
        params.append(limit)
        params.append(offset)
        cur.execute(query, params)

        if _is_postgres():
            columns = [desc[0] for desc in cur.description]
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        else:
            rows = [dict(row) for row in cur.fetchall()]

        return {"news": rows, "total": total_count}
    finally:
        cur.close()


def get_moderation_list(query_params):
    """Returns news with status 'moderation' plus local analysis data."""
    limit = int(query_params.get("limit", ["100"])[0])
    offset = int(query_params.get("offset", ["0"])[0])
    source = query_params.get("source", [""])[0]
    min_score = int(query_params.get("min_score", ["0"])[0])
    q = query_params.get("q", [""])[0]

    conn = get_connection()
    cur = conn.cursor()
    ph = "%s" if _is_postgres() else "?"

    conditions = ["n.status = 'moderation'"]
    params = []

    if source:
        conditions.append(f"n.source = {ph}")
        params.append(source)
    if min_score > 0:
        conditions.append(f"COALESCE(na.total_score, 0) >= {ph}")
        params.append(min_score)
    if q:
        conditions.append(f"LOWER(n.title) LIKE {ph}")
        params.append(f"%{q.lower()}%")

    where = " AND ".join(conditions)

    try:
        cur.execute(f"""
            SELECT n.id, n.source, n.title, n.url, n.published_at, n.parsed_at, n.status,
                   n.description,
                   na.total_score, na.quality_score, na.relevance_score,
                   na.freshness_hours, na.viral_score, na.viral_data,
                   na.sentiment_label,
                   na.tags_data as tags, na.entity_names as entities, na.headline_score, na.momentum_score
            FROM news n
            LEFT JOIN news_analysis na ON na.news_id = n.id
            WHERE {where}
            ORDER BY n.parsed_at DESC
            LIMIT {ph} OFFSET {ph}
        """, (*params, limit, offset))

        if _is_postgres():
            columns = [d[0] for d in cur.description]
            rows = [dict(zip(columns, r)) for r in cur.fetchall()]
        else:
            rows = [dict(r) for r in cur.fetchall()]

        cur.execute(f"SELECT COUNT(*), COALESCE(AVG(na.total_score), 0) FROM news n LEFT JOIN news_analysis na ON na.news_id = n.id WHERE {where}", tuple(params))
        row = cur.fetchone()
        total = row[0]
        avg_score = round(row[1], 1) if row[1] else 0
    finally:
        cur.close()

    return {"status": "ok", "news": rows, "total": total, "avg_score": avg_score}


def get_moderation(body):
    """POST version for compatibility — delegates to get_moderation_list."""
    return get_moderation_list({})


def get_event_chain_by_id(news_id):
    """Returns event chain for a given news (GET endpoint)."""
    try:
        conn = get_connection()
        cur = conn.cursor()
        ph = "%s" if _is_postgres() else "?"
        try:
            cur.execute(f"SELECT id, source, title, published_at, status FROM news WHERE id = {ph}", (news_id,))
            row = cur.fetchone()
            if not row:
                return {"error": "news not found"}
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                news = dict(zip(columns, row))
            else:
                news = dict(row)
        finally:
            cur.close()
        from checks.temporal_clusters import get_event_chain
        return get_event_chain(news)
    except Exception as e:
        logger.error(f"Event chain error: {e}")
        return {"chain": [], "chain_length": 0, "days_span": 0, "phase": "single", "error": str(e)}


def get_event_chain(body):
    """POST version of event chain lookup."""
    news_id = body.get("news_id", "")
    if not news_id:
        return {"status": "error", "message": "news_id required"}
    conn = get_connection()
    cur = conn.cursor()
    try:
        ph = "%s" if _is_postgres() else "?"
        cur.execute(f"SELECT * FROM news WHERE id = {ph}", (news_id,))
        if _is_postgres():
            columns = [desc[0] for desc in cur.description]
            row = cur.fetchone()
            if not row:
                return {"status": "error", "message": "not found"}
            news = dict(zip(columns, row))
        else:
            row = cur.fetchone()
            if not row:
                return {"status": "error", "message": "not found"}
            news = dict(row)
        from checks.temporal_clusters import get_event_chain as _get_chain
        chain = _get_chain(news)
        return {"status": "ok", **chain}
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# POST endpoints — review / approval / rejection
# ---------------------------------------------------------------------------

def run_review(body):
    """Run review pipeline for selected news."""
    news_ids = body.get("news_ids", [])
    if not news_ids:
        return {"status": "error", "message": "No news selected"}
    try:
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            placeholders = ",".join([ph] * len(news_ids))
            cur.execute(f"SELECT id, source, url, title, h1, description, plain_text, published_at, parsed_at, status FROM news WHERE id IN ({placeholders})", news_ids)
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                news_list = [dict(zip(columns, row)) for row in cur.fetchall()]
            else:
                news_list = [dict(row) for row in cur.fetchall()]

            from checks.pipeline import run_review_pipeline
            result = run_review_pipeline(news_list)
            return {"status": "ok", **result}
        finally:
            cur.close()
    except Exception as e:
        return {"status": "error", "message": str(e), "type": type(e).__name__}


def review_batch(body):
    """Review news by status (batch, without changing status)."""
    status = body.get("status", "new")
    limit = int(body.get("limit", 50))
    try:
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            if status:
                cur.execute(f"SELECT id, source, url, title, h1, description, plain_text, published_at, parsed_at, status FROM news WHERE status = {ph} ORDER BY parsed_at DESC LIMIT {ph}", (status, limit))
            else:
                cur.execute(f"SELECT id, source, url, title, h1, description, plain_text, published_at, parsed_at, status FROM news ORDER BY parsed_at DESC LIMIT {ph}", (limit,))
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                news_list = [dict(zip(columns, row)) for row in cur.fetchall()]
            else:
                news_list = [dict(row) for row in cur.fetchall()]

            if not news_list:
                return {"status": "ok", "results": [], "groups": []}

            from checks.pipeline import run_review_pipeline
            from checks.deduplication import tfidf_similarity, build_groups
            from checks.quality import check_quality
            from checks.relevance import check_relevance
            from checks.freshness import check_freshness
            from checks.viral_score import viral_score
            from checks.tags import auto_tag
            from checks.sentiment import analyze_sentiment
            from checks.momentum import get_momentum

            results = []
            for news in news_list:
                result = {
                    "id": news["id"],
                    "title": news.get("title", ""),
                    "source": news.get("source", ""),
                    "url": news.get("url", ""),
                    "published_at": news.get("published_at", ""),
                    "status": news.get("status", ""),
                    "checks": {},
                }
                result["checks"]["quality"] = check_quality(news)
                result["checks"]["relevance"] = check_relevance(news)
                result["checks"]["freshness"] = check_freshness(news)
                result["checks"]["viral"] = viral_score(news)
                result["tags"] = auto_tag(news)
                result["sentiment"] = analyze_sentiment(news)
                result["momentum"] = get_momentum(news)

                all_pass = all(c["pass"] for c in result["checks"].values())
                total_score = sum(c["score"] for c in result["checks"].values()) // 4
                momentum_bonus = result["momentum"]["score"] // 5
                total_score = min(100, total_score + momentum_bonus)
                result["overall_pass"] = all_pass
                result["total_score"] = total_score
                results.append(result)

            titles = [r["title"] for r in results]
            pairs = tfidf_similarity(titles)
            groups = build_groups(results, pairs)
            for group in groups:
                for idx in group.get("duplicate_indices", []):
                    if idx < len(results):
                        results[idx]["overall_pass"] = False
                        results[idx]["is_duplicate"] = True
                for member in group["members"]:
                    member["dedup_status"] = group["status"]

            return {"status": "ok", "results": results, "groups": groups}
        finally:
            cur.close()
    except Exception as e:
        return {"status": "error", "message": str(e), "type": type(e).__name__}


def run_auto_review(body):
    """Run auto-review in batches of 20 (saves results to DB)."""
    try:
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            BATCH_SIZE = 20

            cur.execute("SELECT COUNT(*) FROM news WHERE status = 'new'")
            total_pending = cur.fetchone()[0]

            if total_pending == 0:
                return {"status": "ok", "reviewed": 0, "message": "Нет новых для проверки", "remaining": 0}

            cur.execute(f"""
                SELECT id, source, url, title, h1, description, plain_text, published_at, parsed_at, status
                FROM news WHERE status = 'new'
                ORDER BY parsed_at DESC LIMIT {ph}
            """, (BATCH_SIZE,))
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                news_list = [dict(zip(columns, row)) for row in cur.fetchall()]
            else:
                news_list = [dict(row) for row in cur.fetchall()]

            from checks.pipeline import run_review_pipeline
            result = run_review_pipeline(news_list, update_status=True)
            reviewed = len(result.get("results", []))
            dupes = sum(1 for r in result.get("results", []) if r.get("is_duplicate"))
            rejected = sum(1 for r in result.get("results", []) if r.get("auto_rejected"))
            remaining = total_pending - reviewed
            return {
                "status": "ok",
                "reviewed": reviewed,
                "duplicates": dupes,
                "auto_rejected": rejected,
                "remaining": remaining,
                "message": f"Проверено: {reviewed}, дубликатов: {dupes}, отклонено: {rejected}" +
                           (f". Осталось: {remaining}" if remaining > 0 else "")
            }
        finally:
            cur.close()
    except Exception as e:
        return {"status": "error", "message": str(e)}


def approve_news(body):
    """Approve news and start background enrichment."""
    news_ids = body.get("news_ids", [])
    if not news_ids:
        return {"status": "error", "message": "No news selected"}
    try:
        from checks.pipeline import approve_for_enrichment
        from checks.feedback import record_decision
        approve_for_enrichment(news_ids)
        for nid in news_ids:
            try:
                record_decision(nid, "approved")
            except Exception:
                pass

        def _bg_enrich(ids):
            from scheduler import _process_single_news
            for nid in ids:
                try:
                    _process_single_news(nid)
                except Exception as e:
                    logger.warning("Background enrich failed for %s: %s", nid, e)
        threading.Thread(target=_bg_enrich, args=(list(news_ids),), daemon=True).start()

        return {"status": "ok", "approved": len(news_ids), "enriching": True}
    except Exception as e:
        return {"status": "error", "message": str(e)}


def reject_news(body):
    """Reject one or multiple news."""
    news_ids = body.get("news_ids", [])
    news_id = body.get("news_id")
    if news_id and not news_ids:
        news_ids = [news_id]
    if not news_ids:
        return {"status": "error", "message": "news_ids required"}
    try:
        from checks.feedback import record_decision
        for nid in news_ids:
            update_news_status(nid, "rejected")
            try:
                record_decision(nid, "rejected")
            except Exception:
                pass
        return {"status": "ok", "rejected": len(news_ids)}
    except Exception as e:
        return {"status": "error", "message": str(e)}


def bulk_status(body):
    """Change status for multiple news."""
    news_ids = body.get("news_ids", [])
    new_status = body.get("status", "")
    if not news_ids or not new_status:
        return {"status": "error", "message": "news_ids and status required"}
    for nid in news_ids:
        update_news_status(nid, new_status)
    return {"status": "ok", "updated": len(news_ids)}


def delete_news(body):
    """Delete news (caller must check permissions before calling)."""
    news_ids = body.get("news_ids", [])
    if not news_ids:
        return {"status": "error", "message": "news_ids required"}
    conn = get_connection()
    cur = conn.cursor()
    try:
        if _is_postgres():
            placeholders = ",".join(["%s"] * len(news_ids))
        else:
            placeholders = ",".join(["?"] * len(news_ids))
        cur.execute(f"DELETE FROM news_analysis WHERE news_id IN ({placeholders})", tuple(news_ids))
        cur.execute(f"DELETE FROM news WHERE id IN ({placeholders})", tuple(news_ids))
        conn.commit()
        return {"status": "ok", "deleted": len(news_ids)}
    finally:
        cur.close()


def news_detail(body):
    """Get full news + analysis detail."""
    news_id = body.get("news_id")
    if not news_id:
        return {"status": "error", "message": "news_id required"}
    conn = get_connection()
    cur = conn.cursor()
    try:
        ph = "%s" if _is_postgres() else "?"
        cur.execute(f"SELECT * FROM news WHERE id = {ph}", (news_id,))
        row = cur.fetchone()
        if not row:
            return {"status": "error", "message": "Not found"}
        if _is_postgres():
            columns = [desc[0] for desc in cur.description]
            news = dict(zip(columns, row))
        else:
            news = dict(row)
        cur.execute(f"SELECT * FROM news_analysis WHERE news_id = {ph}", (news_id,))
        arow = cur.fetchone()
        analysis = None
        if arow:
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                analysis = dict(zip(columns, arow))
            else:
                analysis = dict(arow)
        return {"status": "ok", "news": news, "analysis": analysis}
    finally:
        cur.close()


def analyze_news(body):
    """Full analysis of a single news: viral, freshness, quality, relevance, sentiment, tags, trends, keyso."""
    news_id = body.get("news_id")
    if not news_id:
        return {"status": "error", "message": "news_id required"}
    conn = get_connection()
    cur = conn.cursor()
    try:
        ph = "%s" if _is_postgres() else "?"
        cur.execute(f"SELECT * FROM news WHERE id = {ph}", (news_id,))
        row = cur.fetchone()
        if not row:
            return {"status": "error", "message": "Not found"}
        if _is_postgres():
            columns = [desc[0] for desc in cur.description]
            news = dict(zip(columns, row))
        else:
            news = dict(row)

        from checks.viral_score import viral_score
        from checks.freshness import check_freshness
        from checks.quality import check_quality
        from checks.relevance import check_relevance
        from checks.sentiment import analyze_sentiment
        from checks.tags import auto_tag
        from checks.momentum import get_momentum

        result = {
            "viral": viral_score(news),
            "freshness": check_freshness(news),
            "quality": check_quality(news),
            "relevance": check_relevance(news),
            "sentiment": analyze_sentiment(news),
            "tags": auto_tag(news),
            "momentum": get_momentum(news),
        }

        cur.execute(f"SELECT * FROM news_analysis WHERE news_id = {ph}", (news_id,))
        arow = cur.fetchone()
        if arow:
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                analysis = dict(zip(columns, arow))
            else:
                analysis = dict(arow)
            try:
                result["trends_data"] = json.loads(analysis.get("trends_data", "{}"))
            except Exception:
                result["trends_data"] = {}
            try:
                result["keyso_data"] = json.loads(analysis.get("keyso_data", "{}"))
            except Exception:
                result["keyso_data"] = {}
            result["llm_recommendation"] = analysis.get("llm_recommendation", "")
            result["llm_trend_forecast"] = analysis.get("llm_trend_forecast", "")
            try:
                result["bigrams"] = json.loads(analysis.get("bigrams", "[]"))
            except Exception:
                result["bigrams"] = []
        else:
            result["trends_data"] = {}
            result["keyso_data"] = {}
            result["llm_recommendation"] = ""
            result["llm_trend_forecast"] = ""
            result["bigrams"] = []

        total = sum(result[k]["score"] for k in ("viral", "freshness", "quality", "relevance")) // 4
        result["total_score"] = min(100, total + result["momentum"]["score"] // 5)
        return {"status": "ok", "analysis": result}
    finally:
        cur.close()


def merge_news(body):
    """Merge multiple news using LLM."""
    news_ids = body.get("news_ids", [])
    if len(news_ids) < 2:
        return {"status": "error", "message": "Need at least 2 news to merge"}
    try:
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            placeholders = ",".join([ph] * len(news_ids))
            cur.execute(f"SELECT id, source, title, plain_text FROM news WHERE id IN ({placeholders})", news_ids)
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                news_list = [dict(zip(columns, row)) for row in cur.fetchall()]
            else:
                news_list = [dict(row) for row in cur.fetchall()]
            from apis.llm import merge_news as llm_merge_news
            result = llm_merge_news(news_list)
            if result:
                return {"status": "ok", "result": result, "sources": [n["source"] for n in news_list]}
            else:
                return {"status": "error", "message": "LLM returned no result"}
        finally:
            cur.close()
    except Exception as e:
        return {"status": "error", "message": str(e)}


# ---------------------------------------------------------------------------
# Sheets export
# ---------------------------------------------------------------------------

def export_sheets(body):
    """Export single news to Google Sheets."""
    news_id = body.get("news_id")
    try:
        from storage.sheets import write_news_row
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            cur.execute(f"SELECT * FROM news WHERE id = {ph}", (news_id,))
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                news = dict(zip(columns, cur.fetchone()))
            else:
                news = dict(cur.fetchone())

            cur.execute(f"SELECT * FROM news_analysis WHERE news_id = {ph}", (news_id,))
            row = cur.fetchone()
            if row:
                if _is_postgres():
                    columns = [desc[0] for desc in cur.description]
                    analysis = dict(zip(columns, row))
                else:
                    analysis = dict(row)
            else:
                analysis = {"bigrams": "[]", "trends_data": "{}", "keyso_data": "{}",
                           "llm_recommendation": "", "llm_trend_forecast": "", "llm_merged_with": ""}

            sheet_row = write_news_row(news, analysis)
            return {"status": "ok", "row": sheet_row}
        finally:
            cur.close()
    except Exception as e:
        return {"status": "error", "message": str(e)}


def export_sheets_bulk(body):
    """Export multiple news to Google Sheets."""
    news_ids = body.get("news_ids", [])
    if not news_ids:
        return {"status": "error", "message": "news_ids required"}
    try:
        from storage.sheets import write_news_row
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            exported = 0
            skipped = 0
            errors = 0
            for nid in news_ids:
                try:
                    cur.execute(f"SELECT * FROM news WHERE id = {ph}", (nid,))
                    row = cur.fetchone()
                    if not row:
                        continue
                    if _is_postgres():
                        columns = [desc[0] for desc in cur.description]
                        news = dict(zip(columns, row))
                    else:
                        news = dict(row)
                    cur.execute(f"SELECT * FROM news_analysis WHERE news_id = {ph}", (nid,))
                    arow = cur.fetchone()
                    if arow:
                        if _is_postgres():
                            columns = [desc[0] for desc in cur.description]
                            analysis = dict(zip(columns, arow))
                        else:
                            analysis = dict(arow)
                    else:
                        analysis = {"bigrams": "[]", "trends_data": "{}", "keyso_data": "{}",
                                   "llm_recommendation": "", "llm_trend_forecast": "", "llm_merged_with": ""}
                    sheet_row = write_news_row(news, analysis)
                    if sheet_row and sheet_row > 0:
                        exported += 1
                    elif sheet_row == -1:
                        skipped += 1
                except Exception as e:
                    logger.warning("Bulk export error for %s: %s", nid, e)
                    errors += 1
            return {"status": "ok", "exported": exported, "skipped": skipped, "errors": errors}
        finally:
            cur.close()
    except Exception as e:
        return {"status": "error", "message": str(e)}


def export_all_processed(body):
    """Export all processed news to Sheets (background)."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM news WHERE status IN ('processed', 'ready') ORDER BY parsed_at DESC")
        if _is_postgres():
            news_ids = [r[0] for r in cur.fetchall()]
        else:
            news_ids = [r["id"] for r in cur.fetchall()]
    finally:
        cur.close()

    if not news_ids:
        return {"status": "error", "message": "Нет обработанных новостей"}

    from scheduler import _create_task
    task_ids = []
    cur2 = conn.cursor()
    ph = "%s" if _is_postgres() else "?"
    try:
        for nid in news_ids:
            try:
                cur2.execute(f"SELECT title FROM news WHERE id = {ph}", (nid,))
                row = cur2.fetchone()
                title = (row[0] if _is_postgres() else row["title"]) if row else ""
            except Exception:
                title = ""
            tid = _create_task("sheets", nid, title)
            task_ids.append(tid)
    finally:
        cur2.close()

    def _bg_export(ids, tids):
        from storage.sheets import write_news_row
        from scheduler import _update_task, _fetch_news_by_id, _fetch_analysis_by_id
        ok_count = 0
        skip_count = 0
        err_count = 0
        for i, (nid, tid) in enumerate(zip(ids, tids)):
            try:
                _update_task(tid, "running", {"stage": "exporting", "progress": f"{i+1}/{len(ids)}"})
                news = _fetch_news_by_id(nid)
                analysis = _fetch_analysis_by_id(nid)
                if not news:
                    _update_task(tid, "error", {"error": "News not found"})
                    err_count += 1
                    continue
                sheet_row = write_news_row(news, analysis or {})
                if sheet_row and sheet_row > 0:
                    _update_task(tid, "done", {"sheet_row": sheet_row})
                    ok_count += 1
                elif sheet_row == -1:
                    _update_task(tid, "skipped", {"reason": "duplicate in Sheets"})
                    skip_count += 1
                else:
                    _update_task(tid, "error", {"error": "Sheets write returned None"})
                    err_count += 1
            except Exception as e:
                _update_task(tid, "error", {"error": str(e)[:500]})
                err_count += 1
            _time.sleep(1.5)
        logger.info("Mass export done: %d ok, %d skipped, %d errors out of %d", ok_count, skip_count, err_count, len(ids))

    threading.Thread(target=_bg_export, args=(list(news_ids), list(task_ids)), daemon=True).start()
    return {"status": "ok", "queued": len(news_ids), "task_ids": task_ids}


def export_ready_all(body):
    """Export ALL rewritten articles to Sheets/Ready (background)."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT a.id, a.news_id, a.title, a.text, a.seo_title, a.seo_description,
                   a.tags, a.style, a.original_title, a.source_url, a.created_at,
                   n.source, n.parsed_at, n.url, n.title as news_title
            FROM articles a
            LEFT JOIN news n ON n.id = a.news_id
            ORDER BY a.created_at DESC
        """)
        if _is_postgres():
            columns = [d[0] for d in cur.description]
            articles = [dict(zip(columns, r)) for r in cur.fetchall()]
        else:
            articles = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()

    if not articles:
        return {"status": "error", "message": "Нет переписанных статей"}

    def _bg_export_ready(arts):
        from storage.sheets import write_ready_row
        from scheduler import _fetch_analysis_by_id
        import json as _json
        ok = 0
        skip = 0
        err = 0
        for art in arts:
            try:
                news_id = art.get("news_id", "")
                analysis = _fetch_analysis_by_id(news_id) if news_id else None

                news = {
                    "parsed_at": art.get("parsed_at", art.get("created_at", "")),
                    "source": art.get("source", ""),
                    "title": art.get("original_title") or art.get("news_title", ""),
                    "url": art.get("source_url") or art.get("url", ""),
                }

                tags_raw = art.get("tags", "[]")
                try:
                    tags_list = _json.loads(tags_raw) if isinstance(tags_raw, str) else (tags_raw if isinstance(tags_raw, list) else [])
                except Exception:
                    tags_list = []

                rewrite = {
                    "title": art.get("title", ""),
                    "text": art.get("text", ""),
                    "seo_title": art.get("seo_title", ""),
                    "seo_description": art.get("seo_description", ""),
                    "tags": tags_list,
                }

                row = write_ready_row(news, analysis, rewrite)
                if row and row > 0:
                    ok += 1
                elif row == -1:
                    skip += 1
                else:
                    err += 1
            except Exception as e:
                logger.error("Ready export error for article %s: %s", art.get("id"), e)
                err += 1
            _time.sleep(1.5)
        logger.info("Ready export done: %d ok, %d skipped, %d errors out of %d", ok, skip, err, len(arts))

    threading.Thread(target=_bg_export_ready, args=(list(articles),), daemon=True).start()
    return {"status": "ok", "queued": len(articles), "message": f"Экспорт {len(articles)} статей в Ready запущен"}


# ---------------------------------------------------------------------------
# SEO, moderation rewrite, rescore, quick tags, dashboard groups, translate, AI
# ---------------------------------------------------------------------------

def seo_check(body):
    """SEO analysis of an article."""
    from checks.seo_check import analyze_seo
    title = body.get("title", "")
    seo_title = body.get("seo_title", "")
    seo_description = body.get("seo_description", "")
    text = body.get("text", "")
    tags = body.get("tags", [])
    result = analyze_seo(title, seo_title, seo_description, text, tags)
    return {"status": "ok", **result}


def moderation_rewrite(body):
    """Send moderation news for rewrite (LLM only, no API enrichment)."""
    news_ids = body.get("news_ids", [])
    style = body.get("style", "news")
    if not news_ids:
        return {"status": "error", "message": "news_ids required"}

    from scheduler import _create_task
    task_ids = []
    conn = get_connection()
    cur = conn.cursor()
    try:
        ph = "%s" if _is_postgres() else "?"
        for nid in news_ids:
            try:
                cur.execute(f"SELECT title FROM news WHERE id = {ph}", (nid,))
                row = cur.fetchone()
                title = (row[0] if _is_postgres() else row["title"]) if row else ""
            except Exception:
                title = ""
            tid = _create_task("mod_rewrite", nid, title, style)
            task_ids.append(tid)
    finally:
        cur.close()

    def _bg_mod_rewrite(ids, tids, rewrite_style):
        from apis.llm import rewrite_news
        from scheduler import _update_task, _fetch_news_by_id, _fetch_analysis_by_id
        from storage.sheets import write_ready_row
        import uuid as _uuid
        import json as _json2
        for nid, tid in zip(ids, tids):
            try:
                news = _fetch_news_by_id(nid)
                if not news:
                    _update_task(tid, "error", {"error": "News not found"})
                    continue
                _update_task(tid, "running", {"stage": "rewriting"})
                result = rewrite_news(
                    title=news.get("title", ""),
                    text=news.get("plain_text", ""),
                    style=rewrite_style,
                    language="русский",
                )
                if result:
                    conn2 = get_connection()
                    cur2 = conn2.cursor()
                    ph2 = "%s" if _is_postgres() else "?"
                    _now = __import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat()
                    aid = str(_uuid.uuid4())[:12]
                    tags_j = _json2.dumps(result.get("tags", []), ensure_ascii=False)
                    try:
                        cur2.execute(f"""INSERT INTO articles (id, news_id, title, text, seo_title, seo_description, tags,
                            style, language, original_title, original_text, source_url, status, created_at, updated_at)
                            VALUES ({','.join([ph2]*15)})""",
                            (aid, nid, result.get("title", ""), result.get("text", ""),
                             result.get("seo_title", ""), result.get("seo_description", ""), tags_j,
                             rewrite_style, "русский", news.get("title", ""), (news.get("plain_text", "") or "")[:5000],
                             news.get("url", ""), "draft", _now, _now))
                        if not _is_postgres():
                            conn2.commit()
                    finally:
                        cur2.close()

                    try:
                        analysis = _fetch_analysis_by_id(nid)
                        write_ready_row(news, analysis, result)
                    except Exception as se:
                        logger.warning("Sheets Ready export failed for %s: %s", nid, se)

                    _update_task(tid, "done", {
                        "stage": "complete",
                        "rewrite_title": result.get("title", "")[:100],
                        "article_id": aid,
                    })
                    update_news_status(nid, "processed")
                else:
                    _update_task(tid, "error", {"stage": "rewriting", "error": "Rewrite returned None"})
            except Exception as e:
                _update_task(tid, "error", {"error": str(e)[:500]})

    threading.Thread(
        target=_bg_mod_rewrite,
        args=(list(news_ids), list(task_ids), style),
        daemon=True
    ).start()

    return {"status": "ok", "queued": len(news_ids), "task_ids": task_ids}


def rescore_news(body):
    """Re-run scoring pipeline for specific news or all with score=0.
    Caller must check permissions before calling."""
    news_ids = body.get("news_ids", [])
    rescore_zero = body.get("rescore_zero", False)
    conn = get_connection()
    cur = conn.cursor()
    ph = "%s" if _is_postgres() else "?"
    try:
        if rescore_zero:
            cur.execute(f"""
                SELECT n.* FROM news n
                LEFT JOIN news_analysis a ON n.id = a.news_id
                WHERE n.status IN ('in_review', 'new', 'rejected')
                AND (a.total_score IS NULL OR a.total_score = 0 OR a.news_id IS NULL)
                ORDER BY n.parsed_at DESC
                LIMIT 500
            """)
        elif news_ids:
            placeholders = ",".join([ph] * len(news_ids))
            cur.execute(f"SELECT * FROM news WHERE id IN ({placeholders})", tuple(news_ids))
        else:
            return {"status": "error", "message": "news_ids or rescore_zero required"}

        if _is_postgres():
            columns = [desc[0] for desc in cur.description]
            news_list = [dict(zip(columns, r)) for r in cur.fetchall()]
        else:
            news_list = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()

    if not news_list:
        return {"status": "ok", "rescored": 0, "message": "Нет новостей для пересчёта"}

    from checks.pipeline import run_review_pipeline
    result = run_review_pipeline(news_list, update_status=False)
    scored = len(result.get("results", []))
    return {"status": "ok", "rescored": scored}


def quick_tags(body):
    """Quick tag calculation by headlines (without full review)."""
    news_ids = body.get("news_ids", [])
    if not news_ids:
        return {"status": "error", "message": "No news_ids"}
    try:
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            placeholders = ",".join([ph] * len(news_ids))
            cur.execute(f"SELECT id, title, description FROM news WHERE id IN ({placeholders})", news_ids)
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                rows = [dict(zip(columns, row)) for row in cur.fetchall()]
            else:
                rows = [dict(row) for row in cur.fetchall()]

            from checks.tags import auto_tag
            from checks.deduplication import tfidf_similarity

            tags_map = {}
            for r in rows:
                tags = auto_tag(r)
                tags_map[r["id"]] = [{"id": t["id"], "label": t["label"], "hits": t["hits"]} for t in tags[:3]]

            titles = [r.get("title", "") for r in rows]
            ids_ordered = [r["id"] for r in rows]
            pairs = tfidf_similarity(titles)

            from collections import defaultdict
            graph = defaultdict(set)
            for i, j, score in pairs:
                graph[i].add(j)
                graph[j].add(i)
            visited = set()
            groups = []
            group_idx = 0
            id_to_group = {}
            for idx in range(len(rows)):
                if idx in visited:
                    continue
                cluster = set()
                stack = [idx]
                while stack:
                    node = stack.pop()
                    if node in visited:
                        continue
                    visited.add(node)
                    cluster.add(node)
                    stack.extend(graph[node] - visited)
                if len(cluster) >= 2:
                    group_idx += 1
                    member_ids = [ids_ordered[i] for i in sorted(cluster)]
                    member_titles = [titles[i] for i in sorted(cluster)]
                    for mid in member_ids:
                        id_to_group[mid] = group_idx
                    groups.append({
                        "group": group_idx,
                        "count": len(member_ids),
                        "ids": member_ids,
                        "titles": member_titles,
                    })

            return {"status": "ok", "tags": tags_map, "groups": groups, "id_to_group": id_to_group}
        finally:
            cur.close()
    except Exception as e:
        return {"status": "error", "message": str(e), "type": type(e).__name__}


def dashboard_groups(query_params):
    """Returns tags and groups for news (respects status filter)."""
    try:
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            status_filter = query_params.get("status", [None])[0]
            if status_filter:
                cur.execute(f"SELECT id, title, description FROM news WHERE status = {ph} ORDER BY parsed_at DESC LIMIT 100", (status_filter,))
            else:
                cur.execute("SELECT id, title, description FROM news ORDER BY parsed_at DESC LIMIT 100")
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                rows = [dict(zip(columns, row)) for row in cur.fetchall()]
            else:
                rows = [dict(row) for row in cur.fetchall()]

            if not rows:
                return {"status": "ok", "tags": {}, "groups": [], "id_to_group": {}}

            from checks.tags import auto_tag
            from checks.deduplication import tfidf_similarity
            from collections import defaultdict

            tags_map = {}
            for r in rows:
                tags = auto_tag(r)
                tags_map[r["id"]] = [{"id": t["id"], "label": t["label"], "hits": t["hits"]} for t in tags[:3]]

            titles = [r.get("title", "") for r in rows]
            ids_ordered = [r["id"] for r in rows]
            pairs = tfidf_similarity(titles)

            graph = defaultdict(set)
            for i, j, score in pairs:
                graph[i].add(j)
                graph[j].add(i)
            visited = set()
            groups = []
            group_idx = 0
            id_to_group = {}
            for idx in range(len(rows)):
                if idx in visited:
                    continue
                cluster = set()
                stack = [idx]
                while stack:
                    node = stack.pop()
                    if node in visited:
                        continue
                    visited.add(node)
                    cluster.add(node)
                    stack.extend(graph[node] - visited)
                if len(cluster) >= 2:
                    group_idx += 1
                    member_ids = [ids_ordered[i] for i in sorted(cluster)]
                    member_titles = [titles[i] for i in sorted(cluster)]
                    for mid in member_ids:
                        id_to_group[mid] = group_idx
                    groups.append({
                        "group": group_idx,
                        "count": len(member_ids),
                        "ids": member_ids,
                        "titles": member_titles,
                    })

            return {"status": "ok", "tags": tags_map, "groups": groups, "id_to_group": id_to_group}
        finally:
            cur.close()
    except Exception as e:
        return {"status": "error", "message": str(e), "type": type(e).__name__}


def translate_title(body):
    """Translate news title using LLM."""
    news_id = body.get("news_id", "")
    if not news_id:
        return {"status": "error", "message": "news_id required"}
    conn = get_connection()
    cur = conn.cursor()
    try:
        ph = "%s" if _is_postgres() else "?"
        cur.execute(f"SELECT title FROM news WHERE id = {ph}", (news_id,))
        row = cur.fetchone()
        if not row:
            return {"status": "error", "message": "not found"}
        title = row[0] if _is_postgres() else row["title"]
        try:
            from apis.llm import translate_title as llm_translate_title
            result = llm_translate_title(title)
            if result:
                if not result.get("is_russian") and result.get("translated"):
                    cur.execute(f"UPDATE news SET h1 = {ph} WHERE id = {ph}", (result["translated"], news_id))
                    if not _is_postgres():
                        conn.commit()
                return {"status": "ok", **result}
            else:
                return {"status": "error", "message": "LLM not responding. Check API keys and rate limits."}
        except Exception as e:
            logger.error("Translate error: %s", e)
            return {"status": "error", "message": str(e)}
    finally:
        cur.close()


def ai_recommend(body):
    """Get AI recommendation for a news item."""
    news_id = body.get("news_id", "")
    if not news_id:
        return {"status": "error", "message": "news_id required"}
    conn = get_connection()
    cur = conn.cursor()
    try:
        ph = "%s" if _is_postgres() else "?"
        cur.execute(f"SELECT * FROM news WHERE id = {ph}", (news_id,))
        row = cur.fetchone()
        if not row:
            return {"status": "error", "message": "not found"}
        if _is_postgres():
            columns = [desc[0] for desc in cur.description]
            news = dict(zip(columns, row))
        else:
            news = dict(row)

        from checks.quality import check_quality
        from checks.relevance import check_relevance
        from checks.freshness import check_freshness
        from checks.viral_score import viral_score
        checks = {
            "quality": check_quality(news),
            "relevance": check_relevance(news),
            "freshness": check_freshness(news),
            "viral": viral_score(news),
        }
        from apis.llm import ai_recommendation
        result = ai_recommendation(
            title=news.get("title", ""),
            text=news.get("plain_text", "") or news.get("description", ""),
            source=news.get("source", ""),
            checks=checks,
        )
        if result:
            return {"status": "ok", "recommendation": result, "checks": {k: v.get("score", 0) for k, v in checks.items()}}
        else:
            return {"status": "error", "message": "AI recommendation failed"}
    finally:
        cur.close()
