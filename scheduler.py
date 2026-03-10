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
from storage.database import get_unprocessed_news, update_news_status, save_analysis, cleanup_old_plaintext
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

    # Auto-review: бесплатный локальный анализ сразу после парсинга
    if total > 0:
        _auto_review_new()


def _auto_review_new():
    """Автоматическая проверка новых новостей (бесплатно, всё локальное).

    Только скоринг — НЕ запускает обогащение и рерайт.
    Дальнейшая обработка через кнопки "Полный автомат" / "Без LLM".
    """
    try:
        from storage.database import get_connection, _is_postgres
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            cur.execute(f"SELECT * FROM news WHERE status = 'new' ORDER BY parsed_at DESC LIMIT {ph}", (20,))
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                news_list = [dict(zip(columns, row)) for row in cur.fetchall()]
            else:
                news_list = [dict(row) for row in cur.fetchall()]
        finally:
            cur.close()

        if not news_list:
            return

        from checks.pipeline import run_review_pipeline
        result = run_review_pipeline(news_list, update_status=True)
        reviewed = len(result.get("results", []))
        dupes = sum(1 for r in result.get("results", []) if r.get("is_duplicate"))
        logger.info("Auto-review: %d checked, %d duplicates", reviewed, dupes)

    except Exception as e:
        logger.error("Auto-review error: %s", e)


def _auto_approve_high_score(results: list):
    """Автоматически одобряет новости с высоким скором и запускает обогащение."""
    import config
    threshold = getattr(config, "AUTO_APPROVE_THRESHOLD", 70)
    if threshold <= 0:
        return  # Auto-approve disabled

    auto_ids = []
    for r in results:
        score = r.get("total_score", 0)
        is_dup = r.get("is_duplicate", False)
        is_rejected = r.get("auto_rejected", False)
        if score >= threshold and not is_dup and not is_rejected:
            auto_ids.append(r["id"])

    if not auto_ids:
        return

    from checks.pipeline import approve_for_enrichment
    from checks.feedback import record_decision
    approve_for_enrichment(auto_ids)
    for nid in auto_ids:
        try:
            record_decision(nid, "auto_approved")
        except Exception:
            pass
    logger.info("Auto-approved %d news (threshold=%d)", len(auto_ids), threshold)

    # Background enrichment
    import threading
    def _bg_enrich(ids):
        for nid in ids:
            try:
                result = _process_single_news(nid)
                # Auto-rewrite if LLM recommends "publish_now"
                _auto_rewrite_if_recommended(nid, result)
            except Exception as e:
                logger.warning("Auto-enrich failed for %s: %s", nid, e)
    threading.Thread(target=_bg_enrich, args=(list(auto_ids),), daemon=True).start()


def _auto_rewrite_if_recommended(news_id: str, enrich_result: dict):
    """Если LLM рекомендовал publish_now — автоматически ставит в очередь на рерайт."""
    import config
    if not getattr(config, "AUTO_REWRITE_ON_PUBLISH_NOW", True):
        return

    recommendation = enrich_result.get("recommendation", "")
    if recommendation != "publish_now":
        return

    style = getattr(config, "AUTO_REWRITE_STYLE", "news")

    import uuid
    from datetime import datetime, timezone
    from storage.database import get_connection, _is_postgres

    conn = get_connection()
    cur = conn.cursor()
    ph = "%s" if _is_postgres() else "?"
    now = datetime.now(timezone.utc).isoformat()

    try:
        cur.execute(f"SELECT title FROM news WHERE id = {ph}", (news_id,))
        row = cur.fetchone()
        if not row:
            return
        title = row[0] if _is_postgres() else row["title"]
        tid = str(uuid.uuid4())[:12]
        cur.execute(f"""INSERT INTO task_queue (id, task_type, news_id, news_title, style, status, created_at, updated_at)
            VALUES ({','.join([ph]*8)})""",
            (tid, "rewrite", news_id, title[:200], style, "pending", now, now))
        if not _is_postgres():
            conn.commit()
        logger.info("Auto-queued rewrite for %s (publish_now)", news_id)
    except Exception as e:
        logger.warning("Auto-rewrite queue failed for %s: %s", news_id, e)
    finally:
        cur.close()

    # Process the rewrite in background
    try:
        _process_auto_rewrite(tid)
    except Exception as e:
        logger.warning("Auto-rewrite processing failed for %s: %s", tid, e)


def _process_auto_rewrite(task_id: str):
    """Обрабатывает одну задачу рерайта из очереди."""
    import json as _json
    from apis.llm import rewrite_news
    from storage.database import get_connection, _is_postgres

    conn = get_connection()
    cur = conn.cursor()
    ph = "%s" if _is_postgres() else "?"

    try:
        cur.execute(f"SELECT * FROM task_queue WHERE id = {ph}", (task_id,))
        if _is_postgres():
            cols = [d[0] for d in cur.description]
            task = dict(zip(cols, cur.fetchone()))
        else:
            task = dict(cur.fetchone())

        nid = task["news_id"]
        style = task.get("style", "news")

        # Get news content
        cur.execute(f"SELECT * FROM news WHERE id = {ph}", (nid,))
        if _is_postgres():
            cols2 = [d[0] for d in cur.description]
            news = dict(zip(cols2, cur.fetchone()))
        else:
            news = dict(cur.fetchone())

        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        cur.execute(f"UPDATE task_queue SET status = 'running', updated_at = {ph} WHERE id = {ph}", (now, task_id))
        if not _is_postgres():
            conn.commit()

        result = rewrite_news(
            title=news.get("title", ""),
            text=news.get("plain_text", ""),
            style=style,
            language="русский",
        )

        now = datetime.now(timezone.utc).isoformat()
        result_json = _json.dumps(result, ensure_ascii=False) if result else "{}"
        cur.execute(f"UPDATE task_queue SET status = 'done', result = {ph}, updated_at = {ph} WHERE id = {ph}",
                    (result_json, now, task_id))
        if not _is_postgres():
            conn.commit()
        logger.info("Auto-rewrite done for task %s", task_id)
    except Exception as e:
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        cur.execute(f"UPDATE task_queue SET status = 'error', result = {ph}, updated_at = {ph} WHERE id = {ph}",
                    (str(e)[:500], now, task_id))
        if not _is_postgres():
            conn.commit()
        raise
    finally:
        cur.close()


def _process_single_news(news_id: str) -> dict:
    """Обрабатывает одну новость по ID. Возвращает результат."""
    from storage.database import get_connection, _is_postgres
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
    finally:
        cur.close()
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


# ─── Pipeline 1: Full Auto (score → enrich → rewrite → Sheets/Ready) ───

def _update_task(task_id: str, status: str, result_data: dict | str | None = None):
    """Обновляет статус задачи в очереди."""
    import json as _json
    from datetime import datetime, timezone
    from storage.database import get_connection, _is_postgres

    conn = get_connection()
    cur = conn.cursor()
    ph = "%s" if _is_postgres() else "?"
    now = datetime.now(timezone.utc).isoformat()
    result_str = ""
    if result_data:
        result_str = _json.dumps(result_data, ensure_ascii=False) if isinstance(result_data, dict) else str(result_data)
    try:
        cur.execute(f"UPDATE task_queue SET status = {ph}, result = {ph}, updated_at = {ph} WHERE id = {ph}",
                    (status, result_str[:2000], now, task_id))
        if not _is_postgres():
            conn.commit()
    finally:
        cur.close()


def _create_task(task_type: str, news_id: str, news_title: str, style: str = "") -> str:
    """Создаёт задачу в очереди, возвращает task_id."""
    import uuid
    from datetime import datetime, timezone
    from storage.database import get_connection, _is_postgres

    conn = get_connection()
    cur = conn.cursor()
    ph = "%s" if _is_postgres() else "?"
    now = datetime.now(timezone.utc).isoformat()
    tid = str(uuid.uuid4())[:12]
    try:
        cur.execute(f"""INSERT INTO task_queue (id, task_type, news_id, news_title, style, status, created_at, updated_at)
            VALUES ({','.join([ph]*8)})""",
            (tid, task_type, news_id, news_title[:200], style, "pending", now, now))
        if not _is_postgres():
            conn.commit()
    finally:
        cur.close()
    return tid


def _fetch_news_by_id(news_id: str) -> dict | None:
    """Загружает новость из БД по ID."""
    from storage.database import get_connection, _is_postgres
    conn = get_connection()
    cur = conn.cursor()
    ph = "%s" if _is_postgres() else "?"
    try:
        cur.execute(f"SELECT * FROM news WHERE id = {ph}", (news_id,))
        row = cur.fetchone()
        if not row:
            return None
        if _is_postgres():
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))
        return dict(row)
    finally:
        cur.close()


def _fetch_analysis_by_id(news_id: str) -> dict | None:
    """Загружает анализ из БД по news_id."""
    from storage.database import get_connection, _is_postgres
    conn = get_connection()
    cur = conn.cursor()
    ph = "%s" if _is_postgres() else "?"
    try:
        cur.execute(f"SELECT * FROM news_analysis WHERE news_id = {ph}", (news_id,))
        row = cur.fetchone()
        if not row:
            return None
        if _is_postgres():
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))
        return dict(row)
    finally:
        cur.close()


def run_full_auto_pipeline(news_ids: list[str], task_ids: list[str]):
    """Режим 1: Полный автомат.

    score → enrich (Keys.so+Trends+LLM) → фильтр publish_now → rewrite → Sheets/Ready
    """
    from checks.pipeline import run_review_pipeline
    from apis.llm import rewrite_news
    from storage.sheets import write_ready_row

    for news_id, task_id in zip(news_ids, task_ids):
        try:
            news = _fetch_news_by_id(news_id)
            if not news:
                _update_task(task_id, "error", {"stage": "init", "error": "News not found"})
                continue

            # Stage 1: Local scoring (reuse if already scored)
            _update_task(task_id, "running", {"stage": "scoring"})
            status = news.get("status", "new")
            analysis = _fetch_analysis_by_id(news_id)

            if analysis and analysis.get("total_score") is not None and status in ("in_review", "moderation"):
                total_score = analysis.get("total_score", 0)
                is_dup = status == "duplicate"
                is_rejected = status == "rejected" or total_score < 15
            else:
                review_result = run_review_pipeline([news], update_status=True)
                results = review_result.get("results", [])
                if not results:
                    _update_task(task_id, "error", {"stage": "scoring", "error": "No review results"})
                    continue
                check_result = results[0]
                total_score = check_result.get("total_score", 0)
                is_dup = check_result.get("is_duplicate", False)
                is_rejected = check_result.get("auto_rejected", False)

            if is_dup:
                _update_task(task_id, "skipped", {"stage": "scoring", "reason": "duplicate", "score": total_score})
                continue

            if is_rejected:
                _update_task(task_id, "skipped", {"stage": "scoring", "reason": "auto_rejected", "score": total_score})
                continue

            # Stage 2: Enrichment (APIs + LLM analysis)
            _update_task(task_id, "running", {"stage": "enriching", "score": total_score})
            update_news_status(news_id, "approved")
            enrich_result = _do_process(news)
            recommendation = enrich_result.get("recommendation", "")

            # Stage 3: Filter — only publish_now proceeds
            if recommendation != "publish_now":
                _update_task(task_id, "done", {
                    "stage": "filtered",
                    "reason": f"LLM: {recommendation}",
                    "score": total_score,
                    "recommendation": recommendation,
                })
                continue

            # Stage 4: Rewrite
            _update_task(task_id, "running", {"stage": "rewriting", "score": total_score})
            import config
            style = getattr(config, "AUTO_REWRITE_STYLE", "news")
            rewrite = rewrite_news(
                title=news.get("title", ""),
                text=news.get("plain_text", ""),
                style=style,
                language="русский",
            )
            if not rewrite:
                _update_task(task_id, "error", {"stage": "rewriting", "error": "Rewrite returned None"})
                continue

            # Stage 5: Export to Sheets/Ready
            _update_task(task_id, "running", {"stage": "exporting", "score": total_score})
            analysis = _fetch_analysis_by_id(news_id)
            sheet_row = write_ready_row(news, analysis, rewrite)

            update_news_status(news_id, "ready")
            _update_task(task_id, "done", {
                "stage": "complete",
                "score": total_score,
                "recommendation": recommendation,
                "sheet_row": sheet_row,
                "rewrite_title": rewrite.get("title", "")[:100],
            })
            logger.info("Full-auto complete: %s → Ready row %s", news.get("title", "")[:50], sheet_row)

        except Exception as e:
            logger.error("Full-auto pipeline error for %s: %s", news_id, e)
            _update_task(task_id, "error", {"stage": "unknown", "error": str(e)[:500]})


# ─── Pipeline 2: No LLM (score → Sheets/NotReady + Moderation) ───

def _build_check_result_from_analysis(analysis: dict) -> dict:
    """Собирает check_result из сохранённого news_analysis (для уже проскоренных)."""
    import json as _json
    checks = {}
    for name in ("quality", "relevance", "freshness", "viral"):
        score_key = f"{name}_score"
        checks[name] = {"score": analysis.get(score_key, 0), "pass": True}

    tags = []
    try:
        tags = _json.loads(analysis.get("tags", "[]"))
    except Exception:
        pass

    sentiment = {"label": analysis.get("sentiment_label", "neutral"), "score": 0}
    momentum = {"score": analysis.get("momentum_score", 0), "level": "none"}
    headline = {"score": analysis.get("headline_score", 0)}
    game_entities = []
    try:
        game_entities = _json.loads(analysis.get("entities", "[]"))
    except Exception:
        pass

    return {
        "checks": checks,
        "tags": tags,
        "sentiment": sentiment,
        "momentum": momentum,
        "headline": headline,
        "game_entities": game_entities,
        "total_score": analysis.get("total_score", 0),
    }


def run_no_llm_pipeline(news_ids: list[str], task_ids: list[str]):
    """Режим 2: Без LLM.

    Для уже проскоренных (in_review) — берёт существующие результаты.
    Для новых — скорит локально.
    Всех годных → Sheets/NotReady + статус moderation.
    """
    from checks.pipeline import run_review_pipeline
    from storage.sheets import write_not_ready_row

    for news_id, task_id in zip(news_ids, task_ids):
        try:
            news = _fetch_news_by_id(news_id)
            if not news:
                _update_task(task_id, "error", {"stage": "init", "error": "News not found"})
                continue

            _update_task(task_id, "running", {"stage": "scoring"})

            status = news.get("status", "new")
            analysis = _fetch_analysis_by_id(news_id)

            # If already scored (has analysis data), reuse it
            if analysis and analysis.get("total_score") is not None and status in ("in_review", "moderation"):
                check_result = _build_check_result_from_analysis(analysis)
                total_score = check_result.get("total_score", 0)
                is_dup = status == "duplicate"
                is_rejected = status == "rejected"
            else:
                # Score from scratch
                review_result = run_review_pipeline([news], update_status=True)
                results = review_result.get("results", [])
                if not results:
                    _update_task(task_id, "error", {"stage": "scoring", "error": "No review results"})
                    continue
                check_result = results[0]
                total_score = check_result.get("total_score", 0)
                is_dup = check_result.get("is_duplicate", False)
                is_rejected = check_result.get("auto_rejected", False)

            if is_dup:
                _update_task(task_id, "skipped", {"stage": "scoring", "reason": "duplicate", "score": total_score})
                continue

            if is_rejected:
                _update_task(task_id, "skipped", {"stage": "scoring", "reason": "auto_rejected", "score": total_score})
                continue

            _update_task(task_id, "running", {"stage": "exporting", "score": total_score})
            sheet_row = write_not_ready_row(news, check_result)

            # Set status to moderation
            update_news_status(news_id, "moderation")

            _update_task(task_id, "done", {
                "stage": "complete",
                "score": total_score,
                "sheet_row": sheet_row,
                "destination": "NotReady",
            })
            logger.info("No-LLM complete: %s → NotReady row %s, status=moderation", news.get("title", "")[:50], sheet_row)

        except Exception as e:
            logger.error("No-LLM pipeline error for %s: %s", news_id, e)
            _update_task(task_id, "error", {"stage": "unknown", "error": str(e)[:500]})


def start_scheduler():
    """Запускает планировщик задач."""
    scheduler = BlockingScheduler(timezone="Europe/Moscow")

    # Парсинг по интервалам (включает auto-review)
    intervals = sorted(set(s["interval"] for s in config.SOURCES))
    for mins in intervals:
        scheduler.add_job(parse_sources, "interval", minutes=mins, args=[mins], id=f"parse_{mins}min")

    # process_news ОТКЛЮЧЁН из автозапуска — вызывается только вручную через веб-панель
    # Это экономит Keys.so, Google Trends и LLM API

    # Очистка старого plain_text раз в сутки (экономия памяти БД)
    scheduler.add_job(cleanup_old_plaintext, "interval", hours=24, id="cleanup_plaintext")

    # Первый запуск парсинга сразу (включает auto-review)
    for mins in intervals:
        parse_sources(mins)

    logger.info("Scheduler started")
    scheduler.start()
