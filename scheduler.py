import gc
import logging
import json
import threading
import time
from apscheduler.schedulers.blocking import BlockingScheduler

import config
from parsers.rss_parser import parse_rss_source
from parsers.html_parser import parse_html_source
from nlp.tfidf import extract_keywords
from apis.keyso import get_keyword_info, get_similar_keywords
from apis.google_trends import get_trends_for_keyword
from apis.llm import forecast_trend, suggest_keyso_queries
from storage.database import get_unprocessed_news, update_news_status, save_analysis, cleanup_old_plaintext, cleanup_old_tasks
from storage.sheets import write_news_row

logger = logging.getLogger(__name__)

# Thread-safe pipeline stop event
_pipeline_stop_event = threading.Event()

# Circuit breaker: consecutive API failures per service (thread-safe)
_cb_lock = threading.Lock()
_api_failures = {}  # {service: consecutive_count}
_api_failure_times = {}  # {service: timestamp of last failure}
_API_FAILURE_THRESHOLD = 5  # after 5 consecutive failures, skip service
_CIRCUIT_RESET_SECONDS = 300  # auto-reset after 5 minutes


def _api_circuit_open(service: str) -> bool:
    """Returns True if circuit is open (too many failures, should skip).
    Auto-resets after _CIRCUIT_RESET_SECONDS to avoid permanent deadlock."""
    with _cb_lock:
        if _api_failures.get(service, 0) < _API_FAILURE_THRESHOLD:
            return False
        last_failure = _api_failure_times.get(service, 0)
        if time.time() - last_failure > _CIRCUIT_RESET_SECONDS:
            _api_failures[service] = 0
            logger.info("Circuit breaker AUTO-RESET for %s after %ds timeout", service, _CIRCUIT_RESET_SECONDS)
            return False
        return True


def _api_record_failure(service: str):
    """Records an API failure."""
    with _cb_lock:
        _api_failures[service] = _api_failures.get(service, 0) + 1
        _api_failure_times[service] = time.time()
        if _api_failures[service] == _API_FAILURE_THRESHOLD:
            logger.warning("Circuit breaker OPEN for %s after %d consecutive failures", service, _API_FAILURE_THRESHOLD)


def _api_record_success(service: str):
    """Resets failure counter on success."""
    with _cb_lock:
        _api_failures[service] = 0


def pipeline_stop():
    """Сигнал остановки пайплайна."""
    _pipeline_stop_event.set()


def pipeline_reset():
    """Сброс флага остановки."""
    _pipeline_stop_event.clear()


def is_pipeline_stopped():
    return _pipeline_stop_event.is_set()


def parse_sources(interval_min: int):
    """Парсит все источники с указанным интервалом."""
    sources = [s for s in config.SOURCES if s["interval"] == interval_min]
    total = 0
    for source in sources:
        if source["type"] == "rss":
            total += parse_rss_source(source)
        elif source["type"] in ("html", "dtf", "gamesradar", "homepage"):
            total += parse_html_source(source)
        elif source["type"] == "sitemap":
            from parsers.html_parser import parse_sitemap_source
            total += parse_sitemap_source(source)
    logger.info("[%dmin] Total new articles: %d", interval_min, total)

    # Free memory from parsing (BeautifulSoup/lxml trees, response bodies)
    gc.collect()

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
            cur.execute(f"SELECT id, source, url, title, h1, description, plain_text, published_at, parsed_at, status FROM news WHERE status = 'new' ORDER BY parsed_at DESC LIMIT {ph}", (20,))
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

        # Telegram notifications for high-scoring news
        try:
            if getattr(config, "TELEGRAM_BOT_TOKEN", ""):
                from bot.telegram_bot import notify_high_score, notify_pipeline_done
                # Build notification list from results
                high_score_news = []
                for r in result.get("results", []):
                    if not r.get("is_duplicate") and not r.get("auto_rejected"):
                        high_score_news.append({
                            "id": r.get("id", ""),
                            "title": r.get("title", ""),
                            "source": r.get("source", ""),
                            "total_score": r.get("total_score", 0),
                        })
                if high_score_news:
                    notify_high_score(high_score_news)
                notify_pipeline_done("auto_review", {
                    "reviewed": reviewed,
                    "duplicates": dupes,
                })
        except Exception as tg_err:
            logger.debug("Telegram notify skipped: %s", tg_err)

    except Exception as e:
        logger.error("Auto-review error: %s", e)


def _auto_rescore_zero():
    """Ежедневный пересчёт новостей с score=0 или без анализа.

    Подбирает news которые могли получить 0 из-за отсутствия plain_text,
    но текст мог быть извлечён при повторном парсинге.
    """
    try:
        from storage.database import get_connection, _is_postgres
        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT n.* FROM news n
                LEFT JOIN news_analysis a ON n.id = a.news_id
                WHERE n.status IN ('in_review', 'rejected')
                AND (a.total_score IS NULL OR a.total_score = 0 OR a.news_id IS NULL)
                AND n.plain_text != '' AND n.plain_text IS NOT NULL
                ORDER BY n.parsed_at DESC
                LIMIT 200
            """)
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
        rescored = len(result.get("results", []))
        improved = sum(1 for r in result.get("results", []) if r.get("total_score", 0) > 0)
        logger.info("Auto-rescore: %d rescored, %d improved (score>0)", rescored, improved)

    except Exception as e:
        logger.error("Auto-rescore error: %s", e)


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

    # 2. Keys.so (with rate limit + circuit breaker) — region по источнику
    top_bigram = bigrams[0][0] if bigrams else title
    source = news.get("source", "")
    keyso_region = config.keyso_region_for_source(source)
    from apis.cache import rate_check
    if rate_check("keyso") and not _api_circuit_open("keyso"):
        try:
            keyso_info = get_keyword_info(top_bigram, region=keyso_region)
            time.sleep(2)
            similar = get_similar_keywords(top_bigram, limit=10, region=keyso_region)
            time.sleep(2)
            _api_record_success("keyso")
        except Exception as e:
            logger.warning("Keys.so error: %s", e)
            _api_record_failure("keyso")
            keyso_info = {"ws": 0, "wsk": 0}
            similar = []
    else:
        logger.warning("Keys.so skipped (rate limit or circuit breaker)")
        keyso_info = {"ws": 0, "wsk": 0}
        similar = []

    # 3. Google Trends (with rate limit + circuit breaker)
    if rate_check("trends") and not _api_circuit_open("trends"):
        try:
            trends = get_trends_for_keyword(top_bigram)
            time.sleep(3)
            _api_record_success("trends")
        except Exception as e:
            logger.warning("Trends error: %s", e)
            _api_record_failure("trends")
            trends = {}
    else:
        logger.warning("Trends skipped (rate limit or circuit breaker)")
        trends = {}

    # 4. LLM (with rate limit + circuit breaker)
    if rate_check("llm") and not _api_circuit_open("llm"):
        try:
            fc = forecast_trend(
                title=title, text=text, bigrams=bigrams,
                keyso_freq=keyso_info.get("ws", 0), trends=trends,
            )
            time.sleep(2)
            _api_record_success("llm")
        except Exception as e:
            logger.warning("LLM error: %s", e)
            _api_record_failure("llm")
            fc = None
    else:
        logger.warning("LLM skipped (rate limit or circuit breaker)")
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


def _calc_final_score(analysis: dict) -> int:
    """Рассчитывает финальный скор (аналог JS calcFinalScore).

    Формула: internal(40%) + viral(20%) + keyso_bonus(15%) + trends_bonus(10%) + headline(15%)
    """
    import json as _json

    internal = float(analysis.get("total_score") or 0)
    viral = float(analysis.get("viral_score") or 0)
    headline = float(analysis.get("headline_score") or 0)

    # Keys.so bonus
    keyso_bonus = 0
    try:
        kd = analysis.get("keyso_data", "{}")
        if isinstance(kd, str):
            kd = _json.loads(kd) if kd else {}
        freq = float(kd.get("freq") or kd.get("ws") or 0)
        if freq >= 10000:
            keyso_bonus = 100
        elif freq >= 5000:
            keyso_bonus = 80
        elif freq >= 1000:
            keyso_bonus = 60
        elif freq >= 100:
            keyso_bonus = 40
        elif freq > 0:
            keyso_bonus = 20
    except Exception:
        pass

    # Trends bonus
    trends_bonus = 0
    try:
        td = analysis.get("trends_data", "{}")
        if isinstance(td, str):
            td = _json.loads(td) if td else {}
        vals = [float(v) for v in td.values() if str(v).replace(".", "").replace("-", "").isdigit()]
        max_t = max(vals) if vals else 0
        if max_t >= 80:
            trends_bonus = 100
        elif max_t >= 50:
            trends_bonus = 70
        elif max_t >= 20:
            trends_bonus = 40
        elif max_t > 0:
            trends_bonus = 20
    except Exception:
        pass

    return round(internal * 0.4 + viral * 0.2 + keyso_bonus * 0.15 + trends_bonus * 0.1 + headline * 0.15)


# Пороги для полного автомата
FULL_AUTO_SCORE_THRESHOLD = 70    # внутренний скор для отправки на LLM
FULL_AUTO_FINAL_THRESHOLD = 60    # финальный скор для рерайта


def run_full_auto_pipeline(news_ids: list[str], task_ids: list[str]):
    """Режим 1: Полный автомат.

    1) Скоринг → только >70 на обогащение
    2) Обогащение (Keys.so + Trends + LLM)
    3) Финальный скор → только >60 на рерайт
    4) Рерайт → сохранение статьи → Sheets/Ready
    """
    pipeline_reset()
    from checks.pipeline import run_review_pipeline
    from apis.llm import rewrite_news
    from storage.sheets import write_ready_row

    # Decision trace helper (best-effort)
    def _trace(nid, step, decision, reason="", details=None, s_before=0, s_after=0):
        try:
            from core.observability import log_decision
            log_decision(nid, step, decision, reason, details, s_before, s_after)
        except Exception:
            pass

    for i, (news_id, task_id) in enumerate(zip(news_ids, task_ids)):
        if _pipeline_stop_event.is_set():
            # Cancel remaining tasks
            for remaining_tid in task_ids[i:]:
                _update_task(remaining_tid, "cancelled", {"reason": "Остановлено пользователем"})
            logger.info("Full-auto pipeline stopped by user at %d/%d", i, len(news_ids))
            break

        try:
            news = _fetch_news_by_id(news_id)
            if not news:
                _update_task(task_id, "error", {"stage": "init", "error": "News not found"})
                continue

            # Stage 1: Local scoring (reuse if already scored)
            _update_task(task_id, "running", {"stage": "scoring", "progress": f"{i+1}/{len(news_ids)}"})
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
                _trace(news_id, "full_auto", "skipped_duplicate", "Дубликат обнаружен", s_after=total_score)
                continue

            if is_rejected:
                _update_task(task_id, "skipped", {"stage": "scoring", "reason": "auto_rejected", "score": total_score})
                _trace(news_id, "full_auto", "auto_rejected", f"total_score={total_score} < 15", s_after=total_score)
                continue

            # Stage 2: Score threshold — only >70 goes to LLM enrichment
            if total_score < FULL_AUTO_SCORE_THRESHOLD:
                _update_task(task_id, "skipped", {
                    "stage": "score_filter",
                    "reason": f"Скор {total_score} < {FULL_AUTO_SCORE_THRESHOLD}",
                    "score": total_score,
                })
                _trace(news_id, "full_auto", "skipped_low_score",
                       f"total_score={total_score} < порога {FULL_AUTO_SCORE_THRESHOLD}, не отправлен на LLM",
                       s_after=total_score)
                logger.info("Full-auto skip (score %d < %d): %s", total_score, FULL_AUTO_SCORE_THRESHOLD, news.get("title", "")[:50])
                continue

            # Stage 3: Enrichment (Keys.so + Trends + LLM)
            _update_task(task_id, "running", {"stage": "enriching", "score": total_score})
            update_news_status(news_id, "approved")
            enrich_result = _do_process(news)
            recommendation = enrich_result.get("recommendation", "")

            # Stage 4: Calculate final composite score
            analysis = _fetch_analysis_by_id(news_id)
            final_score = _calc_final_score(analysis) if analysis else 0

            _update_task(task_id, "running", {
                "stage": "final_score",
                "score": total_score,
                "final_score": final_score,
                "recommendation": recommendation,
            })

            if final_score < FULL_AUTO_FINAL_THRESHOLD:
                _update_task(task_id, "done", {
                    "stage": "filtered",
                    "reason": f"Финальный скор {final_score} < {FULL_AUTO_FINAL_THRESHOLD}",
                    "score": total_score,
                    "final_score": final_score,
                    "recommendation": recommendation,
                })
                _trace(news_id, "full_auto", "filtered_final_score",
                       f"final_score={final_score} < порога {FULL_AUTO_FINAL_THRESHOLD}, не отправлен на рерайт",
                       {"total_score": total_score, "final_score": final_score, "recommendation": recommendation},
                       s_before=total_score, s_after=final_score)
                logger.info("Full-auto filtered (final %d < %d): %s", final_score, FULL_AUTO_FINAL_THRESHOLD, news.get("title", "")[:50])
                continue

            # Stage 5: Rewrite
            _update_task(task_id, "running", {"stage": "rewriting", "score": total_score, "final_score": final_score})
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
                update_news_status(news_id, "in_review")  # Reset so it can be retried
                continue

            # Save article to DB
            _save_rewrite_article(news_id, news, rewrite, style)

            # Stage 6: Export to Sheets/Ready
            _update_task(task_id, "running", {"stage": "exporting", "score": total_score, "final_score": final_score})
            sheet_row = None
            try:
                sheet_row = write_ready_row(news, analysis, rewrite)
            except Exception as sheets_err:
                logger.error("Full-auto Sheets write failed for %s: %s", news_id, sheets_err)
                # Article saved to DB already — not lost
                time.sleep(10)  # Back off on Sheets error

            if sheet_row:
                update_news_status(news_id, "ready")
            else:
                # Sheets failed — keep as approved so it can be retried
                logger.warning("Sheets export failed for %s, keeping status 'approved'", news_id)
            _update_task(task_id, "done", {
                "stage": "complete",
                "score": total_score,
                "final_score": final_score,
                "recommendation": recommendation,
                "sheet_row": sheet_row,
                "rewrite_title": rewrite.get("title", "")[:100],
            })
            _trace(news_id, "full_auto", "published_ready",
                   f"Прошёл все этапы: score={total_score}, final={final_score}, рерайт выполнен, экспорт в Sheets",
                   {"total_score": total_score, "final_score": final_score, "sheet_row": sheet_row},
                   s_before=total_score, s_after=final_score)
            logger.info("Full-auto complete: %s → final=%d, Ready row %s", news.get("title", "")[:50], final_score, sheet_row)

        except Exception as e:
            logger.error("Full-auto pipeline error for %s: %s", news_id, e)
            _update_task(task_id, "error", {"stage": "unknown", "error": str(e)[:500]})


def _save_rewrite_article(news_id: str, news: dict, rewrite: dict, style: str):
    """Сохраняет результат рерайта как статью в таблицу articles."""
    import uuid
    from datetime import datetime, timezone
    from storage.database import get_connection, _is_postgres

    conn = get_connection()
    cur = conn.cursor()
    ph = "%s" if _is_postgres() else "?"
    now = datetime.now(timezone.utc).isoformat()
    aid = str(uuid.uuid4())[:12]

    try:
        cur.execute(f"""INSERT INTO articles (id, news_id, title, text, seo_title, seo_description,
            tags, style, language, original_title, original_text, source_url, status, created_at)
            VALUES ({','.join([ph]*14)})""", (
            aid, news_id,
            rewrite.get("title", "")[:500],
            rewrite.get("text", ""),
            rewrite.get("seo_title", "")[:500],
            rewrite.get("seo_description", "")[:1000],
            json.dumps(rewrite.get("tags", []), ensure_ascii=False),
            style, "русский",
            news.get("title", "")[:500],
            (news.get("plain_text", "") or "")[:3000],
            news.get("url", ""),
            "draft", now,
        ))
        if not _is_postgres():
            conn.commit()
        logger.info("Saved rewrite article %s for news %s", aid, news_id)
    except Exception as e:
        logger.warning("Failed to save rewrite article for %s: %s", news_id, e)
    finally:
        cur.close()


# ─── Pipeline 2: No LLM (score → Sheets/NotReady + Moderation) ───

def _build_check_result_from_analysis(analysis: dict) -> dict:
    """Собирает check_result из сохранённого news_analysis (для уже проскоренных)."""
    import json as _json

    def _safe_loads(val, default):
        if val is None or val == "":
            return default
        if isinstance(val, (dict, list)):
            return val
        try:
            return _json.loads(val)
        except (ValueError, TypeError):
            return default

    # Viral triggers from viral_data
    viral_triggers = _safe_loads(analysis.get("viral_data"), [])

    checks = {
        "quality": {"score": analysis.get("quality_score", 0), "pass": True},
        "relevance": {"score": analysis.get("relevance_score", 0), "pass": True},
        "freshness": {
            "score": analysis.get("freshness_score", 0) if analysis.get("freshness_score") else 0,
            "pass": True,
            "age_hours": analysis.get("freshness_hours", -1),
            "status": analysis.get("freshness_status", ""),
        },
        "viral": {
            "score": analysis.get("viral_score", 0),
            "pass": True,
            "level": analysis.get("viral_level", ""),
            "triggers": viral_triggers if isinstance(viral_triggers, list) else [],
        },
    }

    # tags_data (not "tags") is the DB column name
    tags = _safe_loads(analysis.get("tags_data") or analysis.get("tags"), [])

    sentiment = {"label": analysis.get("sentiment_label", "neutral") or "neutral", "score": 0}
    momentum = {"score": analysis.get("momentum_score", 0) or 0, "level": "none"}
    headline = {"score": analysis.get("headline_score", 0) or 0}

    # entity_names (not "entities") is the DB column name
    game_entities = _safe_loads(analysis.get("entity_names") or analysis.get("entities"), [])

    return {
        "checks": checks,
        "tags": tags,
        "sentiment": sentiment,
        "momentum": momentum,
        "headline": headline,
        "game_entities": game_entities,
        "total_score": analysis.get("total_score", 0) or 0,
    }


def run_no_llm_pipeline(news_ids: list[str], task_ids: list[str]):
    """Режим 2: Без LLM.

    Для уже проскоренных (in_review) — берёт существующие результаты.
    Для новых — скорит локально.
    Всех годных → Sheets/NotReady (batch по 25 строк) + статус moderation.
    """
    pipeline_reset()
    from checks.pipeline import run_review_pipeline
    from storage.sheets import write_not_ready_batch

    BATCH_SIZE = 25
    batch_items = []        # [(news, check_result), ...]
    batch_task_ids = []     # task_ids for items in current batch
    batch_news_ids = []     # news_ids for items in current batch
    total_written = 0
    total_skipped = 0
    total_errors = 0

    def _flush_batch():
        """Write accumulated batch to Sheets and update tasks/statuses."""
        nonlocal total_written, total_skipped, total_errors
        if not batch_items:
            return

        logger.info("No-LLM: flushing batch of %d items to Sheets...", len(batch_items))
        for tid in batch_task_ids:
            _update_task(tid, "running", {"stage": "exporting"})

        try:
            result = write_not_ready_batch(batch_items)
            written = result.get("written", 0)
            skipped = result.get("skipped", 0)
            errors = result.get("errors", 0)
            total_written += written
            total_skipped += skipped
            total_errors += errors

            # Update all tasks in batch as done
            for tid in batch_task_ids:
                _update_task(tid, "done", {
                    "stage": "complete",
                    "destination": "NotReady",
                    "batch_written": written,
                })

            # Update all news statuses to moderation
            for nid in batch_news_ids:
                update_news_status(nid, "moderation")

            logger.info("No-LLM batch flush: %d written, %d skipped, %d errors (total: %d/%d)",
                        written, skipped, errors, total_written, len(news_ids))

        except Exception as e:
            total_errors += len(batch_items)
            logger.error("No-LLM batch flush failed: %s", e)
            for tid in batch_task_ids:
                _update_task(tid, "error", {"stage": "exporting", "error": str(e)[:300]})

        batch_items.clear()
        batch_task_ids.clear()
        batch_news_ids.clear()

    for i, (news_id, task_id) in enumerate(zip(news_ids, task_ids)):
        if _pipeline_stop_event.is_set():
            _flush_batch()  # write what we have so far
            for remaining_tid in task_ids[i:]:
                _update_task(remaining_tid, "cancelled", {"reason": "Остановлено пользователем"})
            logger.info("No-LLM pipeline stopped by user at %d/%d", i, len(news_ids))
            break

        try:
            news = _fetch_news_by_id(news_id)
            if not news:
                _update_task(task_id, "error", {"stage": "init", "error": "News not found"})
                continue

            _update_task(task_id, "running", {"stage": "scoring", "progress": f"{i+1}/{len(news_ids)}"})

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

            # Accumulate for batch write
            batch_items.append((news, check_result))
            batch_task_ids.append(task_id)
            batch_news_ids.append(news_id)

            # Flush when batch is full
            if len(batch_items) >= BATCH_SIZE:
                _flush_batch()

        except Exception as e:
            logger.error("No-LLM pipeline error for %s: %s", news_id, e)
            _update_task(task_id, "error", {"stage": "unknown", "error": str(e)[:500]})

    # Flush remaining items
    _flush_batch()

    logger.info("No-LLM pipeline complete: %d written, %d skipped, %d errors out of %d total",
                total_written, total_skipped, total_errors, len(news_ids))


def generate_auto_digest():
    """Генерирует авто-дайджест: топ-20 новостей за последние 24 часа."""
    try:
        from storage.database import get_connection, _is_postgres, save_digest
        from apis.digest import generate_daily_digest
        import uuid
        from datetime import datetime, timezone

        conn = get_connection()
        cur = conn.cursor()
        try:
            if _is_postgres():
                cur.execute("""
                    SELECT n.id, n.title, n.source, n.url,
                           COALESCE(a.total_score, 0) as total_score
                    FROM news n
                    LEFT JOIN news_analysis a ON a.news_id = n.id
                    WHERE n.status IN ('approved', 'processed', 'in_review', 'ready')
                      AND n.parsed_at::timestamptz > (NOW() - INTERVAL '24 hours')
                    ORDER BY COALESCE(a.total_score, 0) DESC
                    LIMIT 20
                """)
                columns = [desc[0] for desc in cur.description]
                news_list = [dict(zip(columns, row)) for row in cur.fetchall()]
            else:
                cur.execute("""
                    SELECT n.id, n.title, n.source, n.url,
                           COALESCE(a.total_score, 0) as total_score
                    FROM news n
                    LEFT JOIN news_analysis a ON a.news_id = n.id
                    WHERE n.status IN ('approved', 'processed', 'in_review', 'ready')
                      AND n.parsed_at > datetime('now', '-1 day')
                    ORDER BY COALESCE(a.total_score, 0) DESC
                    LIMIT 20
                """)
                news_list = [dict(row) for row in cur.fetchall()]
        finally:
            cur.close()

        if not news_list:
            logger.info("Auto-digest: no news in last 24h, skipping")
            return

        result = generate_daily_digest(news_list, style="brief")

        digest_id = str(uuid.uuid4())[:12]
        digest_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        save_digest(
            digest_id=digest_id,
            digest_date=digest_date,
            style="brief",
            title=result.get("title", ""),
            text=result.get("text", ""),
            news_count=result.get("news_count", 0),
        )
        logger.info("Auto-digest generated: %s (%d news)", result.get("title", "")[:60], len(news_list))

    except Exception as e:
        logger.error("Auto-digest error: %s", e)


def publish_scheduled_articles():
    """Проверяет запланированные статьи и публикует те, у которых наступило время."""
    try:
        from storage.database import get_connection, _is_postgres
        from datetime import datetime, timezone

        conn = get_connection()
        cur = conn.cursor()
        ph = "%s" if _is_postgres() else "?"
        now = datetime.now(timezone.utc).isoformat()

        try:
            cur.execute(
                f"SELECT id, title FROM articles WHERE status = 'scheduled' AND scheduled_at <= {ph}",
                (now,)
            )
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                due_articles = [dict(zip(columns, row)) for row in cur.fetchall()]
            else:
                due_articles = [dict(row) for row in cur.fetchall()]

            if not due_articles:
                return

            for article in due_articles:
                aid = article["id"]
                cur.execute(
                    f"UPDATE articles SET status = 'published', updated_at = {ph} WHERE id = {ph}",
                    (now, aid)
                )
                logger.info("Auto-published scheduled article: %s", article.get("title", "")[:60])

            if not _is_postgres():
                conn.commit()

            logger.info("Published %d scheduled articles", len(due_articles))
        finally:
            cur.close()

    except Exception as e:
        logger.error("Scheduled publish error: %s", e)


def start_scheduler():
    """Запускает планировщик задач."""
    scheduler = BlockingScheduler(timezone="Europe/Moscow")

    # Парсинг по интервалам (включает auto-review)
    intervals = sorted(set(s["interval"] for s in config.SOURCES))
    for mins in intervals:
        scheduler.add_job(parse_sources, "interval", minutes=mins, args=[mins], id=f"parse_{mins}min")

    # process_news ОТКЛЮЧЁН из автозапуска — вызывается только вручную через веб-панель
    # Это экономит Keys.so, Google Trends и LLM API

    # Очистка старого plain_text раз в сутки (7 дней вместо 14 — экономия RAM в БД)
    scheduler.add_job(lambda: cleanup_old_plaintext(days=7), "interval", hours=24, id="cleanup_plaintext")

    # Очистка старых задач из task_queue раз в сутки
    scheduler.add_job(cleanup_old_tasks, "interval", hours=24, id="cleanup_tasks")

    # Очистка просроченных записей кэша каждые 3 часа (was 6)
    from apis.cache import cache_cleanup
    scheduler.add_job(cache_cleanup, "interval", hours=3, id="cache_cleanup")

    # Публикация запланированных статей: каждую минуту
    scheduler.add_job(publish_scheduled_articles, "interval", minutes=1, id="publish_scheduled")

    # Авто-пересчёт news с score=0: ежедневно в 04:00
    scheduler.add_job(_auto_rescore_zero, "cron", hour=4, minute=0, id="auto_rescore_zero")

    # Авто-дайджест: ежедневно в 23:00 по Москве
    scheduler.add_job(generate_auto_digest, "cron", hour=23, minute=0, id="auto_digest")

    # Первый запуск парсинга сразу (включает auto-review)
    for mins in intervals:
        parse_sources(mins)

    logger.info("Scheduler started")
    scheduler.start()
