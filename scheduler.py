"""Scheduler: APScheduler job configuration and source parsing.

Business logic lives in:
- core/circuit_breaker.py  — circuit breaker + pipeline stop
- pipeline/orchestrator.py — full-auto, no-LLM, enrichment, task queue
"""

import gc
import logging

from apscheduler.schedulers.blocking import BlockingScheduler

import config
from parsers.rss_parser import parse_rss_source
from parsers.html_parser import parse_html_source
from storage.database import cleanup_old_plaintext, cleanup_old_tasks

# Re-export for backward compatibility (web.py, bot, tests import from scheduler)
from core.circuit_breaker import (  # noqa: F401
    _api_circuit_open, _api_record_failure, _api_record_success,
    pipeline_stop, pipeline_reset, is_pipeline_stopped,
)
from pipeline.orchestrator import (  # noqa: F401
    _auto_review_new, _auto_rescore_zero,
    process_news, _process_single_news, _do_process,
    _update_task, _create_task, _fetch_news_by_id, _fetch_analysis_by_id,
    _calc_final_score, run_full_auto_pipeline, run_no_llm_pipeline,
    _save_rewrite_article, _build_check_result_from_analysis,
    generate_auto_digest, publish_scheduled_articles,
    FULL_AUTO_SCORE_THRESHOLD, FULL_AUTO_FINAL_THRESHOLD,
)

logger = logging.getLogger(__name__)


def parse_sources(interval_min: int):
    """Parse all sources with the given interval."""
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

    gc.collect()

    if total > 0:
        _auto_review_new()


def start_scheduler():
    """Start the APScheduler with all configured jobs."""
    scheduler = BlockingScheduler(timezone="Europe/Moscow")

    # Parsing by interval (includes auto-review)
    intervals = sorted(set(s["interval"] for s in config.SOURCES))
    for mins in intervals:
        scheduler.add_job(parse_sources, "interval", minutes=mins, args=[mins], id=f"parse_{mins}min")

    # Cleanup old plain_text daily (7 days)
    scheduler.add_job(lambda: cleanup_old_plaintext(days=7), "interval", hours=24, id="cleanup_plaintext")

    # Cleanup old tasks from task_queue daily
    scheduler.add_job(cleanup_old_tasks, "interval", hours=24, id="cleanup_tasks")

    # Cache cleanup every 3 hours
    from apis.cache import cache_cleanup
    scheduler.add_job(cache_cleanup, "interval", hours=3, id="cache_cleanup")

    # Publish scheduled articles every minute
    scheduler.add_job(publish_scheduled_articles, "interval", minutes=1, id="publish_scheduled")

    # Auto-rescore news with score=0: daily at 04:00
    scheduler.add_job(_auto_rescore_zero, "cron", hour=4, minute=0, id="auto_rescore_zero")

    # Auto-digest: daily at 23:00 Moscow time
    scheduler.add_job(generate_auto_digest, "cron", hour=23, minute=0, id="auto_digest")

    # Storylines auto-export (if enabled in settings)
    try:
        from api.dashboard import get_storylines_settings, export_storylines_to_sheets
        sl_cfg = get_storylines_settings()
        if sl_cfg.get("enabled"):
            sl_days = sl_cfg.get("days", 3)
            scheduler.add_job(
                lambda: export_storylines_to_sheets(days=sl_days),
                "cron", hour=sl_cfg.get("hour", 9), minute=sl_cfg.get("minute", 0),
                id="storylines_auto_export", replace_existing=True,
            )
            logger.info("Storylines auto-export: %02d:%02d, %d days",
                        sl_cfg.get("hour", 9), sl_cfg.get("minute", 0), sl_days)
    except Exception as e:
        logger.debug("Storylines auto-export init skipped: %s", e)

    # Initial parse on startup (includes auto-review)
    for mins in intervals:
        parse_sources(mins)

    logger.info("Scheduler started")
    scheduler.start()
