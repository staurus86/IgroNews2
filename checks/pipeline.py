import logging
from checks.deduplication import tfidf_similarity, build_groups
from checks.quality import check_quality
from checks.relevance import check_relevance
from checks.freshness import check_freshness
from checks.viral_score import viral_score
from checks.tags import auto_tag
from checks.sentiment import analyze_sentiment
from checks.momentum import get_momentum
from storage.database import get_connection, _is_postgres, update_news_status

logger = logging.getLogger(__name__)


def run_review_pipeline(news_list: list[dict]) -> dict:
    """Запускает все этапы проверки для списка новостей."""
    results = []

    for news in news_list:
        result = {
            "id": news["id"],
            "title": news.get("title", ""),
            "source": news.get("source", ""),
            "url": news.get("url", ""),
            "published_at": news.get("published_at", ""),
            "checks": {},
        }

        # 5 основных проверок
        result["checks"]["quality"] = check_quality(news)
        result["checks"]["relevance"] = check_relevance(news)
        result["checks"]["freshness"] = check_freshness(news)
        result["checks"]["viral"] = viral_score(news)

        # Дополнительные анализы
        result["tags"] = auto_tag(news)
        result["sentiment"] = analyze_sentiment(news)
        result["momentum"] = get_momentum(news)

        all_pass = all(c["pass"] for c in result["checks"].values())
        total_score = sum(c["score"] for c in result["checks"].values()) // 4

        # Momentum бустит score
        momentum_bonus = result["momentum"]["score"] // 5
        total_score = min(100, total_score + momentum_bonus)

        result["overall_pass"] = all_pass
        result["total_score"] = total_score

        results.append(result)

    # Dedup across batch
    titles = [r["title"] for r in results]
    pairs = tfidf_similarity(titles)
    groups = build_groups(results, pairs)

    # Mark duplicates
    for group in groups:
        for idx in group.get("duplicate_indices", []):
            if idx < len(results):
                results[idx]["overall_pass"] = False
                results[idx]["is_duplicate"] = True

        for member in group["members"]:
            member["dedup_status"] = group["status"]

    # Update statuses in DB
    for r in results:
        if r.get("is_duplicate"):
            update_news_status(r["id"], "duplicate")
        else:
            update_news_status(r["id"], "in_review")

    return {"results": results, "groups": groups}


def approve_for_enrichment(news_ids: list[str]):
    """Одобряет новости и ставит в очередь на обогащение."""
    for nid in news_ids:
        update_news_status(nid, "approved")
    logger.info("Approved %d news for enrichment", len(news_ids))
