import logging
from checks.deduplication import tfidf_similarity, build_groups
from checks.quality import check_quality
from checks.relevance import check_relevance
from checks.freshness import check_freshness
from checks.viral_score import viral_score
from checks.tags import auto_tag
from checks.sentiment import analyze_sentiment
from checks.momentum import get_momentum
from checks.ner import extract_entities
from checks.headline_score import headline_score
from checks.source_weight import get_source_weight
from nlp.game_entities import find_entities
from storage.database import get_connection, _is_postgres, update_news_status, save_check_results

logger = logging.getLogger(__name__)


def _check_single(news: dict) -> dict:
    """Проверяет одну новость. Возвращает результат без изменения статуса."""
    result = {
        "id": news["id"],
        "title": news.get("title", ""),
        "source": news.get("source", ""),
        "url": news.get("url", ""),
        "published_at": news.get("published_at", ""),
        "checks": {},
    }

    # Основные проверки
    result["checks"]["quality"] = check_quality(news)
    result["checks"]["relevance"] = check_relevance(news)
    result["checks"]["freshness"] = check_freshness(news)
    result["checks"]["viral"] = viral_score(news)

    # Дополнительные анализы
    result["tags"] = auto_tag(news)
    result["sentiment"] = analyze_sentiment(news)
    result["momentum"] = get_momentum(news)
    result["entities"] = extract_entities(news)
    result["headline"] = headline_score(news)
    result["source_weight"] = get_source_weight(news.get("source", ""))

    # Game entities из единой базы
    text = news.get("title", "") + " " + news.get("plain_text", "")
    result["game_entities"] = find_entities(text)

    all_pass = all(c["pass"] for c in result["checks"].values())
    total_score = sum(c["score"] for c in result["checks"].values()) // 4

    # Momentum бустит score
    momentum_bonus = result["momentum"]["score"] // 5
    total_score = min(100, total_score + momentum_bonus)

    # Source weight multiplier
    sw = result["source_weight"]
    total_score = min(100, int(total_score * sw))

    # Headline bonus
    headline_bonus = max(0, (result["headline"]["score"] - 50)) // 10
    total_score = min(100, total_score + headline_bonus)

    result["overall_pass"] = all_pass
    result["total_score"] = total_score
    result["status"] = news.get("status", "new")

    return result


def run_review_pipeline(news_list: list[dict], update_status: bool = True) -> dict:
    """Запускает все этапы проверки для списка новостей.

    Args:
        news_list: список новостей для проверки
        update_status: если True — обновляет статусы в БД (in_review/duplicate)
    """
    results = [_check_single(news) for news in news_list]

    # Dedup across batch (TF-IDF + entity overlap)
    titles = [r["title"] for r in results]
    texts = [r["title"] + " " + (news_list[i].get("plain_text", "") if i < len(news_list) else "")
             for i, r in enumerate(results)]
    pairs = tfidf_similarity(titles, texts)
    groups = build_groups(results, pairs)

    # Mark duplicates
    for group in groups:
        for idx in group.get("duplicate_indices", []):
            if idx < len(results):
                results[idx]["overall_pass"] = False
                results[idx]["is_duplicate"] = True

        for member in group["members"]:
            member["dedup_status"] = group["status"]

    # Save check results in DB (always) + update statuses (optional)
    for r in results:
        if update_status:
            if r.get("is_duplicate"):
                update_news_status(r["id"], "duplicate")
            else:
                update_news_status(r["id"], "in_review")
        try:
            save_check_results(
                r["id"], r["checks"],
                sentiment=r.get("sentiment"),
                tags=r.get("tags"),
                momentum=r.get("momentum"),
                headline=r.get("headline"),
                total_score=r.get("total_score", 0),
                entities=r.get("game_entities"),
            )
        except Exception as e:
            logger.warning("Failed to save check results for %s: %s", r["id"], e)

    return {"results": results, "groups": groups}


def approve_for_enrichment(news_ids: list[str]):
    """Одобряет новости и ставит в очередь на обогащение."""
    for nid in news_ids:
        update_news_status(nid, "approved")
    logger.info("Approved %d news for enrichment", len(news_ids))
