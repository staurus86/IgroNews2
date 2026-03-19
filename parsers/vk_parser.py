import logging
import time
from datetime import datetime, timezone

import config
from storage.database import insert_news, news_exists
from parsers.proxy import fetch_with_retry

logger = logging.getLogger(__name__)


def _truncate_title(text: str, max_len: int = 100) -> str:
    """Обрезает текст до max_len символов по границе слова."""
    if len(text) <= max_len:
        return text
    truncated = text[:max_len]
    last_space = truncated.rfind(" ")
    if last_space > 0:
        truncated = truncated[:last_space]
    return truncated


def _is_ad_post(post: dict) -> bool:
    """Проверяет, является ли пост рекламным."""
    if post.get("marked_as_ads", 0) == 1:
        return True
    text = post.get("text", "").lower()
    if "#ad" in text or "#реклама" in text:
        return True
    return False


def _extract_url(post: dict) -> str:
    """Извлекает URL из вложений поста или генерирует ссылку на стену."""
    attachments = post.get("attachments", [])
    for att in attachments:
        if att.get("type") == "link":
            link = att.get("link", {})
            url = link.get("url", "")
            if url:
                return url
    owner_id = post.get("owner_id", 0)
    post_id = post.get("id", 0)
    return f"https://vk.com/wall{owner_id}_{post_id}"


def parse_vk_source(source: dict) -> int:
    """Парсит VK-группу через wall.get API, возвращает количество новых новостей."""
    if not config.VK_API_TOKEN:
        logger.warning("VK_API_TOKEN not configured, skipping VK source %s", source.get("name", ""))
        return 0

    name = source.get("name", "VK")
    group_id = source.get("group_id", "")
    count = 0

    try:
        api_url = "https://api.vk.com/method/wall.get"
        params = {
            "owner_id": f"-{group_id}",
            "count": 20,
            "filter": "owner",
            "v": config.VK_API_VERSION,
            "access_token": config.VK_API_TOKEN,
        }

        resp = fetch_with_retry(f"{api_url}?{'&'.join(f'{k}={v}' for k, v in params.items())}")
        data = resp.json()

        if "error" in data:
            error_msg = data["error"].get("error_msg", "Unknown VK API error")
            logger.error("VK API error for %s: %s", name, error_msg)
            return 0

        items = data.get("response", {}).get("items", [])

        for post in items:
            text = post.get("text", "").strip()
            if not text:
                continue

            if _is_ad_post(post):
                logger.debug("Skipping ad post in %s", name)
                continue

            url = _extract_url(post)

            if news_exists(url):
                continue

            title = _truncate_title(text)
            plain_text = text
            description = text[:300]

            ts = post.get("date", 0)
            published_at = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else ""

            news_id = insert_news(
                source=name,
                url=url,
                title=title,
                h1="",
                description=description,
                plain_text=plain_text,
                published_at=published_at,
            )
            if news_id:
                count += 1

        time.sleep(0.4)

    except Exception as e:
        logger.error("Error parsing VK source %s: %s", name, e)
        return 0

    logger.info("Parsed VK %s: %d new articles", name, count)
    return count
