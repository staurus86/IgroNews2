import logging
import time
from datetime import datetime, timezone, timedelta

import feedparser
from bs4 import BeautifulSoup

from storage.database import insert_news, news_exists
from parsers.proxy import fetch_with_retry, _get_random_ua

MAX_AGE_DAYS = 30

logger = logging.getLogger(__name__)


def fetch_full_text(url: str) -> tuple[str, str, str, str]:
    """Загружает страницу и извлекает h1, description, plain_text, published_at."""
    try:
        resp = fetch_with_retry(url)
        soup = BeautifulSoup(resp.text, "lxml")

        h1 = ""
        h1_tag = soup.find("h1")
        if h1_tag:
            h1 = h1_tag.get_text(strip=True)

        description = ""
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc:
            description = meta_desc.get("content", "")

        # Извлекаем дату публикации
        from parsers.html_parser import _extract_publish_date
        published_at = _extract_publish_date(soup)

        # Извлекаем основной текст из article или body
        article = soup.find("article") or soup.find("div", class_="article") or soup.body
        plain_text = ""
        if article:
            for tag in article.find_all(["script", "style", "nav", "footer", "header", "aside"]):
                tag.decompose()
            plain_text = article.get_text(separator=" ", strip=True)[:5000]

        return h1, description, plain_text, published_at
    except Exception as e:
        logger.warning("Failed to fetch full text from %s: %s", url, e)
        return "", "", "", ""


def parse_rss_source(source: dict) -> int:
    """Парсит один RSS-источник, возвращает количество новых новостей."""
    name = source["name"]
    url = source["url"]
    count = 0

    try:
        feed = feedparser.parse(url, request_headers={"User-Agent": _get_random_ua()})
        if feed.bozo and not feed.entries:
            logger.warning("Feed error for %s: %s", name, feed.bozo_exception)
            return 0

        cutoff = datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)

        for entry in feed.entries:
            link = entry.get("link", "")
            if not link or news_exists(link):
                continue

            title = entry.get("title", "").strip()
            published = entry.get("published", "")

            # Фильтр по дате: пропускаем старше 30 дней
            if hasattr(entry, "published_parsed") and entry.published_parsed:
                pub_dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                if pub_dt < cutoff:
                    continue

            # Получаем summary из RSS
            summary = ""
            if "summary" in entry:
                soup = BeautifulSoup(entry.summary, "lxml")
                summary = soup.get_text(strip=True)[:500]

            # Загружаем полный текст страницы (с задержкой чтобы не перегружать)
            time.sleep(1)
            h1, description, plain_text, page_date = fetch_full_text(link)

            if not description:
                description = summary

            # Приоритет: дата из RSS, иначе из HTML страницы
            final_date = published or page_date

            news_id = insert_news(
                source=name,
                url=link,
                title=title,
                h1=h1,
                description=description,
                plain_text=plain_text,
                published_at=final_date,
            )
            if news_id:
                count += 1

    except Exception as e:
        logger.error("Error parsing RSS %s: %s", name, e)

    logger.info("Parsed %s: %d new articles", name, count)
    return count
