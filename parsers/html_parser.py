import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from storage.database import insert_news, news_exists

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


def parse_html_source(source: dict) -> int:
    """Парсит HTML-страницу с новостями, возвращает количество новых."""
    name = source["name"]
    url = source["url"]
    selector = source.get("selector", "article")
    count = 0

    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        items = soup.select(selector)
        if not items:
            logger.warning("No items found for %s with selector '%s'", name, selector)
            return 0

        for item in items[:30]:
            # Ищем ссылку внутри элемента
            a_tag = item.find("a", href=True) if item.name != "a" else item
            if not a_tag or not a_tag.get("href"):
                continue

            href = a_tag["href"]
            link = urljoin(url, href)

            # Заголовок: из ссылки или заголовочного тега внутри элемента
            title_tag = item.find(["h2", "h3", "h4"]) or a_tag
            title = title_tag.get_text(strip=True)
            if not title or len(title) < 10:
                continue

            if news_exists(link):
                continue

            h1, description, plain_text = _fetch_article(link)

            news_id = insert_news(
                source=name,
                url=link,
                title=title,
                h1=h1,
                description=description,
                plain_text=plain_text,
            )
            if news_id:
                count += 1

    except Exception as e:
        logger.error("Error parsing HTML %s: %s", name, e)

    logger.info("Parsed %s (HTML): %d new articles", name, count)
    return count


def _fetch_article(url: str) -> tuple[str, str, str]:
    """Загружает статью и извлекает h1, description, plain_text."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        h1 = ""
        h1_tag = soup.find("h1")
        if h1_tag:
            h1 = h1_tag.get_text(strip=True)

        description = ""
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc:
            description = meta_desc.get("content", "")

        article = soup.find("article") or soup.body
        plain_text = ""
        if article:
            for tag in article.find_all(["script", "style", "nav", "footer", "header", "aside"]):
                tag.decompose()
            plain_text = article.get_text(separator=" ", strip=True)[:5000]

        return h1, description, plain_text
    except Exception as e:
        logger.warning("Failed to fetch article %s: %s", url, e)
        return "", "", ""
