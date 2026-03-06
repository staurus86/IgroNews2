import json
import logging
import re
import time

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from storage.database import insert_news, news_exists

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


def parse_html_source(source: dict) -> int:
    """Парсит HTML-страницу с новостями, возвращает количество новых."""
    if source.get("type") == "dtf":
        return _parse_dtf(source)

    name = source["name"]
    url = source["url"]
    selector = source.get("selector", "article")
    title_selector = source.get("title_selector", "")
    count = 0

    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        items = soup.select(selector)
        if not items:
            logger.warning("No items found for %s with selector '%s'", name, selector)
            return 0

        for item in items[:20]:
            # Ищем ссылку внутри элемента
            a_tag = item.find("a", href=True) if item.name != "a" else item
            if not a_tag or not a_tag.get("href"):
                continue

            href = a_tag["href"]
            link = urljoin(url, href)

            # Заголовок: кастомный селектор, или h2/h3/h4, или текст ссылки
            title = ""
            if title_selector:
                title_el = item.select_one(title_selector) if item.name != "a" else item.find(title_selector.split()[-1])
                if title_el:
                    title = title_el.get_text(strip=True)
            if not title:
                title_tag = item.find(["h2", "h3", "h4"]) or a_tag
                title = title_tag.get_text(strip=True)

            if not title or len(title) < 10:
                continue

            if news_exists(link):
                continue

            time.sleep(1)
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


def _parse_dtf(source: dict) -> int:
    """Парсит DTF через __INITIAL_STATE__ JSON (SPA)."""
    name = source["name"]
    url = source["url"]
    count = 0

    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()

        # Ищем JSON в __INITIAL_STATE__
        match = re.search(r'window\.__INITIAL_STATE__\s*=\s*({.+?})\s*;?\s*</script>', resp.text, re.DOTALL)
        if not match:
            # Fallback: попробуем как обычный HTML
            logger.warning("DTF: __INITIAL_STATE__ not found, trying HTML fallback")
            soup = BeautifulSoup(resp.text, "lxml")
            items = soup.select("a[href*='/games/']")
            for item in items[:20]:
                href = item.get("href", "")
                if not href or not re.search(r'/games/\d+', href):
                    continue
                link = urljoin("https://dtf.ru", href)
                h_tag = item.find(["h2", "h3", "h4"])
                title = h_tag.get_text(strip=True) if h_tag else item.get_text(strip=True)
                if not title or len(title) < 10:
                    continue
                if news_exists(link):
                    continue
                time.sleep(1)
                h1, description, plain_text = _fetch_article(link)
                nid = insert_news(source=name, url=link, title=title, h1=h1,
                                  description=description, plain_text=plain_text)
                if nid:
                    count += 1
            logger.info("Parsed %s (DTF HTML fallback): %d new articles", name, count)
            return count

        data = json.loads(match.group(1))

        # Навигация по структуре JSON DTF
        entries = []
        if "entries" in data:
            entries = list(data["entries"].values()) if isinstance(data["entries"], dict) else data["entries"]
        elif "feed" in data and "items" in data["feed"]:
            entries = data["feed"]["items"]

        if not entries:
            # Пробуем найти вложенные посты
            for key in data:
                val = data[key]
                if isinstance(val, dict):
                    for subkey in val:
                        subval = val[subkey]
                        if isinstance(subval, dict) and "title" in subval and "url" in subval:
                            entries.append(subval)

        for entry in entries[:20]:
            title = entry.get("title", "")
            if not title or len(title) < 10:
                continue

            entry_url = entry.get("url", "")
            if not entry_url:
                entry_id = entry.get("id", "")
                slug = entry.get("slug", "")
                if entry_id:
                    entry_url = f"https://dtf.ru/games/{entry_id}-{slug}" if slug else f"https://dtf.ru/games/{entry_id}"
            if not entry_url.startswith("http"):
                entry_url = urljoin("https://dtf.ru", entry_url)

            if news_exists(entry_url):
                continue

            description = entry.get("intro", "") or entry.get("description", "")
            plain_text = entry.get("text", "") or description
            if plain_text:
                plain_text = BeautifulSoup(plain_text, "lxml").get_text(separator=" ", strip=True)[:5000]

            published = entry.get("date", "") or entry.get("dateRFC", "")

            nid = insert_news(
                source=name, url=entry_url, title=title,
                h1=title, description=description[:500],
                plain_text=plain_text, published_at=str(published),
            )
            if nid:
                count += 1

    except Exception as e:
        logger.error("Error parsing DTF: %s", e)

    logger.info("Parsed %s (DTF JSON): %d new articles", name, count)
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
