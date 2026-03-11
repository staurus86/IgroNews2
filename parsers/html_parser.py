import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree

from bs4 import BeautifulSoup
from urllib.parse import urljoin

from storage.database import insert_news, news_exists
from parsers.proxy import fetch_with_retry

logger = logging.getLogger(__name__)


def parse_html_source(source: dict) -> int:
    """Парсит HTML-страницу с новостями, возвращает количество новых."""
    if source.get("type") == "dtf":
        return _parse_dtf(source)
    if source.get("type") == "gamesradar":
        return _parse_gamesradar(source)

    name = source["name"]
    url = source["url"]
    selector = source.get("selector", "article")
    title_selector = source.get("title_selector", "")
    url_pattern = source.get("url_pattern", "")
    count = 0

    try:
        resp = fetch_with_retry(url)
        soup = BeautifulSoup(resp.text, "lxml")

        items = soup.select(selector)
        if not items:
            logger.warning("No items found for %s with selector '%s'", name, selector)
            return 0

        seen_urls = set()
        for item in items[:40]:
            # Ищем ссылку внутри элемента
            a_tag = item.find("a", href=True) if item.name != "a" else item
            if not a_tag or not a_tag.get("href"):
                continue

            href = a_tag["href"]
            # Пропускаем якорные ссылки (#comments и т.п.)
            if "#" in href:
                href = href.split("#")[0]
            link = urljoin(url, href)

            # Фильтр по URL паттерну
            if url_pattern and not re.search(url_pattern, link):
                continue

            # Заголовок: кастомный селектор, или h2/h3/h4, или текст ссылки
            title = ""
            if title_selector:
                title_el = item.select_one(title_selector) if item.name != "a" else item.find(title_selector.split()[-1])
                if title_el:
                    title = title_el.get_text(strip=True)
            if not title:
                title_tag = item.find(["h2", "h3", "h4"]) or a_tag
                title = title_tag.get_text(strip=True)

            if not title or len(title) < 20:
                continue

            # Фильтр мусорных заголовков
            junk = ["комментар", "оставить", "читать далее", "подробнее", "показать"]
            if any(j in title.lower() for j in junk):
                continue

            # Дедуп URL внутри одной страницы
            if link in seen_urls:
                continue
            seen_urls.add(link)

            if news_exists(link):
                continue

            time.sleep(1)
            h1, description, plain_text, published_at = _fetch_article(link)

            news_id = insert_news(
                source=name,
                url=link,
                title=title,
                h1=h1,
                description=description,
                plain_text=plain_text,
                published_at=published_at,
            )
            if news_id:
                count += 1

    except Exception as e:
        logger.error("Error parsing HTML %s: %s", name, e)

    logger.info("Parsed %s (HTML): %d new articles", name, count)
    return count


def _parse_gamesradar(source: dict) -> int:
    """Парсит GamesRadar с главной страницы: Latest News + Trending."""
    name = source["name"]
    url = source["url"]
    count = 0

    try:
        resp = fetch_with_retry(url)
        soup = BeautifulSoup(resp.text, "lxml")

        seen_urls = set()
        links_to_process = []

        # 1. Latest News — .listingResult.small elements
        for item in soup.select(".listingResult.small"):
            a_tag = item.find("a", href=True)
            if not a_tag:
                continue
            href = a_tag["href"]
            if not href.startswith("http"):
                href = urljoin("https://www.gamesradar.com", href)
            h3 = item.find(["h3", "h2", "h4"])
            title = h3.get_text(strip=True) if h3 else a_tag.get_text(strip=True)
            if title and len(title) > 15 and href not in seen_urls:
                seen_urls.add(href)
                links_to_process.append((href, title, "latest"))

        # 2. Trending section — widget-header "Trending" + sibling links
        for header in soup.find_all(["div", "h2"], class_=re.compile(r"widget-header")):
            if "trending" not in header.get_text(strip=True).lower():
                continue
            parent = header.parent
            if not parent:
                continue
            for a_tag in parent.find_all("a", href=True):
                href = a_tag["href"]
                if not href.startswith("http"):
                    href = urljoin("https://www.gamesradar.com", href)
                # Фильтр: только статьи, не хабы/гайды (должны содержать достаточно сегментов пути)
                if href.count("/") < 5:
                    continue
                title = a_tag.get_text(strip=True)
                # Очистка от префиксов типа "Opinion", "Review", "Now Playing"
                for prefix in ["Opinion", "Review", "Now Playing", "Preview"]:
                    if title.startswith(prefix):
                        title = title[len(prefix):].strip()
                if title and len(title) > 15 and href not in seen_urls:
                    seen_urls.add(href)
                    links_to_process.append((href, title, "trending"))

        logger.info("GamesRadar: found %d links (%d latest + %d trending)",
                     len(links_to_process),
                     sum(1 for _, _, t in links_to_process if t == "latest"),
                     sum(1 for _, _, t in links_to_process if t == "trending"))

        for link, title, section in links_to_process[:30]:
            if news_exists(link):
                continue
            time.sleep(1)
            h1, description, plain_text, published_at = _fetch_article(link)
            # Prefer page h1 over scraped title
            final_title = h1 if h1 and len(h1) > 15 else title
            news_id = insert_news(
                source=name, url=link, title=final_title,
                h1=h1, description=description,
                plain_text=plain_text, published_at=published_at,
            )
            if news_id:
                count += 1

    except Exception as e:
        logger.error("Error parsing GamesRadar HTML: %s", e)

    # 3. Also try RSS feed as supplement (may have articles not on homepage)
    rss_url = source.get("rss_url")
    if rss_url:
        try:
            from parsers.rss_parser import parse_rss_source
            rss_source = {**source, "type": "rss", "url": rss_url}
            rss_count = parse_rss_source(rss_source)
            count += rss_count
            if rss_count:
                logger.info("GamesRadar RSS supplement: %d new articles", rss_count)
        except Exception as e:
            logger.debug("GamesRadar RSS fallback failed: %s", e)

    logger.info("Parsed %s (GamesRadar HTML+RSS): %d new articles", name, count)
    return count


def _parse_dtf(source: dict) -> int:
    """Парсит DTF через __INITIAL_STATE__ JSON (SPA)."""
    name = source["name"]
    url = source["url"]
    count = 0

    try:
        resp = fetch_with_retry(url)

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
                h1, description, plain_text, published_at = _fetch_article(link)
                nid = insert_news(source=name, url=link, title=title, h1=h1,
                                  description=description, plain_text=plain_text,
                                  published_at=published_at)
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


def parse_sitemap_source(source: dict) -> int:
    """Парсит sitemap XML, берёт свежие URL (за последние 30 дней) и загружает статьи."""
    name = source["name"]
    url = source["url"]
    url_filter = source.get("url_filter", "")
    max_age_days = source.get("max_age_days", 30)
    count = 0
    cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)

    try:
        resp = fetch_with_retry(url)

        root = ElementTree.fromstring(resp.content)
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

        # Проверяем: это sitemapindex или обычный sitemap?
        if root.tag.endswith("sitemapindex"):
            # Это индекс — берём последние 2 sitemap-файла
            sitemaps = root.findall("sm:sitemap", ns)
            sitemap_urls = [s.find("sm:loc", ns).text for s in sitemaps if s.find("sm:loc", ns) is not None]
            # Берём последние 2
            for sm_url in sitemap_urls[-2:]:
                count += _parse_single_sitemap(name, sm_url, url_filter, cutoff)
        else:
            # Обычный sitemap
            count = _parse_single_sitemap_from_root(name, root, ns, url_filter, cutoff)

    except Exception as e:
        logger.error("Error parsing sitemap %s: %s", name, e)

    logger.info("Parsed %s (sitemap): %d new articles", name, count)
    return count


def _parse_single_sitemap(name: str, sm_url: str, url_filter: str, cutoff: datetime) -> int:
    """Загружает один sitemap и парсит его."""
    try:
        resp = fetch_with_retry(sm_url)
        root = ElementTree.fromstring(resp.content)
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        return _parse_single_sitemap_from_root(name, root, ns, url_filter, cutoff)
    except Exception as e:
        logger.error("Error loading sitemap %s: %s", sm_url, e)
        return 0


def _parse_single_sitemap_from_root(name: str, root, ns: dict, url_filter: str, cutoff: datetime) -> int:
    """Парсит URL из sitemap XML root."""
    count = 0
    urls = root.findall("sm:url", ns)

    # Собираем свежие URL
    fresh_urls = []
    for u in urls:
        loc = u.find("sm:loc", ns)
        lastmod = u.find("sm:lastmod", ns)
        if loc is None:
            continue
        link = loc.text.strip()

        # Фильтр по URL (например только /news/)
        if url_filter and url_filter not in link:
            continue

        # Фильтр по дате
        if lastmod is not None and lastmod.text:
            try:
                dt = datetime.fromisoformat(lastmod.text.strip())
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                if dt < cutoff:
                    continue
                fresh_urls.append((link, lastmod.text.strip()))
            except ValueError:
                fresh_urls.append((link, ""))
        else:
            fresh_urls.append((link, ""))

    # Сортируем по дате (новые первыми) и берём до 20
    fresh_urls.sort(key=lambda x: x[1], reverse=True)

    for link, published_at in fresh_urls[:20]:
        if news_exists(link):
            continue

        time.sleep(1)
        h1, description, plain_text, page_date = _fetch_article(link)

        if not h1 or len(h1) < 10:
            continue

        # Приоритет: дата из sitemap, иначе из HTML страницы
        final_date = published_at or page_date

        nid = insert_news(
            source=name, url=link, title=h1,
            h1=h1, description=description,
            plain_text=plain_text, published_at=final_date,
        )
        if nid:
            count += 1

    return count


def _extract_publish_date(soup) -> str:
    """Извлекает дату публикации из HTML разными способами."""
    # 1. JSON-LD (schema.org) — самый надёжный
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                data = data[0]
            for key in ("datePublished", "dateCreated", "uploadDate"):
                if key in data:
                    return data[key]
        except Exception:
            pass

    # 2. Meta теги
    for meta_name in ("article:published_time", "og:article:published_time",
                      "date", "pubdate", "DC.date.issued", "sailthru.date"):
        tag = soup.find("meta", attrs={"property": meta_name}) or \
              soup.find("meta", attrs={"name": meta_name})
        if tag and tag.get("content"):
            return tag["content"]

    # 3. <time> тег с datetime
    time_tag = soup.find("time", attrs={"datetime": True})
    if time_tag:
        return time_tag["datetime"]

    # 4. <time> тег с текстом
    time_tag = soup.find("time")
    if time_tag and time_tag.get_text(strip=True):
        return time_tag.get_text(strip=True)

    return ""


def _extract_body_text(soup) -> str:
    """Извлекает основной текст статьи, пробуя несколько селекторов."""
    selectors = [
        # Specific body selectors first (cleanest text)
        "div#article-body",
        "div[itemprop='articleBody']",
        "div.article-body", "div.article__body", "div.article-content",
        "div.post-content", "div.entry-content", "div.content-body",
        "div.story-body", "div.news-body", "div.text-body",
        "div.post__body", "div.article__content", "div.prose",
        "section.article-body", "div.content-article",
        # Broader selectors last
        "div[class*='article-body']", "div[class*='post-content']",
        "div[class*='articleBody']", "div[class*='entry-content']",
        "article", "main",
    ]
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            clone = BeautifulSoup(str(el), "lxml")
            for tag in clone.find_all(["script", "style", "nav", "footer", "header", "aside",
                                       "figure", "figcaption", "form", "button", "svg",
                                       "noscript", "iframe"]):
                tag.decompose()
            # Remove share/social/bookmark divs
            for div in clone.find_all(["div", "section"], class_=lambda c: c and any(
                    kw in " ".join(c).lower() for kw in ["share", "social", "bookmark", "comment",
                                                          "sidebar", "related", "newsletter", "promo", "ad-"])):
                div.decompose()
            text = clone.get_text(separator=" ", strip=True)[:5000]
            if len(text) >= 100:
                return text

    # Fallback: <body>
    if soup.body:
        clone = BeautifulSoup(str(soup.body), "lxml")
        for tag in clone.find_all(["script", "style", "nav", "footer", "header", "aside",
                                   "figure", "figcaption", "form", "button", "svg", "noscript", "iframe"]):
            tag.decompose()
        for div in clone.find_all(["div", "section"], class_=lambda c: c and any(
                kw in " ".join(c).lower() for kw in ["share", "social", "bookmark", "comment",
                                                      "sidebar", "related", "newsletter", "promo", "ad-"])):
            div.decompose()
        text = clone.get_text(separator=" ", strip=True)[:5000]
        if len(text) >= 100:
            return text

    return ""


def _fetch_article(url: str) -> tuple[str, str, str, str]:
    """Загружает статью и извлекает h1, description, plain_text, published_at."""
    try:
        resp = fetch_with_retry(url)
        soup = BeautifulSoup(resp.text, "lxml")

        h1 = ""
        h1_tag = soup.find("h1")
        if h1_tag:
            h1 = h1_tag.get_text(strip=True)

        description = ""
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if not meta_desc:
            meta_desc = soup.find("meta", attrs={"property": "og:description"})
        if meta_desc:
            description = meta_desc.get("content", "")

        published_at = _extract_publish_date(soup)

        # Умный поиск текста по множеству селекторов
        plain_text = _extract_body_text(soup)

        # Fallback: если text пуст, используем description
        if not plain_text and description:
            plain_text = description
            logger.debug("Text recovery for %s: using description (%d chars)", url, len(description))

        return h1, description, plain_text, published_at
    except Exception as e:
        logger.warning("Failed to fetch article %s: %s", url, e)
        return "", "", "", ""
