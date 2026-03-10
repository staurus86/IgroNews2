import base64
import json
import logging
import tempfile
import os

import gspread
from google.oauth2.service_account import Credentials

import config

logger = logging.getLogger(__name__)

_client = None


def _get_client():
    global _client
    if _client is not None:
        return _client

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    sa_json = config.GOOGLE_SERVICE_ACCOUNT_JSON
    if not sa_json:
        logger.error("GOOGLE_SERVICE_ACCOUNT_JSON not set")
        return None

    # Если передан base64 — декодируем во временный файл
    try:
        decoded = base64.b64decode(sa_json)
        creds_data = json.loads(decoded)
    except Exception:
        # Возможно передан путь к файлу
        if os.path.exists(sa_json):
            with open(sa_json) as f:
                creds_data = json.load(f)
        else:
            creds_data = json.loads(sa_json)

    creds = Credentials.from_service_account_info(creds_data, scopes=scopes)
    _client = gspread.authorize(creds)
    return _client


def write_news_row(news: dict, analysis: dict) -> int | None:
    """Записывает строку новости в Google Sheets. Возвращает номер строки."""
    client = _get_client()
    if not client:
        return None

    try:
        sheet = client.open_by_key(config.GOOGLE_SHEETS_ID).worksheet(config.SHEETS_TAB)

        # Дедупликация: проверяем, есть ли уже этот URL в таблице (колонка B)
        news_url = news.get("url", "")
        if news_url:
            existing_urls = sheet.col_values(2)  # колонка B = URL
            if news_url in existing_urls:
                logger.info("Skipped duplicate in Sheets: %s", news_url[:80])
                return -1  # уже есть, пропускаем

        bigrams_str = ", ".join(
            b[0] if isinstance(b, list) else b
            for b in json.loads(analysis.get("bigrams", "[]"))
        )

        keyso_data = json.loads(analysis.get("keyso_data", "{}"))
        trends_data = json.loads(analysis.get("trends_data", "{}"))

        row = [
            news.get("parsed_at", ""),                          # A: Дата парсинга
            news.get("url", ""),                                # B: Источник URL
            news.get("title", ""),                              # C: Title
            news.get("h1", ""),                                 # D: H1
            news.get("description", ""),                        # E: Description
            bigrams_str,                                        # F: Топ биграммы
            str(keyso_data.get("freq", "")),                    # G: Частота Keys.so
            str(trends_data.get("RU", "")),                     # H: Trends RU
            str(trends_data.get("US", "")),                     # I: Trends US
            analysis.get("llm_recommendation", ""),             # J: Рекомендация LLM
            analysis.get("llm_trend_forecast", ""),             # K: Прогноз трендовости
            ", ".join(keyso_data.get("similar", [])) if isinstance(keyso_data.get("similar"), list) else str(keyso_data.get("similar", "")),  # L: Похожие запросы
            analysis.get("llm_merged_with", ""),                # M: Объединить с
            news.get("status", "new"),                          # N: Статус
            news.get("plain_text", "")[:1000],                  # O: Plain Text
        ]

        sheet.append_row(row, value_input_option="USER_ENTERED", table_range="A1")
        row_num = len(sheet.col_values(1))
        logger.info("Written to Sheets row %d: %s", row_num, news.get("title", "")[:50])
        return row_num

    except Exception as e:
        logger.error("Sheets write error: %s", e)
        return None


HEADERS = [
    "Дата парсинга", "Источник (URL)", "Title", "H1", "Description",
    "Топ биграммы", "Частота (Keys.so)", "Google Trends RU",
    "Google Trends US", "Рекомендация LLM", "Прогноз трендовости",
    "Похожие запросы Keys.so", "Объединить с", "Статус", "Plain Text",
]

HEADERS_READY = [
    "Дата", "Источник", "Оригинал заголовок", "Рерайт заголовок", "Рерайт текст",
    "Meta Title", "Meta Description", "Теги", "Скор", "Вирал",
    "Keys.so частота", "Тренды RU", "LLM рекомендация", "Прогноз", "URL оригинала",
]

HEADERS_NOT_READY = [
    "Дата", "Источник", "Заголовок", "Скор", "Качество", "Релевантность",
    "Свежесть (ч)", "Вирал", "Тон", "Теги", "Entities",
    "Headline скор", "Momentum", "URL", "Описание",
]


def setup_headers():
    """Создаёт заголовки в первой строке, если их нет."""
    client = _get_client()
    if not client:
        return

    try:
        sheet = client.open_by_key(config.GOOGLE_SHEETS_ID).worksheet(config.SHEETS_TAB)
        first_row = sheet.row_values(1)
        if first_row and first_row[0] == HEADERS[0]:
            return  # headers already exist

        # Insert headers as row 1
        sheet.insert_row(HEADERS, index=1, value_input_option="USER_ENTERED")
        logger.info("Sheet headers created")
    except Exception as e:
        logger.error("Failed to setup sheet headers: %s", e)


def _get_or_create_worksheet(tab_name: str, headers: list):
    """Получает или создаёт вкладку с заголовками."""
    client = _get_client()
    if not client:
        return None
    try:
        spreadsheet = client.open_by_key(config.GOOGLE_SHEETS_ID)
        try:
            ws = spreadsheet.worksheet(tab_name)
        except gspread.exceptions.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title=tab_name, rows=1000, cols=len(headers))
            ws.insert_row(headers, index=1, value_input_option="USER_ENTERED")
            logger.info("Created worksheet '%s' with headers", tab_name)
            return ws

        # Check headers exist
        first_row = ws.row_values(1)
        if not first_row or first_row[0] != headers[0]:
            ws.insert_row(headers, index=1, value_input_option="USER_ENTERED")
        return ws
    except Exception as e:
        logger.error("Failed to get/create worksheet '%s': %s", tab_name, e)
        return None


def write_ready_row(news: dict, analysis: dict, rewrite: dict) -> int | None:
    """Записывает готовую переписанную новость в вкладку Ready."""
    tab_name = getattr(config, "SHEETS_TAB_READY", "Ready")
    ws = _get_or_create_worksheet(tab_name, HEADERS_READY)
    if not ws:
        return None

    try:
        # Dedup by URL
        news_url = news.get("url", "")
        if news_url:
            existing = ws.col_values(15)  # column O = URL
            if news_url in existing:
                logger.info("Skipped duplicate in Ready: %s", news_url[:80])
                return -1

        keyso_data = {}
        trends_data = {}
        if analysis:
            try:
                keyso_data = json.loads(analysis.get("keyso_data", "{}"))
            except Exception:
                pass
            try:
                trends_data = json.loads(analysis.get("trends_data", "{}"))
            except Exception:
                pass

        tags = ""
        try:
            tags_list = json.loads(analysis.get("tags", "[]")) if analysis else []
            tags = ", ".join(tags_list) if isinstance(tags_list, list) else str(tags_list)
        except Exception:
            pass

        row = [
            news.get("parsed_at", ""),                               # A: Дата
            news.get("source", ""),                                  # B: Источник
            news.get("title", ""),                                   # C: Оригинал заголовок
            rewrite.get("title", ""),                                # D: Рерайт заголовок
            rewrite.get("text", "")[:5000],                          # E: Рерайт текст
            rewrite.get("meta_title", ""),                           # F: Meta Title
            rewrite.get("meta_description", ""),                     # G: Meta Description
            tags,                                                    # H: Теги
            str(analysis.get("total_score", "") if analysis else ""),  # I: Скор
            str(analysis.get("viral_score", "") if analysis else ""),  # J: Вирал
            str(keyso_data.get("freq", "")),                         # K: Keys.so частота
            str(trends_data.get("RU", "")),                          # L: Тренды RU
            analysis.get("llm_recommendation", "") if analysis else "",  # M: LLM рекомендация
            analysis.get("llm_trend_forecast", "") if analysis else "",  # N: Прогноз
            news.get("url", ""),                                     # O: URL оригинала
        ]

        ws.append_row(row, value_input_option="USER_ENTERED", table_range="A1")
        row_num = len(ws.col_values(1))
        logger.info("Written to Ready row %d: %s", row_num, news.get("title", "")[:50])
        return row_num
    except Exception as e:
        logger.error("Ready sheet write error: %s", e)
        return None


def write_not_ready_row(news: dict, check_results: dict) -> int | None:
    """Записывает новость с локальным анализом в вкладку NotReady."""
    tab_name = getattr(config, "SHEETS_TAB_NOT_READY", "NotReady")
    ws = _get_or_create_worksheet(tab_name, HEADERS_NOT_READY)
    if not ws:
        return None

    try:
        # Dedup by URL
        news_url = news.get("url", "")
        if news_url:
            existing = ws.col_values(14)  # column N = URL
            if news_url in existing:
                logger.info("Skipped duplicate in NotReady: %s", news_url[:80])
                return -1

        checks = check_results.get("checks", {})
        tags = check_results.get("tags", [])
        tags_str = ", ".join(tags) if isinstance(tags, list) else str(tags)

        entities = check_results.get("game_entities", [])
        ent_names = []
        for e in (entities or []):
            if isinstance(e, dict):
                ent_names.append(e.get("name", ""))
            elif isinstance(e, str):
                ent_names.append(e)
        entities_str = ", ".join(ent_names[:10])

        freshness = checks.get("freshness", {})
        age_h = freshness.get("age_hours", -1)
        age_str = f"{age_h:.1f}" if isinstance(age_h, (int, float)) and age_h >= 0 else "?"

        row = [
            news.get("parsed_at", ""),                                 # A: Дата
            news.get("source", ""),                                    # B: Источник
            news.get("title", ""),                                     # C: Заголовок
            str(check_results.get("total_score", 0)),                  # D: Скор
            str(checks.get("quality", {}).get("score", 0)),            # E: Качество
            str(checks.get("relevance", {}).get("score", 0)),          # F: Релевантность
            age_str,                                                   # G: Свежесть (ч)
            str(checks.get("viral", {}).get("score", 0)),              # H: Вирал
            check_results.get("sentiment", {}).get("label", "neutral"),  # I: Тон
            tags_str,                                                  # J: Теги
            entities_str,                                              # K: Entities
            str(check_results.get("headline", {}).get("score", 0)),    # L: Headline скор
            str(check_results.get("momentum", {}).get("score", 0)),    # M: Momentum
            news.get("url", ""),                                       # N: URL
            (news.get("description", "") or "")[:500],                 # O: Описание
        ]

        ws.append_row(row, value_input_option="USER_ENTERED", table_range="A1")
        row_num = len(ws.col_values(1))
        logger.info("Written to NotReady row %d: %s", row_num, news.get("title", "")[:50])
        return row_num
    except Exception as e:
        logger.error("NotReady sheet write error: %s", e)
        return None
