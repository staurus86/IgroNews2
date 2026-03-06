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
