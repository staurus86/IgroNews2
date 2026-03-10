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


def _safe_json_loads(val, default):
    """Безопасный json.loads: обрабатывает None, пустые строки, уже-распарсенные объекты."""
    if val is None or val == "":
        return default
    if isinstance(val, (dict, list)):
        return val
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return default


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

        bigrams = _safe_json_loads(analysis.get("bigrams"), [])
        bigrams_str = ", ".join(
            b[0] if isinstance(b, list) else str(b)
            for b in bigrams
        )

        keyso_data = _safe_json_loads(analysis.get("keyso_data"), {})
        trends_data = _safe_json_loads(analysis.get("trends_data"), {})

        row = [
            news.get("parsed_at", "") or "",                    # A: Дата парсинга
            news.get("url", "") or "",                          # B: Источник URL
            news.get("title", "") or "",                        # C: Title
            news.get("h1", "") or "",                           # D: H1
            news.get("description", "") or "",                  # E: Description
            bigrams_str,                                        # F: Топ биграммы
            str(keyso_data.get("freq", "")),                    # G: Частота Keys.so
            str(trends_data.get("RU", "")),                     # H: Trends RU
            str(trends_data.get("US", "")),                     # I: Trends US
            analysis.get("llm_recommendation") or "",           # J: Рекомендация LLM
            analysis.get("llm_trend_forecast") or "",           # K: Прогноз трендовости
            ", ".join(keyso_data.get("similar", [])) if isinstance(keyso_data.get("similar"), list) else str(keyso_data.get("similar", "")),  # L: Похожие запросы
            analysis.get("llm_merged_with") or "",              # M: Объединить с
            news.get("status", "new") or "new",                 # N: Статус
            (news.get("plain_text") or "")[:1000],              # O: Plain Text
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
    "Вирал триггеры", "Keys.so частота", "Тренды RU", "LLM рекомендация",
    "Прогноз", "URL оригинала",
]

HEADERS_NOT_READY = [
    "Дата", "Источник", "Заголовок", "Скор", "Качество", "Релевантность",
    "Свежесть (ч)", "Вирал", "Вирал триггеры", "Тон", "Теги", "Entities",
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


def _format_viral_triggers(analysis: dict | None) -> str:
    """Форматирует виральные триггеры из news_analysis для Sheets."""
    if not analysis:
        return ""
    try:
        triggers_raw = analysis.get("viral_data", "")
        if not triggers_raw:
            return ""
        triggers = json.loads(triggers_raw) if isinstance(triggers_raw, str) else triggers_raw
        if not isinstance(triggers, list):
            return ""
        return ", ".join(f"{t.get('label', '?')}({t.get('weight', 0)})" for t in triggers[:10])
    except Exception:
        return ""


def _format_viral_triggers_from_checks(checks: dict) -> str:
    """Форматирует виральные триггеры из check_results для Sheets."""
    viral = checks.get("viral", {})
    triggers = viral.get("triggers", [])
    if not triggers:
        return ""
    return ", ".join(f"{t.get('label', '?')}({t.get('weight', 0)})" for t in triggers[:10])


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
            existing = ws.col_values(16)  # column P = URL (shifted by viral triggers col)
            if news_url in existing:
                logger.info("Skipped duplicate in Ready: %s", news_url[:80])
                return -1

        keyso_data = _safe_json_loads(analysis.get("keyso_data") if analysis else None, {})
        trends_data = _safe_json_loads(analysis.get("trends_data") if analysis else None, {})

        tags = ""
        try:
            rewrite_tags = rewrite.get("tags", [])
            if rewrite_tags and isinstance(rewrite_tags, list):
                tags = ", ".join(rewrite_tags)
            elif analysis:
                tags_list = _safe_json_loads(analysis.get("tags"), [])
                tags = ", ".join(tags_list) if isinstance(tags_list, list) else str(tags_list)
        except Exception:
            pass

        row = [
            news.get("parsed_at", "") or "",                         # A: Дата
            news.get("source", "") or "",                            # B: Источник
            news.get("title", "") or "",                             # C: Оригинал заголовок
            rewrite.get("title", "") or "",                          # D: Рерайт заголовок
            (rewrite.get("text", "") or "")[:5000],                  # E: Рерайт текст
            rewrite.get("seo_title", rewrite.get("meta_title", "")) or "",  # F: Meta Title
            rewrite.get("seo_description", rewrite.get("meta_description", "")) or "",  # G: Meta Description
            tags,                                                    # H: Теги
            str(analysis.get("total_score", "") if analysis else ""),  # I: Скор
            str(analysis.get("viral_score", "") if analysis else ""),  # J: Вирал
            _format_viral_triggers(analysis),                        # K: Вирал триггеры
            str(keyso_data.get("freq", "")),                         # L: Keys.so частота
            str(trends_data.get("RU", "")),                          # M: Тренды RU
            (analysis.get("llm_recommendation") or "") if analysis else "",  # N: LLM рекомендация
            (analysis.get("llm_trend_forecast") or "") if analysis else "",  # O: Прогноз
            news.get("url", "") or "",                               # P: URL оригинала
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
            existing = ws.col_values(15)  # column O = URL (shifted by viral triggers col)
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
            _format_viral_triggers_from_checks(checks),                # I: Вирал триггеры
            check_results.get("sentiment", {}).get("label", "neutral"),  # J: Тон
            tags_str,                                                  # K: Теги
            entities_str,                                              # L: Entities
            str(check_results.get("headline", {}).get("score", 0)),    # M: Headline скор
            str(check_results.get("momentum", {}).get("score", 0)),    # N: Momentum
            news.get("url", ""),                                       # O: URL
            (news.get("description", "") or "")[:500],                 # P: Описание
        ]

        ws.append_row(row, value_input_option="USER_ENTERED", table_range="A1")
        row_num = len(ws.col_values(1))
        logger.info("Written to NotReady row %d: %s", row_num, news.get("title", "")[:50])
        return row_num
    except Exception as e:
        logger.error("NotReady sheet write error: %s", e)
        return None
