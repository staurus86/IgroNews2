import os
from dotenv import load_dotenv

load_dotenv()

# API Keys
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://openrouter.ai/api/v1")
KEYSO_API_KEY = os.getenv("KEYSO_API_KEY", "")

# Google Sheets
GOOGLE_SHEETS_ID = os.getenv("GOOGLE_SHEETS_ID", "")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
SHEETS_TAB = "Лист1"
SHEETS_TAB_READY = os.getenv("SHEETS_TAB_READY", "Ready")
SHEETS_TAB_NOT_READY = os.getenv("SHEETS_TAB_NOT_READY", "NotReady")

# Database
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///news.db")

# LLM
LLM_MODEL = os.getenv("LLM_MODEL", "openai/gpt-4o-mini")

# Automation thresholds
AUTO_APPROVE_THRESHOLD = int(os.getenv("AUTO_APPROVE_THRESHOLD", "0"))  # 0 = disabled, use pipeline buttons
AUTO_REWRITE_ON_PUBLISH_NOW = os.getenv("AUTO_REWRITE_ON_PUBLISH_NOW", "true").lower() == "true"
AUTO_REWRITE_STYLE = os.getenv("AUTO_REWRITE_STYLE", "news")

# Proxy & User-Agent rotation
PROXY_LIST = os.getenv("PROXY_LIST", "")
USER_AGENT_ROTATE = os.getenv("USER_AGENT_ROTATE", "true").lower() == "true"

# Keys.so
KEYSO_REGION = os.getenv("KEYSO_REGION", "ru")
KEYSO_BASE_URL = "https://api.keys.so/api/v2"

# Русскоязычные источники (Keys.so region=ru, остальные — us)
RU_SOURCES = {"StopGame", "DTF", "Playground", "iXBT.games", "VGTimes"}

def keyso_region_for_source(source: str) -> str:
    """Возвращает регион Keys.so по источнику."""
    return "ru" if source in RU_SOURCES else "us"

# Google Trends regions
REGIONS = ["RU", "US", "GB", "DE"]

# Telegram Bot
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_IDS = os.getenv("TELEGRAM_CHAT_IDS", "")  # comma-separated authorized chat IDs
TELEGRAM_NOTIFY_THRESHOLD = int(os.getenv("TELEGRAM_NOTIFY_THRESHOLD", "70"))

# Sources — flat list, scheduler distributes by interval
SOURCES = [
    # RSS
    {"name": "IGN",              "type": "rss",  "url": "https://feeds.ign.com/ign/all",               "interval": 15},
    {"name": "GameSpot",         "type": "rss",  "url": "https://www.gamespot.com/feeds/mashup/",      "interval": 30},
    {"name": "PCGamer",          "type": "rss",  "url": "https://www.pcgamer.com/rss/",                "interval": 30},
    {"name": "Eurogamer",        "type": "rss",  "url": "https://www.eurogamer.net/?format=rss",       "interval": 45},  # может блокировать Cloudflare
    {"name": "Kotaku",            "type": "homepage",  "url": "https://kotaku.com/latest",              "interval": 45, "rss_url": "https://kotaku.com/rss"},
    {"name": "GamesRadar",       "type": "gamesradar",  "url": "https://www.gamesradar.com/",           "interval": 45, "rss_url": "https://www.gamesradar.com/rss/"},
    {"name": "Polygon",          "type": "homepage",  "url": "https://www.polygon.com/",                "interval": 45, "rss_url": "https://www.polygon.com/rss/index.xml"},
    {"name": "Destructoid",      "type": "rss",  "url": "https://www.destructoid.com/feed/",            "interval": 45},
    {"name": "RockPaperShotgun", "type": "homepage",  "url": "https://www.rockpapershotgun.com/news", "interval": 90, "rss_url": "https://feeds.feedburner.com/RockPaperShotgun"},
    {"name": "GameRant",         "type": "rss",  "url": "https://gamerant.com/feed/",                  "interval": 30},
    {"name": "StopGame",         "type": "html", "url": "https://stopgame.ru/news",                    "interval": 30, "selector": "a[href*='/newsdata/']", "title_selector": "", "url_pattern": r"/newsdata/\d+"},
    {"name": "Playground",       "type": "rss",  "url": "https://www.playground.ru/rss/news.xml",      "interval": 45},
    # HTML
    {"name": "DTF",              "type": "dtf",  "url": "https://dtf.ru/games",                        "interval": 30, "selector": "a[href*='/games/']", "title_selector": "", "url_pattern": r"/games/\d+"},
    {"name": "iXBT.games",       "type": "html", "url": "https://ixbt.games/news",                    "interval": 90, "selector": "a[href*='/news/']", "title_selector": "h3", "url_pattern": r"/news/\d{4}/\d{2}/\d{2}/"},
    {"name": "VGTimes",          "type": "html", "url": "https://vgtimes.ru/news/",                    "interval": 90, "selector": "a[href*='.html']", "title_selector": "", "url_pattern": r"/\d+-.*\.html"},
]
