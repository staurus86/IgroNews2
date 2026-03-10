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

# Keys.so
KEYSO_REGION = os.getenv("KEYSO_REGION", "ru")
KEYSO_BASE_URL = "https://api.keys.so/api/v2"

# Google Trends regions
REGIONS = ["RU", "US", "GB", "DE"]

# Sources — flat list, scheduler distributes by interval
SOURCES = [
    # RSS
    {"name": "IGN",              "type": "rss",  "url": "https://feeds.ign.com/ign/all",               "interval": 5},
    {"name": "GameSpot",         "type": "rss",  "url": "https://www.gamespot.com/feeds/mashup/",      "interval": 10},
    {"name": "PCGamer",          "type": "rss",  "url": "https://www.pcgamer.com/rss/",                "interval": 10},
    {"name": "Eurogamer",        "type": "rss",  "url": "https://www.eurogamer.net/?format=rss",       "interval": 15},  # может блокировать Cloudflare
    {"name": "Kotaku",            "type": "rss",  "url": "https://kotaku.com/rss",                      "interval": 15},
    {"name": "GamesRadar",       "type": "rss",  "url": "https://www.gamesradar.com/rss/",              "interval": 15},
    {"name": "Polygon",          "type": "rss",  "url": "https://www.polygon.com/rss/index.xml",       "interval": 15},  # может блокировать Cloudflare
    {"name": "Destructoid",      "type": "rss",  "url": "https://www.destructoid.com/feed/",            "interval": 15},
    {"name": "RockPaperShotgun", "type": "rss",  "url": "https://feeds.feedburner.com/RockPaperShotgun", "interval": 30},
    {"name": "GameRant",         "type": "rss",  "url": "https://gamerant.com/feed/",                  "interval": 10},
    {"name": "StopGame",         "type": "html", "url": "https://stopgame.ru/news",                    "interval": 10, "selector": "a[href*='/newsdata/']", "title_selector": "", "url_pattern": r"/newsdata/\d+"},
    {"name": "Playground",       "type": "rss",  "url": "https://www.playground.ru/rss/news.xml",      "interval": 15},
    # HTML
    {"name": "DTF",              "type": "html", "url": "https://dtf.ru/games",                        "interval": 10, "selector": "a[href*='/games/']", "title_selector": "", "url_pattern": r"/games/\d+"},
    {"name": "iXBT.games",       "type": "html", "url": "https://ixbt.games/news",                    "interval": 30, "selector": "a[href*='/news/']", "title_selector": "h3", "url_pattern": r"/news/\d{4}/\d{2}/\d{2}/"},
    {"name": "VGTimes",          "type": "html", "url": "https://vgtimes.ru/news/",                    "interval": 30, "selector": "a[href*='.html']", "title_selector": "", "url_pattern": r"/\d+-.*\.html"},
]
