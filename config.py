import os
from dotenv import load_dotenv

load_dotenv()

# API Keys
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
KEYSO_API_KEY = os.getenv("KEYSO_API_KEY", "")

# Google Sheets
GOOGLE_SHEETS_ID = os.getenv("GOOGLE_SHEETS_ID", "")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
SHEETS_TAB = "Новости"

# Database
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///news.db")

# LLM
LLM_MODEL = "gpt-4o-mini"

# Google Trends regions
REGIONS = ["RU", "US", "GB", "DE"]

# Sources — flat list, scheduler distributes by interval
SOURCES = [
    # RSS
    {"name": "IGN",              "type": "rss",  "url": "https://feeds.ign.com/ign/all",               "interval": 5},
    {"name": "GameSpot",         "type": "rss",  "url": "https://www.gamespot.com/feeds/mashup/",      "interval": 10},
    {"name": "PCGamer",          "type": "rss",  "url": "https://www.pcgamer.com/rss/",                "interval": 10},
    {"name": "Eurogamer",        "type": "rss",  "url": "https://www.eurogamer.net/?format=rss",       "interval": 15},
    {"name": "GamesRadar",       "type": "rss",  "url": "https://www.gamesradar.com/feeds/tag/games",  "interval": 15},
    {"name": "Polygon",          "type": "rss",  "url": "https://www.polygon.com/rss/index.xml",       "interval": 15},
    {"name": "RockPaperShotgun", "type": "rss",  "url": "https://feeds.feedburner.com/RockPaperShotgun", "interval": 30},
    {"name": "GameRant",         "type": "rss",  "url": "https://gamerant.com/feed/",                  "interval": 10},
    {"name": "StopGame",         "type": "rss",  "url": "https://stopgame.ru/rss/news.xml",            "interval": 10},
    {"name": "Cybersport",       "type": "rss",  "url": "https://cyber.sports.ru/rss/news.xml",        "interval": 15},
    {"name": "Playground",       "type": "rss",  "url": "https://www.playground.ru/rss/news.xml",      "interval": 15},
    # HTML
    {"name": "Metacritic",       "type": "html", "url": "https://www.metacritic.com/news/",            "interval": 30, "selector": "article"},
    {"name": "DTF",              "type": "html", "url": "https://dtf.ru/games",                        "interval": 5,  "selector": ".content-list__item"},
    {"name": "iXBT.games",       "type": "html", "url": "https://www.ixbt.com/live/games/",            "interval": 30, "selector": ".item-live"},
    {"name": "VGTimes",          "type": "html", "url": "https://vgtimes.ru/news/",                    "interval": 30, "selector": ".news-item"},
]

# Keys.so
KEYSO_BASE_URL = "https://api.keys.so"
KEYSO_REGION = "msk"
