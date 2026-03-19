import logging
import os
from dotenv import load_dotenv

load_dotenv()


def _int_env(key: str, default: int) -> int:
    """Safe int parsing from env var with fallback on invalid values."""
    val = os.getenv(key, str(default))
    try:
        return int(val)
    except (ValueError, TypeError):
        logging.warning("Invalid value '%s' for %s, using default %d", val, key, default)
        return default

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
AUTO_APPROVE_THRESHOLD = _int_env("AUTO_APPROVE_THRESHOLD", 0)  # 0 = disabled, use pipeline buttons
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
TELEGRAM_NOTIFY_THRESHOLD = _int_env("TELEGRAM_NOTIFY_THRESHOLD", 70)

# VK API
VK_API_TOKEN = os.getenv("VK_API_TOKEN", "")
VK_API_VERSION = "5.199"

# Telegram Channel Parser (Telethon)
TELEGRAM_API_ID = os.getenv("TELEGRAM_API_ID", "")
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "")
TELEGRAM_SESSION = os.getenv("TELEGRAM_SESSION", "igronews_tg")

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
    {"name": "VGC",              "type": "rss",  "url": "https://www.videogameschronicle.com/feed",    "interval": 45},
    {"name": "Gematsu",          "type": "rss",  "url": "https://www.gematsu.com/feed",                "interval": 45},
    {"name": "TheGamer",         "type": "rss",  "url": "https://www.thegamer.com/feed/",              "interval": 45},
    {"name": "GamingBolt",       "type": "rss",  "url": "https://www.gamingbolt.com/feed",             "interval": 45},
    {"name": "StopGame",         "type": "html", "url": "https://stopgame.ru/news",                    "interval": 30, "selector": "a[href*='/newsdata/']", "title_selector": "", "url_pattern": r"/newsdata/\d+"},
    {"name": "Playground",       "type": "rss",  "url": "https://www.playground.ru/rss/news.xml",      "interval": 45},
    # HTML
    {"name": "DTF",              "type": "dtf",  "url": "https://dtf.ru/editorial",                     "interval": 30, "selector": "a[href*='/editorial/']", "title_selector": "", "url_pattern": r"/editorial/\d+"},
    {"name": "iXBT.games",       "type": "html", "url": "https://ixbt.games/news",                    "interval": 90, "selector": "a[href*='/news/']", "title_selector": "h3", "url_pattern": r"/news/\d{4}/\d{2}/\d{2}/"},
    {"name": "VGTimes",          "type": "html", "url": "https://vgtimes.ru/news/",                    "interval": 90, "selector": "a[href*='.html']", "title_selector": "", "url_pattern": r"/\d+-.*\.html"},
    # New RSS (from audit)
    {"name": "InsiderGaming",   "type": "rss",  "url": "https://insidergaming.com/feed",               "interval": 30},
    {"name": "MP1st",           "type": "rss",  "url": "https://mp1st.com/feed",                       "interval": 45},
    {"name": "GamesIndustry",   "type": "rss",  "url": "https://www.gamesindustry.biz/feed",           "interval": 90},
    {"name": "AutomatonWest",   "type": "rss",  "url": "https://automaton-media.com/en/feed/",         "interval": 90},
    # Telegram channels (web preview, no API needed)
    {"name": "TG:iXBT.games",   "type": "telegram", "channel": "ixbtgames",       "interval": 30},
    {"name": "TG:Игромания",    "type": "telegram", "channel": "igromania",       "interval": 30},
    {"name": "TG:StopGame",     "type": "telegram", "channel": "stopgamenews",    "interval": 30},
    {"name": "TG:Playground",   "type": "telegram", "channel": "playground_ru",   "interval": 45},
    # Bluesky — game devs & journalists (free API, no auth)
    {"name": "BS:Schreier",     "type": "bluesky", "handle": "jasonschreier.bsky.social",                       "interval": 15},
    {"name": "BS:Kojima",       "type": "bluesky", "handle": "hideokojimaen.bsky.kojimaproductions.jp",         "interval": 15},
    {"name": "BS:O'Dwyer",      "type": "bluesky", "handle": "dannyodwyer.bsky.social",                         "interval": 15},
    {"name": "BS:Keighley",     "type": "bluesky", "handle": "geoffkeighley.bsky.social",                       "interval": 90},
    {"name": "BS:EdBoon",       "type": "bluesky", "handle": "noobde.bsky.social",                              "interval": 90},
    {"name": "BS:SamLake",      "type": "bluesky", "handle": "samlakewrites.bsky.social",                       "interval": 90},
    {"name": "BS:Druckmann",    "type": "bluesky", "handle": "druckmann.bsky.social",                           "interval": 90},
    {"name": "BS:Barlog",       "type": "bluesky", "handle": "corybarlog.bsky.social",                          "interval": 90},
    # VK studios (needs VK_API_TOKEN env var)
    {"name": "VK:CDPR",         "type": "vk", "group_id": "20733433",  "interval": 45},
    {"name": "VK:Kojima",       "type": "vk", "group_id": "200465049", "interval": 45},
    {"name": "VK:PlayStation",   "type": "vk", "group_id": "26006257",  "interval": 45},
    {"name": "VK:Xbox",         "type": "vk", "group_id": "48194892",  "interval": 45},
    {"name": "VK:Nintendo",     "type": "vk", "group_id": "115527361", "interval": 45},
    {"name": "VK:Ubisoft",      "type": "vk", "group_id": "41600377",  "interval": 45},
    {"name": "VK:Bethesda",     "type": "vk", "group_id": "167356678", "interval": 45},
    {"name": "VK:Blizzard",     "type": "vk", "group_id": "168409583", "interval": 45},
    {"name": "VK:GSCGameWorld", "type": "vk", "group_id": "172971040", "interval": 45},
    {"name": "VK:Larian",       "type": "vk", "group_id": "38521692",  "interval": 45},
    {"name": "VK:FromSoftware", "type": "vk", "group_id": "76472116",  "interval": 90},
    {"name": "VK:Capcom",       "type": "vk", "group_id": "79177321",  "interval": 90},
    {"name": "VK:Remedy",       "type": "vk", "group_id": "40233595",  "interval": 90},
    {"name": "VK:THQNordic",    "type": "vk", "group_id": "130058778", "interval": 90},
    {"name": "VK:Insomniac",    "type": "vk", "group_id": "96982181",  "interval": 90},
    {"name": "VK:Guerrilla",    "type": "vk", "group_id": "48993447",  "interval": 90},
    {"name": "VK:BioWare",      "type": "vk", "group_id": "31633679",  "interval": 90},
    {"name": "VK:Obsidian",     "type": "vk", "group_id": "169470430", "interval": 90},
    {"name": "VK:QuanticDream", "type": "vk", "group_id": "182276271", "interval": 90},
    {"name": "VK:WBGames",      "type": "vk", "group_id": "192444559", "interval": 90},
    {"name": "VK:Activision",   "type": "vk", "group_id": "170689275", "interval": 90},
    {"name": "VK:SquareEnix",   "type": "vk", "group_id": "10707369",  "interval": 90},
    {"name": "VK:RedBarrels",   "type": "vk", "group_id": "71125581",  "interval": 90},
    {"name": "VK:Frictional",   "type": "vk", "group_id": "22934002",  "interval": 90},
    {"name": "VK:Rocksteady",   "type": "vk", "group_id": "42028449",  "interval": 90},
    {"name": "VK:EA",           "type": "vk", "group_id": "55270284",  "interval": 90},
    {"name": "VK:Konami",       "type": "vk", "group_id": "106520244", "interval": 90},
    {"name": "VK:2K",           "type": "vk", "group_id": "206575709", "interval": 90},
    {"name": "VK:SEGA",         "type": "vk", "group_id": "192435008", "interval": 90},
    {"name": "VK:InfinityWard", "type": "vk", "group_id": "27384116",  "interval": 90},
]
