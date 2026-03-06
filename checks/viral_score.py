from datetime import datetime, timezone
from typing import Optional

VIRAL_TRIGGERS = {
    "scandal_layoffs": {
        "label": "Layoffs",
        "weight": 40,
        "keywords": [
            "уволили", "увольнения", "сокращения", "layoff", "layoffs",
            "закрыли студию", "студия закрыта", "распустили команду",
            "массовые увольнения", "fired", "studio closed", "studio shutdown",
            "job cuts", "redundancies", "team disbanded",
        ],
    },
    "scandal_devs_vs_players": {
        "label": "Dev vs Players",
        "weight": 45,
        "keywords": [
            "игроки против", "бойкот", "игроки недовольны", "возмущение",
            "петиция", "скандал", "backlash", "outrage", "community angry",
            "boycott", "petition", "controversy", "players furious",
        ],
    },
    "scandal_publisher": {
        "label": "Publisher drama",
        "weight": 35,
        "keywords": [
            "монетизация", "микротранзакции", "pay-to-win",
            "microtransactions", "monetization", "drm", "always online",
            "убрали из продажи", "цензура", "removed from sale",
        ],
    },
    "leak_major": {
        "label": "Leak",
        "weight": 50,
        "keywords": [
            "утечка", "слив", "слитый трейлер", "инсайдер",
            "leak", "leaked", "insider", "rumor", "rumour",
            "datamine", "datamined", "anonymous source",
        ],
    },
    "shadow_drop": {
        "label": "Shadow Drop",
        "weight": 55,
        "keywords": [
            "вышла неожиданно", "внезапный релиз", "доступна прямо сейчас",
            "shadow drop", "shadow dropped", "available now", "out now",
            "surprise release", "stealth release",
        ],
    },
    "bad_launch_technical": {
        "label": "Bad launch",
        "weight": 45,
        "keywords": [
            "баги", "вылеты", "неоптимизировано", "критические ошибки",
            "bugs", "crashes", "unoptimized", "broken", "refund",
            "performance issues", "disaster launch",
        ],
    },
    "bad_launch_reviews": {
        "label": "Bad reviews",
        "weight": 40,
        "keywords": [
            "провал", "разочарование", "низкие оценки",
            "flop", "disappointing", "mixed reviews", "negative reviews",
            "overwhelmingly negative", "mostly negative",
        ],
    },
    "ai_controversy": {
        "label": "AI controversy",
        "weight": 40,
        "keywords": [
            "нейросеть в игре", "ии арт", "ai generated", "ai art",
            "ai replaced", "ai backlash", "generative ai",
        ],
    },
    "major_event": {
        "label": "Major event",
        "weight": 35,
        "keywords": [
            "the game awards", "nintendo direct", "xbox showcase",
            "playstation showcase", "state of play", "summer game fest",
            "gamescom", "tokyo game show", "tgs", "e3", "gdc",
        ],
    },
    "sequel": {
        "label": "Sequel",
        "weight": 20,
        "keywords": ["продолжение", "сиквел", "sequel", "part 2", "new installment"],
    },
    "free_content": {
        "label": "Free/Giveaway",
        "weight": 20,
        "keywords": ["бесплатно", "раздача", "free to play", "f2p", "giveaway", "free weekend"],
    },
    "delay": {
        "label": "Delay",
        "weight": 15,
        "keywords": ["перенос", "отложили", "delayed", "postponed", "pushed back"],
    },
    "canceled": {
        "label": "Canceled",
        "weight": 50,
        "keywords": ["отменили", "отмена", "canceled", "cancelled", "игра отменена"],
    },
    "award": {
        "label": "Award/GOTY",
        "weight": 10,
        "keywords": ["goty", "лучшая игра", "игра года", "game of the year", "award"],
    },
    "next_gen": {
        "label": "Next-gen",
        "weight": 25,
        "keywords": [
            "следующего поколения", "нового поколения", "next-gen", "next gen",
            "новая консоль", "ps6", "playstation 6", "xbox next",
        ],
    },
    "big_update": {
        "label": "Big update",
        "weight": 15,
        "keywords": [
            "крупное обновление", "масштабное обновление", "большой патч",
            "major update", "big update", "massive update", "season pass",
            "новый сезон", "new season", "expansion", "дополнение",
            "dlc", "технологичнее", "обновился",
        ],
    },
    "release_date": {
        "label": "Release date",
        "weight": 20,
        "keywords": [
            "дата выхода", "release date", "дата релиза", "выходит",
            "релиз состоится", "launches", "coming soon", "выйдет",
        ],
    },
    "trailer": {
        "label": "Trailer",
        "weight": 15,
        "keywords": [
            "трейлер", "trailer", "тизер", "teaser", "геймплей",
            "gameplay", "первый взгляд", "first look", "показали",
        ],
    },
    "record": {
        "label": "Record",
        "weight": 30,
        "keywords": [
            "рекорд", "record", "побил рекорд", "миллион игроков",
            "million players", "peak players", "пик онлайна",
            "best-selling", "самая продаваемая",
        ],
    },
    "digest": {
        "label": "Digest",
        "weight": 5,
        "keywords": [
            "самое интересное", "дайджест", "итоги дня", "итоги недели",
            "обзор новостей", "digest", "weekly roundup", "recap",
        ],
    },
}

BIG_TITLES = [
    "gta 6", "grand theft auto", "elder scrolls 6", "half-life 3",
    "call of duty", "zelda", "pokemon", "mario", "batman",
    "spider-man", "god of war", "horizon", "red dead",
    "escape from tarkov", "tarkov", "cyberpunk", "elden ring",
    "baldur's gate", "starfield", "diablo", "world of warcraft",
    "final fantasy", "resident evil", "fortnite", "minecraft",
    "the witcher", "mass effect", "dragon age", "halo",
    "xbox", "playstation", "nintendo", "steam deck",
]

GAMING_EVENTS_CALENDAR = [
    (3, 18, 22, "GDC", 20),
    (6, 1, 15, "Summer Game Fest", 30),
    (8, 20, 28, "Gamescom", 25),
    (9, 1, 30, "Tokyo Game Show", 20),
    (12, 5, 10, "The Game Awards", 40),
]


def get_calendar_boost(dt: Optional[datetime] = None) -> tuple[int, str]:
    if dt is None:
        dt = datetime.now(timezone.utc)
    for month, day_start, day_end, name, boost in GAMING_EVENTS_CALENDAR:
        if dt.month == month and day_start <= dt.day <= day_end:
            return boost, name
    return 0, ""


def viral_score(news: dict) -> dict:
    title = news.get("title", "").lower()
    text = (title + " " + news.get("plain_text", "")).lower()

    score = 0
    triggered = []

    for trigger_id, trigger in VIRAL_TRIGGERS.items():
        if any(kw in text for kw in trigger["keywords"]):
            score += trigger["weight"]
            triggered.append({
                "id": trigger_id,
                "label": trigger["label"],
                "weight": trigger["weight"],
            })

    # Big title bonus
    has_big_title = any(t in text for t in BIG_TITLES)
    if has_big_title:
        score += 15
        matched = [t for t in BIG_TITLES if t in text]
        triggered.append({"id": "big_title", "label": f"Big title: {matched[0] if matched else '?'}", "weight": 15})

    # Big title + leak combo
    has_leak = any(kw in text for kw in ["leak", "leaked", "утечка", "слив", "инсайдер"])
    if has_leak and has_big_title:
        score += 45
        triggered.append({"id": "leak_big_title", "label": "Big title leak", "weight": 45})

    # Calendar boost
    cal_boost, event_name = get_calendar_boost()
    if cal_boost > 0:
        score += cal_boost
        triggered.append({"id": "calendar_event", "label": f"Event: {event_name}", "weight": cal_boost})

    score = min(100, score)

    if score >= 70:
        level = "high"
    elif score >= 40:
        level = "medium"
    elif score >= 20:
        level = "low"
    else:
        level = "none"

    return {"score": score, "level": level, "triggers": triggered, "pass": score >= 20}
