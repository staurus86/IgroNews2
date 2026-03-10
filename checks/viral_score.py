from datetime import datetime, timezone
from typing import Optional

VIRAL_TRIGGERS = {
    # === СКАНДАЛЫ И ДРАМА ===
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
    "scandal_ceo_toxic": {
        "label": "Toxic CEO",
        "weight": 50,
        "keywords": [
            "ceo заявил", "глава компании раскритиковал", "bobby kotick",
            "бобби котик", "neil druckmann", "нил дракманн", "ceo criticized",
            "ceo slams", "глава студии обвинил", "founder controversy",
            "руководитель против игроков", "ceo backlash",
        ],
    },
    "scandal_lawsuit": {
        "label": "Lawsuit",
        "weight": 45,
        "keywords": [
            "судебный иск", "подали в суд", "class action", "антимонопольный",
            "lawsuit", "sued", "legal action", "court ruling", "settlement",
            "иск против", "суд обязал", "судебное разбирательство",
            "epic vs apple", "ftc", "иск игроков",
        ],
    },
    "scandal_crunch": {
        "label": "Crunch / Labor",
        "weight": 45,
        "keywords": [
            "кранч", "переработки", "crunch", "overtime", "toxic workplace",
            "токсичная атмосфера", "дискриминация", "harassment", "домогательства",
            "условия труда", "labor abuse", "whistleblower", "инсайдер рассказал",
            "бывший сотрудник", "former employee", "работников заставляли",
        ],
    },
    "scandal_regulatory": {
        "label": "Regulatory pressure",
        "weight": 35,
        "keywords": [
            "антимонопольное расследование", "регулятор заблокировал", "antitrust",
            "регуляторное давление", "cma blocked", "ftc blocked", "eu investigation",
            "запрет сделки", "блокировка слияния", "regulatory review",
        ],
    },
    "scandal_policy_reversal": {
        "label": "Policy reversal",
        "weight": 40,
        "keywords": [
            "отменили обещание", "убрали функцию", "убрали из библиотеки",
            "removed from library", "always-online добавили", "broken promise",
            "обещали но не сделали", "reversal", "u-turn", "backtrack",
            "delisted", "отзыв лицензий", "revoked access",
        ],
    },

    # === УТЕЧКИ ===
    "leak_major": {
        "label": "Leak",
        "weight": 50,
        "keywords": [
            "утечка", "слив", "слитый трейлер", "инсайдер",
            "leak", "leaked", "insider", "rumor", "rumour",
            "anonymous source", "источник сообщает",
        ],
    },
    "leak_datamine": {
        "label": "Datamine",
        "weight": 40,
        "keywords": [
            "datamine", "datamined", "нашли в файлах", "обнаружили в коде",
            "файлы игры содержат", "data mining", "strings found",
            "hidden content", "скрытый контент", "неанонсированный контент",
        ],
    },
    "leak_store": {
        "label": "Store leak",
        "weight": 50,
        "keywords": [
            "появилась в steam", "утечка через магазин", "store listing",
            "playstation store leak", "xbox store leak", "steam page appeared",
            "рейтинг до анонса", "esrb rating", "pegi rating",
            "accidentally listed", "случайно опубликовали",
        ],
    },
    "leak_trademark": {
        "label": "Trademark leak",
        "weight": 30,
        "keywords": [
            "торговая марка", "trademark", "patent filed", "патент",
            "зарегистрировали название", "domain registration",
            "registered trademark", "товарный знак",
        ],
    },
    "leak_playtest": {
        "label": "Playtest leak",
        "weight": 55,
        "keywords": [
            "утечка с плейтеста", "nda нарушение", "закрытый тест",
            "playtest leak", "nda breach", "closed beta leak",
            "alpha footage", "слитый геймплей с теста",
            "скриншоты с закрытого теста", "leaked playtest",
        ],
    },
    "leak_insider_trusted": {
        "label": "Trusted insider",
        "weight": 45,
        "keywords": [
            "jeff grubb", "tom henderson", "jason schreier", "jez corden",
            "nick baker", "известный инсайдер", "reliable leaker",
            "trusted source", "проверенный источник",
        ],
    },

    # === SHADOW DROPS ===
    "shadow_drop": {
        "label": "Shadow Drop",
        "weight": 55,
        "keywords": [
            "вышла неожиданно", "внезапный релиз", "доступна прямо сейчас",
            "shadow drop", "shadow dropped", "available now", "out now",
            "surprise release", "stealth release",
        ],
    },
    "shadow_announce_date": {
        "label": "Announce + Date",
        "weight": 40,
        "keywords": [
            "анонс и дата", "выходит через неделю", "выходит через месяц",
            "releases next week", "launches next month", "выходит завтра",
            "announced and releasing", "date revealed",
        ],
    },
    "shadow_free_giveaway": {
        "label": "Free AAA Giveaway",
        "weight": 35,
        "keywords": [
            "бесплатно раздают", "epic бесплатно", "ps plus добавил",
            "game pass добавил", "free on epic", "free on ps plus",
            "xbox game pass added", "бесплатная раздача aaa",
            "крупная игра бесплатно", "free to keep",
        ],
    },
    "shadow_dead_franchise": {
        "label": "Dead franchise returns",
        "weight": 60,
        "keywords": [
            "возвращение серии", "возрождение франшизы", "franchise revival",
            "series returns", "comeback", "long-awaited sequel",
            "вернулась после", "первая игра за", "years later",
            "silent hill", "castlevania", "timesplitters", "dino crisis",
            "f-zero", "jet set radio", "chrono",
        ],
    },

    # === ПЛОХИЕ РЕЛИЗЫ ===
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
    "bad_refund_wave": {
        "label": "Refund wave",
        "weight": 50,
        "keywords": [
            "массовые возвраты", "возвращают деньги", "refund wave",
            "mass refunds", "steam refund", "возврат средств",
            "требуют возврата", "refund policy", "no man's sky launch",
        ],
    },
    "bad_review_bomb": {
        "label": "Review bombing",
        "weight": 40,
        "keywords": [
            "review bombing", "review bomb", "обвал оценок",
            "скоординированная атака", "занижение рейтинга",
            "steam reviews negative", "metacritic bombed",
            "массовые негативные отзывы",
        ],
    },
    "bad_commercial_flop": {
        "label": "Commercial flop",
        "weight": 35,
        "keywords": [
            "не окупилась", "провал продаж", "commercial failure",
            "underperformed", "ниже ожиданий", "missed sales target",
            "below expectations", "финансовый провал", "продажи разочаровали",
        ],
    },
    "bad_server_crash": {
        "label": "Server crash",
        "weight": 30,
        "keywords": [
            "серверы легли", "серверы упали", "server down", "servers crashed",
            "не работают серверы", "server issues at launch",
            "невозможно подключиться", "login queue", "очередь на вход",
        ],
    },
    "bad_huge_patch": {
        "label": "Huge day-one patch",
        "weight": 15,
        "keywords": [
            "патч на день релиза", "day-one patch", "day one patch",
            "патч 50 гб", "patch size", "гигантский патч",
            "огромный патч", "100gb patch", "50gb update",
        ],
    },

    # === AI CONTROVERSY ===
    "ai_controversy": {
        "label": "AI controversy",
        "weight": 40,
        "keywords": [
            "нейросеть в игре", "ии арт", "ai generated", "ai art",
            "ai replaced", "ai backlash", "generative ai",
        ],
    },
    "ai_fired_artists": {
        "label": "AI replaced artists",
        "weight": 55,
        "keywords": [
            "уволили художников ради ии", "заменили художников",
            "ai replaced artists", "fired artists for ai",
            "replaced by ai", "laid off artists", "ai вместо художников",
        ],
    },
    "ai_voice_no_consent": {
        "label": "AI voice no consent",
        "weight": 50,
        "keywords": [
            "ai голос без согласия", "синтез речи без разрешения",
            "ai voice without consent", "voice actor ai", "deepfake voice",
            "клонировали голос", "voice cloning", "без согласия актёра озвучки",
        ],
    },
    "ai_full_game": {
        "label": "AI-generated game",
        "weight": 45,
        "keywords": [
            "полностью сгенерировано ии", "ai generated game",
            "game made by ai", "ai-created game", "игра созданная ии",
            "нейросеть создала игру",
        ],
    },
    "ai_no_ai_pledge": {
        "label": "No-AI pledge",
        "weight": 25,
        "keywords": [
            "отказались от ии", "no ai", "no ai used", "без использования ии",
            "anti-ai", "human-made", "ручная работа без ии",
        ],
    },
    "ai_npc_glitch": {
        "label": "AI NPC viral",
        "weight": 30,
        "keywords": [
            "ии нпс", "ai npc", "нпс вышел из под контроля",
            "npc went rogue", "ai companion bug", "нпс сказал",
            "ai character viral", "ии персонаж стал мемом",
        ],
    },

    # === КРУПНЫЕ ИВЕНТЫ ===
    "major_event": {
        "label": "Major event",
        "weight": 35,
        "keywords": [
            "the game awards", "nintendo direct", "xbox showcase",
            "playstation showcase", "state of play", "summer game fest",
            "gamescom", "tokyo game show", "tgs", "e3", "gdc",
        ],
    },
    "event_livestream": {
        "label": "Livestream moment",
        "weight": 40,
        "keywords": [
            "прямой эфир", "прямая трансляция", "live stream",
            "смотрите прямо сейчас", "watch live", "трансляция идёт",
            "показали на стриме", "during livestream", "live reveal",
        ],
    },
    "event_show_winner": {
        "label": "Show winner",
        "weight": 35,
        "keywords": [
            "главный анонс шоу", "лучший момент", "show stealer",
            "stole the show", "highlight of the show", "best reveal",
            "самый обсуждаемый анонс", "winner of the show",
        ],
    },
    "event_bad_show": {
        "label": "Bad show",
        "weight": 40,
        "keywords": [
            "разочаровывающее шоу", "слабая презентация",
            "disappointing showcase", "worst showcase", "ничего не показали",
            "nothing new", "boring presentation", "провальная презентация",
        ],
    },
    "event_surprise_guest": {
        "label": "Surprise guest",
        "weight": 50,
        "keywords": [
            "неожиданный анонс", "никто не ожидал", "surprise reveal",
            "unexpected announcement", "shock reveal", "out of nowhere",
            "невероятный анонс", "jaw-dropping", "plot twist",
        ],
    },

    # === ДЕНЬГИ И СДЕЛКИ ===
    "money_ma": {
        "label": "M&A deal",
        "weight": 60,
        "keywords": [
            "купили студию", "покупка студии", "acquisition", "acquired",
            "merger", "слияние", "поглощение", "купили за",
            "billion dollar deal", "сделка на миллиард",
            "microsoft купил", "sony купил", "tencent купил",
            "embracer", "savvy games", "take-two acquired",
        ],
    },
    "money_studio_closed": {
        "label": "Studio closed",
        "weight": 65,
        "keywords": [
            "закрытие студии", "студия закрывается", "studio closure",
            "shut down studio", "developer shut down", "студию закрыли",
            "последний проект студии", "rip studio", "прощай студия",
            "конец студии", "studio's final game",
        ],
    },
    "money_ipo": {
        "label": "IPO / Public",
        "weight": 30,
        "keywords": [
            "ipo", "выход на биржу", "public offering", "went public",
            "stock market", "акции компании", "shares",
        ],
    },
    "money_financial_fail": {
        "label": "Financial report fail",
        "weight": 35,
        "keywords": [
            "убытки", "падение выручки", "revenue decline", "quarterly loss",
            "прибыль упала", "финансовый отчёт", "earnings miss",
            "акции обвалились", "stock drop", "stock plunge",
            "провал квартала", "missed expectations",
        ],
    },

    # === КУЛЬТУРНЫЕ И СОЦИАЛЬНЫЕ ТРИГГЕРЫ ===
    "culture_banned": {
        "label": "Game banned",
        "weight": 40,
        "keywords": [
            "запрещена в", "banned in", "заблокирована в",
            "регуляторный запрет", "refused classification",
            "запрет на продажу", "изъяли из магазинов",
            "banned in china", "banned in australia",
        ],
    },
    "culture_politics": {
        "label": "Game meets politics",
        "weight": 45,
        "keywords": [
            "политический скандал", "затронула политику", "political controversy",
            "обвинения в пропаганде", "propaganda", "woke", "anti-woke",
            "dei controversy", "political statement", "game controversy",
        ],
    },
    "culture_record": {
        "label": "Historical record",
        "weight": 40,
        "keywords": [
            "рекорд steam", "рекорд twitch", "рекорд продаж",
            "steam record", "peak concurrent", "пик одновременных",
            "fastest selling", "самая быстрая", "миллион за день",
            "million copies in", "record-breaking",
        ],
    },
    "culture_adaptation": {
        "label": "Game to film/series",
        "weight": 30,
        "keywords": [
            "экранизация", "сериал по", "фильм по игре",
            "tv series", "movie adaptation", "netflix", "hbo",
            "amazon adaptation", "live action", "animated series",
            "the last of us hbo", "fallout series",
        ],
    },
    "culture_meme": {
        "label": "Viral meme",
        "weight": 25,
        "keywords": [
            "стал мемом", "вирусное видео", "went viral", "tiktok",
            "мем из игры", "gaming meme", "viral clip", "trending on twitter",
            "reddit exploded", "стал вирусным",
        ],
    },

    # === ПЕРСОНАЛЬНЫЕ ТРИГГЕРЫ ===
    "person_key_departure": {
        "label": "Key person leaves",
        "weight": 55,
        "keywords": [
            "ушёл из", "покидает компанию", "уход из студии",
            "leaves studio", "departing", "steps down", "resignation",
            "основатель покинул", "creative director leaves",
            "hideo kojima", "хидео кодзима", "тодд говард", "todd howard",
            "miyazaki", "миядзаки",
        ],
    },
    "person_legend_returns": {
        "label": "Legend returns",
        "weight": 50,
        "keywords": [
            "вернулся в индустрию", "новый проект от", "возвращение",
            "returns to gaming", "new studio by", "legendary developer",
            "культовый разработчик", "основал новую студию",
            "new project from",
        ],
    },
    "person_celebrity_clash": {
        "label": "Celebrity vs Game",
        "weight": 25,
        "keywords": [
            "актёр раскритиковал", "стример против", "celebrity criticized",
            "streamer backlash", "знаменитость", "celebrity endorsement",
            "famous gamer", "influencer drama",
        ],
    },

    # === СКОРОСТЬ И ЭКСКЛЮЗИВНОСТЬ ===
    "speed_first_review": {
        "label": "First review",
        "weight": 30,
        "keywords": [
            "первый обзор", "first review", "review embargo lifted",
            "эмбарго снято", "оценки раскрыты", "scores revealed",
            "review roundup", "первые оценки",
        ],
    },
    "speed_exclusive_content": {
        "label": "Exclusive content",
        "weight": 40,
        "keywords": [
            "эксклюзивный геймплей", "exclusive gameplay", "exclusive reveal",
            "эксклюзивные скриншоты", "exclusive screenshots",
            "hands-on preview", "exclusive interview",
            "first hands-on", "эксклюзивный материал",
        ],
    },
    "speed_day_one_sales": {
        "label": "Day-one sales",
        "weight": 35,
        "keywords": [
            "продажи за первый день", "first day sales", "day one sales",
            "миллион копий за день", "first 24 hours",
            "launch sales figures", "первые продажи",
        ],
    },

    # === БАЗОВЫЕ КАТЕГОРИИ ===
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
            "dlc",
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

from nlp.game_entities import get_entity_boost, TIER_BOOST

GAMING_EVENTS_CALENDAR = [
    (1, 15, 25, "Xbox Developer Direct", 20),
    (2, 1, 28, "Nintendo Direct (Feb)", 25),
    (3, 18, 22, "GDC", 20),
    (5, 20, 30, "PlayStation Showcase", 30),
    (6, 1, 15, "Summer Game Fest", 30),
    (6, 10, 12, "Xbox Games Showcase", 30),
    (6, 15, 18, "Nintendo Direct (June)", 30),
    (8, 20, 28, "Gamescom", 25),
    (9, 1, 5, "PlayStation State of Play (Sep)", 20),
    (9, 20, 30, "Tokyo Game Show", 20),
    (12, 5, 15, "The Game Awards", 40),
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

    # Entity-based boost (из единой базы с тирами)
    entity_boost, entities = get_entity_boost(text)
    has_big_title = entity_boost >= TIER_BOOST["A"]  # A-tier и выше
    if entity_boost > 0:
        score += entity_boost
        best = entities[0] if entities else {}
        triggered.append({
            "id": "entity_boost",
            "label": f"{best.get('tier', '?')}-tier: {best.get('name', '?')} (freq={best.get('freq', 0)})",
            "weight": entity_boost,
        })

    # Big title + leak combo
    has_leak = any(kw in text for kw in ["leak", "leaked", "утечка", "слив", "инсайдер"])
    if has_leak and has_big_title:
        score += 45
        triggered.append({"id": "leak_big_title", "label": "Big title leak", "weight": 45})

    # Big title + studio closure combo
    has_closure = any(kw in text for kw in ["закрытие студии", "studio closure", "shut down", "студию закрыли"])
    if has_closure and has_big_title:
        score += 30
        triggered.append({"id": "closure_big_title", "label": "Big studio closure", "weight": 30})

    # Lawsuit + Big company combo (из entity базы — студии)
    has_lawsuit = any(kw in text for kw in ["lawsuit", "судебный иск", "court", "sued"])
    has_big_company = any(e.get("type") == "studio" and e.get("tier") in ("S", "A", "B") for e in entities)
    if has_lawsuit and has_big_company:
        score += 20
        triggered.append({"id": "lawsuit_big_company", "label": "Big company lawsuit", "weight": 20})

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
