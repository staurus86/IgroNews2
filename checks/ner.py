"""Named Entity Recognition — словарный подход для извлечения студий, игр, чисел."""

import re

STUDIOS = [
    "rockstar", "rockstar games", "valve", "blizzard", "activision", "ubisoft",
    "ea", "electronic arts", "bethesda", "cd projekt red", "cd projekt",
    "epic games", "riot games", "bungie", "bioware", "obsidian", "insomniac",
    "naughty dog", "santa monica", "guerrilla", "playground games",
    "from software", "fromsoftware", "capcom", "konami", "sega", "bandai namco",
    "square enix", "nintendo", "sony", "microsoft", "xbox game studios",
    "team cherry", "supergiant", "devolver", "annapurna", "larian",
    "respawn", "infinity ward", "treyarch", "sledgehammer", "rare",
    "mojang", "mihoyo", "hoyoverse", "tencent", "netease", "krafton",
    "innersloth", "re-logic", "coffee stain", "paradox", "firaxis",
    "id software", "machine games", "arkane", "remedy", "sam lake",
    "kojima productions", "hideo kojima", "neil druckmann", "todd howard",
    "phil spencer", "shuhei yoshida", "tim sweeney", "gabe newell",
    "geoff keighley", "jason schreier",
]

GAMES = [
    "gta 6", "gta vi", "grand theft auto 6", "grand theft auto vi",
    "elder scrolls 6", "elder scrolls vi", "half-life 3", "half-life alyx",
    "call of duty", "modern warfare", "warzone", "black ops",
    "zelda", "tears of the kingdom", "breath of the wild",
    "pokemon", "mario", "mario kart", "super mario", "metroid",
    "batman arkham", "spider-man", "god of war", "god of war ragnarok",
    "horizon", "horizon forbidden west", "horizon zero dawn",
    "red dead redemption", "red dead", "cyberpunk 2077", "cyberpunk",
    "elden ring", "baldur's gate 3", "baldur's gate", "starfield",
    "diablo", "diablo 4", "diablo iv", "world of warcraft", "wow",
    "final fantasy", "final fantasy xvi", "final fantasy 16",
    "resident evil", "fortnite", "minecraft", "the witcher",
    "mass effect", "dragon age", "halo", "halo infinite",
    "overwatch", "overwatch 2", "valorant", "league of legends", "lol",
    "dota 2", "counter-strike", "cs2", "csgo",
    "destiny 2", "the last of us", "tlou", "uncharted",
    "assassin's creed", "far cry", "ghost of tsushima",
    "death stranding", "metal gear solid", "mgs", "silent hill",
    "alan wake", "control", "hollow knight", "silksong",
    "palworld", "lethal company", "helldivers", "helldivers 2",
    "stellar blade", "black myth wukong", "skull and bones",
    "starcraft", "age of empires", "civilization",
    "the sims", "sims 5", "cities skylines", "factorio",
    "stardew valley", "terraria", "among us", "fall guys",
    "apex legends", "pubg", "escape from tarkov", "tarkov",
    "genshin impact", "honkai star rail", "zenless zone zero",
    "armored core", "tekken", "street fighter", "mortal kombat",
]

PLATFORMS = [
    "ps5", "ps4", "playstation 5", "playstation 4", "playstation 6", "ps6",
    "xbox series x", "xbox series s", "xbox one", "xbox",
    "nintendo switch", "switch 2", "switch oled",
    "steam", "steam deck", "epic games store", "egs",
    "pc", "mac", "ios", "android", "mobile",
    "vr", "psvr2", "meta quest", "oculus",
]


def extract_entities(news: dict) -> dict:
    """Извлекает именованные сущности из новости."""
    title = news.get("title", "").lower()
    text = (title + " " + news.get("plain_text", "")).lower()

    found_studios = []
    for s in STUDIOS:
        if s in text:
            found_studios.append(s)

    found_games = []
    for g in GAMES:
        if g in text:
            found_games.append(g)

    found_platforms = []
    for p in PLATFORMS:
        if p in text:
            found_platforms.append(p)

    # Числа (большие, значимые)
    numbers = []
    for m in re.finditer(r'\b(\d[\d\s,.]*\d)\b|\b(\d+)\b', text):
        raw = (m.group(1) or m.group(2)).replace(" ", "").replace(",", "").replace(".", "")
        try:
            n = int(raw)
            if n >= 10 and n != 2024 and n != 2025 and n != 2026:
                numbers.append(n)
        except ValueError:
            pass
    # Deduplicate and keep unique large numbers
    numbers = sorted(set(numbers), reverse=True)[:5]

    # Events mentioned
    events = []
    event_kws = {
        "The Game Awards": ["the game awards", "tga"],
        "E3": ["e3"],
        "Gamescom": ["gamescom"],
        "GDC": ["gdc"],
        "Nintendo Direct": ["nintendo direct"],
        "Xbox Showcase": ["xbox showcase"],
        "PlayStation Showcase": ["playstation showcase", "state of play"],
        "Summer Game Fest": ["summer game fest", "sgf"],
        "Tokyo Game Show": ["tokyo game show", "tgs"],
    }
    for event_name, kws in event_kws.items():
        if any(kw in text for kw in kws):
            events.append(event_name)

    return {
        "studios": found_studios[:10],
        "games": found_games[:10],
        "platforms": found_platforms[:5],
        "numbers": numbers,
        "events": events,
        "total_entities": len(found_studios) + len(found_games) + len(found_platforms),
    }
