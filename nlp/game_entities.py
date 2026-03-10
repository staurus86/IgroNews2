"""Единая база игровых сущностей с тирами и частотами.

Используется в:
- nlp/tfidf.py — буст биграмм при совпадении с известной сущностью
- checks/viral_score.py — замена BIG_TITLES на тиры
- checks/deduplication.py — entity overlap с весами
- checks/ner.py — может импортировать отсюда
"""

# Тиры: S (мега-хайп), A (крупные), B (заметные), C (нишевые)
# freq: условная частота поиска 0-100, влияет на буст

GAME_ENTITIES = {
    # === S-TIER: максимальный хайп, любая новость — событие ===
    "gta 6":                {"tier": "S", "freq": 100, "aliases": ["gta vi", "grand theft auto 6", "grand theft auto vi"]},
    "elder scrolls 6":      {"tier": "S", "freq": 95, "aliases": ["elder scrolls vi", "tes 6", "tes vi"]},
    "half-life 3":          {"tier": "S", "freq": 90, "aliases": ["half life 3"]},
    "zelda":                {"tier": "S", "freq": 90, "aliases": ["the legend of zelda", "tears of the kingdom", "breath of the wild"]},
    "gta":                  {"tier": "S", "freq": 95, "aliases": ["grand theft auto"]},

    # === A-TIER: крупные франшизы, стабильный интерес ===
    "elden ring":           {"tier": "A", "freq": 85, "aliases": ["elden ring nightreign"]},
    "cyberpunk 2077":       {"tier": "A", "freq": 80, "aliases": ["cyberpunk", "cyberpunk 2"]},
    "baldur's gate 3":      {"tier": "A", "freq": 82, "aliases": ["baldur's gate", "bg3"]},
    "call of duty":         {"tier": "A", "freq": 85, "aliases": ["cod", "modern warfare", "warzone", "black ops"]},
    "the witcher":          {"tier": "A", "freq": 78, "aliases": ["witcher 4", "witcher", "ведьмак"]},
    "god of war":           {"tier": "A", "freq": 80, "aliases": ["god of war ragnarok"]},
    "spider-man":           {"tier": "A", "freq": 78, "aliases": ["marvel's spider-man"]},
    "resident evil":        {"tier": "A", "freq": 75, "aliases": ["re9", "resident evil 9"]},
    "final fantasy":        {"tier": "A", "freq": 75, "aliases": ["final fantasy xvi", "final fantasy 16", "final fantasy 7", "ff7", "ff16"]},
    "red dead redemption":  {"tier": "A", "freq": 78, "aliases": ["red dead", "rdr", "rdr2"]},
    "the last of us":       {"tier": "A", "freq": 78, "aliases": ["tlou", "last of us"]},
    "death stranding":      {"tier": "A", "freq": 72, "aliases": ["death stranding 2"]},
    "metal gear solid":     {"tier": "A", "freq": 72, "aliases": ["mgs", "metal gear"]},
    "silent hill":          {"tier": "A", "freq": 70, "aliases": ["silent hill 2", "silent hill f"]},
    "mass effect":          {"tier": "A", "freq": 72, "aliases": ["mass effect 5"]},
    "starfield":            {"tier": "A", "freq": 70, "aliases": []},
    "diablo":               {"tier": "A", "freq": 75, "aliases": ["diablo 4", "diablo iv"]},
    "hollow knight silksong": {"tier": "A", "freq": 80, "aliases": ["silksong", "hollow knight"]},
    "black myth wukong":    {"tier": "A", "freq": 82, "aliases": ["black myth", "wukong"]},
    "minecraft":            {"tier": "A", "freq": 85, "aliases": []},
    "fortnite":             {"tier": "A", "freq": 85, "aliases": []},

    # === B-TIER: популярные, но без мега-хайпа ===
    "pokemon":              {"tier": "B", "freq": 68, "aliases": ["pokémon"]},
    "mario":                {"tier": "B", "freq": 65, "aliases": ["super mario", "mario kart"]},
    "halo":                 {"tier": "B", "freq": 55, "aliases": ["halo infinite"]},
    "dragon age":           {"tier": "B", "freq": 60, "aliases": ["dragon age veilguard", "dragon age dreadwolf"]},
    "assassin's creed":     {"tier": "B", "freq": 65, "aliases": ["assassins creed", "ac shadows", "ac mirage"]},
    "horizon":              {"tier": "B", "freq": 58, "aliases": ["horizon forbidden west", "horizon zero dawn"]},
    "overwatch":            {"tier": "B", "freq": 55, "aliases": ["overwatch 2"]},
    "valorant":             {"tier": "B", "freq": 60, "aliases": []},
    "league of legends":    {"tier": "B", "freq": 65, "aliases": ["lol", "лига легенд"]},
    "dota 2":               {"tier": "B", "freq": 60, "aliases": ["dota", "дота"]},
    "counter-strike":       {"tier": "B", "freq": 65, "aliases": ["cs2", "csgo", "cs:go", "counter strike"]},
    "escape from tarkov":   {"tier": "B", "freq": 60, "aliases": ["tarkov", "тарков"]},
    "genshin impact":       {"tier": "B", "freq": 62, "aliases": ["genshin", "геншин"]},
    "destiny 2":            {"tier": "B", "freq": 50, "aliases": ["destiny"]},
    "apex legends":         {"tier": "B", "freq": 55, "aliases": ["apex"]},
    "helldivers 2":         {"tier": "B", "freq": 65, "aliases": ["helldivers"]},
    "palworld":             {"tier": "B", "freq": 60, "aliases": []},
    "alan wake":            {"tier": "B", "freq": 50, "aliases": ["alan wake 2"]},
    "uncharted":            {"tier": "B", "freq": 50, "aliases": []},
    "fable":                {"tier": "B", "freq": 55, "aliases": []},
    "far cry":              {"tier": "B", "freq": 52, "aliases": []},
    "ghost of tsushima":    {"tier": "B", "freq": 55, "aliases": []},
    "world of warcraft":    {"tier": "B", "freq": 58, "aliases": ["wow"]},
    "honkai star rail":     {"tier": "B", "freq": 55, "aliases": ["honkai", "хонкай"]},
    "stellar blade":        {"tier": "B", "freq": 50, "aliases": []},
    "batman arkham":        {"tier": "B", "freq": 55, "aliases": ["batman"]},
    "indiana jones":        {"tier": "B", "freq": 52, "aliases": []},
    "bioshock":             {"tier": "B", "freq": 50, "aliases": ["bioshock 4"]},
    "doom":                 {"tier": "B", "freq": 55, "aliases": ["doom the dark ages"]},
    "pubg":                 {"tier": "B", "freq": 50, "aliases": []},

    # === C-TIER: нишевые, но с лояльной аудиторией ===
    "stardew valley":       {"tier": "C", "freq": 40, "aliases": []},
    "terraria":             {"tier": "C", "freq": 38, "aliases": []},
    "among us":             {"tier": "C", "freq": 35, "aliases": []},
    "fall guys":            {"tier": "C", "freq": 30, "aliases": []},
    "lethal company":       {"tier": "C", "freq": 42, "aliases": []},
    "factorio":             {"tier": "C", "freq": 38, "aliases": []},
    "cities skylines":      {"tier": "C", "freq": 35, "aliases": ["cities skylines 2"]},
    "the sims":             {"tier": "C", "freq": 40, "aliases": ["sims 5", "sims 4"]},
    "civilization":         {"tier": "C", "freq": 42, "aliases": ["civ 7", "civilization 7"]},
    "age of empires":       {"tier": "C", "freq": 35, "aliases": ["aoe"]},
    "starcraft":            {"tier": "C", "freq": 30, "aliases": []},
    "tekken":               {"tier": "C", "freq": 38, "aliases": ["tekken 8"]},
    "street fighter":       {"tier": "C", "freq": 38, "aliases": ["street fighter 6", "sf6"]},
    "mortal kombat":        {"tier": "C", "freq": 40, "aliases": ["mk1"]},
    "persona":              {"tier": "C", "freq": 42, "aliases": ["persona 6", "persona 5"]},
    "kingdom hearts":       {"tier": "C", "freq": 38, "aliases": []},
    "dark souls":           {"tier": "C", "freq": 42, "aliases": []},
    "bloodborne":           {"tier": "C", "freq": 45, "aliases": []},
    "sekiro":               {"tier": "C", "freq": 38, "aliases": []},
    "armored core":         {"tier": "C", "freq": 35, "aliases": ["armored core 6"]},
    "skull and bones":      {"tier": "C", "freq": 28, "aliases": []},
    "zenless zone zero":    {"tier": "C", "freq": 40, "aliases": ["zzz"]},
    "perfect dark":         {"tier": "C", "freq": 35, "aliases": []},
    "avowed":               {"tier": "C", "freq": 38, "aliases": []},
    "wolverine":            {"tier": "C", "freq": 42, "aliases": ["marvel's wolverine"]},
}

STUDIO_ENTITIES = {
    # S-tier studios
    "rockstar games":       {"tier": "S", "freq": 90, "aliases": ["rockstar"]},
    "nintendo":             {"tier": "S", "freq": 88, "aliases": []},
    "valve":                {"tier": "S", "freq": 85, "aliases": []},

    # A-tier studios
    "fromsoftware":         {"tier": "A", "freq": 78, "aliases": ["from software"]},
    "cd projekt red":       {"tier": "A", "freq": 75, "aliases": ["cd projekt", "cdpr"]},
    "naughty dog":          {"tier": "A", "freq": 72, "aliases": []},
    "larian":               {"tier": "A", "freq": 72, "aliases": ["larian studios"]},
    "kojima productions":   {"tier": "A", "freq": 70, "aliases": ["hideo kojima", "кодзима"]},
    "bethesda":             {"tier": "A", "freq": 72, "aliases": ["bethesda game studios", "bethesda softworks"]},
    "insomniac":            {"tier": "A", "freq": 68, "aliases": ["insomniac games"]},
    "santa monica studio":  {"tier": "A", "freq": 65, "aliases": ["santa monica"]},
    "remedy":               {"tier": "A", "freq": 62, "aliases": ["remedy entertainment"]},

    # B-tier studios
    "blizzard":             {"tier": "B", "freq": 65, "aliases": ["blizzard entertainment"]},
    "activision":           {"tier": "B", "freq": 62, "aliases": ["activision blizzard"]},
    "ubisoft":              {"tier": "B", "freq": 60, "aliases": []},
    "ea":                   {"tier": "B", "freq": 60, "aliases": ["electronic arts"]},
    "epic games":           {"tier": "B", "freq": 62, "aliases": ["epic"]},
    "riot games":           {"tier": "B", "freq": 60, "aliases": ["riot"]},
    "capcom":               {"tier": "B", "freq": 58, "aliases": []},
    "square enix":          {"tier": "B", "freq": 55, "aliases": []},
    "bioware":              {"tier": "B", "freq": 52, "aliases": []},
    "obsidian":             {"tier": "B", "freq": 50, "aliases": ["obsidian entertainment"]},
    "bungie":               {"tier": "B", "freq": 50, "aliases": []},
    "respawn":              {"tier": "B", "freq": 48, "aliases": ["respawn entertainment"]},
    "guerrilla":            {"tier": "B", "freq": 48, "aliases": ["guerrilla games"]},

    # C-tier studios
    "konami":               {"tier": "C", "freq": 42, "aliases": []},
    "sega":                 {"tier": "C", "freq": 42, "aliases": []},
    "bandai namco":         {"tier": "C", "freq": 40, "aliases": ["bandai"]},
    "team cherry":          {"tier": "C", "freq": 45, "aliases": []},
    "supergiant":           {"tier": "C", "freq": 40, "aliases": ["supergiant games"]},
    "devolver":             {"tier": "C", "freq": 35, "aliases": ["devolver digital"]},
    "annapurna":            {"tier": "C", "freq": 35, "aliases": ["annapurna interactive"]},
    "paradox":              {"tier": "C", "freq": 38, "aliases": ["paradox interactive"]},
    "firaxis":              {"tier": "C", "freq": 35, "aliases": []},
    "id software":          {"tier": "C", "freq": 40, "aliases": []},
    "machine games":        {"tier": "C", "freq": 35, "aliases": ["machinegames"]},
    "arkane":               {"tier": "C", "freq": 38, "aliases": ["arkane studios"]},
    "rare":                 {"tier": "C", "freq": 32, "aliases": []},
    "mojang":               {"tier": "C", "freq": 40, "aliases": []},
    "hoyoverse":            {"tier": "C", "freq": 42, "aliases": ["mihoyo"]},
    "tencent":              {"tier": "C", "freq": 38, "aliases": []},
}

PLATFORM_ENTITIES = {
    "playstation":  {"tier": "A", "freq": 80, "aliases": ["ps5", "ps4", "ps6", "playstation 5", "playstation 6", "psvr2"]},
    "xbox":         {"tier": "A", "freq": 78, "aliases": ["xbox series x", "xbox series s", "xbox one"]},
    "nintendo switch": {"tier": "A", "freq": 75, "aliases": ["switch", "switch 2", "switch oled"]},
    "steam":        {"tier": "A", "freq": 82, "aliases": ["steam deck"]},
    "pc":           {"tier": "B", "freq": 60, "aliases": []},
    "epic games store": {"tier": "B", "freq": 45, "aliases": ["egs"]},
    "game pass":    {"tier": "B", "freq": 55, "aliases": ["xbox game pass", "pc game pass"]},
    "ps plus":      {"tier": "B", "freq": 50, "aliases": ["playstation plus"]},
    "vr":           {"tier": "C", "freq": 30, "aliases": ["meta quest", "oculus", "psvr"]},
}

# Tier -> viral boost (используется в viral_score.py)
TIER_BOOST = {"S": 30, "A": 15, "B": 8, "C": 3}

# --- Lookup index: строится при импорте для быстрого поиска ---

_LOOKUP = {}  # {"gta 6": {"name": "gta 6", "type": "game", "tier": "S", "freq": 100}, ...}


def _build_lookup():
    """Строит плоский индекс alias->entity для быстрого поиска в тексте."""
    for name, data in GAME_ENTITIES.items():
        entry = {"name": name, "type": "game", "tier": data["tier"], "freq": data["freq"]}
        _LOOKUP[name] = entry
        for alias in data.get("aliases", []):
            if alias and alias not in _LOOKUP:
                _LOOKUP[alias] = entry

    for name, data in STUDIO_ENTITIES.items():
        entry = {"name": name, "type": "studio", "tier": data["tier"], "freq": data["freq"]}
        _LOOKUP[name] = entry
        for alias in data.get("aliases", []):
            if alias and alias not in _LOOKUP:
                _LOOKUP[alias] = entry

    for name, data in PLATFORM_ENTITIES.items():
        entry = {"name": name, "type": "platform", "tier": data["tier"], "freq": data["freq"]}
        _LOOKUP[name] = entry
        for alias in data.get("aliases", []):
            if alias and alias not in _LOOKUP:
                _LOOKUP[alias] = entry


_build_lookup()


import re

# Короткие ключи (<=3 символов) требуют word boundary, иначе ловят подстроки
_SHORT_KEY_THRESHOLD = 3


def find_entities(text: str) -> list[dict]:
    """Находит все известные сущности в тексте.

    Возвращает список: [{"name": "gta 6", "type": "game", "tier": "S", "freq": 100}, ...]
    Сортировка: по freq (высокие сначала).
    Короткие ключи (<=3 символов) проверяются с word boundary.
    """
    text_lower = text.lower()
    found = {}  # name -> entry (дедупликация по canonical name)

    # Сортируем по длине ключа — длинные сначала (greedy match)
    for key in sorted(_LOOKUP.keys(), key=len, reverse=True):
        if len(key) <= _SHORT_KEY_THRESHOLD:
            # Для коротких ключей — word boundary regex
            if re.search(r'\b' + re.escape(key) + r'\b', text_lower):
                entry = _LOOKUP[key]
                canonical = entry["name"]
                if canonical not in found:
                    found[canonical] = entry
        else:
            if key in text_lower:
                entry = _LOOKUP[key]
                canonical = entry["name"]
                if canonical not in found:
                    found[canonical] = entry

    return sorted(found.values(), key=lambda e: e["freq"], reverse=True)


def get_entity_boost(text: str) -> tuple[int, list[dict]]:
    """Считает суммарный буст от сущностей в тексте.

    Возвращает (total_boost, entities).
    Буст считается от лучшего тира найденных сущностей (не суммируется).
    """
    entities = find_entities(text)
    if not entities:
        return 0, []

    best_tier = entities[0]["tier"]  # уже отсортировано по freq
    boost = TIER_BOOST.get(best_tier, 0)

    return boost, entities
