import hashlib
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


def normalize(text: str) -> str:
    text = text.lower()
    text = re.sub(r'[^\w\s]', '', text)
    return text.strip()


def exact_duplicate(title1: str, title2: str) -> bool:
    return hashlib.md5(normalize(title1).encode()).hexdigest() == \
           hashlib.md5(normalize(title2).encode()).hexdigest()


def tfidf_similarity(titles: list[str]) -> list[tuple]:
    if len(titles) < 2:
        return []
    vectorizer = TfidfVectorizer(ngram_range=(1, 2))
    matrix = vectorizer.fit_transform([normalize(t) for t in titles])
    sim = cosine_similarity(matrix)

    pairs = []
    for i in range(len(titles)):
        for j in range(i + 1, len(titles)):
            score = sim[i][j]
            if score > 0.55:
                pairs.append((i, j, round(float(score), 2)))
    return pairs


GAMING_ENTITIES = [
    "gta", "gta 6", "elder scrolls", "call of duty", "cod", "cyberpunk",
    "starfield", "diablo", "zelda", "mario", "pokemon", "final fantasy",
    "resident evil", "assassin's creed", "god of war", "spider-man",
    "hollow knight", "elden ring", "baldur's gate", "mass effect",
    "dragon age", "witcher", "minecraft", "fortnite", "valorant",
    "overwatch", "destiny", "halo", "doom", "red dead", "horizon",
    "steam", "xbox", "playstation", "nintendo", "epic games",
    "blizzard", "ea", "ubisoft", "bethesda", "rockstar", "valve",
    "cd projekt", "riot", "capcom", "square enix", "sony", "microsoft",
]


def entity_overlap(text1: str, text2: str) -> float:
    found1 = set(e for e in GAMING_ENTITIES if e in text1.lower())
    found2 = set(e for e in GAMING_ENTITIES if e in text2.lower())
    if not found1 and not found2:
        return 0
    return len(found1 & found2) / max(len(found1 | found2), 1)


DUPLICATE_STATUSES = {
    "unique": "unique",
    "popular": "popular",
    "trending": "trending",
    "duplicate": "duplicate",
}


def build_groups(results: list[dict], pairs: list[tuple]) -> list[dict]:
    """Группирует похожие новости."""
    from collections import defaultdict
    graph = defaultdict(set)
    for i, j, score in pairs:
        graph[i].add(j)
        graph[j].add(i)

    visited = set()
    groups = []

    for idx in range(len(results)):
        if idx in visited:
            continue
        group = set()
        stack = [idx]
        while stack:
            node = stack.pop()
            if node in visited:
                continue
            visited.add(node)
            group.add(node)
            stack.extend(graph[node] - visited)

        members = [results[i] for i in sorted(group)]
        count = len(members)

        if count >= 4:
            dup_status = "trending"
        elif count >= 2:
            dup_status = "popular"
        else:
            dup_status = "unique"

        # Помечаем дубликаты (score > 0.85)
        duplicates = set()
        for i, j, score in pairs:
            if i in group and j in group and score > 0.85:
                duplicates.add(j)

        groups.append({
            "status": dup_status,
            "members": members,
            "duplicate_indices": list(duplicates),
        })

    return groups
