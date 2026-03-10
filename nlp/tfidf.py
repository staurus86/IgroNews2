"""Гибридное извлечение ключевых фраз: словарь сущностей + TF-IDF.

Оптимизировано: vectorizer кешируется, transform вместо fit_transform.
"""

import logging
import re

from sklearn.feature_extraction.text import TfidfVectorizer
from nlp.game_entities import find_entities, TIER_BOOST

logger = logging.getLogger(__name__)

# Стоп-слова для игровых новостей (рус + англ)
STOP_WORDS = {
    "это", "как", "что", "для", "при", "все", "они", "его", "она", "мне",
    "так", "или", "уже", "без", "тоже", "может", "будет", "если", "еще",
    "них", "нет", "есть", "был", "быть", "были", "было", "свой", "свои",
    "том", "тот", "этот", "эти", "где", "когда", "чем", "кто", "под",
    "the", "and", "for", "that", "with", "this", "from", "has", "are",
    "was", "will", "but", "not", "you", "all", "can", "had", "her",
    "one", "our", "out", "its", "have", "been", "who", "more", "new",
}

# Фоновый корпус: типичные фразы из игровых новостей.
BACKGROUND_CORPUS = [
    "новая игра вышла на pc и консоли с большим обновлением",
    "разработчики выпустили патч обновление для игры",
    "трейлер новой игры показали на презентации",
    "студия анонсировала продолжение популярной серии",
    "релиз игры перенесли на следующий год",
    "new game announced with release date trailer",
    "developer studio released major update patch",
    "upcoming game revealed gameplay trailer first look",
    "publisher announced new title coming to platforms",
    "early access launch available on steam epic store",
    "game review scores metacritic opencritic rating",
    "esports tournament championship prize pool winner",
    "dlc expansion season pass new content update",
    "free to play battle royale shooter open world",
    "console exclusive port remaster remake collection",
]

_STOP_WORDS_LIST = list(STOP_WORDS)

# Pre-compiled regex for text cleaning
_RE_HTML = re.compile(r"<[^>]+>")
_RE_NONWORD = re.compile(r"[^\w\s]")
_RE_SPACES = re.compile(r"\s+")

# Cached fitted vectorizers (created once, reused with transform)
_fitted_vectorizers = {}


def clean_text(text: str) -> str:
    """Очищает текст от HTML, лишних символов."""
    text = _RE_HTML.sub(" ", text)
    text = _RE_NONWORD.sub(" ", text)
    text = _RE_SPACES.sub(" ", text)
    return text.strip().lower()


def _get_fitted_vectorizer(ngram_range: tuple) -> TfidfVectorizer:
    """Returns a vectorizer pre-fitted on background corpus. Created once."""
    if ngram_range not in _fitted_vectorizers:
        v = TfidfVectorizer(
            ngram_range=ngram_range,
            max_features=100,
            stop_words=_STOP_WORDS_LIST,
            min_df=1,
        )
        v.fit(BACKGROUND_CORPUS)
        _fitted_vectorizers[ngram_range] = v
    return _fitted_vectorizers[ngram_range]


def _tfidf_with_background(text: str, ngram_range: tuple, top_n: int) -> list[list]:
    """TF-IDF с фоновым корпусом.

    Uses pre-fitted vectorizer's vocabulary, fits a new one on corpus+text
    to get proper IDF scores. Vocabulary is fixed so fit is cheap.
    """
    corpus = [text] + BACKGROUND_CORPUS
    try:
        base = _get_fitted_vectorizer(ngram_range)
        # Create lightweight vectorizer with fixed vocabulary — fit is O(vocab) not O(features)
        vectorizer = TfidfVectorizer(
            ngram_range=ngram_range,
            max_features=100,
            stop_words=_STOP_WORDS_LIST,
            min_df=1,
            vocabulary=base.vocabulary_,
        )
        tfidf_matrix = vectorizer.fit_transform(corpus)
        feature_names = vectorizer.get_feature_names_out()
        scores = tfidf_matrix.toarray()[0]

        ranked = sorted(
            zip(feature_names, scores),
            key=lambda x: x[1],
            reverse=True,
        )
        return [[phrase, round(float(score), 4)] for phrase, score in ranked if score > 0][:top_n]
    except ValueError:
        return []


def extract_keywords(text: str, top_n: int = 10) -> dict:
    """Извлекает ключевые фразы гибридным методом."""
    cleaned = clean_text(text)
    if len(cleaned.split()) < 3:
        return {"bigrams": [], "trigrams": [], "entities": []}

    # 1. Извлекаем известные сущности
    entities = find_entities(text)

    # 2. TF-IDF с фоновым корпусом
    tfidf_bigrams = _tfidf_with_background(cleaned, (2, 2), top_n * 2)
    tfidf_trigrams = _tfidf_with_background(cleaned, (3, 3), top_n)

    # 3. Бустим биграммы/триграммы, совпадающие с сущностями
    entity_names_lower = set()
    entity_tier_map = {}
    for ent in entities:
        name = ent["name"].lower()
        entity_names_lower.add(name)
        entity_tier_map[name] = ent["tier"]

    def boost_ngrams(ngrams: list[list]) -> list[list]:
        boosted = []
        seen_entities = set()
        for phrase, score in ngrams:
            matched_entity = None
            for ename in entity_names_lower:
                if ename in phrase or phrase in ename:
                    matched_entity = ename
                    break

            if matched_entity:
                tier = entity_tier_map[matched_entity]
                multiplier = {"S": 2.0, "A": 1.5, "B": 1.2, "C": 1.1}.get(tier, 1.0)
                boosted.append([phrase, round(score * multiplier, 4)])
                seen_entities.add(matched_entity)
            else:
                boosted.append([phrase, score])

        for ent in entities:
            name = ent["name"].lower()
            if name not in seen_entities and " " in name:
                tier_score = {"S": 1.0, "A": 0.8, "B": 0.6, "C": 0.4}.get(ent["tier"], 0.3)
                boosted.append([name, tier_score])
                seen_entities.add(name)

        boosted.sort(key=lambda x: x[1], reverse=True)
        return boosted

    bigrams = boost_ngrams(tfidf_bigrams)[:top_n]
    trigrams = boost_ngrams(tfidf_trigrams)[:top_n]

    return {
        "bigrams": bigrams,
        "trigrams": trigrams,
        "entities": entities,
    }
