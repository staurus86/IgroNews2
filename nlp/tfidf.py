import logging
import re

from sklearn.feature_extraction.text import TfidfVectorizer

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


def clean_text(text: str) -> str:
    """Очищает текст от HTML, лишних символов."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip().lower()


def extract_keywords(text: str, top_n: int = 10) -> dict:
    """Извлекает топ биграмм и триграмм через TF-IDF."""
    text = clean_text(text)
    if len(text.split()) < 5:
        return {"bigrams": [], "trigrams": []}

    result = {}

    for ngram_range, label in [((2, 2), "bigrams"), ((3, 3), "trigrams")]:
        try:
            vectorizer = TfidfVectorizer(
                ngram_range=ngram_range,
                max_features=50,
                stop_words=list(STOP_WORDS),
            )
            tfidf_matrix = vectorizer.fit_transform([text])
            feature_names = vectorizer.get_feature_names_out()
            scores = tfidf_matrix.toarray()[0]

            ranked = sorted(
                zip(feature_names, scores),
                key=lambda x: x[1],
                reverse=True,
            )[:top_n]

            result[label] = [[phrase, round(float(score), 4)] for phrase, score in ranked]
        except ValueError:
            result[label] = []

    return result
