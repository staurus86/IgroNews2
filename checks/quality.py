def check_quality(news: dict) -> dict:
    issues = []
    score = 100

    plain_text = news.get("plain_text", "")
    title = news.get("title", "")

    if len(plain_text) < 150:
        issues.append("Текст слишком короткий")
        score -= 40

    if len(title) < 20:
        issues.append("Заголовок слишком короткий")
        score -= 20

    clickbait = ["ШОК", "НЕВЕРОЯТНО", "ВЫ НЕ ПОВЕРИТЕ", "!!!", "???"]
    if any(c in title.upper() for c in clickbait):
        issues.append("Возможный кликбейт")
        score -= 15

    if not news.get("description"):
        issues.append("Нет description")
        score -= 10

    return {"score": max(0, score), "issues": issues, "pass": score >= 50}
