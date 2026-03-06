from datetime import datetime, timezone


def check_freshness(news: dict) -> dict:
    published_at = news.get("published_at", "") or news.get("parsed_at", "")
    if not published_at:
        return {"age_hours": -1, "status": "unknown", "score": 30, "pass": True}

    try:
        pub = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        if pub.tzinfo is None:
            pub = pub.replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - pub).total_seconds() / 3600
    except Exception:
        return {"age_hours": -1, "status": "unknown", "score": 30, "pass": True}

    if age_hours < 2:
        status = "hot"
        score = 100
    elif age_hours < 6:
        status = "fresh"
        score = 80
    elif age_hours < 24:
        status = "today"
        score = 50
    elif age_hours < 72:
        status = "recent"
        score = 25
    else:
        status = "old"
        score = 10

    return {
        "age_hours": round(age_hours, 1),
        "status": status,
        "score": score,
        "pass": age_hours < 72,
    }
