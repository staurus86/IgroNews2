import logging
from pytrends.request import TrendReq
import config

logger = logging.getLogger(__name__)


def get_trends_for_keyword(keyword: str) -> dict:
    """Проверяет популярность ключевого слова в Google Trends по регионам (кэш 6ч)."""
    from apis.cache import cache_get, cache_set, cache_key, rate_check, rate_increment
    ck = cache_key("trends", keyword)
    cached = cache_get(ck)
    if cached is not None:
        return cached
    if not rate_check("trends"):
        logger.warning("Google Trends rate limit exceeded")
        return {r: 0 for r in config.REGIONS}

    result = {}
    try:
        pytrends = TrendReq(hl="ru-RU", tz=180)
        for region in config.REGIONS:
            rate_increment("trends")
            try:
                pytrends.build_payload([keyword], cat=0, timeframe="now 1-d", geo=region)
                data = pytrends.interest_over_time()
                if not data.empty and keyword in data.columns:
                    result[region] = int(data[keyword].iloc[-1])
                else:
                    result[region] = 0
            except Exception as e:
                logger.warning("Trends error for %s/%s: %s", keyword, region, e)
                result[region] = 0
    except Exception as e:
        logger.error("Google Trends init error: %s", e)
        result = {r: 0 for r in config.REGIONS}

    cache_set(ck, result, ttl=86400)  # 24 hours
    return result
