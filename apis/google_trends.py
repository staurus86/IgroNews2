import logging
from pytrends.request import TrendReq
import config

logger = logging.getLogger(__name__)


def get_trends_for_keyword(keyword: str) -> dict:
    """Проверяет популярность ключевого слова в Google Trends по регионам."""
    result = {}
    try:
        pytrends = TrendReq(hl="ru-RU", tz=180)
        for region in config.REGIONS:
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

    return result
