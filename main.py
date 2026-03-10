import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

import config
from storage.database import init_db
from storage.sheets import setup_headers
from scheduler import start_scheduler
from web import start_web
from apis.cache import setup_dashboard_logging


def main():
    logging.info("IgroNews starting...")

    # Подключаем логи для дашборда
    setup_dashboard_logging()

    # Инициализация БД
    init_db()

    # Создание заголовков в Sheets (если пусто)
    setup_headers()

    # Запуск веб-дашборда на порту 8080
    start_web()
    logging.info("Dashboard running on port 8080")

    # Запуск Telegram-бота (если токен задан)
    if config.TELEGRAM_BOT_TOKEN:
        from bot.telegram_bot import start_bot_polling
        start_bot_polling()
        logging.info("Telegram bot started")

    # Запуск планировщика
    start_scheduler()


if __name__ == "__main__":
    main()
