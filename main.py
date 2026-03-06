import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

from storage.database import init_db
from storage.sheets import setup_headers
from scheduler import start_scheduler


def main():
    logging.info("IgroNews starting...")

    # Инициализация БД
    init_db()

    # Создание заголовков в Sheets (если пусто)
    setup_headers()

    # Запуск планировщика
    start_scheduler()


if __name__ == "__main__":
    main()
