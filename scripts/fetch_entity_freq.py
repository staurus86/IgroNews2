"""Скрипт для снятия реальной частоты из Keys.so для базы game_entities.

Запуск вручную: python scripts/fetch_entity_freq.py
Обновляет freq в nlp/game_entities.py на основе реальных данных Keys.so.

ВНИМАНИЕ: тратит API Keys.so (~150 запросов). Запускать 1 раз.
"""

import sys
import os
import time
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from apis.keyso import get_keyword_info
from nlp.game_entities import GAME_ENTITIES, STUDIO_ENTITIES, PLATFORM_ENTITIES


def fetch_all():
    results = {}

    all_entities = {}
    all_entities.update({f"game:{k}": k for k in GAME_ENTITIES})
    all_entities.update({f"studio:{k}": k for k in STUDIO_ENTITIES})
    all_entities.update({f"platform:{k}": k for k in PLATFORM_ENTITIES})

    total = len(all_entities)
    print(f"Total entities to check: {total}")

    for i, (key, name) in enumerate(all_entities.items()):
        print(f"[{i+1}/{total}] {name}...", end=" ")
        try:
            info = get_keyword_info(name)
            ws = info.get("ws", 0)
            results[key] = {"name": name, "ws": ws}
            print(f"ws={ws}")
        except Exception as e:
            print(f"ERROR: {e}")
            results[key] = {"name": name, "ws": 0}

        time.sleep(1.5)  # Rate limit

    # Сохраняем raw результат
    out_path = os.path.join(os.path.dirname(__file__), "entity_freq_results.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"\nResults saved to {out_path}")

    # Нормализуем ws -> freq (0-100)
    all_ws = [v["ws"] for v in results.values() if v["ws"] > 0]
    if all_ws:
        max_ws = max(all_ws)
        print(f"\nMax ws: {max_ws}")
        print("\n--- Suggested freq values ---")
        for key, data in sorted(results.items(), key=lambda x: x[1]["ws"], reverse=True):
            if data["ws"] > 0:
                freq = max(5, round(data["ws"] / max_ws * 100))
                print(f'    "{data["name"]}": freq={freq} (ws={data["ws"]})')


if __name__ == "__main__":
    if not config.KEYSO_API_KEY:
        print("ERROR: KEYSO_API_KEY not set")
        sys.exit(1)
    fetch_all()
