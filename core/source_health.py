"""Трекер здоровья источников: автоотключение при сбоях, автовключение при восстановлении."""

import logging
import threading
import time

logger = logging.getLogger(__name__)


class SourceHealth:
    def __init__(self, threshold=5, cooldown=600):
        """
        threshold: сколько подряд ошибок для отключения
        cooldown: через сколько секунд пробуем снова (probe)
        """
        # RLock для безопасного вызова is_healthy() из get_status()
        self._lock = threading.RLock()
        self._threshold = threshold
        self._cooldown = cooldown
        self._sources: dict[str, dict] = {}

    def _ensure(self, source: str):
        if source not in self._sources:
            self._sources[source] = {
                "failures": 0, "total_failures": 0, "total_success": 0,
                "disabled_at": None, "last_error": "",
            }

    def record_success(self, source: str):
        with self._lock:
            self._ensure(source)
            s = self._sources[source]
            was_disabled = s["disabled_at"] is not None
            s["failures"] = 0
            s["total_success"] += 1
            s["disabled_at"] = None
            s["last_error"] = ""
            if was_disabled:
                logger.info("Source RECOVERED: %s", source)

    def record_failure(self, source: str, error: str = ""):
        with self._lock:
            self._ensure(source)
            s = self._sources[source]
            s["failures"] += 1
            s["total_failures"] += 1
            s["last_error"] = str(error)[:200]
            if s["failures"] >= self._threshold:
                if s["disabled_at"] is None:
                    logger.warning("Source DISABLED: %s after %d consecutive failures: %s",
                                   source, s["failures"], error)
                # Always refresh disabled_at to restart cooldown timer (including after failed probe)
                s["disabled_at"] = time.time()

    def is_healthy(self, source: str) -> bool:
        with self._lock:
            self._ensure(source)
            s = self._sources[source]
            if s["disabled_at"] is None:
                return True
            if time.time() - s["disabled_at"] > self._cooldown:
                logger.info("Source PROBE: %s — trying again after %ds cooldown",
                            source, self._cooldown)
                return True
            return False

    def get_status(self) -> dict:
        """Возвращает статус всех источников. Safe: RLock позволяет вызов is_healthy()."""
        with self._lock:
            return {
                name: {
                    "healthy": self.is_healthy(name),
                    "consecutive_failures": s["failures"],
                    "total_failures": s["total_failures"],
                    "total_success": s["total_success"],
                    "last_error": s["last_error"],
                    "disabled_at": s["disabled_at"],
                }
                for name, s in self._sources.items()
            }


# Глобальный инстанс
source_health = SourceHealth(threshold=5, cooldown=600)
