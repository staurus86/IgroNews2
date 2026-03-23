"""Tests for self-healing infrastructure: timeouts, source health, watchdog."""

import time
import threading
from core.timeouts import run_with_timeout, get_zombie_thread_count


def test_timeout_returns_default_on_slow_function():
    """Функция дольше таймаута — возвращает default."""
    def slow():
        time.sleep(10)
        return "never"
    result = run_with_timeout(slow, timeout=1, default="timed_out")
    assert result == "timed_out"


def test_timeout_passes_fast_function():
    """Быстрая функция возвращает свой результат."""
    def fast():
        return 42
    result = run_with_timeout(fast, timeout=5, default=None)
    assert result == 42


def test_timeout_catches_exception():
    """Исключение не крашит — возвращает default."""
    def broken():
        raise ValueError("boom")
    result = run_with_timeout(broken, timeout=5, default="safe")
    assert result == "safe"


def test_zombie_thread_counter_increments_on_timeout():
    """Счётчик zombie потоков растёт при таймауте."""
    before = get_zombie_thread_count()
    def hang():
        time.sleep(100)
    run_with_timeout(hang, timeout=0.5, default=None)
    after = get_zombie_thread_count()
    assert after > before


from core.source_health import SourceHealth


def test_source_starts_healthy():
    sh = SourceHealth()
    assert sh.is_healthy("ign") is True


def test_source_disabled_after_failures():
    sh = SourceHealth(threshold=3, cooldown=60)
    sh.record_failure("ign")
    sh.record_failure("ign")
    sh.record_failure("ign")
    assert sh.is_healthy("ign") is False


def test_source_recovers_after_success():
    sh = SourceHealth(threshold=2, cooldown=60)
    sh.record_failure("ign")
    sh.record_failure("ign")
    assert sh.is_healthy("ign") is False
    sh.record_success("ign")
    assert sh.is_healthy("ign") is True


def test_source_status_report_no_deadlock():
    """get_status() не должен deadlock'ить (использует RLock)."""
    sh = SourceHealth(threshold=2, cooldown=60)
    sh.record_success("ign")
    sh.record_failure("pcgamer")
    sh.record_failure("pcgamer")
    report = sh.get_status()
    assert report["ign"]["healthy"] is True
    assert report["pcgamer"]["healthy"] is False
    assert report["pcgamer"]["consecutive_failures"] == 2


# ---------------------------------------------------------------------------
# Watchdog tests
# ---------------------------------------------------------------------------
from unittest.mock import patch
from core.watchdog import Watchdog


def test_watchdog_detects_stale_heartbeat():
    wd = Watchdog(max_stale_seconds=60)
    wd.heartbeat("scheduler")
    with patch("core.watchdog.time") as mock_time:
        mock_time.time.return_value = wd._components["scheduler"]["last_heartbeat"] + 120
        report = wd.check_health()
        assert report["scheduler"]["stale"] is True


def test_watchdog_healthy_heartbeat():
    wd = Watchdog(max_stale_seconds=60)
    wd.heartbeat("scheduler")
    report = wd.check_health()
    assert report["scheduler"]["stale"] is False


def test_watchdog_overall_status():
    wd = Watchdog(max_stale_seconds=60)
    wd.heartbeat("scheduler")
    wd.heartbeat("web")
    assert wd.is_alive() is True


def test_watchdog_recovery_fires_for_stale():
    """Recovery action должен вызываться для stale компонентов."""
    wd = Watchdog(max_stale_seconds=60)
    wd.heartbeat("scheduler")
    recovered = []
    wd.register_recovery("scheduler", lambda: recovered.append("scheduler"))
    with patch("core.watchdog.time") as mock_time:
        mock_time.time.return_value = wd._components["scheduler"]["last_heartbeat"] + 120
        wd.run_recovery()
    assert "scheduler" in recovered


def test_watchdog_recovery_handles_exception():
    """Recovery с ошибкой не крашит watchdog."""
    wd = Watchdog(max_stale_seconds=60)
    wd.heartbeat("scheduler")
    wd.register_recovery("scheduler", lambda: 1/0)
    with patch("core.watchdog.time") as mock_time:
        mock_time.time.return_value = wd._components["scheduler"]["last_heartbeat"] + 120
        wd.run_recovery()  # Не должен крашить


def test_watchdog_system_health_has_thread_info():
    """get_system_health() должен включать zombie threads и active threads."""
    wd = Watchdog(max_stale_seconds=60)
    wd.heartbeat("scheduler")
    health = wd.get_system_health()
    assert "zombie_threads" in health
    assert "active_threads" in health
    assert health["active_threads"] > 0
