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
