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
