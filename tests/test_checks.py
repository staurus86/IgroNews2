"""Unit tests for checks modules — pure logic, no external dependencies."""

import unittest
from datetime import datetime, timezone, timedelta


class TestQuality(unittest.TestCase):
    def setUp(self):
        from checks.quality import check_quality
        self.check = check_quality

    def test_full_quality_news(self):
        news = {"title": "This is a valid game title", "description": "Some desc", "plain_text": "A" * 200}
        result = self.check(news)
        self.assertEqual(result["score"], 100)
        self.assertTrue(result["pass"])
        self.assertEqual(result["issues"], [])

    def test_empty_text(self):
        news = {"title": "Valid title here enough", "description": "", "plain_text": ""}
        result = self.check(news)
        self.assertLessEqual(result["score"], 50)
        self.assertIn("Нет текста", result["issues"])

    def test_short_text(self):
        news = {"title": "Valid title here enough", "description": "", "plain_text": "A" * 80}
        result = self.check(news)
        self.assertIn("Текст слишком короткий", result["issues"])

    def test_short_title(self):
        news = {"title": "Short", "description": "desc", "plain_text": "A" * 200}
        result = self.check(news)
        self.assertIn("Заголовок слишком короткий", result["issues"])

    def test_clickbait_detected(self):
        news = {"title": "ШОК!!! Новая игра!", "description": "desc", "plain_text": "A" * 200}
        result = self.check(news)
        self.assertIn("Возможный кликбейт", result["issues"])

    def test_no_description_penalty(self):
        news = {"title": "Valid title here enough", "description": "", "plain_text": "A" * 200}
        result = self.check(news)
        self.assertIn("Нет description", result["issues"])

    def test_score_never_negative(self):
        news = {"title": "", "description": "", "plain_text": ""}
        result = self.check(news)
        self.assertGreaterEqual(result["score"], 0)

    def test_empty_news(self):
        result = self.check({})
        self.assertGreaterEqual(result["score"], 0)


class TestFreshness(unittest.TestCase):
    def setUp(self):
        from checks.freshness import check_freshness
        self.check = check_freshness

    def test_hot_news(self):
        now = datetime.now(timezone.utc)
        news = {"published_at": (now - timedelta(hours=1)).isoformat()}
        result = self.check(news)
        self.assertEqual(result["status"], "hot")
        self.assertEqual(result["score"], 100)
        self.assertTrue(result["pass"])

    def test_fresh_news(self):
        now = datetime.now(timezone.utc)
        news = {"published_at": (now - timedelta(hours=4)).isoformat()}
        result = self.check(news)
        self.assertEqual(result["status"], "fresh")
        self.assertEqual(result["score"], 80)

    def test_today_news(self):
        now = datetime.now(timezone.utc)
        news = {"published_at": (now - timedelta(hours=12)).isoformat()}
        result = self.check(news)
        self.assertEqual(result["status"], "today")
        self.assertEqual(result["score"], 50)

    def test_old_news(self):
        now = datetime.now(timezone.utc)
        news = {"published_at": (now - timedelta(days=5)).isoformat()}
        result = self.check(news)
        self.assertEqual(result["status"], "old")
        self.assertFalse(result["pass"])

    def test_no_date(self):
        result = self.check({})
        self.assertEqual(result["status"], "unknown")
        self.assertFalse(result["pass"])

    def test_fallback_to_parsed_at(self):
        now = datetime.now(timezone.utc)
        news = {"published_at": "", "parsed_at": (now - timedelta(hours=1)).isoformat()}
        result = self.check(news)
        self.assertEqual(result["status"], "hot")

    def test_rfc822_date(self):
        # RSS-style date
        news = {"published_at": "Wed, 18 Mar 2026 10:00:00 GMT"}
        result = self.check(news)
        self.assertIn(result["status"], ["hot", "fresh", "today", "recent", "old"])


class TestHeadlineScore(unittest.TestCase):
    def setUp(self):
        from checks.headline_score import headline_score
        self.score = headline_score

    def test_base_score(self):
        result = self.score({"title": "A normal game news headline here"})
        self.assertGreaterEqual(result["score"], 0)
        self.assertLessEqual(result["score"], 100)

    def test_breaking_news_bonus(self):
        result = self.score({"title": "Breaking: New game announced today"})
        self.assertGreater(result["score"], 50)
        triggers = [t["id"] for t in result["triggers"]]
        self.assertIn("breaking", triggers)

    def test_exclusive_bonus(self):
        result = self.score({"title": "Exclusive first look at the upcoming RPG"})
        triggers = [t["id"] for t in result["triggers"]]
        self.assertIn("exclusive", triggers)

    def test_question_bonus(self):
        result = self.score({"title": "Is this the best game of the year?"})
        triggers = [t["id"] for t in result["triggers"]]
        self.assertIn("question", triggers)

    def test_too_short_penalty(self):
        result = self.score({"title": "Short"})
        triggers = [t["id"] for t in result["triggers"]]
        self.assertIn("too_short", triggers)

    def test_optimal_length_bonus(self):
        result = self.score({"title": "This is an optimal length headline for SEO purposes"})
        triggers = [t["id"] for t in result["triggers"]]
        self.assertIn("optimal_length", triggers)

    def test_empty_title(self):
        result = self.score({"title": ""})
        self.assertGreaterEqual(result["score"], 0)

    def test_level_classification(self):
        result = self.score({"title": "Breaking: Official confirmed world premiere of the new game"})
        self.assertIn(result["level"], ["low", "medium", "high"])


class TestSentiment(unittest.TestCase):
    def setUp(self):
        from checks.sentiment import analyze_sentiment
        self.analyze = analyze_sentiment

    def test_positive_text(self):
        news = {"title": "Шедевр года!", "plain_text": "Великолепная игра получила награду GOTY"}
        result = self.analyze(news)
        self.assertEqual(result["label"], "positive")
        self.assertGreater(result["score"], 0)

    def test_negative_text(self):
        news = {"title": "Провал года", "plain_text": "Ужасная игра с багами и вылетами, скандал и бойкот"}
        result = self.analyze(news)
        self.assertEqual(result["label"], "negative")
        self.assertLess(result["score"], 0)

    def test_neutral_text(self):
        news = {"title": "Game update released", "plain_text": "Version 1.2 is now available for download"}
        result = self.analyze(news)
        self.assertEqual(result["label"], "neutral")

    def test_empty_text(self):
        result = self.analyze({})
        self.assertEqual(result["label"], "neutral")
        self.assertEqual(result["score"], 0.0)

    def test_english_positive(self):
        news = {"title": "Amazing game wins award", "plain_text": "Incredible masterpiece praised by critics"}
        result = self.analyze(news)
        self.assertEqual(result["label"], "positive")

    def test_score_range(self):
        news = {"title": "test", "plain_text": "отлично шедевр лучший"}
        result = self.analyze(news)
        self.assertGreaterEqual(result["score"], -1.0)
        self.assertLessEqual(result["score"], 1.0)


class TestDateParsing(unittest.TestCase):
    def setUp(self):
        from checks.freshness import _parse_date
        self.parse = _parse_date

    def test_iso_format(self):
        dt = self.parse("2026-03-18T10:00:00+00:00")
        self.assertIsNotNone(dt)
        self.assertEqual(dt.year, 2026)

    def test_iso_z_format(self):
        dt = self.parse("2026-03-18T10:00:00Z")
        self.assertIsNotNone(dt)

    def test_rfc822(self):
        dt = self.parse("Wed, 18 Mar 2026 10:00:00 GMT")
        self.assertIsNotNone(dt)

    def test_russian_format(self):
        dt = self.parse("18.03.2026 10:30")
        self.assertIsNotNone(dt)

    def test_empty_string(self):
        self.assertIsNone(self.parse(""))

    def test_none(self):
        self.assertIsNone(self.parse(None))

    def test_garbage(self):
        self.assertIsNone(self.parse("not a date at all"))


class TestDatabaseHelpers(unittest.TestCase):
    def test_db_cursor_context_manager(self):
        from storage.database import db_cursor
        with db_cursor() as cur:
            cur.execute("SELECT 1")
            row = cur.fetchone()
            self.assertIsNotNone(row)

    def test_ph_returns_placeholder(self):
        from storage.database import ph
        p = ph()
        self.assertIn(p, ["%s", "?"])

    def test_rows_to_dicts(self):
        from storage.database import db_cursor, rows_to_dicts
        with db_cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS _test_helpers (id TEXT, val TEXT)")
            cur.execute("INSERT OR IGNORE INTO _test_helpers VALUES ('1', 'hello')")
            cur.execute("SELECT * FROM _test_helpers WHERE id = '1'")
            rows = rows_to_dicts(cur)
            self.assertIsInstance(rows, list)
            if rows:
                self.assertIsInstance(rows[0], dict)
                self.assertEqual(rows[0]["id"], "1")

    def test_cursor_closed_after_context(self):
        from storage.database import db_cursor
        with db_cursor() as cur:
            cur.execute("SELECT 1")
        # Cursor should be closed — calling execute should fail
        with self.assertRaises(Exception):
            cur.execute("SELECT 1")


class TestCircuitBreakerThreadSafety(unittest.TestCase):
    def test_circuit_breaker_with_lock(self):
        try:
            from scheduler import _api_record_failure, _api_record_success, _api_circuit_open
        except ImportError:
            self.skipTest("scheduler dependencies not available locally")
        service = "test_service_ts"
        _api_record_success(service)
        self.assertFalse(_api_circuit_open(service))
        for _ in range(5):
            _api_record_failure(service)
        self.assertTrue(_api_circuit_open(service))
        _api_record_success(service)
        self.assertFalse(_api_circuit_open(service))

    def test_pipeline_stop_event(self):
        try:
            from scheduler import pipeline_stop, pipeline_reset, is_pipeline_stopped
        except ImportError:
            self.skipTest("scheduler dependencies not available locally")
        pipeline_reset()
        self.assertFalse(is_pipeline_stopped())
        pipeline_stop()
        self.assertTrue(is_pipeline_stopped())
        pipeline_reset()
        self.assertFalse(is_pipeline_stopped())


if __name__ == "__main__":
    unittest.main()
