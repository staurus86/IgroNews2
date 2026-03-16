"""Stability audit tests — TDD for critical fixes.

Tests for:
1. Circuit breaker auto-reset after timeout
2. Full-auto pipeline: rewrite fail resets news status
3. Full-auto pipeline: Sheets fail sets correct status
4. Cursor leak in _export_all_processed (source code check)
5. Double-click protection on pipeline buttons (source code check)
"""
import os
import sys
import time
import unittest
from unittest.mock import patch, MagicMock

# Ensure project root is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Force SQLite for tests
os.environ["DATABASE_URL"] = "sqlite:///test_stability.db"

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _mock_heavy_imports():
    """Mock modules that aren't installed in test env."""
    import types
    for mod_name in [
        'pytrends', 'pytrends.request',
        'gspread', 'gspread.exceptions', 'gspread.utils',
        'nltk', 'nltk.corpus', 'nltk.tokenize',
        'sklearn', 'sklearn.feature_extraction', 'sklearn.feature_extraction.text',
        'sklearn.metrics', 'sklearn.metrics.pairwise',
        'feedparser',
        'openai',
        'google', 'google.oauth2', 'google.oauth2.service_account',
    ]:
        if mod_name not in sys.modules:
            sys.modules[mod_name] = types.ModuleType(mod_name)

    # bs4 needs BeautifulSoup as importable attribute
    bs4_mock = types.ModuleType('bs4')
    bs4_mock.BeautifulSoup = MagicMock
    sys.modules['bs4'] = bs4_mock

    # feedparser needs to be callable
    sys.modules['feedparser'].parse = MagicMock(return_value={"entries": []})

    # pytrends.request needs TrendReq
    sys.modules['pytrends.request'].TrendReq = MagicMock

    # gspread needs service_account_from_dict
    sys.modules['gspread'].service_account_from_dict = MagicMock()
    sys.modules['gspread'].exceptions = types.ModuleType('gspread.exceptions')
    sys.modules['gspread'].exceptions.APIError = type('APIError', (Exception,), {})
    sys.modules['gspread.exceptions'].APIError = sys.modules['gspread'].exceptions.APIError

    # sklearn
    sys.modules['sklearn.feature_extraction.text'].TfidfVectorizer = MagicMock
    sys.modules['sklearn.metrics.pairwise'].cosine_similarity = MagicMock

    # openai needs OpenAI class
    sys.modules['openai'].OpenAI = MagicMock

    # google.oauth2.service_account needs Credentials
    sys.modules['google.oauth2.service_account'].Credentials = MagicMock


_mock_heavy_imports()


class TestCircuitBreaker(unittest.TestCase):
    """Circuit breaker must auto-reset after timeout period."""

    def setUp(self):
        import scheduler
        scheduler._api_failures = {}
        if hasattr(scheduler, '_api_failure_times'):
            scheduler._api_failure_times = {}

    def test_circuit_opens_after_threshold(self):
        from scheduler import _api_record_failure, _api_circuit_open
        for _ in range(5):
            _api_record_failure("keyso")
        self.assertTrue(_api_circuit_open("keyso"))

    def test_circuit_closed_below_threshold(self):
        from scheduler import _api_record_failure, _api_circuit_open
        for _ in range(4):
            _api_record_failure("keyso")
        self.assertFalse(_api_circuit_open("keyso"))

    def test_success_resets_circuit(self):
        from scheduler import _api_record_failure, _api_record_success, _api_circuit_open
        for _ in range(5):
            _api_record_failure("keyso")
        self.assertTrue(_api_circuit_open("keyso"))
        _api_record_success("keyso")
        self.assertFalse(_api_circuit_open("keyso"))

    def test_circuit_auto_resets_after_timeout(self):
        """Open circuit should auto-reset after CIRCUIT_RESET_SECONDS."""
        from scheduler import _api_record_failure, _api_circuit_open
        for _ in range(5):
            _api_record_failure("keyso")
        self.assertTrue(_api_circuit_open("keyso"))

        import scheduler
        self.assertTrue(hasattr(scheduler, '_api_failure_times'),
                        "scheduler must have _api_failure_times dict for timed reset")
        scheduler._api_failure_times["keyso"] = time.time() - 400  # > 300s

        self.assertFalse(_api_circuit_open("keyso"),
                         "Circuit should auto-reset after timeout period")

    def test_circuit_stays_open_within_timeout(self):
        """Open circuit should stay open before timeout expires."""
        from scheduler import _api_record_failure, _api_circuit_open
        for _ in range(5):
            _api_record_failure("keyso")

        import scheduler
        self.assertTrue(hasattr(scheduler, '_api_failure_times'),
                        "scheduler must have _api_failure_times dict for timed reset")
        scheduler._api_failure_times["keyso"] = time.time() - 60  # < 300s

        self.assertTrue(_api_circuit_open("keyso"),
                        "Circuit should remain open before timeout")

    def test_independent_services(self):
        from scheduler import _api_record_failure, _api_circuit_open
        for _ in range(5):
            _api_record_failure("keyso")
        self.assertTrue(_api_circuit_open("keyso"))
        self.assertFalse(_api_circuit_open("llm"))
        self.assertFalse(_api_circuit_open("trends"))


class TestFullAutoPipelineRewriteFail(unittest.TestCase):
    """When LLM rewrite fails, news status must be reset (not stuck in 'approved')."""

    @patch('scheduler._update_task')
    @patch('scheduler._fetch_analysis_by_id')
    @patch('scheduler._fetch_news_by_id')
    @patch('scheduler._do_process')
    @patch('scheduler._calc_final_score')
    @patch('scheduler.update_news_status')
    @patch('checks.pipeline.run_review_pipeline')
    @patch('apis.llm.rewrite_news', return_value=None)
    def test_rewrite_fail_resets_status(self, mock_rewrite, mock_review, mock_status,
                                        mock_calc, mock_process, mock_fetch_news,
                                        mock_fetch_analysis, mock_update_task):
        from scheduler import run_full_auto_pipeline

        mock_fetch_news.return_value = {
            "id": "test1", "title": "Test News", "plain_text": "Some text",
            "status": "new", "source": "IGN"
        }
        mock_fetch_analysis.side_effect = [
            None,
            {"total_score": 80, "viral_score": 50, "keyso_data": "{}", "trends_data": "{}",
             "headline_score": 70, "entity_names": "[]"},
        ]
        mock_process.return_value = {"recommendation": "publish"}
        mock_calc.return_value = 75
        mock_review.return_value = {
            "results": [{"total_score": 80, "is_duplicate": False, "auto_rejected": False}]
        }

        run_full_auto_pipeline(["test1"], ["task1"])

        status_calls = [c for c in mock_status.call_args_list if c[0][0] == "test1"]
        last_status = status_calls[-1][0][1] if status_calls else None
        self.assertEqual(last_status, "in_review",
                         f"After rewrite failure, status should be 'in_review', got '{last_status}'")


class TestFullAutoPipelineSheetsFail(unittest.TestCase):
    """When Sheets export fails, status should NOT be 'ready'."""

    @patch('scheduler._update_task')
    @patch('scheduler._fetch_analysis_by_id')
    @patch('scheduler._fetch_news_by_id')
    @patch('scheduler._do_process')
    @patch('scheduler._calc_final_score')
    @patch('scheduler._save_rewrite_article')
    @patch('scheduler.update_news_status')
    @patch('checks.pipeline.run_review_pipeline')
    @patch('apis.llm.rewrite_news')
    @patch('storage.sheets.write_ready_row', side_effect=Exception("Sheets 429"))
    @patch('scheduler.time')
    def test_sheets_fail_status_not_ready(self, mock_time, mock_sheets, mock_rewrite,
                                           mock_review, mock_status,
                                           mock_save_article, mock_calc, mock_process,
                                           mock_fetch_news, mock_fetch_analysis, mock_update_task):
        from scheduler import run_full_auto_pipeline

        mock_time.sleep = MagicMock()
        mock_fetch_news.return_value = {
            "id": "test2", "title": "Test", "plain_text": "Text",
            "status": "new", "source": "IGN"
        }
        mock_fetch_analysis.side_effect = [
            None,
            {"total_score": 85, "viral_score": 60, "keyso_data": "{}", "trends_data": "{}",
             "headline_score": 80, "entity_names": "[]"},
        ]
        mock_process.return_value = {"recommendation": "publish"}
        mock_calc.return_value = 80
        mock_rewrite.return_value = {"title": "Rewritten", "text": "Rewritten text",
                                     "seo_title": "SEO", "seo_description": "Desc"}
        mock_review.return_value = {
            "results": [{"total_score": 85, "is_duplicate": False, "auto_rejected": False}]
        }

        run_full_auto_pipeline(["test2"], ["task2"])

        status_calls = [c for c in mock_status.call_args_list if c[0][0] == "test2"]
        last_status = status_calls[-1][0][1] if status_calls else None
        self.assertNotEqual(last_status, "ready",
                            "Status should not be 'ready' when Sheets export failed")


class TestCursorLeakExportProcessed(unittest.TestCase):
    """cur2 in _export_all_processed must be closed even on exception."""

    def test_cursor_closed_on_exception(self):
        """Verify cur2 is properly wrapped in try/finally."""
        web_path = os.path.join(PROJECT_ROOT, "web.py")
        with open(web_path, "r", encoding="utf-8") as f:
            source = f.read()

        # Find _export_all_processed method
        func_start = source.find("def _export_all_processed")
        self.assertNotEqual(func_start, -1, "_export_all_processed must exist")

        # Find next def (end of function)
        next_def = source.find("\n    def ", func_start + 1)
        func_body = source[func_start:next_def] if next_def != -1 else source[func_start:]

        # Find cur2 = conn.cursor()
        cur2_pos = func_body.find("cur2")
        self.assertNotEqual(cur2_pos, -1, "cur2 should exist in _export_all_processed")

        # After cur2, there must be try/finally before cur2.close()
        after_cur2 = func_body[cur2_pos:]
        try_pos = after_cur2.find("try:")
        finally_pos = after_cur2.find("finally:")
        close_pos = after_cur2.find("cur2.close()")

        self.assertNotEqual(finally_pos, -1,
                            "cur2 must be wrapped in try/finally to prevent cursor leak")
        self.assertNotEqual(close_pos, -1,
                            "cur2.close() must exist")
        if finally_pos != -1 and close_pos != -1:
            self.assertLess(finally_pos, close_pos,
                            "cur2.close() must be inside finally block")


class TestPipelineButtonDoubleClick(unittest.TestCase):
    """JS pipeline buttons must be disabled immediately on click."""

    def _get_web_source(self):
        web_path = os.path.join(PROJECT_ROOT, "web.py")
        with open(web_path, "r", encoding="utf-8") as f:
            return f.read()

    def test_full_auto_disables_before_api(self):
        """runFullAuto() must disable buttons before calling API."""
        source = self._get_web_source()

        func_start = source.find('async function runFullAuto()')
        self.assertNotEqual(func_start, -1)

        func_body = source[func_start:func_start + 2000]
        api_call_pos = func_body.find("api('/api/pipeline/full_auto'")
        self.assertNotEqual(api_call_pos, -1, "API call must exist")

        # Look for any disable mechanism before the api call
        before_api = func_body[:api_call_pos]
        has_disable = (
            "disabled = true" in before_api or
            "_pipelineRunning" in before_api or
            "setPipelineActive" in before_api
        )
        self.assertTrue(has_disable,
                        "Buttons must be disabled before API call to prevent double-click")

    def test_no_llm_disables_before_api(self):
        """runNoLLM() must disable buttons before calling API."""
        source = self._get_web_source()

        func_start = source.find('async function runNoLLM()')
        self.assertNotEqual(func_start, -1)

        func_body = source[func_start:func_start + 2000]
        api_call_pos = func_body.find("api('/api/pipeline/no_llm'")
        self.assertNotEqual(api_call_pos, -1, "API call must exist")

        before_api = func_body[:api_call_pos]
        has_disable = (
            "disabled = true" in before_api or
            "_pipelineRunning" in before_api or
            "setPipelineActive" in before_api
        )
        self.assertTrue(has_disable,
                        "Buttons must be disabled before API call to prevent double-click")


if __name__ == "__main__":
    unittest.main()
