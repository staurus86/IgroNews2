"""Tests for feature flag system."""
import os
import sys
import unittest

# Ensure project root is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Force SQLite for tests
os.environ.setdefault("DATABASE_URL", "sqlite:///test_flags.db")


class TestFeatureFlags(unittest.TestCase):
    """Test feature flag CRUD and caching."""

    @classmethod
    def setUpClass(cls):
        """Init test DB."""
        from storage.database import init_db
        init_db()

    def test_default_flags_exist(self):
        from core.feature_flags import get_all_flags
        flags = get_all_flags()
        flag_ids = {f["flag_id"] for f in flags}
        assert "dashboard_v2" in flag_ids
        assert "explainability_v1" in flag_ids
        assert "api_cost_tracking_v1" in flag_ids

    def test_is_enabled_defaults(self):
        from core.feature_flags import is_enabled
        # Phase 0 flags should be enabled by default
        assert is_enabled("api_cost_tracking_v1") is True
        assert is_enabled("decision_trace_v1") is True
        # Key feature flags enabled by default
        assert is_enabled("dashboard_v2") is True
        assert is_enabled("explainability_v1") is True
        assert is_enabled("storyline_mode_v1") is True
        assert is_enabled("source_health_plus_v1") is True
        assert is_enabled("seo_extended_v1") is True

    def test_unknown_flag_returns_false(self):
        from core.feature_flags import is_enabled
        assert is_enabled("nonexistent_flag_xyz") is False

    def test_set_flag(self):
        from core.feature_flags import set_flag, is_enabled, invalidate_cache
        set_flag("dashboard_v2", True, updated_by="test")
        invalidate_cache()
        assert is_enabled("dashboard_v2") is True
        set_flag("dashboard_v2", False, updated_by="test")
        invalidate_cache()
        assert is_enabled("dashboard_v2") is False

    def test_get_all_flags_structure(self):
        from core.feature_flags import get_all_flags
        flags = get_all_flags()
        assert len(flags) > 0
        for f in flags:
            assert "flag_id" in f
            assert "enabled" in f
            assert "description" in f
            assert "phase" in f
            assert isinstance(f["enabled"], bool)

    @classmethod
    def tearDownClass(cls):
        """Cleanup test DB."""
        try:
            os.remove("test_flags.db")
        except OSError:
            pass


class TestObservability(unittest.TestCase):
    """Test observability: cost tracking, decision trace, config audit."""

    @classmethod
    def setUpClass(cls):
        from storage.database import init_db
        init_db()

    def test_track_api_call(self):
        from core.observability import track_api_call, get_cost_summary
        track_api_call("llm", model="test-model", tokens_in=100, tokens_out=50,
                       cost_usd=0.001, latency_ms=500, news_id="test123")
        summary = get_cost_summary(days=1)
        assert summary["total_calls"] >= 1
        assert summary["total_cost_usd"] >= 0.001

    def test_log_decision(self):
        from core.observability import log_decision, get_decision_trace
        log_decision("news_test_1", "review_pipeline", "in_review",
                     reason="test reason", score_after=75)
        trace = get_decision_trace("news_test_1")
        assert len(trace) >= 1
        last = trace[-1]
        assert last["step"] == "review_pipeline"
        assert last["decision"] == "in_review"
        assert last["reason"] == "test reason"

    def test_log_config_change(self):
        from core.observability import log_config_change, get_config_audit
        log_config_change("llm_model", "old-model", "new-model", changed_by="test")
        audit = get_config_audit(limit=5)
        assert len(audit) >= 1
        last = audit[0]
        assert last["setting_name"] == "llm_model"
        assert last["old_value"] == "old-model"

    def test_correlation_id(self):
        from core.observability import set_correlation_id, get_correlation_id
        cid = set_correlation_id("test-cid-123")
        assert cid == "test-cid-123"
        assert get_correlation_id() == "test-cid-123"

    @classmethod
    def tearDownClass(cls):
        try:
            os.remove("test_flags.db")
        except OSError:
            pass


def _calc_final_score_standalone(analysis):
    """Pure-function mirror of scheduler._calc_final_score for testing without apscheduler."""
    import json
    if not analysis:
        return 0
    internal = analysis.get("total_score") or 0
    viral = analysis.get("viral_score") or 0
    headline = analysis.get("headline_score") or 0
    # keyso_bonus
    keyso_raw = analysis.get("keyso_data", "{}")
    if isinstance(keyso_raw, str):
        try:
            keyso = json.loads(keyso_raw)
        except (ValueError, TypeError):
            keyso = {}
    else:
        keyso = keyso_raw or {}
    freq = keyso.get("ws", 0) or 0
    if freq >= 10000: keyso_bonus = 100
    elif freq >= 5000: keyso_bonus = 80
    elif freq >= 1000: keyso_bonus = 60
    elif freq >= 100: keyso_bonus = 40
    elif freq > 0: keyso_bonus = 20
    else: keyso_bonus = 0
    # trends_bonus
    trends_raw = analysis.get("trends_data", "{}")
    if isinstance(trends_raw, str):
        try:
            trends = json.loads(trends_raw)
        except (ValueError, TypeError):
            trends = {}
    else:
        trends = trends_raw or {}
    max_t = max(trends.values()) if trends else 0
    if max_t >= 80: trends_bonus = 100
    elif max_t >= 50: trends_bonus = 70
    elif max_t >= 20: trends_bonus = 40
    elif max_t > 0: trends_bonus = 20
    else: trends_bonus = 0
    final = round(internal * 0.4 + viral * 0.2 + keyso_bonus * 0.15 + trends_bonus * 0.1 + headline * 0.15)
    return final


class TestScoringInvariants(unittest.TestCase):
    """Test that scoring formulas produce consistent results."""

    def test_calc_final_score_basic(self):
        """Ensure final_score formula is stable."""
        import json
        analysis = {
            "total_score": 80,
            "viral_score": 60,
            "headline_score": 70,
            "keyso_data": json.dumps({"ws": 5000}),
            "trends_data": json.dumps({"RU": 50, "US": 30}),
        }
        # internal=80, viral=60, headline=70, keyso_bonus=80, trends_bonus=70
        # final = 80*0.4 + 60*0.2 + 80*0.15 + 70*0.1 + 70*0.15 = 73.5 → 74
        result = _calc_final_score_standalone(analysis)
        assert result == 74, f"Expected 74, got {result}"

    def test_calc_final_score_empty(self):
        result = _calc_final_score_standalone({})
        assert result == 0

    def test_calc_final_score_none(self):
        result = _calc_final_score_standalone(None)
        assert result == 0

    def test_calc_final_score_high_trends(self):
        import json
        analysis = {
            "total_score": 90,
            "viral_score": 80,
            "headline_score": 85,
            "keyso_data": json.dumps({"ws": 15000}),
            "trends_data": json.dumps({"RU": 90, "US": 80}),
        }
        result = _calc_final_score_standalone(analysis)
        # 90*0.4 + 80*0.2 + 100*0.15 + 100*0.1 + 85*0.15 = 36+16+15+10+12.75 = 89.75 → 90
        assert result == 90, f"Expected 90, got {result}"


class TestDatabaseMigrations(unittest.TestCase):
    """Test that new columns are safely added."""

    @classmethod
    def setUpClass(cls):
        from storage.database import init_db
        init_db()

    def test_new_columns_exist(self):
        """Verify Phase 0 columns were added."""
        from storage.database import get_connection, _is_postgres
        conn = get_connection()
        cur = conn.cursor()
        # Check news_analysis has new columns
        cur.execute("SELECT decision_reason, score_breakdown, confidence_score, cluster_id FROM news_analysis LIMIT 0")
        columns = [desc[0] for desc in cur.description] if _is_postgres() else [desc[0] for desc in cur.description]
        assert "decision_reason" in columns
        assert "score_breakdown" in columns
        assert "confidence_score" in columns
        assert "cluster_id" in columns
        cur.close()

    def test_feature_flags_table_exists(self):
        from storage.database import get_connection
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM feature_flags")
        count = cur.fetchone()[0]
        assert count > 0, "Feature flags table should have seeded rows"
        cur.close()

    def test_api_cost_log_table_exists(self):
        from storage.database import get_connection
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM api_cost_log")
        cur.close()

    def test_decision_trace_table_exists(self):
        from storage.database import get_connection
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM decision_trace")
        cur.close()

    def test_config_audit_table_exists(self):
        from storage.database import get_connection
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM config_audit")
        cur.close()

    @classmethod
    def tearDownClass(cls):
        try:
            os.remove("test_flags.db")
        except OSError:
            pass


if __name__ == "__main__":
    unittest.main()
