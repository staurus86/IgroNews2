import json
import hmac
import os
import hashlib
import secrets
import threading
import logging
import time as _time
from http.server import HTTPServer, BaseHTTPRequestHandler
from http.cookies import SimpleCookie
from urllib.parse import urlparse, parse_qs

from storage.database import get_connection, _is_postgres, get_unprocessed_news, update_news_status, init_db

logger = logging.getLogger(__name__)
PORT = int(os.getenv("PORT", 8080))

# Secret key for signing cookies — stable across redeploys via env var
_COOKIE_SECRET = os.getenv("COOKIE_SECRET", "igronews-default-secret-key-2024")

# Users: {username: {"hash": password_hash, "role": "admin"|"editor"|"viewer"}}
USERS = {
    "admin": {"hash": hashlib.sha256("admin123".encode()).hexdigest(), "role": "admin"},
}

# Role permissions
ROLE_PERMISSIONS = {
    "admin": {"read", "write", "approve", "settings", "users", "flags", "delete", "pipeline"},
    "editor": {"read", "write", "approve", "pipeline"},
    "viewer": {"read"},
}

def _user_role(username: str) -> str:
    entry = USERS.get(username, {})
    if isinstance(entry, dict):
        return entry.get("role", "editor")
    return "editor"  # legacy compat

def _user_has_perm(username: str, perm: str) -> bool:
    role = _user_role(username)
    return perm in ROLE_PERMISSIONS.get(role, set())


def _sign_cookie(username: str) -> str:
    """Создаёт подписанную куку: username.expiry.signature"""
    expiry = int(_time.time()) + 86400 * 7  # 7 дней
    payload = f"{username}:{expiry}"
    sig = hmac.new(_COOKIE_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()[:24]
    return f"{payload}:{sig}"


def _verify_cookie(value: str) -> str | None:
    """Проверяет подписанную куку. Возвращает username или None."""
    try:
        parts = value.split(":")
        if len(parts) != 3:
            return None
        username, expiry_str, sig = parts
        expiry = int(expiry_str)
        if _time.time() > expiry:
            return None
        payload = f"{username}:{expiry_str}"
        expected = hmac.new(_COOKIE_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()[:24]
        if not hmac.compare_digest(sig, expected):
            return None
        if username not in USERS:
            return None
        return username
    except Exception:
        return None


class AdminHandler(BaseHTTPRequestHandler):

    def _get_session_user(self):
        cookie_header = self.headers.get("Cookie", "")
        cookie = SimpleCookie(cookie_header)
        token = cookie.get("session")
        if token:
            return _verify_cookie(token.value)
        return None

    def _require_auth(self):
        """Returns True if authorized, False if redirected to login."""
        if self._get_session_user():
            return True
        path = urlparse(self.path).path
        if path == "/login":
            return True
        if path.startswith("/api/"):
            self._json({"error": "unauthorized"}, 401)
        else:
            self.send_response(302)
            self.send_header("Location", "/login")
            self.end_headers()
        return False

    def do_GET(self):
        path = urlparse(self.path).path
        # Static files that don't require auth
        if path == "/robots.txt":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"User-agent: *\nDisallow: /\n")
            return
        if path == "/favicon.ico":
            self.send_response(204)
            self.end_headers()
            return
        if path == "/login":
            self._serve_login()
            return
        if path == "/logout":
            self._do_logout()
            return
        if path == "/api/diag":
            self._serve_diag()
            return
        if not self._require_auth():
            return

        routes = {
            "/": self._serve_dashboard,
            "/api/stats": lambda: self._json(self._get_stats()),
            "/api/pipeline/status": lambda: self._json(self._get_pipeline_status()),
            "/api/news": lambda: self._json(self._get_news()),
            "/api/sources": lambda: self._json(self._get_sources()),
            "/api/prompts": lambda: self._json(self._get_prompts()),
            "/api/settings": lambda: self._json(self._get_settings()),
            "/api/users": lambda: self._json(self._get_users()),
            "/api/health": lambda: self._json(self._get_health()),
            "/api/dashboard_groups": lambda: self._dashboard_groups(),
            "/api/sources_stats": lambda: self._json(self._get_sources_stats()),
            "/api/db_info": lambda: self._json(self._get_db_info()),
            "/api/articles": lambda: self._json(self._get_articles()),
            "/api/queue": lambda: self._json(self._get_queue()),
            "/api/analytics": lambda: self._json(self._get_analytics()),
            "/api/prompt_versions": lambda: self._json(self._get_prompt_versions()),
            "/api/viral": lambda: self._json(self._get_viral()),
            "/api/logs": lambda: self._json(self._get_logs()),
            "/api/rate_stats": lambda: self._json(self._get_rate_stats()),
            "/api/cache_stats": lambda: self._json(self._get_cache_stats()),
            "/api/editorial": lambda: self._json(self._get_editorial()),
            "/api/moderation_list": lambda: self._json(self._get_moderation_list()),
            "/api/digests": lambda: self._json(self._get_digests()),
            "/api/viral_triggers": lambda: self._json(self._get_viral_triggers()),
            "/api/final": lambda: self._json(self._get_final()),
            # Phase 0: feature flags, observability, dashboard v2
            "/api/feature_flags": lambda: self._json(self._get_feature_flags()),
            "/api/cost_summary": lambda: self._json(self._get_cost_summary()),
            "/api/config_audit": lambda: self._json(self._get_config_audit()),
            "/api/ops_dashboard": lambda: self._json(self._get_ops_dashboard()),
            # Phase 3: analytics funnel, source intelligence
            "/api/analytics/funnel": lambda: self._json(self._get_funnel_analytics()),
            "/api/analytics/cost_by_source": lambda: self._json(self._get_cost_by_source()),
            # Phase 4: storylines, source health plus, threshold simulator
            "/api/storylines": lambda: self._json(self._get_storylines()),
            "/api/source_health_plus": lambda: self._json(self._get_source_health_plus()),
        }

        # DOCX download (GET with query param)
        if path == "/api/articles/docx":
            from urllib.parse import parse_qs
            qs = parse_qs(urlparse(self.path).query)
            aid = qs.get("id", [""])[0]
            if aid:
                self._serve_docx(aid)
            else:
                self._json({"error": "id required"}, 400)
            return

        # Bulk DOCX (ZIP with multiple DOCX)
        if path == "/api/articles/docx_bulk":
            from urllib.parse import parse_qs
            qs = parse_qs(urlparse(self.path).query)
            ids = qs.get("ids", [""])[0]
            if ids:
                self._serve_docx_bulk(ids.split(","))
            else:
                self._json({"error": "ids required"}, 400)
            return

        # Event chain (temporal clusters)
        if path == "/api/event_chain":
            qs = parse_qs(urlparse(self.path).query)
            news_id = qs.get("news_id", [""])[0]
            if news_id:
                self._json(self._get_event_chain_by_id(news_id))
            else:
                self._json({"error": "news_id required"}, 400)
            return

        handler = routes.get(path)
        if handler:
            handler()
        else:
            self._json({"error": "not found"}, 404)

    def do_POST(self):
        path = urlparse(self.path).path
        body = self._read_body()

        if path == "/api/login":
            self._do_login(body)
            return
        if not self._require_auth():
            return

        routes = {
            "/api/process": self._run_process,
            "/api/process_one": lambda: self._process_one(body),
            "/api/export_sheets": lambda: self._export_sheets(body),
            "/api/sources/add": lambda: self._add_source(body),
            "/api/sources/edit": lambda: self._edit_source(body),
            "/api/sources/delete": lambda: self._delete_source(body),
            "/api/prompts/save": lambda: self._save_prompts(body),
            "/api/settings/save": lambda: self._save_settings(body),
            "/api/test_llm": lambda: self._test_llm(body),
            "/api/test_keyso": lambda: self._test_keyso(body),
            "/api/reparse": lambda: self._reparse_source(body),
            "/api/test_sheets": lambda: self._test_sheets(body),
            "/api/quick_tags": lambda: self._quick_tags(body),
            "/api/review": lambda: self._run_review(body),
            "/api/review_batch": lambda: self._review_batch(body),
            "/api/approve": lambda: self._approve_news(body),
            "/api/reject": lambda: self._reject_news(body),
            "/api/users/add": lambda: self._add_user(body),
            "/api/users/delete": lambda: self._delete_user(body),
            "/api/users/change_password": lambda: self._change_password(body),
            "/api/news/bulk_status": lambda: self._bulk_status(body),
            "/api/news/delete": lambda: self._delete_news(body),
            "/api/test_parse": lambda: self._test_parse(body),
            "/api/setup_headers": lambda: self._setup_headers(body),
            "/api/reparse_all": lambda: self._reparse_all(body),
            "/api/rewrite": lambda: self._rewrite_news(body),
            "/api/merge": lambda: self._merge_news(body),
            "/api/news/detail": lambda: self._news_detail(body),
            "/api/export_sheets_bulk": lambda: self._export_sheets_bulk(body),
            "/api/analyze_news": lambda: self._analyze_news(body),
            "/api/batch_rewrite": lambda: self._batch_rewrite(body),
            "/api/articles/save": lambda: self._save_article(body),
            "/api/articles/update": lambda: self._update_article(body),
            "/api/articles/delete": lambda: self._delete_article(body),
            "/api/articles/rewrite": lambda: self._rewrite_article(body),
            "/api/articles/improve": lambda: self._improve_article(body),
            "/api/articles/detail": lambda: self._article_detail(body),
            "/api/articles/schedule": lambda: self._schedule_article(body),
            "/api/prompt_versions/save": lambda: self._save_prompt_version(body),
            "/api/prompt_versions/activate": lambda: self._activate_prompt_version(body),
            "/api/generate_digest": lambda: self._generate_digest(body),
            "/api/digest/generate": lambda: self._generate_and_save_digest(body),
            "/api/event_chain": lambda: self._get_event_chain(body),
            "/api/queue/cancel": lambda: self._cancel_queue_task(body),
            "/api/queue/cancel_all": lambda: self._cancel_all_queue(body),
            "/api/queue/clear_done": lambda: self._clear_done_queue(body),
            "/api/queue/retry": lambda: self._retry_queue_tasks(body),
            "/api/viral_triggers/save": lambda: self._save_viral_trigger(body),
            "/api/viral_triggers/delete": lambda: self._delete_viral_trigger(body),
            "/api/run_auto_review": lambda: self._run_auto_review(body),
            "/api/queue/rewrite": lambda: self._queue_batch_rewrite(body),
            "/api/queue/sheets": lambda: self._queue_sheets_export(body),
            "/api/translate_title": lambda: self._translate_title(body),
            "/api/ai_recommend": lambda: self._ai_recommend(body),
            "/api/cache/clear": lambda: self._clear_cache(body),
            "/api/export_all_processed": lambda: self._export_all_processed(body),
            "/api/export_ready_all": lambda: self._export_ready_all(body),
            "/api/pipeline/full_auto": lambda: self._pipeline_full_auto(body),
            "/api/pipeline/no_llm": lambda: self._pipeline_no_llm(body),
            "/api/pipeline/stop": lambda: self._pipeline_stop(body),
            "/api/moderation": lambda: self._get_moderation(body),
            "/api/moderation/rewrite": lambda: self._moderation_rewrite(body),
            "/api/seo_check": lambda: self._seo_check(body),
            # Phase 0: feature flags, decision trace
            "/api/feature_flags/toggle": lambda: self._toggle_feature_flag(body),
            "/api/decision_trace": lambda: self._get_decision_trace(body),
            # Phase 2: content versioning, multi-output
            "/api/articles/versions": lambda: self._get_article_versions(body),
            "/api/articles/multi_output": lambda: self._generate_multi_output(body),
            "/api/articles/regenerate_field": lambda: self._regenerate_field(body),
            # Phase 4: threshold simulator
            "/api/simulate_thresholds": lambda: self._simulate_thresholds(body),
        }
        handler = routes.get(path)
        if handler:
            handler()
        else:
            self._json({"error": "not found"}, 404)

    # --- Helpers ---
    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        try:
            return json.loads(self.rfile.read(length))
        except Exception:
            return {}

    def _json(self, data, code=200):
        body = json.dumps(data, ensure_ascii=False, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-cache, no-store")
        self.end_headers()
        self.wfile.write(body)

    # --- Auth ---
    def _do_login(self, body):
        username = body.get("username", "")
        password = body.get("password", "")
        pw_hash = hashlib.sha256(password.encode()).hexdigest()
        entry = USERS.get(username, {})
        stored_hash = entry.get("hash", entry) if isinstance(entry, dict) else entry
        if stored_hash == pw_hash:
            signed = _sign_cookie(username)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Set-Cookie", f"session={signed}; Path=/; HttpOnly; SameSite=Lax; Max-Age=604800")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "ok", "role": _user_role(username)}).encode())
        else:
            self._json({"status": "error", "message": "Invalid credentials"}, 401)

    def _do_logout(self):
        self.send_response(302)
        self.send_header("Set-Cookie", "session=; Path=/; Max-Age=0")
        self.send_header("Location", "/login")
        self.end_headers()

    def _get_users(self):
        return [{"username": u, "role": _user_role(u)} for u in USERS.keys()]

    def _add_user(self, body):
        username = body.get("username", "")
        password = body.get("password", "")
        role = body.get("role", "editor")
        if role not in ROLE_PERMISSIONS:
            role = "editor"
        if not username or not password:
            self._json({"status": "error", "message": "Username and password required"})
            return
        user = self._get_session_user()
        if not _user_has_perm(user, "users"):
            self._json({"status": "error", "message": "Недостаточно прав"}, 403)
            return
        USERS[username] = {"hash": hashlib.sha256(password.encode()).hexdigest(), "role": role}
        self._json({"status": "ok", "users": self._get_users()})

    def _delete_user(self, body):
        username = body.get("username", "")
        if username == "admin":
            self._json({"status": "error", "message": "Cannot delete admin"})
            return
        user = self._get_session_user()
        if not _user_has_perm(user, "users"):
            self._json({"status": "error", "message": "Недостаточно прав"}, 403)
            return
        USERS.pop(username, None)
        self._json({"status": "ok", "users": self._get_users()})

    def _require_perm(self, perm: str) -> bool:
        """Check permission. Returns True if allowed, sends 403 and returns False if denied."""
        user = self._get_session_user()
        if not user:
            self._json({"error": "unauthorized"}, 401)
            return False
        if not _user_has_perm(user, perm):
            self._json({"error": "Недостаточно прав"}, 403)
            return False
        return True

    def _serve_diag(self):
        """Публичный диагностический endpoint — проверка БД и парсинга."""
        import json as _json
        import config
        diag = {}
        try:
            # DB connection
            db_url = config.DATABASE_URL
            diag["db_type"] = "PostgreSQL" if db_url.startswith("postgres") else "SQLite"
            diag["db_url_set"] = bool(db_url and db_url != "sqlite:///news.db")

            conn = get_connection()
            cur = conn.cursor()
            try:
                diag["db_connected"] = True

                # Counts
                cur.execute("SELECT COUNT(*) FROM news")
                diag["total_news"] = cur.fetchone()[0]

                cur.execute("SELECT status, COUNT(*) FROM news GROUP BY status")
                status_counts = {}
                for row in cur.fetchall():
                    if _is_postgres():
                        status_counts[row[0]] = row[1]
                    else:
                        status_counts[row[0]] = row[1]
                diag["by_status"] = status_counts

                cur.execute("SELECT COUNT(*) FROM news_analysis")
                diag["total_analyzed"] = cur.fetchone()[0]

                cur.execute("SELECT COUNT(*) FROM news_analysis WHERE reviewed_at IS NOT NULL AND reviewed_at != ''")
                diag["total_reviewed"] = cur.fetchone()[0]

                # Last parsed
                cur.execute("SELECT MAX(parsed_at) FROM news")
                row = cur.fetchone()
                diag["last_parsed"] = str(row[0]) if row and row[0] else "never"

                # Sources parsed
                cur.execute("SELECT source, COUNT(*) FROM news GROUP BY source ORDER BY COUNT(*) DESC")
                src_counts = {}
                for row in cur.fetchall():
                    if _is_postgres():
                        src_counts[row[0]] = row[1]
                    else:
                        src_counts[row[0]] = row[1]
                diag["sources"] = src_counts

                diag["configured_sources"] = len(config.SOURCES)

            finally:
                cur.close()
        except Exception as e:
            diag["db_connected"] = False
            diag["error"] = str(e)

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(_json.dumps(diag, indent=2, ensure_ascii=False).encode())

    def _serve_login(self):
        html = """<!DOCTYPE html>
<html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>IgroNews Login</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><rect width='100' height='100' rx='20' fill='%23192734'/><text x='50' y='38' text-anchor='middle' font-size='28' font-family='sans-serif' font-weight='bold' fill='%231da1f2'>IGR</text><text x='50' y='70' text-anchor='middle' font-size='20' font-family='sans-serif' fill='%2317bf63'>NEWS</text><circle cx='82' cy='20' r='8' fill='%23e0245e'/></svg>">
<style>
* { margin:0;padding:0;box-sizing:border-box; }
body { font-family:-apple-system,sans-serif; background:#0f1923; color:#e1e8ed; display:flex; justify-content:center; align-items:center; height:100vh; }
.login { background:#192734; padding:40px; border-radius:12px; width:340px; }
.login h2 { color:#1da1f2; margin-bottom:20px; text-align:center; }
.login input { width:100%; padding:10px 14px; margin-bottom:12px; background:#22303c; border:1px solid #38444d; color:#e1e8ed; border-radius:6px; font-size:0.95em; }
.login input:focus { outline:none; border-color:#1da1f2; }
.login button { width:100%; padding:10px; background:#1da1f2; color:#fff; border:none; border-radius:6px; font-size:1em; cursor:pointer; }
.login button:hover { background:#1a91da; }
.error { color:#e0245e; font-size:0.85em; margin-bottom:10px; display:none; }
</style></head><body>
<div class="login">
  <div style="text-align:center;margin-bottom:10px">
    <svg width="48" height="48" viewBox="0 0 100 100">
      <rect width="100" height="100" rx="20" fill="#1da1f2"/>
      <text x="50" y="40" text-anchor="middle" font-size="30" font-family="sans-serif" font-weight="bold" fill="#fff">IGR</text>
      <text x="50" y="72" text-anchor="middle" font-size="22" font-family="sans-serif" fill="#fff" opacity="0.8">NEWS</text>
      <circle cx="85" cy="18" r="9" fill="#e0245e"/>
    </svg>
  </div>
  <h2>IgroNews</h2>
  <div class="error" id="err">Invalid credentials</div>
  <input id="username" placeholder="Username" autofocus>
  <input id="password" type="password" placeholder="Password">
  <button onclick="login()">Login</button>
</div>
<script>
document.getElementById('password').addEventListener('keypress', e => { if(e.key==='Enter') login(); });
async function login() {
  const r = await fetch('/api/login', {method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({username: document.getElementById('username').value, password: document.getElementById('password').value})
  });
  if (r.ok) { window.location.href = '/'; }
  else { document.getElementById('err').style.display = 'block'; }
}
</script></body></html>"""
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(html.encode())

    def _get_health(self):
        from checks.health import get_sources_health
        return get_sources_health()

    # --- Data ---
    def _get_stats(self):
        conn = get_connection()
        cur = conn.cursor()
        try:
            # Single query instead of 7+2 separate queries
            cur.execute("SELECT status, COUNT(*) FROM news GROUP BY status")
            stats = {}
            total = 0
            for row in cur.fetchall():
                s, c = (row[0], row[1]) if _is_postgres() else (row[0], row[1])
                stats[s] = c
                total += c
            stats["total"] = total
            # Ensure all expected statuses exist
            for s in ["new", "in_review", "duplicate", "approved", "processed", "rejected", "ready", "moderation"]:
                stats.setdefault(s, 0)
            cur.execute("SELECT COUNT(*) FROM news_analysis")
            stats["analyzed"] = cur.fetchone()[0]
            return stats
        finally:
            cur.close()

    def _get_news(self):
        conn = get_connection()
        cur = conn.cursor()
        try:
            return self._get_news_impl(cur)
        finally:
            cur.close()

    def _get_news_impl(self, cur):
        qs = parse_qs(urlparse(self.path).query)
        limit = int(qs.get("limit", [100])[0])
        offset = int(qs.get("offset", [0])[0])
        status_filter = qs.get("status", [None])[0]
        source_filter = qs.get("source", [None])[0]
        date_from = qs.get("date_from", [None])[0]
        date_to = qs.get("date_to", [None])[0]
        llm_filter = qs.get("llm", [None])[0]

        ph = "%s" if _is_postgres() else "?"
        conditions = []
        params = []
        # Need LEFT JOIN for LLM filter on count query too
        need_join_for_count = False
        if status_filter:
            conditions.append(f"n.status = {ph}")
            params.append(status_filter)
        else:
            # Default: show only enrichment-relevant statuses
            conditions.append("n.status IN ('approved', 'processed', 'ready')")
        if source_filter:
            conditions.append(f"n.source = {ph}")
            params.append(source_filter)
        if date_from:
            conditions.append(f"n.parsed_at >= {ph}")
            params.append(date_from)
        if date_to:
            conditions.append(f"n.parsed_at <= {ph}")
            params.append(date_to + "T23:59:59")
        if llm_filter:
            need_join_for_count = True
            if llm_filter == "has_rec":
                conditions.append("a.llm_recommendation IS NOT NULL AND a.llm_recommendation != ''")
            elif llm_filter == "no_rec":
                conditions.append("(a.llm_recommendation IS NULL OR a.llm_recommendation = '')")
            else:
                conditions.append(f"LOWER(a.llm_recommendation) LIKE {ph}")
                params.append(f"%{llm_filter.lower()}%")

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        # Count total matching
        count_join = "LEFT JOIN news_analysis a ON n.id = a.news_id" if need_join_for_count else ""
        cur.execute(f"SELECT COUNT(*) FROM news n {count_join} {where}", params[:])
        total_count = cur.fetchone()[0]

        query = f"""
            SELECT n.id, n.source, n.title, n.url, n.h1, n.description,
                   n.published_at, n.parsed_at, n.status,
                   a.bigrams, a.trigrams, a.trends_data, a.keyso_data,
                   a.llm_recommendation, a.llm_trend_forecast, a.sheets_row, a.processed_at,
                   a.viral_score, a.viral_level, a.viral_data,
                   a.sentiment_label, a.sentiment_score,
                   a.freshness_status, a.freshness_hours,
                   a.tags_data, a.momentum_score, a.headline_score, a.total_score
            FROM news n
            LEFT JOIN news_analysis a ON n.id = a.news_id
            {where}
            ORDER BY n.parsed_at DESC LIMIT {ph} OFFSET {ph}
        """
        params.append(limit)
        params.append(offset)
        cur.execute(query, params)

        if _is_postgres():
            columns = [desc[0] for desc in cur.description]
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        else:
            rows = [dict(row) for row in cur.fetchall()]

        return {"news": rows, "total": total_count, "limit": limit, "offset": offset}

    def _get_final(self):
        """Финальная подборка: только publish_now с финальным скором."""
        conn = get_connection()
        cur = conn.cursor()
        try:
            return self._get_final_impl(cur)
        finally:
            cur.close()

    def _get_final_impl(self, cur):
        qs = parse_qs(urlparse(self.path).query)
        limit = int(qs.get("limit", [100])[0])
        offset = int(qs.get("offset", [0])[0])
        source_filter = qs.get("source", [None])[0]
        sort_field = qs.get("sort", ["final_score"])[0]
        sort_dir = qs.get("dir", ["desc"])[0]

        ph = "%s" if _is_postgres() else "?"
        conditions = [
            "n.status IN ('processed', 'ready')",
            "LOWER(a.llm_recommendation) = 'publish_now'",
        ]
        params = []

        if source_filter:
            conditions.append(f"n.source = {ph}")
            params.append(source_filter)

        where = "WHERE " + " AND ".join(conditions)

        cur.execute(f"SELECT COUNT(*) FROM news n JOIN news_analysis a ON n.id = a.news_id {where}", params[:])
        total_count = cur.fetchone()[0]

        # Финальный скор: внутренний (40%) + viral (15%) + keyso freq бонус (15%) + trends бонус (15%) + headline (15%)
        # keyso/trends бонусы вычисляются в JS, здесь берём raw данные
        allowed_sorts = {
            "final_score": "a.total_score",
            "total_score": "a.total_score",
            "viral_score": "a.viral_score",
            "freshness_hours": "a.freshness_hours",
            "source": "n.source",
            "parsed_at": "n.parsed_at",
        }
        order_col = allowed_sorts.get(sort_field, "a.total_score")
        order_dir = "ASC" if sort_dir == "asc" else "DESC"

        query = f"""
            SELECT n.id, n.source, n.title, n.url, n.h1,
                   n.published_at, n.parsed_at, n.status,
                   a.bigrams, a.trigrams, a.trends_data, a.keyso_data,
                   a.llm_recommendation, a.llm_trend_forecast,
                   a.viral_score, a.viral_level, a.viral_data,
                   a.sentiment_label, a.sentiment_score,
                   a.freshness_status, a.freshness_hours,
                   a.tags_data, a.momentum_score, a.headline_score,
                   a.total_score, a.quality_score, a.relevance_score,
                   a.entity_names, a.entity_best_tier, a.processed_at
            FROM news n
            JOIN news_analysis a ON n.id = a.news_id
            {where}
            ORDER BY {order_col} {order_dir} LIMIT {ph} OFFSET {ph}
        """
        params.append(limit)
        params.append(offset)
        cur.execute(query, params)

        if _is_postgres():
            columns = [desc[0] for desc in cur.description]
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        else:
            rows = [dict(row) for row in cur.fetchall()]

        return {"news": rows, "total": total_count}

    def _get_editorial(self):
        """Единый endpoint для вкладки Редакция — все данные в одном запросе."""
        conn = get_connection()
        cur = conn.cursor()
        try:
            return self._get_editorial_impl(cur)
        finally:
            cur.close()

    def _get_editorial_impl(self, cur):
        qs = parse_qs(urlparse(self.path).query)
        limit = int(qs.get("limit", [100])[0])
        offset = int(qs.get("offset", [0])[0])
        status_filter = qs.get("status", [None])[0]
        source_filter = qs.get("source", [None])[0]
        min_score = int(qs.get("min_score", [0])[0])
        viral_level = qs.get("viral_level", [None])[0]
        tier_filter = qs.get("tier", [None])[0]
        search = qs.get("q", [None])[0]

        ph = "%s" if _is_postgres() else "?"
        conditions = []
        params = []

        # Исключаем дубликаты и отклонённые по умолчанию (если не запрошены)
        if status_filter:
            conditions.append(f"n.status = {ph}")
            params.append(status_filter)
        else:
            conditions.append(f"n.status NOT IN ('duplicate', 'rejected')")

        if source_filter:
            conditions.append(f"n.source = {ph}")
            params.append(source_filter)
        if min_score > 0:
            conditions.append(f"COALESCE(a.total_score, 0) >= {ph}")
            params.append(min_score)
        if viral_level:
            conditions.append(f"a.viral_level = {ph}")
            params.append(viral_level)
        if tier_filter:
            conditions.append(f"a.entity_best_tier = {ph}")
            params.append(tier_filter)
        if search:
            conditions.append(f"LOWER(n.title) LIKE {ph}")
            params.append(f"%{search.lower()}%")

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        # Count
        cur.execute(f"SELECT COUNT(*) FROM news n LEFT JOIN news_analysis a ON n.id = a.news_id {where}", params[:])
        total_count = cur.fetchone()[0]

        # Stats (counts by status)
        stat_params = []
        cur.execute("SELECT status, COUNT(*) FROM news GROUP BY status")
        status_counts = {}
        for row in cur.fetchall():
            if _is_postgres():
                status_counts[row[0]] = row[1]
            else:
                status_counts[row[0]] = row[1]

        query = f"""
            SELECT n.id, n.source, n.title, n.description, n.url, n.published_at, n.parsed_at, n.status,
                   COALESCE(a.total_score, 0) as total_score,
                   COALESCE(a.quality_score, 0) as quality_score,
                   COALESCE(a.relevance_score, 0) as relevance_score,
                   COALESCE(a.viral_score, 0) as viral_score,
                   COALESCE(a.viral_level, '') as viral_level,
                   COALESCE(a.viral_data, '[]') as viral_data,
                   COALESCE(a.sentiment_label, '') as sentiment_label,
                   COALESCE(a.sentiment_score, 0) as sentiment_score,
                   COALESCE(a.freshness_status, '') as freshness_status,
                   COALESCE(a.freshness_hours, -1) as freshness_hours,
                   COALESCE(a.tags_data, '[]') as tags_data,
                   COALESCE(a.momentum_score, 0) as momentum_score,
                   COALESCE(a.headline_score, 0) as headline_score,
                   COALESCE(a.all_checks_pass, 0) as all_checks_pass,
                   COALESCE(a.entity_names, '[]') as entity_names,
                   COALESCE(a.entity_best_tier, '') as entity_best_tier,
                   COALESCE(a.reviewed_at, '') as reviewed_at,
                   COALESCE(a.score_breakdown, '{{}}') as score_breakdown,
                   a.bigrams, a.llm_recommendation, a.llm_trend_forecast,
                   a.keyso_data, a.trends_data
            FROM news n
            LEFT JOIN news_analysis a ON n.id = a.news_id
            {where}
            ORDER BY n.parsed_at DESC
            LIMIT {ph} OFFSET {ph}
        """
        params.append(limit)
        params.append(offset)
        cur.execute(query, params)

        if _is_postgres():
            columns = [desc[0] for desc in cur.description]
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        else:
            rows = [dict(row) for row in cur.fetchall()]

        return {
            "news": rows,
            "total": total_count,
            "limit": limit,
            "offset": offset,
            "stats": status_counts,
        }

    def _get_event_chain_by_id(self, news_id):
        """Возвращает цепочку событий для указанной новости (GET endpoint)."""
        try:
            conn = get_connection()
            cur = conn.cursor()
            ph = "%s" if _is_postgres() else "?"
            try:
                cur.execute(f"SELECT id, source, title, published_at, status FROM news WHERE id = {ph}", (news_id,))
                row = cur.fetchone()
                if not row:
                    return {"error": "news not found"}
                if _is_postgres():
                    columns = [desc[0] for desc in cur.description]
                    news = dict(zip(columns, row))
                else:
                    news = dict(row)
            finally:
                cur.close()
            from checks.temporal_clusters import get_event_chain
            return get_event_chain(news)
        except Exception as e:
            logger.error(f"Event chain error: {e}")
            return {"chain": [], "chain_length": 0, "days_span": 0, "phase": "single", "error": str(e)}

    def _get_sources(self):
        import config
        return config.SOURCES

    def _get_prompts(self):
        from apis.llm import PROMPT_TREND_FORECAST, PROMPT_MERGE_ANALYSIS, PROMPT_KEYSO_QUERIES
        return {
            "trend_forecast": PROMPT_TREND_FORECAST,
            "merge_analysis": PROMPT_MERGE_ANALYSIS,
            "keyso_queries": PROMPT_KEYSO_QUERIES,
        }

    def _get_settings(self):
        import config
        return {
            "llm_model": config.LLM_MODEL,
            "keyso_region": getattr(config, "KEYSO_REGION", "ru"),
            "regions": config.REGIONS,
            "sheets_id": config.GOOGLE_SHEETS_ID,
            "sheets_tab": config.SHEETS_TAB,
            "openai_key_set": bool(config.OPENAI_API_KEY),
            "keyso_key_set": bool(config.KEYSO_API_KEY),
            "google_sa_set": bool(config.GOOGLE_SERVICE_ACCOUNT_JSON),
            "auto_approve_threshold": getattr(config, "AUTO_APPROVE_THRESHOLD", 70),
            "auto_rewrite_on_publish_now": getattr(config, "AUTO_REWRITE_ON_PUBLISH_NOW", True),
            "auto_rewrite_style": getattr(config, "AUTO_REWRITE_STYLE", "news"),
        }

    # --- Actions ---
    def _run_process(self):
        try:
            from scheduler import process_news
            threading.Thread(target=process_news, daemon=True).start()
            self._json({"status": "ok", "message": "Processing started in background"})
        except Exception as e:
            self._json({"status": "error", "message": str(e)})

    def _process_one(self, body):
        news_id = body.get("news_id")
        if not news_id:
            self._json({"status": "error", "message": "news_id required"})
            return
        try:
            from scheduler import _process_single_news
            result = _process_single_news(news_id)
            self._json({"status": "ok", "result": result})
        except Exception as e:
            self._json({"status": "error", "message": str(e)})

    def _export_sheets(self, body):
        news_id = body.get("news_id")
        try:
            from storage.sheets import write_news_row
            conn = get_connection()
            cur = conn.cursor()
            try:
                ph = "%s" if _is_postgres() else "?"
                cur.execute(f"SELECT * FROM news WHERE id = {ph}", (news_id,))
                if _is_postgres():
                    columns = [desc[0] for desc in cur.description]
                    news = dict(zip(columns, cur.fetchone()))
                else:
                    news = dict(cur.fetchone())

                cur.execute(f"SELECT * FROM news_analysis WHERE news_id = {ph}", (news_id,))
                row = cur.fetchone()
                if row:
                    if _is_postgres():
                        columns = [desc[0] for desc in cur.description]
                        analysis = dict(zip(columns, row))
                    else:
                        analysis = dict(row)
                else:
                    analysis = {"bigrams": "[]", "trends_data": "{}", "keyso_data": "{}",
                               "llm_recommendation": "", "llm_trend_forecast": "", "llm_merged_with": ""}

                sheet_row = write_news_row(news, analysis)
                self._json({"status": "ok", "row": sheet_row})
            finally:
                cur.close()
        except Exception as e:
            self._json({"status": "error", "message": str(e)})

    def _add_source(self, body):
        import config
        source = {
            "name": body.get("name", ""),
            "type": body.get("type", "rss"),
            "url": body.get("url", ""),
            "interval": int(body.get("interval", 15)),
        }
        if body.get("selector"):
            source["selector"] = body["selector"]
        config.SOURCES.append(source)
        self._json({"status": "ok", "sources": config.SOURCES})

    def _edit_source(self, body):
        import config
        old_name = body.get("old_name", "")
        for s in config.SOURCES:
            if s["name"] == old_name:
                s["name"] = body.get("name", s["name"])
                s["type"] = body.get("type", s["type"])
                s["url"] = body.get("url", s["url"])
                s["interval"] = int(body.get("interval", s["interval"]))
                if body.get("selector"):
                    s["selector"] = body["selector"]
                elif "selector" in s and body.get("type") == "rss":
                    del s["selector"]
                break
        self._json({"status": "ok", "sources": config.SOURCES})

    def _delete_source(self, body):
        import config
        name = body.get("name")
        config.SOURCES[:] = [s for s in config.SOURCES if s["name"] != name]
        self._json({"status": "ok", "sources": config.SOURCES})

    def _save_prompts(self, body):
        import apis.llm as llm
        if "trend_forecast" in body:
            llm.PROMPT_TREND_FORECAST = body["trend_forecast"]
        if "merge_analysis" in body:
            llm.PROMPT_MERGE_ANALYSIS = body["merge_analysis"]
        if "keyso_queries" in body:
            llm.PROMPT_KEYSO_QUERIES = body["keyso_queries"]
        self._json({"status": "ok"})

    def _save_settings(self, body):
        if not self._require_perm("settings"):
            return
        import config
        user = self._get_session_user() or "admin"
        changes = []
        if "llm_model" in body and body["llm_model"] != config.LLM_MODEL:
            changes.append(("llm_model", config.LLM_MODEL, body["llm_model"]))
            config.LLM_MODEL = body["llm_model"]
        if "keyso_region" in body and body["keyso_region"] != config.KEYSO_REGION:
            changes.append(("keyso_region", config.KEYSO_REGION, body["keyso_region"]))
            config.KEYSO_REGION = body["keyso_region"]
        if "sheets_tab" in body and body["sheets_tab"] != config.SHEETS_TAB:
            changes.append(("sheets_tab", config.SHEETS_TAB, body["sheets_tab"]))
            config.SHEETS_TAB = body["sheets_tab"]
        if "auto_approve_threshold" in body:
            try:
                new_val = int(body["auto_approve_threshold"])
                if new_val != config.AUTO_APPROVE_THRESHOLD:
                    changes.append(("auto_approve_threshold", str(config.AUTO_APPROVE_THRESHOLD), str(new_val)))
                    config.AUTO_APPROVE_THRESHOLD = new_val
            except (ValueError, TypeError):
                pass
        if "auto_rewrite_on_publish_now" in body:
            new_val = bool(body["auto_rewrite_on_publish_now"])
            if new_val != config.AUTO_REWRITE_ON_PUBLISH_NOW:
                changes.append(("auto_rewrite_on_publish_now", str(config.AUTO_REWRITE_ON_PUBLISH_NOW), str(new_val)))
                config.AUTO_REWRITE_ON_PUBLISH_NOW = new_val
        if "auto_rewrite_style" in body and body["auto_rewrite_style"] != config.AUTO_REWRITE_STYLE:
            changes.append(("auto_rewrite_style", config.AUTO_REWRITE_STYLE, body["auto_rewrite_style"]))
            config.AUTO_REWRITE_STYLE = body["auto_rewrite_style"]

        # Audit log config changes
        for setting_name, old_val, new_val in changes:
            try:
                from core.observability import log_config_change
                log_config_change(setting_name, old_val, new_val, changed_by=user)
            except Exception:
                pass

        self._json({"status": "ok"})

    def _test_llm(self, body):
        try:
            import config
            from openai import OpenAI
            import json as _json
            prompt = body.get("prompt", "Ответь JSON: {\"test\": \"ok\"}")
            client = OpenAI(api_key=config.OPENAI_API_KEY, base_url=config.OPENAI_BASE_URL)
            response = client.chat.completions.create(
                model=config.LLM_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
            )
            text = response.choices[0].message.content
            # Try parse JSON
            cleaned = text.strip()
            if cleaned.startswith("```"):
                cleaned = "\n".join(cleaned.split("\n")[1:])
                if cleaned.endswith("```"):
                    cleaned = cleaned[:-3]
                cleaned = cleaned.strip()
            try:
                parsed = _json.loads(cleaned)
            except Exception:
                parsed = None
            self._json({"status": "ok", "model": config.LLM_MODEL, "base_url": config.OPENAI_BASE_URL, "raw": text, "result": parsed})
        except Exception as e:
            self._json({"status": "error", "message": str(e), "type": type(e).__name__})

    def _test_keyso(self, body):
        try:
            import config
            import requests as _req
            keyword = body.get("keyword", "gta 6")
            # Raw request for debugging
            url = f"{config.KEYSO_BASE_URL}/report/simple/keyword_dashboard"
            params = {"auth-token": config.KEYSO_API_KEY, "base": config.KEYSO_REGION, "keyword": keyword}
            resp = _req.get(url, params=params, timeout=15)
            raw = resp.json()
            self._json({"status": "ok", "http_code": resp.status_code, "raw_response": raw})
        except Exception as e:
            self._json({"status": "error", "message": str(e), "type": type(e).__name__})

    def _quick_tags(self, body):
        """Быстрый расчёт тегов по заголовкам (без полного review)."""
        news_ids = body.get("news_ids", [])
        if not news_ids:
            self._json({"status": "error", "message": "No news_ids"})
            return
        try:
            conn = get_connection()
            cur = conn.cursor()
            try:
                ph = "%s" if _is_postgres() else "?"
                placeholders = ",".join([ph] * len(news_ids))
                cur.execute(f"SELECT id, title, description, plain_text FROM news WHERE id IN ({placeholders})", news_ids)
                if _is_postgres():
                    columns = [desc[0] for desc in cur.description]
                    rows = [dict(zip(columns, row)) for row in cur.fetchall()]
                else:
                    rows = [dict(row) for row in cur.fetchall()]

                from checks.tags import auto_tag
                from checks.deduplication import tfidf_similarity

                # Tags per news
                tags_map = {}
                for r in rows:
                    tags = auto_tag(r)
                    tags_map[r["id"]] = [{"id": t["id"], "label": t["label"], "hits": t["hits"]} for t in tags[:3]]

                # Similarity groups
                titles = [r.get("title", "") for r in rows]
                ids_ordered = [r["id"] for r in rows]
                pairs = tfidf_similarity(titles)

                # Build groups from pairs
                from collections import defaultdict
                graph = defaultdict(set)
                for i, j, score in pairs:
                    graph[i].add(j)
                    graph[j].add(i)
                visited = set()
                groups = []
                group_idx = 0
                id_to_group = {}
                for idx in range(len(rows)):
                    if idx in visited:
                        continue
                    cluster = set()
                    stack = [idx]
                    while stack:
                        node = stack.pop()
                        if node in visited:
                            continue
                        visited.add(node)
                        cluster.add(node)
                        stack.extend(graph[node] - visited)
                    if len(cluster) >= 2:
                        group_idx += 1
                        member_ids = [ids_ordered[i] for i in sorted(cluster)]
                        member_titles = [titles[i] for i in sorted(cluster)]
                        for mid in member_ids:
                            id_to_group[mid] = group_idx
                        groups.append({
                            "group": group_idx,
                            "count": len(member_ids),
                            "ids": member_ids,
                            "titles": member_titles,
                        })

                self._json({"status": "ok", "tags": tags_map, "groups": groups, "id_to_group": id_to_group})
            finally:
                cur.close()
        except Exception as e:
            self._json({"status": "error", "message": str(e), "type": type(e).__name__})

    def _dashboard_groups(self):
        """Возвращает теги и группы для новостей (учитывает фильтр статуса)."""
        try:
            conn = get_connection()
            cur = conn.cursor()
            try:
                ph = "%s" if _is_postgres() else "?"
                qs = parse_qs(urlparse(self.path).query)
                status_filter = qs.get("status", [None])[0]
                if status_filter:
                    cur.execute(f"SELECT id, title, description, plain_text FROM news WHERE status = {ph} ORDER BY parsed_at DESC LIMIT 200", (status_filter,))
                else:
                    cur.execute("SELECT id, title, description, plain_text FROM news ORDER BY parsed_at DESC LIMIT 200")
                if _is_postgres():
                    columns = [desc[0] for desc in cur.description]
                    rows = [dict(zip(columns, row)) for row in cur.fetchall()]
                else:
                    rows = [dict(row) for row in cur.fetchall()]

                if not rows:
                    self._json({"status": "ok", "tags": {}, "groups": [], "id_to_group": {}})
                    return

                from checks.tags import auto_tag
                from checks.deduplication import tfidf_similarity
                from collections import defaultdict

                tags_map = {}
                for r in rows:
                    tags = auto_tag(r)
                    tags_map[r["id"]] = [{"id": t["id"], "label": t["label"], "hits": t["hits"]} for t in tags[:3]]

                titles = [r.get("title", "") for r in rows]
                ids_ordered = [r["id"] for r in rows]
                pairs = tfidf_similarity(titles)

                graph = defaultdict(set)
                for i, j, score in pairs:
                    graph[i].add(j)
                    graph[j].add(i)
                visited = set()
                groups = []
                group_idx = 0
                id_to_group = {}
                for idx in range(len(rows)):
                    if idx in visited:
                        continue
                    cluster = set()
                    stack = [idx]
                    while stack:
                        node = stack.pop()
                        if node in visited:
                            continue
                        visited.add(node)
                        cluster.add(node)
                        stack.extend(graph[node] - visited)
                    if len(cluster) >= 2:
                        group_idx += 1
                        member_ids = [ids_ordered[i] for i in sorted(cluster)]
                        member_titles = [titles[i] for i in sorted(cluster)]
                        for mid in member_ids:
                            id_to_group[mid] = group_idx
                        groups.append({
                            "group": group_idx,
                            "count": len(member_ids),
                            "ids": member_ids,
                            "titles": member_titles,
                        })

                self._json({"status": "ok", "tags": tags_map, "groups": groups, "id_to_group": id_to_group})
            finally:
                cur.close()
        except Exception as e:
            self._json({"status": "error", "message": str(e), "type": type(e).__name__})

    def _run_review(self, body):
        """Запускает pipeline проверки для выбранных новостей."""
        news_ids = body.get("news_ids", [])
        if not news_ids:
            self._json({"status": "error", "message": "No news selected"})
            return
        try:
            conn = get_connection()
            cur = conn.cursor()
            try:
                ph = "%s" if _is_postgres() else "?"
                placeholders = ",".join([ph] * len(news_ids))
                cur.execute(f"SELECT * FROM news WHERE id IN ({placeholders})", news_ids)
                if _is_postgres():
                    columns = [desc[0] for desc in cur.description]
                    news_list = [dict(zip(columns, row)) for row in cur.fetchall()]
                else:
                    news_list = [dict(row) for row in cur.fetchall()]

                from checks.pipeline import run_review_pipeline
                result = run_review_pipeline(news_list)
                self._json({"status": "ok", **result})
            finally:
                cur.close()
        except Exception as e:
            self._json({"status": "error", "message": str(e), "type": type(e).__name__})

    def _review_batch(self, body):
        """Проверяет новости по статусу (batch, без изменения статуса)."""
        status = body.get("status", "new")
        limit = int(body.get("limit", 50))
        try:
            conn = get_connection()
            cur = conn.cursor()
            try:
                ph = "%s" if _is_postgres() else "?"
                if status:
                    cur.execute(f"SELECT * FROM news WHERE status = {ph} ORDER BY parsed_at DESC LIMIT {ph}", (status, limit))
                else:
                    cur.execute(f"SELECT * FROM news ORDER BY parsed_at DESC LIMIT {ph}", (limit,))
                if _is_postgres():
                    columns = [desc[0] for desc in cur.description]
                    news_list = [dict(zip(columns, row)) for row in cur.fetchall()]
                else:
                    news_list = [dict(row) for row in cur.fetchall()]

                if not news_list:
                    self._json({"status": "ok", "results": [], "groups": []})
                    return

                from checks.pipeline import run_review_pipeline
                # Прогоняем pipeline но НЕ меняем статусы
                from checks.deduplication import tfidf_similarity, build_groups
                from checks.quality import check_quality
                from checks.relevance import check_relevance
                from checks.freshness import check_freshness
                from checks.viral_score import viral_score
                from checks.tags import auto_tag
                from checks.sentiment import analyze_sentiment
                from checks.momentum import get_momentum

                results = []
                for news in news_list:
                    result = {
                        "id": news["id"],
                        "title": news.get("title", ""),
                        "source": news.get("source", ""),
                        "url": news.get("url", ""),
                        "published_at": news.get("published_at", ""),
                        "status": news.get("status", ""),
                        "checks": {},
                    }
                    result["checks"]["quality"] = check_quality(news)
                    result["checks"]["relevance"] = check_relevance(news)
                    result["checks"]["freshness"] = check_freshness(news)
                    result["checks"]["viral"] = viral_score(news)
                    result["tags"] = auto_tag(news)
                    result["sentiment"] = analyze_sentiment(news)
                    result["momentum"] = get_momentum(news)

                    all_pass = all(c["pass"] for c in result["checks"].values())
                    total_score = sum(c["score"] for c in result["checks"].values()) // 4
                    momentum_bonus = result["momentum"]["score"] // 5
                    total_score = min(100, total_score + momentum_bonus)
                    result["overall_pass"] = all_pass
                    result["total_score"] = total_score
                    results.append(result)

                # Dedup
                titles = [r["title"] for r in results]
                pairs = tfidf_similarity(titles)
                groups = build_groups(results, pairs)
                for group in groups:
                    for idx in group.get("duplicate_indices", []):
                        if idx < len(results):
                            results[idx]["overall_pass"] = False
                            results[idx]["is_duplicate"] = True
                    for member in group["members"]:
                        member["dedup_status"] = group["status"]

                self._json({"status": "ok", "results": results, "groups": groups})
            finally:
                cur.close()
        except Exception as e:
            self._json({"status": "error", "message": str(e), "type": type(e).__name__})

    def _run_auto_review(self, body):
        """Запускает авто-ревью батчами по 20 (сохраняет результаты в БД)."""
        try:
            conn = get_connection()
            cur = conn.cursor()
            try:
                ph = "%s" if _is_postgres() else "?"
                BATCH_SIZE = 20

                # Считаем сколько всего непроверенных (только new)
                cur.execute("SELECT COUNT(*) FROM news WHERE status = 'new'")
                total_pending = cur.fetchone()[0]

                if total_pending == 0:
                    self._json({"status": "ok", "reviewed": 0, "message": "Нет новых для проверки", "remaining": 0})
                    return

                # Берём один батч
                cur.execute(f"""
                    SELECT * FROM news WHERE status = 'new'
                    ORDER BY parsed_at DESC LIMIT {ph}
                """, (BATCH_SIZE,))
                if _is_postgres():
                    columns = [desc[0] for desc in cur.description]
                    news_list = [dict(zip(columns, row)) for row in cur.fetchall()]
                else:
                    news_list = [dict(row) for row in cur.fetchall()]

                from checks.pipeline import run_review_pipeline
                result = run_review_pipeline(news_list, update_status=True)
                reviewed = len(result.get("results", []))
                dupes = sum(1 for r in result.get("results", []) if r.get("is_duplicate"))
                rejected = sum(1 for r in result.get("results", []) if r.get("auto_rejected"))
                remaining = total_pending - reviewed
                self._json({
                    "status": "ok",
                    "reviewed": reviewed,
                    "duplicates": dupes,
                    "auto_rejected": rejected,
                    "remaining": remaining,
                    "message": f"Проверено: {reviewed}, дубликатов: {dupes}, отклонено: {rejected}" +
                               (f". Осталось: {remaining}" if remaining > 0 else "")
                })
            finally:
                cur.close()
        except Exception as e:
            self._json({"status": "error", "message": str(e)})

    def _approve_news(self, body):
        """Одобряет новости и запускает обогащение в фоне."""
        news_ids = body.get("news_ids", [])
        if not news_ids:
            self._json({"status": "error", "message": "No news selected"})
            return
        try:
            from checks.pipeline import approve_for_enrichment
            from checks.feedback import record_decision
            approve_for_enrichment(news_ids)
            for nid in news_ids:
                try:
                    record_decision(nid, "approved")
                except Exception:
                    pass

            # Auto-enrich: запускаем обогащение в фоновом потоке
            import threading
            def _bg_enrich(ids):
                from scheduler import _process_single_news
                for nid in ids:
                    try:
                        _process_single_news(nid)
                    except Exception as e:
                        logger.warning("Background enrich failed for %s: %s", nid, e)
            threading.Thread(target=_bg_enrich, args=(list(news_ids),), daemon=True).start()

            self._json({"status": "ok", "approved": len(news_ids), "enriching": True})
        except Exception as e:
            self._json({"status": "error", "message": str(e)})

    def _reject_news(self, body):
        """Отклоняет новости (одну или массив)."""
        news_ids = body.get("news_ids", [])
        news_id = body.get("news_id")
        if news_id and not news_ids:
            news_ids = [news_id]
        if not news_ids:
            self._json({"status": "error", "message": "news_ids required"})
            return
        try:
            from checks.feedback import record_decision
            for nid in news_ids:
                update_news_status(nid, "rejected")
                try:
                    record_decision(nid, "rejected")
                except Exception:
                    pass
            self._json({"status": "ok", "rejected": len(news_ids)})
        except Exception as e:
            self._json({"status": "error", "message": str(e)})

    def _export_all_processed(self, body):
        """Экспортирует все обработанные новости (processed) в Sheets/Лист1."""
        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT id FROM news WHERE status IN ('processed', 'ready') ORDER BY parsed_at DESC")
            if _is_postgres():
                news_ids = [r[0] for r in cur.fetchall()]
            else:
                news_ids = [r["id"] for r in cur.fetchall()]
        finally:
            cur.close()

        if not news_ids:
            self._json({"status": "error", "message": "Нет обработанных новостей"})
            return

        # Queue sheets export for all
        from scheduler import _create_task
        task_ids = []
        cur2 = conn.cursor()
        ph = "%s" if _is_postgres() else "?"
        for nid in news_ids:
            try:
                cur2.execute(f"SELECT title FROM news WHERE id = {ph}", (nid,))
                row = cur2.fetchone()
                title = (row[0] if _is_postgres() else row["title"]) if row else ""
            except Exception:
                title = ""
            tid = _create_task("sheets", nid, title)
            task_ids.append(tid)
        cur2.close()

        # Process in background with rate limiting
        import threading
        import time as _time
        def _bg_export(ids, tids):
            from storage.sheets import write_news_row
            from scheduler import _update_task, _fetch_news_by_id, _fetch_analysis_by_id
            ok_count = 0
            skip_count = 0
            err_count = 0
            for i, (nid, tid) in enumerate(zip(ids, tids)):
                try:
                    _update_task(tid, "running", {"stage": "exporting", "progress": f"{i+1}/{len(ids)}"})
                    news = _fetch_news_by_id(nid)
                    analysis = _fetch_analysis_by_id(nid)
                    if not news:
                        _update_task(tid, "error", {"error": "News not found"})
                        err_count += 1
                        continue
                    sheet_row = write_news_row(news, analysis or {})
                    if sheet_row and sheet_row > 0:
                        _update_task(tid, "done", {"sheet_row": sheet_row})
                        ok_count += 1
                    elif sheet_row == -1:
                        _update_task(tid, "skipped", {"reason": "duplicate in Sheets"})
                        skip_count += 1
                    else:
                        _update_task(tid, "error", {"error": "Sheets write returned None"})
                        err_count += 1
                except Exception as e:
                    _update_task(tid, "error", {"error": str(e)[:500]})
                    err_count += 1
                # Rate limit: ~1.5 sec between writes to stay under Google Sheets 60 req/min
                _time.sleep(1.5)
            logger.info("Mass export done: %d ok, %d skipped, %d errors out of %d", ok_count, skip_count, err_count, len(ids))

        threading.Thread(target=_bg_export, args=(list(news_ids), list(task_ids)), daemon=True).start()
        self._json({"status": "ok", "queued": len(news_ids), "task_ids": task_ids})

    def _export_ready_all(self, body):
        """Экспортирует ВСЕ переписанные статьи (articles) в Sheets/Ready."""
        conn = get_connection()
        cur = conn.cursor()
        try:
            # Get all articles with their news data
            cur.execute("""
                SELECT a.id, a.news_id, a.title, a.text, a.seo_title, a.seo_description,
                       a.tags, a.style, a.original_title, a.source_url, a.created_at,
                       n.source, n.parsed_at, n.url, n.title as news_title
                FROM articles a
                LEFT JOIN news n ON n.id = a.news_id
                ORDER BY a.created_at DESC
            """)
            if _is_postgres():
                columns = [d[0] for d in cur.description]
                articles = [dict(zip(columns, r)) for r in cur.fetchall()]
            else:
                articles = [dict(r) for r in cur.fetchall()]
        finally:
            cur.close()

        if not articles:
            self._json({"status": "error", "message": "Нет переписанных статей"})
            return

        import threading
        import time as _time

        def _bg_export_ready(arts):
            from storage.sheets import write_ready_row
            from scheduler import _fetch_analysis_by_id
            import json as _json
            ok = 0
            skip = 0
            err = 0
            for art in arts:
                try:
                    news_id = art.get("news_id", "")
                    analysis = _fetch_analysis_by_id(news_id) if news_id else None

                    # Build news dict
                    news = {
                        "parsed_at": art.get("parsed_at", art.get("created_at", "")),
                        "source": art.get("source", ""),
                        "title": art.get("original_title") or art.get("news_title", ""),
                        "url": art.get("source_url") or art.get("url", ""),
                    }

                    # Build rewrite dict
                    tags_raw = art.get("tags", "[]")
                    try:
                        tags_list = _json.loads(tags_raw) if isinstance(tags_raw, str) else (tags_raw if isinstance(tags_raw, list) else [])
                    except Exception:
                        tags_list = []

                    rewrite = {
                        "title": art.get("title", ""),
                        "text": art.get("text", ""),
                        "seo_title": art.get("seo_title", ""),
                        "seo_description": art.get("seo_description", ""),
                        "tags": tags_list,
                    }

                    row = write_ready_row(news, analysis, rewrite)
                    if row and row > 0:
                        ok += 1
                    elif row == -1:
                        skip += 1
                    else:
                        err += 1
                except Exception as e:
                    logger.error("Ready export error for article %s: %s", art.get("id"), e)
                    err += 1
                _time.sleep(1.5)  # Rate limit
            logger.info("Ready export done: %d ok, %d skipped, %d errors out of %d", ok, skip, err, len(arts))

        threading.Thread(target=_bg_export_ready, args=(list(articles),), daemon=True).start()
        self._json({"status": "ok", "queued": len(articles), "message": f"Экспорт {len(articles)} статей в Ready запущен"})

    # ─── Pipeline endpoints ───

    def _pipeline_full_auto(self, body):
        """Режим 1: Полный автомат — score → >70 на LLM → финальный скор → >60 на рерайт → Sheets/Ready."""
        if not self._require_perm("pipeline"):
            return
        news_ids = body.get("news_ids", [])
        select_all = body.get("all_new", False)

        if select_all and not news_ids:
            # Выбрать все new/in_review
            conn = get_connection()
            cur = conn.cursor()
            try:
                cur.execute("SELECT id FROM news WHERE status IN ('new', 'in_review') ORDER BY parsed_at DESC LIMIT 50")
                if _is_postgres():
                    news_ids = [r[0] for r in cur.fetchall()]
                else:
                    news_ids = [r["id"] for r in cur.fetchall()]
            finally:
                cur.close()

        if not news_ids:
            self._json({"status": "error", "message": "Нет новостей для обработки"})
            return

        # Create tasks
        from scheduler import _create_task, run_full_auto_pipeline
        task_ids = []
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            for nid in news_ids:
                try:
                    cur.execute(f"SELECT title FROM news WHERE id = {ph}", (nid,))
                    row = cur.fetchone()
                    title = (row[0] if _is_postgres() else row["title"]) if row else ""
                except Exception:
                    title = ""
                tid = _create_task("full_auto", nid, title)
                task_ids.append(tid)
        finally:
            cur.close()

        # Run in background
        import threading
        threading.Thread(
            target=run_full_auto_pipeline,
            args=(list(news_ids), list(task_ids)),
            daemon=True
        ).start()

        self._json({"status": "ok", "queued": len(news_ids), "task_ids": task_ids})

    def _pipeline_no_llm(self, body):
        """Режим 2: Без LLM — score → Sheets/NotReady + Модерация."""
        news_ids = body.get("news_ids", [])
        select_all = body.get("all_new", False)

        if select_all and not news_ids:
            conn = get_connection()
            cur = conn.cursor()
            try:
                cur.execute("SELECT id FROM news WHERE status IN ('new', 'in_review') ORDER BY parsed_at DESC LIMIT 50")
                if _is_postgres():
                    news_ids = [r[0] for r in cur.fetchall()]
                else:
                    news_ids = [r["id"] for r in cur.fetchall()]
            finally:
                cur.close()

        if not news_ids:
            self._json({"status": "error", "message": "Нет новостей для обработки"})
            return

        from scheduler import _create_task, run_no_llm_pipeline
        task_ids = []
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            for nid in news_ids:
                try:
                    cur.execute(f"SELECT title FROM news WHERE id = {ph}", (nid,))
                    row = cur.fetchone()
                    title = (row[0] if _is_postgres() else row["title"]) if row else ""
                except Exception:
                    title = ""
                tid = _create_task("no_llm", nid, title)
                task_ids.append(tid)
        finally:
            cur.close()

        import threading
        threading.Thread(
            target=run_no_llm_pipeline,
            args=(list(news_ids), list(task_ids)),
            daemon=True
        ).start()

        self._json({"status": "ok", "queued": len(news_ids), "task_ids": task_ids})

    def _pipeline_stop(self, body):
        """Остановка текущего пайплайна + отмена pending задач в БД."""
        from scheduler import pipeline_stop
        pipeline_stop()
        # Cancel all pending/running pipeline tasks in DB
        conn = get_connection()
        cur = conn.cursor()
        try:
            from datetime import datetime, timezone
            now = datetime.now(timezone.utc).isoformat()
            if _is_postgres():
                cur.execute(
                    "UPDATE task_queue SET status = 'cancelled', result = %s, updated_at = %s "
                    "WHERE status IN ('pending', 'running') AND task_type IN ('full_auto', 'no_llm')",
                    ('{"reason":"Остановлено пользователем"}', now))
                cancelled = cur.rowcount
            else:
                cur.execute(
                    "UPDATE task_queue SET status = 'cancelled', result = ?, updated_at = ? "
                    "WHERE status IN ('pending', 'running') AND task_type IN ('full_auto', 'no_llm')",
                    ('{"reason":"Остановлено пользователем"}', now))
                cancelled = cur.rowcount
                conn.commit()
        finally:
            cur.close()
        self._json({"status": "ok", "message": f"Остановлено, отменено задач: {cancelled}"})

    def _get_pipeline_status(self):
        """Возвращает текущий статус пайплайна (running tasks)."""
        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT task_type, status, COUNT(*) as cnt
                FROM task_queue
                WHERE status IN ('pending', 'running')
                GROUP BY task_type, status
            """)
            if _is_postgres():
                rows = [{"type": r[0], "status": r[1], "count": r[2]} for r in cur.fetchall()]
            else:
                rows = [{"type": r[0], "status": r[1], "count": r[2]} for r in cur.fetchall()]

            # Any running pipeline?
            running = any(r["status"] == "running" for r in rows)
            pending = sum(r["count"] for r in rows if r["status"] == "pending")
            running_count = sum(r["count"] for r in rows if r["status"] == "running")
            # Determine active type
            active_type = ""
            for r in rows:
                if r["status"] == "running" and r["type"] in ("full_auto", "no_llm"):
                    active_type = r["type"]
                    break
            if not active_type:
                for r in rows:
                    if r["status"] == "pending" and r["type"] in ("full_auto", "no_llm"):
                        active_type = r["type"]
                        break

            # Count done tasks and find earliest running start
            total_done = 0
            started_at = ""
            try:
                cur.execute("""
                    SELECT COUNT(*) FROM task_queue
                    WHERE status = 'done' AND task_type IN ('full_auto', 'no_llm')
                """)
                total_done = cur.fetchone()[0]
                cur.execute("""
                    SELECT MIN(created_at) FROM task_queue
                    WHERE status IN ('pending', 'running') AND task_type IN ('full_auto', 'no_llm')
                """)
                row = cur.fetchone()
                started_at = str(row[0]) if row and row[0] else ""
            except Exception:
                pass

            return {
                "running": running or pending > 0,
                "active_type": active_type,
                "running_count": running_count,
                "pending_count": pending,
                "total_done": total_done,
                "started_at": started_at,
                "details": rows,
            }
        finally:
            cur.close()

    def _get_moderation_list(self):
        """Возвращает новости со статусом moderation (с данными локального анализа)."""
        from urllib.parse import parse_qs
        qs = parse_qs(urlparse(self.path).query)
        limit = int(qs.get("limit", ["100"])[0])
        offset = int(qs.get("offset", ["0"])[0])
        source = qs.get("source", [""])[0]
        min_score = int(qs.get("min_score", ["0"])[0])
        q = qs.get("q", [""])[0]

        conn = get_connection()
        cur = conn.cursor()
        ph = "%s" if _is_postgres() else "?"

        conditions = ["n.status = 'moderation'"]
        params = []

        if source:
            conditions.append(f"n.source = {ph}")
            params.append(source)
        if min_score > 0:
            conditions.append(f"COALESCE(na.total_score, 0) >= {ph}")
            params.append(min_score)
        if q:
            conditions.append(f"LOWER(n.title) LIKE {ph}")
            params.append(f"%{q.lower()}%")

        where = " AND ".join(conditions)

        try:
            cur.execute(f"""
                SELECT n.id, n.source, n.title, n.url, n.published_at, n.parsed_at, n.status,
                       n.description,
                       na.total_score, na.quality_score, na.relevance_score,
                       na.freshness_hours, na.viral_score, na.viral_data,
                       na.sentiment_label,
                       na.tags_data as tags, na.entity_names as entities, na.headline_score, na.momentum_score
                FROM news n
                LEFT JOIN news_analysis na ON na.news_id = n.id
                WHERE {where}
                ORDER BY n.parsed_at DESC
                LIMIT {ph} OFFSET {ph}
            """, (*params, limit, offset))

            if _is_postgres():
                columns = [d[0] for d in cur.description]
                rows = [dict(zip(columns, r)) for r in cur.fetchall()]
            else:
                rows = [dict(r) for r in cur.fetchall()]

            # Count total + avg score
            cur.execute(f"SELECT COUNT(*), COALESCE(AVG(na.total_score), 0) FROM news n LEFT JOIN news_analysis na ON na.news_id = n.id WHERE {where}", tuple(params))
            row = cur.fetchone()
            total = row[0]
            avg_score = round(row[1], 1) if row[1] else 0
        finally:
            cur.close()

        return {"status": "ok", "news": rows, "total": total, "avg_score": avg_score}

    def _get_moderation(self, body):
        """POST версия для совместимости."""
        self._json(self._get_moderation_list())

    def _seo_check(self, body):
        """SEO-анализ статьи."""
        from checks.seo_check import analyze_seo
        title = body.get("title", "")
        seo_title = body.get("seo_title", "")
        seo_description = body.get("seo_description", "")
        text = body.get("text", "")
        tags = body.get("tags", [])
        result = analyze_seo(title, seo_title, seo_description, text, tags)
        self._json({"status": "ok", **result})

    def _moderation_rewrite(self, body):
        """Отправляет новости из Модерации на рерайт (без API анализа, только LLM рерайт)."""
        news_ids = body.get("news_ids", [])
        style = body.get("style", "news")
        if not news_ids:
            self._json({"status": "error", "message": "news_ids required"})
            return

        from scheduler import _create_task
        task_ids = []
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            for nid in news_ids:
                try:
                    cur.execute(f"SELECT title FROM news WHERE id = {ph}", (nid,))
                    row = cur.fetchone()
                    title = (row[0] if _is_postgres() else row["title"]) if row else ""
                except Exception:
                    title = ""
                tid = _create_task("mod_rewrite", nid, title, style)
                task_ids.append(tid)
        finally:
            cur.close()

        # Process rewrites in background (no enrichment, just LLM rewrite)
        import threading
        def _bg_mod_rewrite(ids, tids, rewrite_style):
            from apis.llm import rewrite_news
            from scheduler import _update_task, _fetch_news_by_id, _fetch_analysis_by_id
            from storage.sheets import write_ready_row
            import uuid as _uuid
            import json as _json2
            for nid, tid in zip(ids, tids):
                try:
                    news = _fetch_news_by_id(nid)
                    if not news:
                        _update_task(tid, "error", {"error": "News not found"})
                        continue
                    _update_task(tid, "running", {"stage": "rewriting"})
                    result = rewrite_news(
                        title=news.get("title", ""),
                        text=news.get("plain_text", ""),
                        style=rewrite_style,
                        language="русский",
                    )
                    if result:
                        # Save article to DB
                        conn2 = get_connection()
                        cur2 = conn2.cursor()
                        ph2 = "%s" if _is_postgres() else "?"
                        _now = __import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat()
                        aid = str(_uuid.uuid4())[:12]
                        tags_j = _json2.dumps(result.get("tags", []), ensure_ascii=False)
                        try:
                            cur2.execute(f"""INSERT INTO articles (id, news_id, title, text, seo_title, seo_description, tags,
                                style, language, original_title, original_text, source_url, status, created_at, updated_at)
                                VALUES ({','.join([ph2]*15)})""",
                                (aid, nid, result.get("title", ""), result.get("text", ""),
                                 result.get("seo_title", ""), result.get("seo_description", ""), tags_j,
                                 rewrite_style, "русский", news.get("title", ""), (news.get("plain_text", "") or "")[:5000],
                                 news.get("url", ""), "draft", _now, _now))
                            if not _is_postgres():
                                conn2.commit()
                        finally:
                            cur2.close()

                        # Export to Sheets/Ready
                        try:
                            analysis = _fetch_analysis_by_id(nid)
                            write_ready_row(news, analysis, result)
                        except Exception as se:
                            logger.warning("Sheets Ready export failed for %s: %s", nid, se)

                        _update_task(tid, "done", {
                            "stage": "complete",
                            "rewrite_title": result.get("title", "")[:100],
                            "article_id": aid,
                        })
                        update_news_status(nid, "processed")
                    else:
                        _update_task(tid, "error", {"stage": "rewriting", "error": "Rewrite returned None"})
                except Exception as e:
                    _update_task(tid, "error", {"error": str(e)[:500]})

        threading.Thread(
            target=_bg_mod_rewrite,
            args=(list(news_ids), list(task_ids), style),
            daemon=True
        ).start()

        self._json({"status": "ok", "queued": len(news_ids), "task_ids": task_ids})

    def _test_sheets(self, body):
        try:
            import config
            from storage.sheets import _get_client
            client = _get_client()
            if not client:
                self._json({"status": "error", "message": "Google client init failed. Check GOOGLE_SERVICE_ACCOUNT_JSON"})
                return
            sheet = client.open_by_key(config.GOOGLE_SHEETS_ID)
            worksheets = [ws.title for ws in sheet.worksheets()]
            tab = sheet.worksheet(config.SHEETS_TAB)
            rows = len(tab.get_all_values())
            self._json({"status": "ok", "sheets_id": config.GOOGLE_SHEETS_ID, "tabs": worksheets, "active_tab": config.SHEETS_TAB, "rows": rows})
        except Exception as e:
            self._json({"status": "error", "message": str(e), "type": type(e).__name__})

    def _reparse_source(self, body):
        name = body.get("name")
        import config
        source = next((s for s in config.SOURCES if s["name"] == name), None)
        if not source:
            self._json({"status": "error", "message": "Source not found"})
            return
        try:
            if source["type"] == "rss":
                from parsers.rss_parser import parse_rss_source
                count = parse_rss_source(source)
            elif source["type"] == "sitemap":
                from parsers.html_parser import parse_sitemap_source
                count = parse_sitemap_source(source)
            else:
                from parsers.html_parser import parse_html_source
                count = parse_html_source(source)
            self._json({"status": "ok", "new_articles": count})
        except Exception as e:
            self._json({"status": "error", "message": str(e)})

    def _change_password(self, body):
        username = body.get("username", "")
        password = body.get("password", "")
        if not username or not password:
            self._json({"status": "error", "message": "Username and password required"})
            return
        if username not in USERS:
            self._json({"status": "error", "message": "User not found"})
            return
        USERS[username] = hashlib.sha256(password.encode()).hexdigest()
        self._json({"status": "ok"})

    def _bulk_status(self, body):
        news_ids = body.get("news_ids", [])
        new_status = body.get("status", "")
        if not news_ids or not new_status:
            self._json({"status": "error", "message": "news_ids and status required"})
            return
        for nid in news_ids:
            update_news_status(nid, new_status)
        self._json({"status": "ok", "updated": len(news_ids)})

    def _delete_news(self, body):
        if not self._require_perm("delete"):
            return
        news_ids = body.get("news_ids", [])
        if not news_ids:
            self._json({"status": "error", "message": "news_ids required"})
            return
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            for nid in news_ids:
                cur.execute(f"DELETE FROM news_analysis WHERE news_id = {ph}", (nid,))
                cur.execute(f"DELETE FROM news WHERE id = {ph}", (nid,))
            conn.commit()
            self._json({"status": "ok", "deleted": len(news_ids)})

        finally:
            cur.close()
    def _test_parse(self, body):
        url = body.get("url", "")
        if not url:
            self._json({"status": "error", "message": "URL required"})
            return
        try:
            from parsers.html_parser import _fetch_article
            h1, description, plain_text = _fetch_article(url)
            self._json({
                "status": "ok",
                "h1": h1,
                "description": description[:500],
                "plain_text": plain_text[:1000],
                "text_length": len(plain_text),
            })
        except Exception as e:
            self._json({"status": "error", "message": str(e)})

    def _setup_headers(self, body):
        try:
            from storage.sheets import setup_headers
            setup_headers()
            self._json({"status": "ok"})
        except Exception as e:
            self._json({"status": "error", "message": str(e)})

    def _reparse_all(self, body):
        try:
            import config
            from parsers.rss_parser import parse_rss_source
            from parsers.html_parser import parse_html_source, parse_sitemap_source
            total = 0
            for source in config.SOURCES:
                try:
                    if source["type"] == "rss":
                        total += parse_rss_source(source)
                    elif source["type"] == "sitemap":
                        total += parse_sitemap_source(source)
                    else:
                        total += parse_html_source(source)
                except Exception as e:
                    logger.error("Reparse %s error: %s", source["name"], e)
            self._json({"status": "ok", "new_articles": total})
        except Exception as e:
            self._json({"status": "error", "message": str(e)})

    def _get_sources_stats(self):
        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT source, COUNT(*) as cnt, MAX(parsed_at) as last_parsed FROM news GROUP BY source ORDER BY cnt DESC")
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]
            else:
                return [dict(row) for row in cur.fetchall()]

        finally:
            cur.close()
    def _get_db_info(self):
        conn = get_connection()
        cur = conn.cursor()
        try:
            info = {"type": "PostgreSQL" if _is_postgres() else "SQLite"}
            cur.execute("SELECT COUNT(*) FROM news")
            info["total_news"] = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM news_analysis")
            info["total_analyzed"] = cur.fetchone()[0]
            for status in ["new", "in_review", "approved", "processed", "rejected", "duplicate"]:
                ph = "%s" if _is_postgres() else "?"
                cur.execute(f"SELECT COUNT(*) FROM news WHERE status = {ph}", (status,))
                info[f"status_{status}"] = cur.fetchone()[0]
            cur.execute("SELECT MIN(parsed_at), MAX(parsed_at) FROM news")
            row = cur.fetchone()
            info["oldest"] = str(row[0]) if row[0] else "-"
            info["newest"] = str(row[1]) if row[1] else "-"
            return info

        finally:
            cur.close()
    def _export_sheets_bulk(self, body):
        news_ids = body.get("news_ids", [])
        if not news_ids:
            self._json({"status": "error", "message": "news_ids required"})
            return
        try:
            from storage.sheets import write_news_row
            conn = get_connection()
            cur = conn.cursor()
            try:
                ph = "%s" if _is_postgres() else "?"
                exported = 0
                skipped = 0
                errors = 0
                for nid in news_ids:
                    try:
                        cur.execute(f"SELECT * FROM news WHERE id = {ph}", (nid,))
                        row = cur.fetchone()
                        if not row:
                            continue
                        if _is_postgres():
                            columns = [desc[0] for desc in cur.description]
                            news = dict(zip(columns, row))
                        else:
                            news = dict(row)
                        cur.execute(f"SELECT * FROM news_analysis WHERE news_id = {ph}", (nid,))
                        arow = cur.fetchone()
                        if arow:
                            if _is_postgres():
                                columns = [desc[0] for desc in cur.description]
                                analysis = dict(zip(columns, arow))
                            else:
                                analysis = dict(arow)
                        else:
                            analysis = {"bigrams": "[]", "trends_data": "{}", "keyso_data": "{}",
                                       "llm_recommendation": "", "llm_trend_forecast": "", "llm_merged_with": ""}
                        sheet_row = write_news_row(news, analysis)
                        if sheet_row and sheet_row > 0:
                            exported += 1
                        elif sheet_row == -1:
                            skipped += 1
                    except Exception as e:
                        logger.warning("Bulk export error for %s: %s", nid, e)
                        errors += 1
                self._json({"status": "ok", "exported": exported, "skipped": skipped, "errors": errors})
            finally:
                cur.close()
        except Exception as e:
            self._json({"status": "error", "message": str(e)})

    def _rewrite_news(self, body):
        news_id = body.get("news_id")
        style = body.get("style", "news")
        language = body.get("language", "русский")
        try:
            conn = get_connection()
            cur = conn.cursor()
            try:
                ph = "%s" if _is_postgres() else "?"
                cur.execute(f"SELECT title, plain_text, description FROM news WHERE id = {ph}", (news_id,))
                row = cur.fetchone()
                if not row:
                    self._json({"status": "error", "message": "News not found"})
                    return
                if _is_postgres():
                    columns = [desc[0] for desc in cur.description]
                    news = dict(zip(columns, row))
                else:
                    news = dict(row)
                title = news.get("title", "")
                text = news.get("plain_text", "") or news.get("description", "")
                from apis.llm import rewrite_news
                result = rewrite_news(title, text, style, language)
                if result:
                    self._json({"status": "ok", "result": result, "original_title": title})
                else:
                    self._json({"status": "error", "message": "LLM returned no result"})
            finally:
                cur.close()
        except Exception as e:
            self._json({"status": "error", "message": str(e)})

    def _merge_news(self, body):
        news_ids = body.get("news_ids", [])
        if len(news_ids) < 2:
            self._json({"status": "error", "message": "Need at least 2 news to merge"})
            return
        try:
            conn = get_connection()
            cur = conn.cursor()
            try:
                ph = "%s" if _is_postgres() else "?"
                placeholders = ",".join([ph] * len(news_ids))
                cur.execute(f"SELECT id, source, title, plain_text FROM news WHERE id IN ({placeholders})", news_ids)
                if _is_postgres():
                    columns = [desc[0] for desc in cur.description]
                    news_list = [dict(zip(columns, row)) for row in cur.fetchall()]
                else:
                    news_list = [dict(row) for row in cur.fetchall()]
                from apis.llm import merge_news
                result = merge_news(news_list)
                if result:
                    self._json({"status": "ok", "result": result, "sources": [n["source"] for n in news_list]})
                else:
                    self._json({"status": "error", "message": "LLM returned no result"})
            finally:
                cur.close()
        except Exception as e:
            self._json({"status": "error", "message": str(e)})

    def _news_detail(self, body):
        news_id = body.get("news_id")
        if not news_id:
            self._json({"status": "error", "message": "news_id required"})
            return
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            cur.execute(f"SELECT * FROM news WHERE id = {ph}", (news_id,))
            row = cur.fetchone()
            if not row:
                self._json({"status": "error", "message": "Not found"})
                return
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                news = dict(zip(columns, row))
            else:
                news = dict(row)
            # Get analysis too
            cur.execute(f"SELECT * FROM news_analysis WHERE news_id = {ph}", (news_id,))
            arow = cur.fetchone()
            analysis = None
            if arow:
                if _is_postgres():
                    columns = [desc[0] for desc in cur.description]
                    analysis = dict(zip(columns, arow))
                else:
                    analysis = dict(arow)
            self._json({"status": "ok", "news": news, "analysis": analysis})

        finally:
            cur.close()
    def _analyze_news(self, body):
        """Полный анализ одной новости: viral, freshness, quality, relevance, sentiment, tags, trends, keyso."""
        news_id = body.get("news_id")
        if not news_id:
            self._json({"status": "error", "message": "news_id required"})
            return
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            cur.execute(f"SELECT * FROM news WHERE id = {ph}", (news_id,))
            row = cur.fetchone()
            if not row:
                self._json({"status": "error", "message": "Not found"})
                return
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                news = dict(zip(columns, row))
            else:
                news = dict(row)

            from checks.viral_score import viral_score
            from checks.freshness import check_freshness
            from checks.quality import check_quality
            from checks.relevance import check_relevance
            from checks.sentiment import analyze_sentiment
            from checks.tags import auto_tag
            from checks.momentum import get_momentum

            result = {
                "viral": viral_score(news),
                "freshness": check_freshness(news),
                "quality": check_quality(news),
                "relevance": check_relevance(news),
                "sentiment": analyze_sentiment(news),
                "tags": auto_tag(news),
                "momentum": get_momentum(news),
            }

            # Get existing analysis data (trends, keyso, llm)
            cur.execute(f"SELECT * FROM news_analysis WHERE news_id = {ph}", (news_id,))
            arow = cur.fetchone()
            if arow:
                if _is_postgres():
                    columns = [desc[0] for desc in cur.description]
                    analysis = dict(zip(columns, arow))
                else:
                    analysis = dict(arow)
                import json
                try:
                    result["trends_data"] = json.loads(analysis.get("trends_data", "{}"))
                except Exception:
                    result["trends_data"] = {}
                try:
                    result["keyso_data"] = json.loads(analysis.get("keyso_data", "{}"))
                except Exception:
                    result["keyso_data"] = {}
                result["llm_recommendation"] = analysis.get("llm_recommendation", "")
                result["llm_trend_forecast"] = analysis.get("llm_trend_forecast", "")
                try:
                    result["bigrams"] = json.loads(analysis.get("bigrams", "[]"))
                except Exception:
                    result["bigrams"] = []
            else:
                result["trends_data"] = {}
                result["keyso_data"] = {}
                result["llm_recommendation"] = ""
                result["llm_trend_forecast"] = ""
                result["bigrams"] = []

            total = sum(result[k]["score"] for k in ("viral", "freshness", "quality", "relevance")) // 4
            result["total_score"] = min(100, total + result["momentum"]["score"] // 5)
            self._json({"status": "ok", "analysis": result})

        finally:
            cur.close()
    def _batch_rewrite(self, body):
        """Батч-переписка новостей: создаёт статьи из списка news_ids."""
        news_ids = body.get("news_ids", [])
        style = body.get("style", "news")
        language = body.get("language", "русский")
        if not news_ids:
            self._json({"status": "error", "message": "news_ids required"})
            return
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            from apis.llm import rewrite_news
            import uuid, json
            from datetime import datetime, timezone

            results = []
            for nid in news_ids:
                try:
                    cur.execute(f"SELECT id, title, plain_text, description, url, source FROM news WHERE id = {ph}", (nid,))
                    row = cur.fetchone()
                    if not row:
                        results.append({"news_id": nid, "ok": False, "error": "not found"})
                        continue
                    if _is_postgres():
                        columns = [desc[0] for desc in cur.description]
                        news = dict(zip(columns, row))
                    else:
                        news = dict(row)
                    title = news.get("title", "")
                    text = news.get("plain_text", "") or news.get("description", "")
                    result = rewrite_news(title, text, style, language)
                    if not result:
                        results.append({"news_id": nid, "ok": False, "error": "LLM failed"})
                        continue
                    # Save as article
                    aid = str(uuid.uuid4())[:12]
                    now = datetime.now(timezone.utc).isoformat()
                    tags = json.dumps(result.get("tags", []), ensure_ascii=False)
                    cur.execute(f"""INSERT INTO articles (id, news_id, title, text, seo_title, seo_description, tags,
                        style, language, original_title, original_text, source_url, status, created_at, updated_at)
                        VALUES ({','.join([ph]*15)})""",
                        (aid, nid, result.get("title", ""), result.get("text", ""),
                         result.get("seo_title", ""), result.get("seo_description", ""), tags,
                         style, language, title, text[:5000],
                         news.get("url", ""), "draft", now, now))
                    if not _is_postgres():
                        conn.commit()
                    results.append({"news_id": nid, "ok": True, "article_id": aid, "title": result.get("title", "")})
                except Exception as e:
                    logger.warning("Batch rewrite error for %s: %s", nid, e)
                    results.append({"news_id": nid, "ok": False, "error": str(e)})

            ok_count = sum(1 for r in results if r.get("ok"))
            self._json({"status": "ok", "total": len(news_ids), "success": ok_count,
                         "failed": len(news_ids) - ok_count, "results": results})

        finally:
            cur.close()
    # ---- Analytics methods ----

    def _get_analytics(self):
        """Возвращает аналитику для дашборда."""
        import json
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"

            # 1. Top sources (7 days)
            if _is_postgres():
                cur.execute("""SELECT source, COUNT(*) as cnt FROM news
                    WHERE parsed_at::timestamptz > (NOW() - INTERVAL '7 days') GROUP BY source ORDER BY cnt DESC LIMIT 15""")
            else:
                cur.execute("SELECT source, COUNT(*) as cnt FROM news WHERE parsed_at > datetime('now', '-7 days') GROUP BY source ORDER BY cnt DESC LIMIT 15")
            top_sources = []
            for row in cur.fetchall():
                if _is_postgres():
                    top_sources.append({"source": row[0], "count": row[1]})
                else:
                    top_sources.append({"source": row["source"], "count": row["cnt"]})

            # 2. Status distribution
            cur.execute("SELECT status, COUNT(*) as cnt FROM news GROUP BY status")
            statuses = {}
            for row in cur.fetchall():
                if _is_postgres():
                    statuses[row[0]] = row[1]
                else:
                    statuses[row["status"]] = row["cnt"]

            # 3. Approval rate
            total_decisions = statuses.get("approved", 0) + statuses.get("processed", 0) + statuses.get("rejected", 0) + statuses.get("duplicate", 0)
            approved_total = statuses.get("approved", 0) + statuses.get("processed", 0)
            approval_rate = round(approved_total / total_decisions * 100, 1) if total_decisions > 0 else 0

            # 4. Top viral triggers (from review results in last 7 days of news_analysis)
            if _is_postgres():
                cur.execute("SELECT bigrams FROM news_analysis WHERE processed_at > (NOW() - INTERVAL '7 days')::text LIMIT 500")
            else:
                cur.execute("SELECT bigrams FROM news_analysis WHERE processed_at > datetime('now', '-7 days') LIMIT 500")
            all_bigrams = {}
            for row in cur.fetchall():
                raw = row[0] if _is_postgres() else row["bigrams"]
                try:
                    for bg in json.loads(raw or "[]"):
                        term = bg[0] if isinstance(bg, list) else bg
                        all_bigrams[term] = all_bigrams.get(term, 0) + 1
                except Exception:
                    pass
            top_bigrams = sorted(all_bigrams.items(), key=lambda x: x[1], reverse=True)[:20]

            # 5. News per day (last 14 days)
            if _is_postgres():
                cur.execute("""SELECT DATE(parsed_at::timestamp) as d, COUNT(*) as cnt FROM news
                    WHERE parsed_at::timestamptz > (NOW() - INTERVAL '14 days') GROUP BY d ORDER BY d""")
            else:
                cur.execute("SELECT DATE(parsed_at) as d, COUNT(*) as cnt FROM news WHERE parsed_at > datetime('now', '-14 days') GROUP BY d ORDER BY d")
            daily = []
            for row in cur.fetchall():
                if _is_postgres():
                    daily.append({"date": str(row[0]), "count": row[1]})
                else:
                    daily.append({"date": row["d"], "count": row["cnt"]})

            # 6. Peak hours
            if _is_postgres():
                cur.execute("""SELECT EXTRACT(HOUR FROM parsed_at::timestamp)::int as h, COUNT(*) as cnt FROM news
                    WHERE parsed_at::timestamptz > (NOW() - INTERVAL '7 days') GROUP BY h ORDER BY cnt DESC""")
            else:
                cur.execute("SELECT CAST(strftime('%H', parsed_at) AS INTEGER) as h, COUNT(*) as cnt FROM news WHERE parsed_at > datetime('now', '-7 days') GROUP BY h ORDER BY cnt DESC")
            peak_hours = []
            for row in cur.fetchall():
                if _is_postgres():
                    peak_hours.append({"hour": row[0], "count": row[1]})
                else:
                    peak_hours.append({"hour": row["h"], "count": row["cnt"]})

            # 7. Source weights
            try:
                from checks.source_weight import get_source_stats
                source_stats = get_source_stats()
            except Exception:
                source_stats = []

            # 8. Feedback summary
            try:
                from checks.feedback import get_feedback_summary
                feedback = get_feedback_summary()
            except Exception:
                feedback = {"sources": [], "tags": []}

            # 9. Articles stats
            cur.execute("SELECT status, COUNT(*) as cnt FROM articles GROUP BY status")
            art_stats = {}
            for row in cur.fetchall():
                if _is_postgres():
                    art_stats[row[0]] = row[1]
                else:
                    art_stats[row["status"]] = row["cnt"]

            # 10. Avg score per day (14 days)
            if _is_postgres():
                cur.execute("""SELECT DATE(n.parsed_at::timestamp) as d,
                    ROUND(AVG(COALESCE(a.total_score,0))::numeric, 1) as avg_score,
                    COUNT(*) as cnt
                    FROM news n LEFT JOIN news_analysis a ON n.id = a.news_id
                    WHERE n.parsed_at::timestamptz > (NOW() - INTERVAL '14 days') AND a.total_score > 0
                    GROUP BY d ORDER BY d""")
            else:
                cur.execute("""SELECT DATE(n.parsed_at) as d,
                    ROUND(AVG(COALESCE(a.total_score,0)), 1) as avg_score,
                    COUNT(*) as cnt
                    FROM news n LEFT JOIN news_analysis a ON n.id = a.news_id
                    WHERE n.parsed_at > datetime('now', '-14 days') AND a.total_score > 0
                    GROUP BY d ORDER BY d""")
            score_trend = []
            for row in cur.fetchall():
                if _is_postgres():
                    score_trend.append({"date": str(row[0]), "avg_score": float(row[1]), "count": row[2]})
                else:
                    score_trend.append({"date": row["d"], "avg_score": float(row["avg_score"]), "count": row["cnt"]})

            # 11. Conversion per day (approved vs rejected, 14 days)
            if _is_postgres():
                cur.execute("""SELECT DATE(parsed_at::timestamp) as d, status, COUNT(*) as cnt FROM news
                    WHERE parsed_at::timestamptz > (NOW() - INTERVAL '14 days')
                    AND status IN ('approved','processed','ready','rejected','duplicate')
                    GROUP BY d, status ORDER BY d""")
            else:
                cur.execute("""SELECT DATE(parsed_at) as d, status, COUNT(*) as cnt FROM news
                    WHERE parsed_at > datetime('now', '-14 days')
                    AND status IN ('approved','processed','ready','rejected','duplicate')
                    GROUP BY d, status ORDER BY d""")
            conv_raw = {}
            for row in cur.fetchall():
                d = str(row[0]) if _is_postgres() else row["d"]
                st = row[1] if _is_postgres() else row["status"]
                cnt = row[2] if _is_postgres() else row["cnt"]
                if d not in conv_raw:
                    conv_raw[d] = {"date": d, "approved": 0, "rejected": 0}
                if st in ("approved", "processed", "ready"):
                    conv_raw[d]["approved"] += cnt
                elif st in ("rejected", "duplicate"):
                    conv_raw[d]["rejected"] += cnt
            conversion_daily = sorted(conv_raw.values(), key=lambda x: x["date"])

            return {
                "status": "ok",
                "top_sources": top_sources,
                "statuses": statuses,
                "approval_rate": approval_rate,
                "top_bigrams": top_bigrams,
                "daily": daily,
                "peak_hours": peak_hours[:5],
                "source_stats": source_stats,
                "feedback": feedback,
                "article_stats": art_stats,
                "total_news": sum(statuses.values()),
                "total_articles": sum(art_stats.values()),
                "score_trend": score_trend,
                "conversion_daily": conversion_daily,
            }

        finally:
            cur.close()
    def _get_prompt_versions(self):
        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT * FROM prompt_versions ORDER BY prompt_name, version DESC")
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                rows = [dict(zip(columns, row)) for row in cur.fetchall()]
            else:
                rows = [dict(row) for row in cur.fetchall()]
            return {"status": "ok", "versions": rows}

        finally:
            cur.close()
    def _save_prompt_version(self, body):
        import uuid
        from datetime import datetime, timezone
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            name = body.get("prompt_name", "")
            content = body.get("content", "")
            notes = body.get("notes", "")
            if not name or not content:
                self._json({"status": "error", "message": "name and content required"})
                return
            # Get next version
            cur.execute(f"SELECT MAX(version) as mv FROM prompt_versions WHERE prompt_name = {ph}", (name,))
            row = cur.fetchone()
            if _is_postgres():
                max_v = row[0] if row and row[0] else 0
            else:
                max_v = row["mv"] if row and row["mv"] else 0
            if max_v is None:
                max_v = 0
            version = max_v + 1
            vid = str(uuid.uuid4())[:12]
            now = datetime.now(timezone.utc).isoformat()
            cur.execute(f"""INSERT INTO prompt_versions (id, prompt_name, version, content, is_active, created_at, notes)
                VALUES ({','.join([ph]*7)})""", (vid, name, version, content, 0, now, notes))
            if not _is_postgres():
                conn.commit()
            self._json({"status": "ok", "id": vid, "version": version})

        finally:
            cur.close()
    def _activate_prompt_version(self, body):
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            vid = body.get("id", "")
            if not vid:
                self._json({"status": "error", "message": "id required"})
                return
            # Get prompt name and content
            cur.execute(f"SELECT prompt_name, content FROM prompt_versions WHERE id = {ph}", (vid,))
            row = cur.fetchone()
            if not row:
                self._json({"status": "error", "message": "not found"})
                return
            if _is_postgres():
                name, content = row[0], row[1]
            else:
                name, content = row["prompt_name"], row["content"]
            # Deactivate all for this name
            cur.execute(f"UPDATE prompt_versions SET is_active = 0 WHERE prompt_name = {ph}", (name,))
            # Activate this one
            cur.execute(f"UPDATE prompt_versions SET is_active = 1 WHERE id = {ph}", (vid,))
            if not _is_postgres():
                conn.commit()
            # Apply to live prompts
            import apis.llm as llm
            prompt_map = {
                "trend_forecast": "PROMPT_TREND_FORECAST",
                "merge_analysis": "PROMPT_MERGE_ANALYSIS",
                "keyso_queries": "PROMPT_KEYSO_QUERIES",
                "rewrite": "PROMPT_REWRITE",
            }
            attr = prompt_map.get(name)
            if attr and hasattr(llm, attr):
                setattr(llm, attr, content)
                logger.info("Activated prompt version %s for %s", vid, name)
            self._json({"status": "ok", "prompt_name": name, "applied": bool(attr)})

        finally:
            cur.close()
    def _generate_digest(self, body):
        """Генерирует дайджест за указанный период."""
        period = body.get("period", "today")  # today, week
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            if period == "week":
                interval = "7 days"
            else:
                interval = "1 day"
            if _is_postgres():
                cur.execute(f"SELECT id, title, source, url FROM news WHERE status IN ('approved', 'processed') AND parsed_at::timestamptz > (NOW() - INTERVAL '{interval}') ORDER BY parsed_at DESC LIMIT 30")
                columns = [desc[0] for desc in cur.description]
                news_list = [dict(zip(columns, row)) for row in cur.fetchall()]
            else:
                cur.execute(f"SELECT id, title, source, url FROM news WHERE status IN ('approved', 'processed') AND parsed_at > datetime('now', '-{interval}') ORDER BY parsed_at DESC LIMIT 30")
                news_list = [dict(row) for row in cur.fetchall()]

            if not news_list:
                self._json({"status": "ok", "digest": {"title": "Нет данных", "summary": "Нет одобренных новостей за выбранный период.", "top_news": [], "trends": []}, "news_count": 0})
                return

            from apis.llm import _call_llm
            news_text = "\n".join(f"- [{n['source']}] {n['title']}" for n in news_list)
            period_label = 'неделю' if period == 'week' else 'день'
            prompt = f"""Ты — главный редактор крупного игрового портала. Составь профессиональный дайджест «Главное за {period_label}» из новостей ниже.
    
    ## Новости ({len(news_list)} шт.):
    {news_text}
    
    ## Правила:
    1. title — яркий заголовок дайджеста (напр. «Игровой дайджест: GTA 6, новый патч Elden Ring и скандал вокруг Ubisoft»)
    2. summary — связный текст на 4-6 предложений, охватывающий самые значимые события, не простое перечисление
    3. top_news — 3-5 самых важных новостей, одной фразой каждая (не копируй заголовки дословно, перефразируй)
    4. trends — 2-3 тенденции, которые прослеживаются в потоке новостей (напр. «Рост интереса к ретро-играм», «Волна переносов релизов»)
    5. Язык: русский
    
    Ответь строго JSON без markdown:
    {{
      "title": "Заголовок дайджеста",
      "summary": "Связный обзорный текст",
      "top_news": ["Ключевая новость 1", "Ключевая новость 2", "Ключевая новость 3"],
      "trends": ["Тенденция 1", "Тенденция 2"]
    }}"""
            result = _call_llm(prompt)
            if result:
                self._json({"status": "ok", "digest": result, "news_count": len(news_list)})
            else:
                self._json({"status": "error", "message": "LLM failed"})

        finally:
            cur.close()

    def _get_digests(self):
        """Возвращает последние 10 сохранённых дайджестов."""
        from storage.database import get_digests
        digests = get_digests(limit=10)
        return {"status": "ok", "digests": digests}

    def _generate_and_save_digest(self, body):
        """Ручная генерация дайджеста с сохранением в БД."""
        style = body.get("style", "brief")
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            if _is_postgres():
                cur.execute("""
                    SELECT n.id, n.title, n.source, n.url,
                           COALESCE(a.total_score, 0) as total_score
                    FROM news n
                    LEFT JOIN news_analysis a ON a.news_id = n.id
                    WHERE n.status IN ('approved', 'processed', 'in_review', 'ready')
                      AND n.parsed_at::timestamptz > (NOW() - INTERVAL '24 hours')
                    ORDER BY COALESCE(a.total_score, 0) DESC
                    LIMIT 20
                """)
                columns = [desc[0] for desc in cur.description]
                news_list = [dict(zip(columns, row)) for row in cur.fetchall()]
            else:
                cur.execute("""
                    SELECT n.id, n.title, n.source, n.url,
                           COALESCE(a.total_score, 0) as total_score
                    FROM news n
                    LEFT JOIN news_analysis a ON a.news_id = n.id
                    WHERE n.status IN ('approved', 'processed', 'in_review', 'ready')
                      AND n.parsed_at > datetime('now', '-1 day')
                    ORDER BY COALESCE(a.total_score, 0) DESC
                    LIMIT 20
                """)
                news_list = [dict(row) for row in cur.fetchall()]

            if not news_list:
                self._json({"status": "ok", "digest": {"title": "Нет данных", "text": "Нет новостей за последние 24 часа.", "news_count": 0}})
                return

            from apis.digest import generate_daily_digest
            result = generate_daily_digest(news_list, style=style)

            # Save to DB
            import uuid
            from datetime import datetime, timezone
            from storage.database import save_digest
            digest_id = str(uuid.uuid4())[:12]
            digest_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            save_digest(
                digest_id=digest_id,
                digest_date=digest_date,
                style=style,
                title=result.get("title", ""),
                text=result.get("text", ""),
                news_count=result.get("news_count", 0),
            )

            self._json({"status": "ok", "digest": result})

        except Exception as e:
            self._json({"status": "error", "message": str(e)[:500]})
        finally:
            cur.close()

    def _get_event_chain(self, body):
        news_id = body.get("news_id", "")
        if not news_id:
            self._json({"status": "error", "message": "news_id required"})
            return
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            cur.execute(f"SELECT * FROM news WHERE id = {ph}", (news_id,))
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                row = cur.fetchone()
                if not row:
                    self._json({"status": "error", "message": "not found"})
                    return
                news = dict(zip(columns, row))
            else:
                row = cur.fetchone()
                if not row:
                    self._json({"status": "error", "message": "not found"})
                    return
                news = dict(row)
            from checks.temporal_clusters import get_event_chain
            chain = get_event_chain(news)
            self._json({"status": "ok", **chain})

        finally:
            cur.close()
    # ---- Queue methods ----

    def _get_queue(self):
        conn = get_connection()
        cur = conn.cursor()
        try:
            q = "SELECT * FROM task_queue ORDER BY created_at DESC LIMIT 200"
            cur.execute(q)
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                rows = [dict(zip(columns, row)) for row in cur.fetchall()]
            else:
                rows = [dict(row) for row in cur.fetchall()]
            return {"status": "ok", "tasks": rows}

        finally:
            cur.close()
    def _cancel_queue_task(self, body):
        task_id = body.get("task_id")
        if not task_id:
            self._json({"status": "error", "message": "task_id required"})
            return
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            now = __import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat()
            cur.execute(f"UPDATE task_queue SET status = 'cancelled', updated_at = {ph} WHERE id = {ph} AND status = 'pending'", (now, task_id))
            if not _is_postgres():
                conn.commit()
            self._json({"status": "ok"})

        finally:
            cur.close()
    def _cancel_all_queue(self, body):
        task_type = body.get("task_type", "")
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            now = __import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat()
            if task_type:
                cur.execute(f"UPDATE task_queue SET status = 'cancelled', updated_at = {ph} WHERE status = 'pending' AND task_type = {ph}", (now, task_type))
            else:
                cur.execute(f"UPDATE task_queue SET status = 'cancelled', updated_at = {ph} WHERE status = 'pending'", (now,))
            if not _is_postgres():
                conn.commit()
            self._json({"status": "ok"})

        finally:
            cur.close()
    def _clear_done_queue(self, body):
        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute("DELETE FROM task_queue WHERE status IN ('done', 'cancelled', 'skipped', 'error')")
            if not _is_postgres():
                conn.commit()
            self._json({"status": "ok"})

        finally:
            cur.close()
    def _retry_queue_tasks(self, body):
        """Повторяет выбранные задачи из очереди (error → pending)."""
        task_ids = body.get("task_ids", [])
        if not task_ids:
            self._json({"status": "error", "message": "task_ids required"})
            return
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            from datetime import datetime, timezone
            now = datetime.now(timezone.utc).isoformat()
            count = 0
            for tid in task_ids:
                cur.execute(f"UPDATE task_queue SET status = 'pending', result = '', updated_at = {ph} WHERE id = {ph} AND status IN ('error', 'cancelled', 'skipped', 'done')", (now, tid))
                count += cur.rowcount if hasattr(cur, 'rowcount') else 1
            if not _is_postgres():
                conn.commit()

            # Re-run pending tasks in background
            cur.execute("SELECT id, task_type, news_id, news_title, style FROM task_queue WHERE status = 'pending' ORDER BY created_at")
            if _is_postgres():
                cols = [d[0] for d in cur.description]
                pending = [dict(zip(cols, r)) for r in cur.fetchall()]
            else:
                pending = [dict(r) for r in cur.fetchall()]

            if pending:
                import threading
                no_llm_tasks = [t for t in pending if t["task_type"] == "no_llm"]
                full_auto_tasks = [t for t in pending if t["task_type"] == "full_auto"]

                if no_llm_tasks:
                    nids = [t["news_id"] for t in no_llm_tasks]
                    tids = [t["id"] for t in no_llm_tasks]
                    from scheduler import run_no_llm_pipeline
                    threading.Thread(target=run_no_llm_pipeline, args=(nids, tids), daemon=True).start()
                if full_auto_tasks:
                    nids = [t["news_id"] for t in full_auto_tasks]
                    tids = [t["id"] for t in full_auto_tasks]
                    from scheduler import run_full_auto_pipeline
                    threading.Thread(target=run_full_auto_pipeline, args=(nids, tids), daemon=True).start()

            self._json({"status": "ok", "retried": count})
        finally:
            cur.close()

    def _get_viral_triggers(self):
        """Возвращает все триггеры виральности (дефолтные + кастомные из БД)."""
        from checks.viral_score import VIRAL_TRIGGERS
        conn = get_connection()
        cur = conn.cursor()
        try:
            # Load DB overrides
            db_triggers = {}
            try:
                cur.execute("SELECT trigger_id, label, weight, keywords, is_active, is_custom FROM viral_triggers_config")
                for row in cur.fetchall():
                    if _is_postgres():
                        tid, label, weight, kw_json, active, custom = row
                    else:
                        tid, label, weight, kw_json, active, custom = row["trigger_id"], row["label"], row["weight"], row["keywords"], row["is_active"], row["is_custom"]
                    import json as _j
                    kws = _j.loads(kw_json) if isinstance(kw_json, str) else (kw_json or [])
                    db_triggers[tid] = {"label": label, "weight": weight, "keywords": kws, "is_active": bool(active), "is_custom": bool(custom)}
            except Exception:
                pass

            result = []
            # Default triggers
            for tid, tdata in VIRAL_TRIGGERS.items():
                if tid in db_triggers:
                    dt = db_triggers[tid]
                    result.append({"id": tid, "label": dt["label"], "weight": dt["weight"], "keywords": dt["keywords"], "is_active": dt["is_active"], "is_custom": False, "modified": True})
                else:
                    result.append({"id": tid, "label": tdata["label"], "weight": tdata["weight"], "keywords": tdata["keywords"], "is_active": True, "is_custom": False, "modified": False})

            # Custom-only triggers (not in defaults)
            for tid, dt in db_triggers.items():
                if tid not in VIRAL_TRIGGERS:
                    result.append({"id": tid, "label": dt["label"], "weight": dt["weight"], "keywords": dt["keywords"], "is_active": dt["is_active"], "is_custom": True, "modified": False})

            result.sort(key=lambda x: (-x["weight"], x["label"]))
            return {"triggers": result, "total": len(result)}
        finally:
            cur.close()

    def _save_viral_trigger(self, body):
        """Сохраняет/обновляет триггер виральности."""
        trigger_id = body.get("trigger_id", "").strip()
        label = body.get("label", "").strip()
        weight = int(body.get("weight", 0))
        keywords = body.get("keywords", [])
        is_active = bool(body.get("is_active", True))
        is_custom = bool(body.get("is_custom", False))

        if not trigger_id or not label:
            self._json({"status": "error", "message": "trigger_id and label required"})
            return

        if isinstance(keywords, str):
            keywords = [k.strip() for k in keywords.split(",") if k.strip()]

        import json as _j
        from datetime import datetime, timezone
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            now = datetime.now(timezone.utc).isoformat()
            kw_json = _j.dumps(keywords, ensure_ascii=False)

            if _is_postgres():
                cur.execute(f"""
                    INSERT INTO viral_triggers_config (trigger_id, label, weight, keywords, is_active, is_custom, updated_at)
                    VALUES ({','.join([ph]*7)})
                    ON CONFLICT (trigger_id) DO UPDATE SET label={ph}, weight={ph}, keywords={ph}, is_active={ph}, updated_at={ph}
                """, (trigger_id, label, weight, kw_json, 1 if is_active else 0, 1 if is_custom else 0, now,
                      label, weight, kw_json, 1 if is_active else 0, now))
            else:
                cur.execute(f"INSERT OR REPLACE INTO viral_triggers_config (trigger_id, label, weight, keywords, is_active, is_custom, updated_at) VALUES ({','.join([ph]*7)})",
                            (trigger_id, label, weight, kw_json, 1 if is_active else 0, 1 if is_custom else 0, now))
                conn.commit()

            # Rebuild index
            from checks.viral_score import reload_viral_triggers
            reload_viral_triggers()

            self._json({"status": "ok", "trigger_id": trigger_id})
        finally:
            cur.close()

    def _delete_viral_trigger(self, body):
        """Удаляет кастомный триггер или сбрасывает изменённый дефолтный."""
        trigger_id = body.get("trigger_id", "")
        if not trigger_id:
            self._json({"status": "error", "message": "trigger_id required"})
            return
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            cur.execute(f"DELETE FROM viral_triggers_config WHERE trigger_id = {ph}", (trigger_id,))
            if not _is_postgres():
                conn.commit()
            from checks.viral_score import reload_viral_triggers
            reload_viral_triggers()
            self._json({"status": "ok"})
        finally:
            cur.close()

    def _queue_batch_rewrite(self, body):
        """Ставит новости в очередь на переписку и запускает обработку в фоне."""
        news_ids = body.get("news_ids", [])
        style = body.get("style", "news")
        language = body.get("language", "русский")
        if not news_ids:
            self._json({"status": "error", "message": "news_ids required"})
            return

        import uuid
        from datetime import datetime, timezone
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            now = datetime.now(timezone.utc).isoformat()
            created = []

            for nid in news_ids:
                cur.execute(f"SELECT title FROM news WHERE id = {ph}", (nid,))
                row = cur.fetchone()
                title = ""
                if row:
                    title = row[0] if _is_postgres() else row["title"]
                tid = str(uuid.uuid4())[:12]
                cur.execute(f"""INSERT INTO task_queue (id, task_type, news_id, news_title, style, status, created_at, updated_at)
                    VALUES ({','.join([ph]*8)})""",
                    (tid, "rewrite", nid, title[:200], style, "pending", now, now))
                created.append(tid)

            if not _is_postgres():
                conn.commit()

            # Process in background thread
            def _process_rewrite_queue():
                import json as _json
                from apis.llm import rewrite_news
                conn2 = get_connection()
                cur2 = conn2.cursor()
                try:
                    for tid in created:
                        cur2.execute(f"SELECT * FROM task_queue WHERE id = {ph}", (tid,))
                        if _is_postgres():
                            cols = [d[0] for d in cur2.description]
                            task = dict(zip(cols, cur2.fetchone()))
                        else:
                            task = dict(cur2.fetchone())
                        if task["status"] != "pending":
                            continue
                        nid = task["news_id"]
                        _now = datetime.now(timezone.utc).isoformat()
                        cur2.execute(f"UPDATE task_queue SET status = 'processing', updated_at = {ph} WHERE id = {ph}", (_now, tid))
                        if not _is_postgres():
                            conn2.commit()
                        try:
                            cur2.execute(f"SELECT id, title, plain_text, description, url, source FROM news WHERE id = {ph}", (nid,))
                            row = cur2.fetchone()
                            if not row:
                                raise Exception("news not found")
                            if _is_postgres():
                                cols = [d[0] for d in cur2.description]
                                news = dict(zip(cols, row))
                            else:
                                news = dict(row)
                            ntitle = news.get("title", "")
                            ntext = news.get("plain_text", "") or news.get("description", "")
                            result = rewrite_news(ntitle, ntext, style, language)
                            if not result:
                                raise Exception("LLM failed")
                            aid = str(uuid.uuid4())[:12]
                            tags = _json.dumps(result.get("tags", []), ensure_ascii=False)
                            cur2.execute(f"""INSERT INTO articles (id, news_id, title, text, seo_title, seo_description, tags,
                                style, language, original_title, original_text, source_url, status, created_at, updated_at)
                                VALUES ({','.join([ph]*15)})""",
                                (aid, nid, result.get("title", ""), result.get("text", ""),
                                 result.get("seo_title", ""), result.get("seo_description", ""), tags,
                                 style, language, ntitle, ntext[:5000],
                                 news.get("url", ""), "draft", _now, _now))
                            if not _is_postgres():
                                conn2.commit()

                            # Export to Sheets/Ready
                            try:
                                from storage.sheets import write_ready_row
                                # Fetch full news + analysis for Sheets
                                cur2.execute(f"SELECT * FROM news WHERE id = {ph}", (nid,))
                                if _is_postgres():
                                    nc = [d[0] for d in cur2.description]
                                    full_news = dict(zip(nc, cur2.fetchone()))
                                else:
                                    full_news = dict(cur2.fetchone())
                                cur2.execute(f"SELECT * FROM news_analysis WHERE news_id = {ph}", (nid,))
                                arow = cur2.fetchone()
                                analysis = None
                                if arow:
                                    if _is_postgres():
                                        ac = [d[0] for d in cur2.description]
                                        analysis = dict(zip(ac, arow))
                                    else:
                                        analysis = dict(arow)
                                write_ready_row(full_news, analysis, result)
                            except Exception as sheets_err:
                                logger.warning("Sheets Ready export failed for %s: %s", nid, sheets_err)

                            _now2 = datetime.now(timezone.utc).isoformat()
                            res_data = _json.dumps({"article_id": aid, "title": result.get("title", "")}, ensure_ascii=False)
                            cur2.execute(f"UPDATE task_queue SET status = 'done', result = {ph}, updated_at = {ph} WHERE id = {ph}", (res_data, _now2, tid))
                            if not _is_postgres():
                                conn2.commit()
                        except Exception as e:
                            logger.warning("Queue rewrite error %s: %s", tid, e)
                            _now2 = datetime.now(timezone.utc).isoformat()
                            cur2.execute(f"UPDATE task_queue SET status = 'error', result = {ph}, updated_at = {ph} WHERE id = {ph}", (str(e), _now2, tid))
                            if not _is_postgres():
                                conn2.commit()

                finally:
                    cur2.close()
            t = threading.Thread(target=_process_rewrite_queue, daemon=True)
            t.start()
            self._json({"status": "ok", "queued": len(created), "task_ids": created})

        finally:
            cur.close()
    def _queue_sheets_export(self, body):
        """Ставит новости в очередь на экспорт в Sheets и запускает обработку в фоне."""
        news_ids = body.get("news_ids", [])
        if not news_ids:
            self._json({"status": "error", "message": "news_ids required"})
            return

        import uuid
        from datetime import datetime, timezone
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            now = datetime.now(timezone.utc).isoformat()
            created = []

            for nid in news_ids:
                cur.execute(f"SELECT title FROM news WHERE id = {ph}", (nid,))
                row = cur.fetchone()
                title = ""
                if row:
                    title = row[0] if _is_postgres() else row["title"]
                tid = str(uuid.uuid4())[:12]
                cur.execute(f"""INSERT INTO task_queue (id, task_type, news_id, news_title, style, status, created_at, updated_at)
                    VALUES ({','.join([ph]*8)})""",
                    (tid, "sheets", nid, title[:200], "", "pending", now, now))
                created.append(tid)

            if not _is_postgres():
                conn.commit()

            # Process in background
            def _process_sheets_queue():
                import json as _json
                from storage.sheets import write_news_row
                conn2 = get_connection()
                cur2 = conn2.cursor()
                try:
                    for tid in created:
                        cur2.execute(f"SELECT * FROM task_queue WHERE id = {ph}", (tid,))
                        if _is_postgres():
                            cols = [d[0] for d in cur2.description]
                            task = dict(zip(cols, cur2.fetchone()))
                        else:
                            task = dict(cur2.fetchone())
                        if task["status"] != "pending":
                            continue
                        nid = task["news_id"]
                        _now = datetime.now(timezone.utc).isoformat()
                        cur2.execute(f"UPDATE task_queue SET status = 'processing', updated_at = {ph} WHERE id = {ph}", (_now, tid))
                        if not _is_postgres():
                            conn2.commit()
                        try:
                            cur2.execute(f"SELECT * FROM news WHERE id = {ph}", (nid,))
                            row = cur2.fetchone()
                            if not row:
                                raise Exception("news not found")
                            if _is_postgres():
                                cols = [d[0] for d in cur2.description]
                                news = dict(zip(cols, row))
                            else:
                                news = dict(row)
                            cur2.execute(f"SELECT * FROM news_analysis WHERE news_id = {ph}", (nid,))
                            arow = cur2.fetchone()
                            if arow:
                                if _is_postgres():
                                    cols = [d[0] for d in cur2.description]
                                    analysis = dict(zip(cols, arow))
                                else:
                                    analysis = dict(arow)
                            else:
                                analysis = {"bigrams": "[]", "trends_data": "{}", "keyso_data": "{}",
                                           "llm_recommendation": "", "llm_trend_forecast": "", "llm_merged_with": ""}
                            sheet_row = write_news_row(news, analysis)
                            _now2 = datetime.now(timezone.utc).isoformat()
                            if sheet_row and sheet_row > 0:
                                res_data = _json.dumps({"row": sheet_row}, ensure_ascii=False)
                                cur2.execute(f"UPDATE task_queue SET status = 'done', result = {ph}, updated_at = {ph} WHERE id = {ph}", (res_data, _now2, tid))
                            elif sheet_row == -1:
                                cur2.execute(f"UPDATE task_queue SET status = 'skipped', result = 'duplicate', updated_at = {ph} WHERE id = {ph}", (_now2, tid))
                            else:
                                cur2.execute(f"UPDATE task_queue SET status = 'error', result = 'no row', updated_at = {ph} WHERE id = {ph}", (_now2, tid))
                            if not _is_postgres():
                                conn2.commit()
                        except Exception as e:
                            logger.warning("Queue sheets error %s: %s", tid, e)
                            _now2 = datetime.now(timezone.utc).isoformat()
                            cur2.execute(f"UPDATE task_queue SET status = 'error', result = {ph}, updated_at = {ph} WHERE id = {ph}", (str(e), _now2, tid))
                            if not _is_postgres():
                                conn2.commit()

                finally:
                    cur2.close()
            t = threading.Thread(target=_process_sheets_queue, daemon=True)
            t.start()
            self._json({"status": "ok", "queued": len(created), "task_ids": created})

        finally:
            cur.close()
    def _serve_docx_bulk(self, article_ids):
        """Генерирует ZIP с несколькими DOCX файлами."""
        import io
        import json
        import zipfile
        from docx import Document
        from docx.shared import Pt, RGBColor
        from docx.enum.text import WD_ALIGN_PARAGRAPH

        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"

            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
                for aid in article_ids:
                    cur.execute(f"SELECT * FROM articles WHERE id = {ph}", (aid,))
                    row = cur.fetchone()
                    if not row:
                        continue
                    if _is_postgres():
                        columns = [desc[0] for desc in cur.description]
                        article = dict(zip(columns, row))
                    else:
                        article = dict(row)

                    doc = Document()
                    style = doc.styles['Normal']
                    style.font.name = 'Calibri'
                    style.font.size = Pt(11)

                    doc.add_heading(article.get("title", ""), level=1)

                    meta_p = doc.add_paragraph()
                    run = meta_p.add_run(f"Стиль: {article.get('style', '')} | Язык: {article.get('language', '')}")
                    run.font.size = Pt(9)
                    run.font.color.rgb = RGBColor(128, 128, 128)
                    if article.get("source_url"):
                        run2 = meta_p.add_run(f"\nИсточник: {article['source_url']}")
                        run2.font.size = Pt(9)
                        run2.font.color.rgb = RGBColor(128, 128, 128)

                    if article.get("seo_title") or article.get("seo_description"):
                        doc.add_heading("SEO", level=2)
                        if article.get("seo_title"):
                            p = doc.add_paragraph()
                            p.add_run("Title: ").bold = True
                            p.add_run(article["seo_title"])
                        if article.get("seo_description"):
                            p = doc.add_paragraph()
                            p.add_run("Description: ").bold = True
                            p.add_run(article["seo_description"])

                    tags = []
                    try:
                        tags = json.loads(article.get("tags", "[]"))
                    except Exception:
                        pass
                    if tags:
                        p = doc.add_paragraph()
                        p.add_run("Теги: ").bold = True
                        p.add_run(", ".join(tags))

                    doc.add_paragraph("")
                    doc.add_heading("Текст статьи", level=2)
                    text = article.get("text", "")
                    for paragraph in text.split("\n"):
                        paragraph = paragraph.strip()
                        if paragraph:
                            if paragraph.startswith("## "):
                                doc.add_heading(paragraph[3:], level=3)
                            elif paragraph.startswith("# "):
                                doc.add_heading(paragraph[2:], level=2)
                            else:
                                doc.add_paragraph(paragraph)

                    doc_buffer = io.BytesIO()
                    doc.save(doc_buffer)
                    safe_title = "".join(c for c in article.get("title", "article")[:40] if c.isalnum() or c in " _-").strip() or "article"
                    zf.writestr(f"{safe_title}.docx", doc_buffer.getvalue())

            data = zip_buffer.getvalue()
            self.send_response(200)
            self.send_header("Content-Type", "application/zip")
            self.send_header("Content-Disposition", 'attachment; filename="articles.zip"')
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        finally:
            cur.close()
    # --- Articles ---
    def _get_articles(self):
        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT * FROM articles ORDER BY updated_at DESC")
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]
            return [dict(row) for row in cur.fetchall()]

        finally:
            cur.close()
    def _save_article(self, body):
        import uuid
        from datetime import datetime, timezone
        aid = str(uuid.uuid4())[:12]
        now = datetime.now(timezone.utc).isoformat()
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            import json
            tags = json.dumps(body.get("tags", []), ensure_ascii=False)
            cur.execute(f"""INSERT INTO articles (id, news_id, title, text, seo_title, seo_description, tags,
                style, language, original_title, original_text, source_url, status, created_at, updated_at)
                VALUES ({','.join([ph]*15)})""",
                (aid, body.get("news_id", ""), body.get("title", ""), body.get("text", ""),
                 body.get("seo_title", ""), body.get("seo_description", ""), tags,
                 body.get("style", ""), body.get("language", "русский"),
                 body.get("original_title", ""), body.get("original_text", ""),
                 body.get("source_url", ""), "draft", now, now))
            if not _is_postgres():
                conn.commit()
            self._json({"status": "ok", "id": aid})

        finally:
            cur.close()
    def _update_article(self, body):
        from datetime import datetime, timezone
        aid = body.get("id")
        if not aid:
            self._json({"status": "error", "message": "id required"})
            return

        # Save version snapshot before update (Phase 2)
        try:
            self._save_article_version(aid, body, change_type="manual",
                                        changed_by=self._get_session_user() or "admin")
        except Exception:
            pass

        now = datetime.now(timezone.utc).isoformat()
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            import json
            tags = json.dumps(body.get("tags", []), ensure_ascii=False)
            cur.execute(f"""UPDATE articles SET title={ph}, text={ph}, seo_title={ph},
                seo_description={ph}, tags={ph}, status={ph}, updated_at={ph} WHERE id={ph}""",
                (body.get("title", ""), body.get("text", ""), body.get("seo_title", ""),
                 body.get("seo_description", ""), tags, body.get("status", "draft"), now, aid))
            if not _is_postgres():
                conn.commit()
            self._json({"status": "ok"})

        finally:
            cur.close()
    def _delete_article(self, body):
        aid = body.get("id")
        if not aid:
            self._json({"status": "error", "message": "id required"})
            return
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            cur.execute(f"DELETE FROM articles WHERE id = {ph}", (aid,))
            if not _is_postgres():
                conn.commit()
            self._json({"status": "ok"})

        finally:
            cur.close()
    def _article_detail(self, body):
        aid = body.get("id")
        if not aid:
            self._json({"status": "error", "message": "id required"})
            return
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            cur.execute(f"SELECT * FROM articles WHERE id = {ph}", (aid,))
            row = cur.fetchone()
            if not row:
                self._json({"status": "error", "message": "Not found"})
                return
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                article = dict(zip(columns, row))
            else:
                article = dict(row)
            self._json({"status": "ok", "article": article})

        finally:
            cur.close()
    def _schedule_article(self, body):
        """Запланировать публикацию статьи на указанное время."""
        from datetime import datetime, timezone
        aid = body.get("article_id") or body.get("id")
        scheduled_at = body.get("scheduled_at")
        if not aid:
            self._json({"status": "error", "message": "article_id required"})
            return
        if not scheduled_at:
            self._json({"status": "error", "message": "scheduled_at required"})
            return
        now = datetime.now(timezone.utc).isoformat()
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            cur.execute(f"UPDATE articles SET scheduled_at={ph}, status='scheduled', updated_at={ph} WHERE id={ph}",
                        (scheduled_at, now, aid))
            if not _is_postgres():
                conn.commit()
            self._json({"status": "ok", "scheduled_at": scheduled_at})
        finally:
            cur.close()
    def _rewrite_article(self, body):
        """Переписать существующую статью в другом стиле."""
        aid = body.get("id")
        style = body.get("style", "news")
        language = body.get("language", "русский")
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            cur.execute(f"SELECT title, text, original_title, original_text FROM articles WHERE id = {ph}", (aid,))
            row = cur.fetchone()
            if not row:
                self._json({"status": "error", "message": "Article not found"})
                return
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                article = dict(zip(columns, row))
            else:
                article = dict(row)
            # Use original text for rewriting to avoid degradation
            src_title = article.get("original_title") or article.get("title", "")
            src_text = article.get("original_text") or article.get("text", "")
            from apis.llm import rewrite_news
            result = rewrite_news(src_title, src_text, style, language)
            if result:
                self._json({"status": "ok", "result": result})
            else:
                self._json({"status": "error", "message": "LLM returned no result"})

        finally:
            cur.close()
    def _improve_article(self, body):
        """Улучшить текст статьи через LLM (грамматика, стиль, SEO)."""
        aid = body.get("id")
        action = body.get("action", "improve")  # improve, expand, shorten, fix_grammar, add_seo
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            cur.execute(f"SELECT title, text FROM articles WHERE id = {ph}", (aid,))
            row = cur.fetchone()
            if not row:
                self._json({"status": "error", "message": "Article not found"})
                return
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                article = dict(zip(columns, row))
            else:
                article = dict(row)

            actions_map = {
                "improve": "Улучши текст: исправь стилистические ошибки, сделай более профессиональным, сохрани факты.",
                "expand": "Расширь текст: добавь подробностей, контекста, аналитики. Увеличь объём в 1.5-2 раза, не добавляя вымышленных фактов.",
                "shorten": "Сократи текст в 2 раза, оставив только ключевые факты. Убери воду и повторы.",
                "fix_grammar": "Исправь все грамматические, пунктуационные и стилистические ошибки. Не меняй смысл и структуру.",
                "add_seo": "Добавь SEO-оптимизацию: включи ключевые слова естественно, добавь подзаголовки (## H2), улучши мета-описание.",
                "make_engaging": "Сделай текст более вовлекающим: добавь интригу, живые примеры, вопросы к читателю. Сохрани факты.",
            }
            instruction = actions_map.get(action, actions_map["improve"])

            from apis.llm import _call_llm
            prompt = f"""Ты — профессиональный редактор игровых новостей.
    
    Задача: {instruction}
    
    Заголовок: {article['title']}
    Текст: {article['text'][:4000]}
    
    Верни строго JSON (без markdown):
    {{
      "title": "обновлённый заголовок",
      "text": "обновлённый текст",
      "seo_title": "SEO title до 60 символов",
      "seo_description": "meta description до 155 символов",
      "changes_summary": "что было изменено (1-2 предложения)"
    }}"""
            result = _call_llm(prompt)
            if result:
                self._json({"status": "ok", "result": result})
            else:
                self._json({"status": "error", "message": "LLM returned no result"})

        finally:
            cur.close()
    def _serve_docx(self, article_id):
        """Генерация и отдача DOCX файла."""
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            cur.execute(f"SELECT * FROM articles WHERE id = {ph}", (article_id,))
            row = cur.fetchone()
            if not row:
                self.send_response(404)
                self.end_headers()
                return
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                article = dict(zip(columns, row))
            else:
                article = dict(row)

            import io
            import json
            from docx import Document
            from docx.shared import Pt, Inches, RGBColor
            from docx.enum.text import WD_ALIGN_PARAGRAPH

            doc = Document()
            style = doc.styles['Normal']
            style.font.name = 'Calibri'
            style.font.size = Pt(11)

            # Title
            title_p = doc.add_heading(article.get("title", ""), level=1)
            title_p.alignment = WD_ALIGN_PARAGRAPH.LEFT

            # Meta info block
            meta_p = doc.add_paragraph()
            meta_p.paragraph_format.space_after = Pt(6)
            run = meta_p.add_run(f"Стиль: {article.get('style', '')} | Язык: {article.get('language', '')}")
            run.font.size = Pt(9)
            run.font.color.rgb = RGBColor(128, 128, 128)
            if article.get("source_url"):
                run2 = meta_p.add_run(f"\nИсточник: {article['source_url']}")
                run2.font.size = Pt(9)
                run2.font.color.rgb = RGBColor(128, 128, 128)

            # SEO block
            if article.get("seo_title") or article.get("seo_description"):
                doc.add_heading("SEO", level=2)
                if article.get("seo_title"):
                    p = doc.add_paragraph()
                    p.add_run("Title: ").bold = True
                    p.add_run(article["seo_title"])
                if article.get("seo_description"):
                    p = doc.add_paragraph()
                    p.add_run("Description: ").bold = True
                    p.add_run(article["seo_description"])

            # Tags
            tags = []
            try:
                tags = json.loads(article.get("tags", "[]"))
            except Exception:
                pass
            if tags:
                p = doc.add_paragraph()
                p.add_run("Теги: ").bold = True
                p.add_run(", ".join(tags))

            doc.add_paragraph("")  # spacer

            # Article text
            doc.add_heading("Текст статьи", level=2)
            text = article.get("text", "")
            for paragraph in text.split("\n"):
                paragraph = paragraph.strip()
                if paragraph:
                    if paragraph.startswith("## "):
                        doc.add_heading(paragraph[3:], level=3)
                    elif paragraph.startswith("# "):
                        doc.add_heading(paragraph[2:], level=2)
                    else:
                        doc.add_paragraph(paragraph)

            # Original text if exists
            if article.get("original_text"):
                doc.add_page_break()
                doc.add_heading("Оригинал", level=2)
                orig_p = doc.add_paragraph()
                if article.get("original_title"):
                    run = orig_p.add_run(article["original_title"] + "\n\n")
                    run.bold = True
                for line in article["original_text"][:3000].split("\n"):
                    line = line.strip()
                    if line:
                        p = doc.add_paragraph(line)
                        for run in p.runs:
                            run.font.color.rgb = RGBColor(128, 128, 128)
                            run.font.size = Pt(10)

            buffer = io.BytesIO()
            doc.save(buffer)
            data = buffer.getvalue()

            safe_title = "".join(c for c in article.get("title", "article")[:40] if c.isalnum() or c in " _-").strip() or "article"
            filename = f"{safe_title}.docx"

            self.send_response(200)
            self.send_header("Content-Type", "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        finally:
            cur.close()
    # --- Logs, Cache, Rate, Translate, AI ---

    def _get_viral(self):
        """Анализ виральности: прогоняет все новости через viral_score + sentiment + momentum."""
        from checks.viral_score import viral_score, VIRAL_TRIGGERS, get_calendar_boost
        from checks.sentiment import analyze_sentiment
        from checks.tags import auto_tag
        from apis.cache import cache_get, cache_set, cache_key

        qs = parse_qs(urlparse(self.path).query)
        limit = int(qs.get("limit", [200])[0])
        level_filter = qs.get("level", [None])[0]
        category_filter = qs.get("category", [None])[0]
        sentiment_filter = qs.get("sentiment", [None])[0]
        source_filter = qs.get("source", [None])[0]
        date_from = qs.get("date_from", [None])[0]
        date_to = qs.get("date_to", [None])[0]
        trigger_filter = qs.get("trigger", [None])[0]
        min_score = int(qs.get("min_score", [0])[0])

        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"

            conditions = []
            params = []
            if source_filter:
                conditions.append(f"n.source = {ph}")
                params.append(source_filter)
            if date_from:
                conditions.append(f"n.parsed_at >= {ph}")
                params.append(date_from)
            if date_to:
                conditions.append(f"n.parsed_at <= {ph}")
                params.append(date_to + "T23:59:59")
            where = "WHERE " + " AND ".join(conditions) if conditions else ""

            cur.execute(f"""
                SELECT n.id, n.source, n.title, n.url, n.description, n.plain_text,
                       n.published_at, n.parsed_at, n.status
                FROM news n {where}
                ORDER BY n.parsed_at DESC LIMIT {ph}
            """, params + [limit])

            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                rows = [dict(zip(columns, row)) for row in cur.fetchall()]
            else:
                rows = [dict(row) for row in cur.fetchall()]

            items = []
            stats = {"total": 0, "high": 0, "medium": 0, "low": 0, "none": 0}
            trigger_counts = {}
            category_counts = {}
            sentiment_counts = {"positive": 0, "negative": 0, "neutral": 0}
            source_scores = {}

            # Category mapping from trigger_id prefix
            CATEGORY_MAP = {
                "scandal": "Скандалы", "leak": "Утечки", "shadow": "Shadow Drops",
                "bad": "Плохие релизы", "ai": "AI", "major_event": "Ивенты",
                "event": "Ивенты", "money": "Деньги", "culture": "Культура",
                "person": "Персоны", "speed": "Скорость",
                "sequel": "Базовые", "free_content": "Базовые", "delay": "Базовые",
                "canceled": "Базовые", "award": "Базовые", "next_gen": "Базовые",
                "big_update": "Базовые", "release_date": "Базовые",
                "trailer": "Базовые", "record": "Базовые", "digest": "Базовые",
            }

            for row in rows:
                ck = cache_key("viral_tab", row["id"])
                cached = cache_get(ck)
                if cached:
                    vr = cached["viral"]
                    sent = cached["sentiment"]
                    tags = cached["tags"]
                else:
                    vr = viral_score(row)
                    sent = analyze_sentiment(row)
                    tags = auto_tag(row)
                    cache_set(ck, {"viral": vr, "sentiment": sent, "tags": tags}, ttl=3600)

                # Determine categories of triggers
                trigger_categories = set()
                for t in vr["triggers"]:
                    tid = t["id"]
                    prefix = tid.split("_")[0]
                    cat = CATEGORY_MAP.get(tid, CATEGORY_MAP.get(prefix, "Прочее"))
                    trigger_categories.add(cat)

                # Apply filters
                if level_filter and vr["level"] != level_filter:
                    continue
                if min_score and vr["score"] < min_score:
                    continue
                if sentiment_filter and sent["label"] != sentiment_filter:
                    continue
                if trigger_filter:
                    if not any(t["id"] == trigger_filter for t in vr["triggers"]):
                        continue
                if category_filter:
                    if category_filter not in trigger_categories:
                        continue

                item = {
                    "id": row["id"],
                    "source": row["source"],
                    "title": row["title"],
                    "url": row["url"],
                    "published_at": row["published_at"],
                    "parsed_at": row["parsed_at"],
                    "status": row["status"],
                    "viral_score": vr["score"],
                    "viral_level": vr["level"],
                    "triggers": vr["triggers"],
                    "sentiment": sent["label"],
                    "sentiment_score": sent["score"],
                    "tags": [{"id": t["id"], "label": t["label"]} for t in tags[:3]],
                }
                items.append(item)

                # Aggregate stats
                stats["total"] += 1
                stats[vr["level"]] = stats.get(vr["level"], 0) + 1
                sentiment_counts[sent["label"]] = sentiment_counts.get(sent["label"], 0) + 1
                for t in vr["triggers"]:
                    trigger_counts[t["label"]] = trigger_counts.get(t["label"], 0) + 1
                    prefix = t["id"].split("_")[0]
                    cat = CATEGORY_MAP.get(t["id"], CATEGORY_MAP.get(prefix, "Прочее"))
                    category_counts[cat] = category_counts.get(cat, 0) + 1
                src = row["source"]
                if src not in source_scores:
                    source_scores[src] = {"total": 0, "sum": 0}
                source_scores[src]["total"] += 1
                source_scores[src]["sum"] += vr["score"]

            # Sort by viral_score desc
            items.sort(key=lambda x: x["viral_score"], reverse=True)

            # Top triggers sorted
            top_triggers = sorted(trigger_counts.items(), key=lambda x: x[1], reverse=True)[:20]

            # Top categories sorted
            top_categories = sorted(category_counts.items(), key=lambda x: x[1], reverse=True)

            # Source avg scores
            source_avg = []
            for src, data in source_scores.items():
                source_avg.append({"source": src, "avg": round(data["sum"] / data["total"], 1), "count": data["total"]})
            source_avg.sort(key=lambda x: x["avg"], reverse=True)

            # Calendar event
            cal_boost, cal_event = get_calendar_boost()

            # Available triggers for filter
            all_triggers = [{"id": k, "label": v["label"], "category": CATEGORY_MAP.get(k, CATEGORY_MAP.get(k.split("_")[0], "Прочее"))} for k, v in VIRAL_TRIGGERS.items()]

            return {
                "items": items,
                "stats": stats,
                "sentiment": sentiment_counts,
                "top_triggers": top_triggers,
                "top_categories": top_categories,
                "source_avg": source_avg[:15],
                "calendar": {"boost": cal_boost, "event": cal_event},
                "all_triggers": all_triggers,
            }

        finally:
            cur.close()
    def _get_logs(self):
        qs = parse_qs(urlparse(self.path).query)
        limit = int(qs.get("limit", [100])[0])
        level = qs.get("level", [""])[0]
        from apis.cache import get_logs
        return {"logs": get_logs(limit=limit, level=level)}

    def _get_rate_stats(self):
        from apis.cache import get_rate_stats
        return get_rate_stats()

    def _get_cache_stats(self):
        from apis.cache import get_cache_stats
        return get_cache_stats()

    def _clear_cache(self, body):
        from apis.cache import clear_cache
        clear_cache()
        self._json({"status": "ok"})

    def _translate_title(self, body):
        news_id = body.get("news_id", "")
        if not news_id:
            self._json({"status": "error", "message": "news_id required"})
            return
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            cur.execute(f"SELECT title FROM news WHERE id = {ph}", (news_id,))
            row = cur.fetchone()
            if not row:
                self._json({"status": "error", "message": "not found"})
                return
            title = row[0] if _is_postgres() else row["title"]
            try:
                from apis.llm import translate_title
                result = translate_title(title)
                if result:
                    # Save translated title as h1 if not Russian
                    if not result.get("is_russian") and result.get("translated"):
                        cur.execute(f"UPDATE news SET h1 = {ph} WHERE id = {ph}", (result["translated"], news_id))
                        if not _is_postgres():
                            conn.commit()
                    self._json({"status": "ok", **result})
                else:
                    self._json({"status": "error", "message": "LLM not responding. Check API keys and rate limits."})
            except Exception as e:
                logger.error("Translate error: %s", e)
                self._json({"status": "error", "message": str(e)})

        finally:
            cur.close()
    def _ai_recommend(self, body):
        news_id = body.get("news_id", "")
        if not news_id:
            self._json({"status": "error", "message": "news_id required"})
            return
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            cur.execute(f"SELECT * FROM news WHERE id = {ph}", (news_id,))
            row = cur.fetchone()
            if not row:
                self._json({"status": "error", "message": "not found"})
                return
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                news = dict(zip(columns, row))
            else:
                news = dict(row)

            # Run checks for context
            from checks.quality import check_quality
            from checks.relevance import check_relevance
            from checks.freshness import check_freshness
            from checks.viral_score import viral_score
            checks = {
                "quality": check_quality(news),
                "relevance": check_relevance(news),
                "freshness": check_freshness(news),
                "viral": viral_score(news),
            }
            from apis.llm import ai_recommendation
            result = ai_recommendation(
                title=news.get("title", ""),
                text=news.get("plain_text", "") or news.get("description", ""),
                source=news.get("source", ""),
                checks=checks,
            )
            if result:
                self._json({"status": "ok", "recommendation": result, "checks": {k: v.get("score", 0) for k, v in checks.items()}})
            else:
                self._json({"status": "error", "message": "AI recommendation failed"})

        finally:
            cur.close()
    # --- Phase 3: Analytics Funnel & Source Intelligence ---

    def _get_funnel_analytics(self):
        """Full pipeline funnel: parsed → reviewed → approved → enriched → final_passed → rewritten → exported → published."""
        conn = get_connection()
        cur = conn.cursor()
        try:
            ph = "%s" if _is_postgres() else "?"
            funnel = {}

            # Total parsed
            cur.execute("SELECT COUNT(*) FROM news")
            funnel["parsed"] = cur.fetchone()[0]

            # Reviewed (has analysis)
            cur.execute("SELECT COUNT(*) FROM news_analysis WHERE total_score > 0")
            funnel["reviewed"] = cur.fetchone()[0]

            # By status
            for status in ["in_review", "approved", "processed", "moderation", "ready", "rejected", "duplicate"]:
                cur.execute(f"SELECT COUNT(*) FROM news WHERE status = {ph}", (status,))
                funnel[status] = cur.fetchone()[0]

            # Articles created (rewritten)
            cur.execute("SELECT COUNT(*) FROM articles")
            funnel["rewritten"] = cur.fetchone()[0]

            # Published articles
            cur.execute(f"SELECT COUNT(*) FROM articles WHERE status = 'published'")
            funnel["published"] = cur.fetchone()[0]

            # Conversion by source
            cur.execute("""
                SELECT n.source,
                       COUNT(*) as total,
                       SUM(CASE WHEN n.status = 'ready' THEN 1 ELSE 0 END) as ready_count,
                       SUM(CASE WHEN n.status = 'rejected' THEN 1 ELSE 0 END) as rejected_count,
                       SUM(CASE WHEN n.status = 'duplicate' THEN 1 ELSE 0 END) as dup_count
                FROM news n
                GROUP BY n.source
                ORDER BY total DESC
            """)
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                by_source = [dict(zip(columns, r)) for r in cur.fetchall()]
            else:
                by_source = [dict(r) for r in cur.fetchall()]

            # Score distribution
            cur.execute("""
                SELECT
                    SUM(CASE WHEN total_score >= 70 THEN 1 ELSE 0 END) as high,
                    SUM(CASE WHEN total_score >= 40 AND total_score < 70 THEN 1 ELSE 0 END) as medium,
                    SUM(CASE WHEN total_score >= 15 AND total_score < 40 THEN 1 ELSE 0 END) as low,
                    SUM(CASE WHEN total_score < 15 THEN 1 ELSE 0 END) as rejected_range
                FROM news_analysis WHERE total_score > 0
            """)
            row = cur.fetchone()
            if row:
                if _is_postgres():
                    funnel["score_distribution"] = {"high": row[0] or 0, "medium": row[1] or 0, "low": row[2] or 0, "rejected_range": row[3] or 0}
                else:
                    funnel["score_distribution"] = {"high": row[0] or 0, "medium": row[1] or 0, "low": row[2] or 0, "rejected_range": row[3] or 0}

            funnel["by_source"] = by_source
            return funnel
        except Exception as e:
            return {"error": str(e)}
        finally:
            cur.close()

    def _get_cost_by_source(self):
        """API cost broken down by source (via news_id correlation)."""
        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT n.source,
                       COUNT(c.id) as api_calls,
                       COALESCE(SUM(c.cost_usd), 0) as total_cost,
                       COALESCE(AVG(c.latency_ms), 0) as avg_latency
                FROM api_cost_log c
                JOIN news n ON c.news_id = n.id
                WHERE c.news_id != ''
                GROUP BY n.source
                ORDER BY total_cost DESC
            """)
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                rows = [dict(zip(columns, r)) for r in cur.fetchall()]
            else:
                rows = [dict(r) for r in cur.fetchall()]
            return {"by_source": rows}
        except Exception as e:
            return {"by_source": [], "error": str(e)}
        finally:
            cur.close()

    # --- Phase 2: Content Versioning & Multi-Output ---

    def _get_article_versions(self, body):
        """Get version history for an article."""
        article_id = body.get("article_id", "")
        if not article_id:
            self._json({"error": "article_id required"}, 400)
            return
        conn = get_connection()
        cur = conn.cursor()
        ph = "%s" if _is_postgres() else "?"
        try:
            cur.execute(f"""
                SELECT * FROM article_versions
                WHERE article_id = {ph}
                ORDER BY version DESC
            """, (article_id,))
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                rows = [dict(zip(columns, r)) for r in cur.fetchall()]
            else:
                rows = [dict(r) for r in cur.fetchall()]
            self._json({"versions": rows})
        except Exception as e:
            self._json({"versions": [], "error": str(e)})
        finally:
            cur.close()

    def _save_article_version(self, article_id, article_data, change_type="manual", changed_by="system"):
        """Save a version snapshot before modification (internal helper)."""
        try:
            from core.feature_flags import is_enabled
            if not is_enabled("content_versions_v1"):
                return
        except Exception:
            return

        import uuid
        from datetime import datetime, timezone
        conn = get_connection()
        cur = conn.cursor()
        ph = "%s" if _is_postgres() else "?"
        try:
            # Get current max version
            cur.execute(f"SELECT COALESCE(MAX(version), 0) FROM article_versions WHERE article_id = {ph}", (article_id,))
            max_ver = cur.fetchone()[0]
            now = datetime.now(timezone.utc).isoformat()
            ver_id = uuid.uuid4().hex[:12]

            import json
            cur.execute(f"""
                INSERT INTO article_versions (id, article_id, version, title, text, seo_title, seo_description, tags, change_type, changed_by, created_at)
                VALUES ({','.join([ph]*11)})
            """, (ver_id, article_id, max_ver + 1,
                  article_data.get("title", "")[:500],
                  article_data.get("text", "")[:5000],
                  article_data.get("seo_title", "")[:500],
                  article_data.get("seo_description", "")[:1000],
                  json.dumps(article_data.get("tags", []), ensure_ascii=False),
                  change_type, changed_by, now))
            if not _is_postgres():
                conn.commit()
        except Exception as e:
            logger.debug("Failed to save article version: %s", e)
        finally:
            cur.close()

    def _generate_multi_output(self, body):
        """Generate multiple output formats from one article."""
        article_id = body.get("article_id", "")
        formats = body.get("formats", ["social", "short"])
        if not article_id:
            self._json({"error": "article_id required"}, 400)
            return

        conn = get_connection()
        cur = conn.cursor()
        ph = "%s" if _is_postgres() else "?"
        try:
            cur.execute(f"SELECT title, text FROM articles WHERE id = {ph}", (article_id,))
            row = cur.fetchone()
            if not row:
                self._json({"error": "article not found"}, 404)
                return
            if _is_postgres():
                title, text = row[0], row[1]
            else:
                title, text = row["title"], row["text"]

            from apis.llm import rewrite_news
            results = {}
            for fmt in formats[:3]:  # Max 3 formats at once
                result = rewrite_news(title=title, text=text, style=fmt, language="русский")
                if result:
                    results[fmt] = result

            self._json({"article_id": article_id, "outputs": results})
        except Exception as e:
            self._json({"error": str(e)}, 500)
        finally:
            cur.close()

    def _regenerate_field(self, body):
        """Regenerate a single field (title, seo_title, seo_description, tags) via LLM."""
        article_id = body.get("article_id", "")
        field = body.get("field", "")
        if not article_id or field not in ("title", "seo_title", "seo_description", "tags"):
            self._json({"error": "article_id and valid field required"}, 400)
            return

        conn = get_connection()
        cur = conn.cursor()
        ph = "%s" if _is_postgres() else "?"
        try:
            cur.execute(f"SELECT title, text FROM articles WHERE id = {ph}", (article_id,))
            row = cur.fetchone()
            if not row:
                self._json({"error": "article not found"}, 404)
                return
            if _is_postgres():
                title, text = row[0], row[1]
            else:
                title, text = row["title"], row["text"]

            from apis.llm import _call_llm
            prompts = {
                "title": f"Придумай новый заголовок для игровой новости. Текст:\n{text[:1500]}\n\nОтветь JSON: {{\"title\": \"новый заголовок\"}}",
                "seo_title": f"Придумай SEO-заголовок до 60 символов для:\n{title}\n\nОтветь JSON: {{\"seo_title\": \"SEO заголовок\"}}",
                "seo_description": f"Придумай meta description до 155 символов для:\n{title}\n{text[:500]}\n\nОтветь JSON: {{\"seo_description\": \"описание\"}}",
                "tags": f"Предложи 5 тегов для игровой новости:\n{title}\n\nОтветь JSON: {{\"tags\": [\"тег1\", \"тег2\", \"тег3\", \"тег4\", \"тег5\"]}}",
            }

            result = _call_llm(prompts[field])
            if result and field in result:
                self._json({"field": field, "value": result[field]})
            else:
                self._json({"error": "LLM returned no result"}, 500)
        except Exception as e:
            self._json({"error": str(e)}, 500)
        finally:
            cur.close()

    # --- Phase 0: Feature Flags & Observability API ---

    def _get_feature_flags(self):
        try:
            from core.feature_flags import get_all_flags
            return {"flags": get_all_flags()}
        except Exception as e:
            return {"flags": [], "error": str(e)}

    def _toggle_feature_flag(self, body):
        if not self._require_perm("flags"):
            return
        flag_id = body.get("flag_id", "")
        enabled = body.get("enabled", False)
        if not flag_id:
            self._json({"error": "flag_id required"}, 400)
            return
        try:
            from core.feature_flags import set_flag
            user = self._get_session_user() or "admin"
            set_flag(flag_id, bool(enabled), updated_by=user)
            self._json({"status": "ok", "flag_id": flag_id, "enabled": enabled})
        except Exception as e:
            self._json({"error": str(e)}, 500)

    def _get_cost_summary(self):
        try:
            from core.observability import get_cost_summary
            return get_cost_summary(days=1)
        except Exception as e:
            return {"error": str(e), "total_cost_usd": 0, "total_calls": 0, "by_type": []}

    def _get_config_audit(self):
        try:
            from core.observability import get_config_audit
            return {"audit": get_config_audit(limit=50)}
        except Exception as e:
            return {"audit": [], "error": str(e)}

    def _get_decision_trace(self, body):
        news_id = body.get("news_id", "")
        if not news_id:
            self._json({"error": "news_id required"}, 400)
            return
        try:
            from core.observability import get_decision_trace
            trace = get_decision_trace(news_id)
            self._json({"news_id": news_id, "trace": trace})
        except Exception as e:
            self._json({"error": str(e)}, 500)

    def _get_ops_dashboard(self):
        """Operational dashboard: action items, counts, health summary."""
        conn = get_connection()
        cur = conn.cursor()
        try:
            result = {}

            # News counts by status
            cur.execute("SELECT status, COUNT(*) as cnt FROM news GROUP BY status")
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                status_rows = [dict(zip(columns, r)) for r in cur.fetchall()]
            else:
                status_rows = [dict(r) for r in cur.fetchall()]
            status_counts = {r["status"]: r["cnt"] for r in status_rows}
            result["status_counts"] = status_counts

            # Pending review
            result["pending_review"] = status_counts.get("in_review", 0)

            # Ready to publish
            result["ready_to_publish"] = status_counts.get("ready", 0)

            # Articles in moderation
            result["in_moderation"] = status_counts.get("moderation", 0)

            # Queue stats
            cur.execute("SELECT status, COUNT(*) as cnt FROM task_queue GROUP BY status")
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                q_rows = [dict(zip(columns, r)) for r in cur.fetchall()]
            else:
                q_rows = [dict(r) for r in cur.fetchall()]
            queue_counts = {r["status"]: r["cnt"] for r in q_rows}
            result["queue_counts"] = queue_counts
            result["queue_errors"] = queue_counts.get("error", 0)
            result["queue_running"] = queue_counts.get("running", 0) + queue_counts.get("pending", 0)

            # High-score candidates (final_score >= 60 not yet ready)
            ph = "%s" if _is_postgres() else "?"
            try:
                cur.execute(f"""
                    SELECT COUNT(*) FROM news n
                    JOIN news_analysis na ON n.id = na.news_id
                    WHERE n.status IN ('processed', 'approved', 'in_review')
                    AND na.total_score >= 60
                """)
                row = cur.fetchone()
                result["high_score_candidates"] = row[0] if row else 0
            except Exception:
                result["high_score_candidates"] = 0

            # Source health: count sources with recent failures
            try:
                from checks.health import get_source_health
                health_data = get_source_health()
                degraded = sum(1 for s in health_data if s.get("status") == "degraded" or s.get("error_rate", 0) > 0.3)
                result["degraded_sources"] = degraded
            except Exception:
                result["degraded_sources"] = 0

            # API cost today
            try:
                from core.observability import get_cost_summary
                cost = get_cost_summary(days=1)
                result["api_cost_today"] = cost.get("total_cost_usd", 0)
                result["api_calls_today"] = cost.get("total_calls", 0)
            except Exception:
                result["api_cost_today"] = 0
                result["api_calls_today"] = 0

            # Draft articles count
            try:
                cur.execute("SELECT COUNT(*) FROM articles WHERE status = 'draft'")
                row = cur.fetchone()
                result["draft_articles"] = row[0] if row else 0
            except Exception:
                result["draft_articles"] = 0

            # Action items (prioritized recommendations)
            actions = []
            if result["pending_review"] > 0:
                actions.append({
                    "priority": 1,
                    "type": "review",
                    "title": f"Проверь {result['pending_review']} новост{'ь' if result['pending_review'] == 1 else 'ей'} на модерации",
                    "tab": "editorial",
                    "count": result["pending_review"],
                })
            if result["queue_errors"] > 0:
                actions.append({
                    "priority": 2,
                    "type": "error",
                    "title": f"{result['queue_errors']} задач{'а' if result['queue_errors'] == 1 else ''} с ошибкой в очереди",
                    "tab": "queue",
                    "count": result["queue_errors"],
                })
            if result["high_score_candidates"] > 0:
                actions.append({
                    "priority": 3,
                    "type": "opportunity",
                    "title": f"{result['high_score_candidates']} кандидат{'ов' if result['high_score_candidates'] != 1 else ''} с высоким скором",
                    "tab": "final",
                    "count": result["high_score_candidates"],
                })
            if result["degraded_sources"] > 0:
                actions.append({
                    "priority": 4,
                    "type": "warning",
                    "title": f"{result['degraded_sources']} источник{'ов' if result['degraded_sources'] != 1 else ''} деградируют",
                    "tab": "health",
                    "count": result["degraded_sources"],
                })
            if result["ready_to_publish"] > 0:
                actions.append({
                    "priority": 5,
                    "type": "publish",
                    "title": f"{result['ready_to_publish']} материал{'ов' if result['ready_to_publish'] != 1 else ''} готовы к публикации",
                    "tab": "editor",
                    "count": result["ready_to_publish"],
                })
            if result["draft_articles"] > 0:
                actions.append({
                    "priority": 6,
                    "type": "draft",
                    "title": f"{result['draft_articles']} черновик{'ов' if result['draft_articles'] != 1 else ''} ждут доработки",
                    "tab": "editor",
                    "count": result["draft_articles"],
                })

            result["actions"] = sorted(actions, key=lambda a: a["priority"])
            return result
        except Exception as e:
            logger.error("Ops dashboard error: %s", e)
            return {"error": str(e)}
        finally:
            cur.close()

    # --- Phase 4: Storylines, Source Health Plus, Threshold Simulator ---

    def _get_storylines(self):
        """Return clustered news storylines from last 3 days."""
        conn = get_connection()
        cur = conn.cursor()
        ph = "%s" if _is_postgres() else "?"
        try:
            from datetime import datetime as dt_mod, timezone, timedelta
            cutoff = (dt_mod.now(timezone.utc) - timedelta(days=3)).isoformat()
            cur.execute(f"""
                SELECT n.id, n.source, n.title, n.published_at, n.status,
                       COALESCE(a.total_score, 0) as total_score,
                       COALESCE(a.viral_score, 0) as viral_score
                FROM news n
                LEFT JOIN news_analysis a ON n.id = a.news_id
                WHERE n.parsed_at > {ph}
                ORDER BY n.published_at DESC
                LIMIT 500
            """, (cutoff,))
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                news_list = [dict(zip(columns, r)) for r in cur.fetchall()]
            else:
                news_list = [dict(r) for r in cur.fetchall()]

            if len(news_list) < 2:
                return {"storylines": [], "total_news": len(news_list)}

            from checks.deduplication import tfidf_similarity, build_groups
            titles = [n["title"] for n in news_list]
            pairs = tfidf_similarity(titles)
            groups = build_groups(news_list, pairs)

            storylines = []
            for g in groups:
                members = g["members"]
                if len(members) < 2:
                    continue
                sources = list(set(m.get("source", "") for m in members))
                avg_score = round(sum(m.get("total_score", 0) for m in members) / len(members)) if members else 0
                max_viral = max((m.get("viral_score", 0) for m in members), default=0)
                count = len(members)
                phase = "trending" if count >= 5 else "developing" if count >= 3 else "emerging"

                storylines.append({
                    "count": count,
                    "phase": phase,
                    "status": g["status"],
                    "sources": sources,
                    "avg_score": avg_score,
                    "max_viral": max_viral,
                    "members": [{
                        "id": m.get("id", ""),
                        "title": m.get("title", ""),
                        "source": m.get("source", ""),
                        "published_at": m.get("published_at", ""),
                        "status": m.get("status", ""),
                        "total_score": m.get("total_score", 0),
                    } for m in members[:10]],
                    "duplicate_indices": g.get("duplicate_indices", []),
                })

            storylines.sort(key=lambda s: (-s["count"], -s["avg_score"]))
            return {"storylines": storylines[:50], "total_news": len(news_list)}
        except Exception as e:
            logger.error("Storylines error: %s", e)
            return {"storylines": [], "error": str(e)}
        finally:
            cur.close()

    def _get_source_health_plus(self):
        """Enhanced source health: 7-day trend, score stats, conversion, recommendations."""
        conn = get_connection()
        cur = conn.cursor()
        ph = "%s" if _is_postgres() else "?"
        try:
            from datetime import datetime as dt_mod, timezone, timedelta
            from checks.health import get_sources_health
            base_health = get_sources_health()

            cutoff_7d = (dt_mod.now(timezone.utc) - timedelta(days=7)).isoformat()
            day_expr = "CAST(parsed_at AS TEXT)" if _is_postgres() else "parsed_at"
            cur.execute(f"""
                SELECT source,
                       SUBSTRING({day_expr}, 1, 10) as day,
                       COUNT(*) as cnt
                FROM news
                WHERE parsed_at > {ph}
                GROUP BY source, SUBSTRING({day_expr}, 1, 10)
                ORDER BY source, day
            """, (cutoff_7d,))
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                trend_rows = [dict(zip(columns, r)) for r in cur.fetchall()]
            else:
                trend_rows = [dict(r) for r in cur.fetchall()]

            from collections import defaultdict
            trend_data = defaultdict(dict)
            for r in trend_rows:
                trend_data[r["source"]][r["day"]] = r["cnt"]

            days_list = [(dt_mod.now(timezone.utc) - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(6, -1, -1)]

            cur.execute(f"""
                SELECT n.source,
                       COUNT(*) as total,
                       COALESCE(AVG(a.total_score), 0) as avg_score,
                       COALESCE(MAX(a.total_score), 0) as max_score,
                       SUM(CASE WHEN n.status IN ('ready', 'processed', 'approved') THEN 1 ELSE 0 END) as good_count,
                       SUM(CASE WHEN n.status IN ('rejected', 'duplicate') THEN 1 ELSE 0 END) as bad_count
                FROM news n
                LEFT JOIN news_analysis a ON n.id = a.news_id
                WHERE n.parsed_at > {ph}
                GROUP BY n.source
            """, (cutoff_7d,))
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                score_rows = [dict(zip(columns, r)) for r in cur.fetchall()]
            else:
                score_rows = [dict(r) for r in cur.fetchall()]
            score_map = {r["source"]: r for r in score_rows}

            results = []
            for h in base_health:
                src = h["source"]
                trend = [trend_data.get(src, {}).get(d, 0) for d in days_list]
                stats = score_map.get(src, {})
                total = stats.get("total", 0)
                good = stats.get("good_count", 0)
                bad = stats.get("bad_count", 0)
                conversion = round(good / total * 100) if total > 0 else 0

                recs = []
                if h["status"] in ("dead", "down"):
                    recs.append({"type": "error", "text": "Проверьте RSS/URL — источник не отвечает"})
                elif h["status"] == "warning":
                    recs.append({"type": "warning", "text": "Источник нестабилен, возможны проблемы с доступом"})
                if total > 10 and conversion < 10:
                    recs.append({"type": "warning", "text": f"Низкая конверсия ({conversion}%) — рассмотрите снижение веса"})
                avg_score = round(float(stats.get("avg_score", 0)))
                if total > 10 and avg_score < 20:
                    recs.append({"type": "info", "text": f"Средний скор {avg_score} — контент низкого качества"})
                if sum(trend[-3:]) == 0 and h["status"] != "dead":
                    recs.append({"type": "warning", "text": "Нет статей за последние 3 дня"})
                trend_direction = "stable"
                if len(trend) >= 4:
                    first_half = sum(trend[:3])
                    second_half = sum(trend[4:])
                    if second_half > first_half * 1.5:
                        trend_direction = "up"
                    elif second_half < first_half * 0.5:
                        trend_direction = "down"

                results.append({
                    **h,
                    "trend_7d": trend,
                    "trend_days": days_list,
                    "trend_direction": trend_direction,
                    "avg_score": avg_score,
                    "max_score": stats.get("max_score", 0),
                    "total_7d": total,
                    "good_count": good,
                    "bad_count": bad,
                    "conversion_pct": conversion,
                    "recommendations": recs,
                })

            results.sort(key=lambda x: (0 if x["recommendations"] else 1, -x.get("count_24h", 0)))
            return {"sources": results, "days": days_list}
        except Exception as e:
            logger.error("Source health plus error: %s", e)
            return {"sources": [], "error": str(e)}
        finally:
            cur.close()

    def _simulate_thresholds(self, body):
        """Simulate how many articles would pass at given thresholds."""
        score_min = int(body.get("score_min", 0))
        score_max = int(body.get("score_max", 100))
        final_min = int(body.get("final_min", 0))
        final_max = int(body.get("final_max", 100))
        conn = get_connection()
        cur = conn.cursor()
        ph = "%s" if _is_postgres() else "?"
        try:
            from datetime import datetime as dt_mod, timezone, timedelta
            cutoff = (dt_mod.now(timezone.utc) - timedelta(days=7)).isoformat()
            cur.execute(f"""
                SELECT n.source, n.status,
                       COALESCE(a.total_score, 0) as total_score,
                       COALESCE(a.viral_score, 0) as viral_score,
                       COALESCE(a.final_score, 0) as final_score
                FROM news n
                LEFT JOIN news_analysis a ON n.id = a.news_id
                WHERE n.parsed_at > {ph} AND a.total_score > 0
                ORDER BY a.total_score DESC
            """, (cutoff,))
            if _is_postgres():
                columns = [desc[0] for desc in cur.description]
                rows = [dict(zip(columns, r)) for r in cur.fetchall()]
            else:
                rows = [dict(r) for r in cur.fetchall()]

            total = len(rows)
            pass_score = sum(1 for r in rows if score_min <= r["total_score"] <= score_max)
            pass_final = sum(1 for r in rows if final_min <= (r.get("final_score") or 0) <= final_max)
            pass_both = sum(1 for r in rows if score_min <= r["total_score"] <= score_max and final_min <= (r.get("final_score") or 0) <= final_max)

            buckets = {"0-19": 0, "20-39": 0, "40-59": 0, "60-79": 0, "80-100": 0}
            for r in rows:
                s = r["total_score"]
                if s < 20: buckets["0-19"] += 1
                elif s < 40: buckets["20-39"] += 1
                elif s < 60: buckets["40-59"] += 1
                elif s < 80: buckets["60-79"] += 1
                else: buckets["80-100"] += 1

            final_buckets = {"0-19": 0, "20-39": 0, "40-59": 0, "60-79": 0, "80-100": 0}
            for r in rows:
                s = r.get("final_score") or 0
                if s < 20: final_buckets["0-19"] += 1
                elif s < 40: final_buckets["20-39"] += 1
                elif s < 60: final_buckets["40-59"] += 1
                elif s < 80: final_buckets["60-79"] += 1
                else: final_buckets["80-100"] += 1

            from collections import defaultdict
            by_source = defaultdict(lambda: {"total": 0, "pass_score": 0, "pass_final": 0})
            for r in rows:
                src = r["source"]
                by_source[src]["total"] += 1
                if score_min <= r["total_score"] <= score_max:
                    by_source[src]["pass_score"] += 1
                if final_min <= (r.get("final_score") or 0) <= final_max:
                    by_source[src]["pass_final"] += 1

            self._json({
                "total": total,
                "pass_score": pass_score,
                "pass_final": pass_final,
                "pass_both": pass_both,
                "pct_score": round(pass_score / total * 100) if total > 0 else 0,
                "pct_final": round(pass_final / total * 100) if total > 0 else 0,
                "score_distribution": buckets,
                "final_distribution": final_buckets,
                "by_source": dict(by_source),
            })
        except Exception as e:
            self._json({"error": str(e), "total": 0})
        finally:
            cur.close()

    # --- Dashboard HTML ---
    def _serve_dashboard(self):
        html = DASHBOARD_HTML
        body = html.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>IgroNews Admin</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><rect width='100' height='100' rx='20' fill='%23192734'/><text x='50' y='38' text-anchor='middle' font-size='28' font-family='sans-serif' font-weight='bold' fill='%231da1f2'>IGR</text><text x='50' y='70' text-anchor='middle' font-size='20' font-family='sans-serif' fill='%2317bf63'>NEWS</text><circle cx='82' cy='20' r='8' fill='%23e0245e'/></svg>">
<style>
* { margin:0; padding:0; box-sizing:border-box; }
body { font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; background:#0f1923; color:#e1e8ed; font-size:15px; }
.container { max-width:1600px; margin:0 auto; padding:15px; }
h1 { color:#1da1f2; font-size:1.5em; }
h2 { color:#1da1f2; font-size:1.1em; margin-bottom:10px; }
header { background:linear-gradient(135deg,#192734 0%,#1a3a4a 100%); padding:10px 20px; display:flex; align-items:center; justify-content:space-between; border-bottom:1px solid #22303c; box-shadow:0 2px 8px rgba(0,0,0,0.3); }

/* Tabs */
.tabs { display:flex; gap:0; background:#192734; border-radius:8px; margin:15px 0; overflow:hidden; box-shadow:0 1px 4px rgba(0,0,0,0.2); }
.tab { padding:10px 20px; cursor:pointer; color:#8899a6; border:none; background:none; font-size:0.9em; transition:all .2s; position:relative; }
.tab:hover { color:#e1e8ed; background:#22303c; }
.tab.active { color:#1da1f2; background:#22303c; border-bottom:2px solid #1da1f2; }

.panel { display:none; animation:fadeIn .3s; }
.panel.active { display:block; }
@keyframes fadeIn { from{opacity:0;transform:translateY(6px)} to{opacity:1;transform:translateY(0)} }

/* Settings sub-tabs */
.settings-nav { display:flex; gap:0; background:#192734; border-radius:8px; margin-bottom:15px; overflow:hidden; box-shadow:0 1px 4px rgba(0,0,0,0.2); flex-wrap:wrap; }
.settings-tab { padding:8px 16px; cursor:pointer; color:#8899a6; font-size:0.85em; transition:all .2s; }
.settings-tab:hover { color:#e1e8ed; background:#22303c; }
.settings-tab.active { color:#ffad1f; background:#22303c; border-bottom:2px solid #ffad1f; }
.settings-section { display:none; animation:fadeIn .3s; }
.settings-section.active { display:block; }

/* Stats */
.stats { display:flex; gap:12px; margin-bottom:20px; flex-wrap:wrap; }
.stat { background:#192734; border-radius:10px; padding:15px 20px; min-width:120px; cursor:pointer; transition:all .2s; border:1px solid transparent; }
.stat:hover { border-color:#38444d; transform:translateY(-2px); box-shadow:0 4px 12px rgba(0,0,0,0.3); }
.stat.active-filter { border-color:#1da1f2; box-shadow:0 0 0 1px #1da1f2; }
.stat .num { font-size:1.8em; font-weight:bold; color:#1da1f2; }
.stat .lbl { color:#8899a6; font-size:0.8em; }
.stat.new .num { color:#ffad1f; }
.stat.proc .num { color:#17bf63; }

/* Buttons */
.btn { padding:8px 16px; border:none; border-radius:6px; cursor:pointer; font-size:0.85em; transition:all .2s; }
.btn:active { transform:scale(0.96); }
.btn-primary { background:#1da1f2; color:#fff; }
.btn-primary:hover { background:#1a91da; box-shadow:0 2px 8px rgba(29,161,242,0.3); }
.btn-success { background:#17bf63; color:#fff; }
.btn-success:hover { background:#14a857; box-shadow:0 2px 8px rgba(23,191,99,0.3); }
.btn-danger { background:#e0245e; color:#fff; }
.btn-danger:hover { background:#c81e52; }
.btn-secondary { background:#38444d; color:#e1e8ed; }
.btn-secondary:hover { background:#4a5568; }
.btn-sm { padding:4px 10px; font-size:0.8em; }
.btn-icon { padding:4px 8px; font-size:0.9em; min-width:28px; text-align:center; }
@keyframes spin { to { transform:rotate(360deg); } }
.btn-group { display:flex; gap:8px; margin-bottom:15px; flex-wrap:wrap; align-items:center; }
.btn-warning { background:#ffad1f; color:#000; }
.btn-warning:hover { background:#e69d1c; }

/* Table */
table { width:100%; border-collapse:collapse; background:#192734; border-radius:10px; overflow:hidden; font-size:0.88em; }
th { background:#22303c; text-align:left; padding:10px 12px; color:#8899a6; font-size:0.82em; white-space:nowrap; position:sticky; top:0; z-index:2; user-select:none; }
th.sortable { cursor:pointer; transition:color .2s; }
th.sortable:hover { color:#1da1f2; }
th.sortable .sort-arrow { margin-left:3px; font-size:0.75em; opacity:0.4; }
th.sortable.sort-active .sort-arrow { opacity:1; color:#1da1f2; }
td { padding:8px 12px; border-bottom:1px solid #22303c; max-width:400px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
td.td-tip { overflow:visible; position:relative; }
td.td-title { white-space:normal; overflow:hidden; max-width:420px; }
td.td-title > a, td.td-title > span { display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical; overflow:hidden; line-height:1.4; }
tr { transition:background .15s; }
tr:hover { background:#22303c; }
tr.highlighted { background:#1da1f215; }
a { color:#1da1f2; text-decoration:none; }
a:hover { text-decoration:underline; }
.badge { padding:2px 8px; border-radius:10px; font-size:0.75em; font-weight:500; }
.badge-new { background:#ffad1f22; color:#ffad1f; }
.badge-in_review { background:#794bc422; color:#b48eff; }
.badge-duplicate { background:#e0245e22; color:#e0245e; }
.badge-processed { background:#17bf6322; color:#17bf63; }
.badge-approved { background:#1da1f222; color:#1da1f2; }
.badge-rejected { background:#e0245e22; color:#e0245e; }
.badge-ready { background:#17bf6322; color:#17bf63; }

/* Forms */
.form-group { margin-bottom:12px; }
.form-group label { display:block; color:#8899a6; font-size:0.85em; margin-bottom:4px; }
input, select { background:#22303c; border:1px solid #38444d; color:#e1e8ed; padding:8px 12px; border-radius:6px; width:100%; font-size:0.9em; transition:border-color .2s; }
textarea { background:#22303c; border:1px solid #38444d; color:#e1e8ed; padding:10px; border-radius:6px; width:100%; font-size:0.85em; font-family:monospace; min-height:150px; resize:vertical; }
input:focus, textarea:focus, select:focus { outline:none; border-color:#1da1f2; box-shadow:0 0 0 2px #1da1f233; }

/* Grid */
.grid-2 { display:grid; grid-template-columns:1fr 1fr; gap:15px; }
.card { background:#192734; border-radius:10px; padding:15px; transition:box-shadow .2s; }
.card:hover { box-shadow:0 2px 12px rgba(0,0,0,0.2); }

/* Toast */
.toast { position:fixed; bottom:20px; right:20px; background:#17bf63; color:#fff; padding:12px 20px; border-radius:8px; z-index:999; display:none; font-size:0.9em; box-shadow:0 4px 16px rgba(0,0,0,0.4); animation:slideUp .3s; }
.toast.error { background:#e0245e; }
@keyframes slideUp { from{transform:translateY(20px);opacity:0} to{transform:translateY(0);opacity:1} }

/* Dashboard Filters */
.dash-filters { display:flex; gap:8px; margin-bottom:12px; align-items:center; flex-wrap:wrap; background:#192734; padding:12px 15px; border-radius:10px; }
.dash-filters select, .dash-filters input { width:auto; min-width:130px; padding:6px 10px; font-size:0.85em; }
.dash-filters input[type="search"] { min-width:200px; }
.dash-filters input[type="date"] { min-width:140px; }
.dash-filters .filter-label { color:#8899a6; font-size:0.8em; margin-right:-4px; }
.dash-filters .filter-sep { width:1px; height:24px; background:#38444d; margin:0 4px; }
.active-filters { display:flex; gap:6px; margin-bottom:10px; flex-wrap:wrap; }
.active-filter-chip { background:#1da1f233; color:#1da1f2; padding:3px 10px; border-radius:12px; font-size:0.78em; display:flex; align-items:center; gap:4px; cursor:pointer; transition:all .2s; }
.active-filter-chip:hover { background:#1da1f255; }
.active-filter-chip .chip-x { font-weight:bold; }

/* Filters (news tab) */
.filters { display:flex; gap:10px; margin-bottom:15px; align-items:center; flex-wrap:wrap; }
.filters select, .filters input { width:auto; min-width:150px; }

/* Modal */
.modal-overlay { display:none; position:fixed; top:0;left:0;right:0;bottom:0; background:rgba(0,0,0,0.7); z-index:100; justify-content:center; align-items:center; }
.modal-overlay.show { display:flex; }
.modal { background:#192734; border-radius:12px; padding:25px; width:450px; max-width:90vw; box-shadow:0 8px 32px rgba(0,0,0,0.5); animation:fadeIn .2s; }
.modal h2 { margin-bottom:15px; }
.modal-buttons { display:flex; gap:10px; margin-top:15px; justify-content:flex-end; }

/* Tags */
.tag { display:inline-block; padding:2px 7px; border-radius:8px; font-size:0.7em; margin:1px; white-space:nowrap; cursor:pointer; transition:all .15s; }
.tag:hover { filter:brightness(1.3); transform:scale(1.05); }
.tag-release { background:#17bf6333; color:#17bf63; }
.tag-update { background:#1da1f233; color:#1da1f2; }
.tag-announcement { background:#794bc433; color:#b48eff; }
.tag-esports { background:#ff630033; color:#ff6300; }
.tag-hardware { background:#8899a633; color:#8899a6; }
.tag-controversy { background:#e0245e33; color:#e0245e; }
.tag-rumor { background:#ffad1f33; color:#ffad1f; }
.tag-review { background:#17bf6333; color:#1da1f2; }
.tag-industry { background:#794bc433; color:#b48eff; }
.group-marker { display:inline-block; padding:2px 7px; border-radius:8px; font-size:0.72em; font-weight:bold; cursor:pointer; transition:all .15s; }
.group-marker:hover { filter:brightness(1.3); transform:scale(1.1); }

/* Copyable tooltip */
.tip-wrap { position:relative; display:inline-block; cursor:pointer; }
.tip-wrap .tip-count { color:#8899a6; border-bottom:1px dashed #38444d; }
.tip-wrap .tip-box { display:none; position:absolute; z-index:50; left:0; top:0; padding-top:28px; font-size:0.82em; }
.tip-wrap .tip-box-inner { background:#22303c; border:1px solid #38444d; border-radius:8px; padding:8px 0; min-width:220px; max-width:320px; max-height:260px; overflow-y:auto; box-shadow:0 8px 24px rgba(0,0,0,0.5); }
.tip-wrap:hover .tip-box { display:block; }
.tip-box .tip-row { display:flex; justify-content:space-between; align-items:center; padding:4px 10px; transition:background .1s; }
.tip-box .tip-row:hover { background:#192734; }
.tip-box .tip-word { color:#e1e8ed; flex:1; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.tip-box .tip-ws { color:#17bf63; font-size:0.9em; margin-left:8px; white-space:nowrap; }
.tip-box .tip-copy { opacity:0; background:none; border:none; color:#1da1f2; cursor:pointer; font-size:0.85em; padding:0 4px; margin-left:4px; transition:opacity .15s; }
.tip-box .tip-row:hover .tip-copy { opacity:1; }
.tip-box .tip-header { padding:4px 10px 6px; border-bottom:1px solid #38444d; margin-bottom:4px; display:flex; justify-content:space-between; align-items:center; }
.tip-box .tip-header span { color:#8899a6; font-size:0.85em; }
.tip-box .tip-copy-all { background:none; border:none; color:#1da1f2; cursor:pointer; font-size:0.8em; padding:2px 6px; }
.tip-box .tip-copy-all:hover { text-decoration:underline; }

/* Table counter */
.table-info { display:flex; justify-content:space-between; align-items:center; margin-bottom:8px; color:#8899a6; font-size:0.82em; }

/* Empty state */
.empty-state { text-align:center; padding:40px 20px; color:#8899a6; }
.empty-state .empty-icon { font-size:2.5em; margin-bottom:10px; opacity:0.5; }

/* Responsive */
@media(max-width:768px) {
  .grid-2 { grid-template-columns:1fr; }
  .stats { gap:8px; }
  .stat { min-width:80px; padding:10px; }
  .stat .num { font-size:1.4em; }
  .dash-filters { flex-direction:column; }
  .dash-filters select, .dash-filters input { width:100%; }
}

/* Ops Dashboard */
.ops-action-card { background:#192734; border-radius:8px; padding:12px 16px; margin-bottom:8px; display:flex; align-items:center; gap:12px; cursor:pointer; border-left:4px solid #38444d; transition:all .2s; }
.ops-action-card:hover { background:#22303c; transform:translateX(4px); }
.ops-action-card[data-type="review"] { border-left-color:#1da1f2; }
.ops-action-card[data-type="error"] { border-left-color:#e0245e; }
.ops-action-card[data-type="opportunity"] { border-left-color:#ffad1f; }
.ops-action-card[data-type="warning"] { border-left-color:#f5a623; }
.ops-action-card[data-type="publish"] { border-left-color:#17bf63; }
.ops-action-card[data-type="draft"] { border-left-color:#8899a6; }
.ops-action-count { font-size:1.4em; font-weight:bold; min-width:36px; text-align:center; }
.ops-action-title { font-size:0.95em; }

/* Feature Flag Toggle */
.flag-row { display:flex; align-items:center; justify-content:space-between; background:#192734; border-radius:6px; padding:8px 14px; margin-bottom:4px; }
.flag-row .flag-name { font-weight:600; font-size:0.9em; }
.flag-row .flag-desc { color:#8899a6; font-size:0.8em; }
.flag-row .flag-phase { color:#8899a6; font-size:0.75em; background:#22303c; padding:2px 6px; border-radius:4px; }
.flag-toggle { position:relative; width:40px; height:22px; cursor:pointer; }
.flag-toggle input { opacity:0; width:0; height:0; }
.flag-toggle .slider { position:absolute; top:0; left:0; right:0; bottom:0; background:#38444d; border-radius:22px; transition:.3s; }
.flag-toggle .slider:before { content:''; position:absolute; width:16px; height:16px; left:3px; bottom:3px; background:#8899a6; border-radius:50%; transition:.3s; }
.flag-toggle input:checked + .slider { background:#17bf63; }
.flag-toggle input:checked + .slider:before { transform:translateX(18px); background:#fff; }

/* Explain Drawer */
.explain-drawer { position:fixed; top:0; right:-480px; width:460px; height:100vh; background:#192734; border-left:1px solid #22303c; z-index:1000; transition:right .3s ease; overflow-y:auto; padding:20px; box-shadow:-4px 0 20px rgba(0,0,0,0.4); }
.explain-drawer.open { right:0; }
.explain-drawer .close-btn { position:absolute; top:12px; right:16px; cursor:pointer; color:#8899a6; font-size:1.3em; background:none; border:none; }
.explain-drawer .close-btn:hover { color:#e1e8ed; }
.explain-section { margin-bottom:16px; }
.explain-section h3 { color:#1da1f2; font-size:0.95em; margin-bottom:8px; }
.score-bar { display:flex; align-items:center; gap:8px; margin-bottom:4px; }
.score-bar .bar-label { min-width:100px; font-size:0.82em; color:#8899a6; }
.score-bar .bar-track { flex:1; height:8px; background:#22303c; border-radius:4px; overflow:hidden; }
.score-bar .bar-fill { height:100%; border-radius:4px; transition:width .5s; }
.score-bar .bar-val { min-width:32px; text-align:right; font-size:0.82em; font-weight:600; }
.trace-step { background:#22303c; border-radius:6px; padding:8px 12px; margin-bottom:6px; font-size:0.85em; border-left:3px solid #38444d; }
.trace-step[data-decision="in_review"] { border-left-color:#1da1f2; }
.trace-step[data-decision="approved"] { border-left-color:#17bf63; }
.trace-step[data-decision="rejected"], .trace-step[data-decision="auto_rejected"] { border-left-color:#e0245e; }
.trace-step[data-decision="duplicate"] { border-left-color:#8899a6; }
.trace-step[data-decision="enriched"] { border-left-color:#ffad1f; }
.reason-badge { display:inline-block; padding:2px 8px; border-radius:12px; font-size:0.75em; font-weight:600; }
.reason-badge.positive { background:rgba(23,191,99,0.2); color:#17bf63; }
.reason-badge.negative { background:rgba(224,36,94,0.2); color:#e0245e; }
.reason-badge.neutral { background:rgba(136,153,166,0.2); color:#8899a6; }

/* Triage Mode Switcher */
.triage-modes { display:flex; gap:0; margin-bottom:12px; background:#192734; border-radius:8px; overflow:hidden; }
.triage-mode { padding:6px 14px; cursor:pointer; color:#8899a6; font-size:0.82em; border:none; background:none; transition:all .2s; }
.triage-mode:hover { color:#e1e8ed; background:#22303c; }
.triage-mode.active { color:#1da1f2; background:#22303c; border-bottom:2px solid #1da1f2; }

/* Card mode (triage flow) */
.triage-card { background:#192734; border-radius:12px; padding:24px; max-width:800px; margin:0 auto; border:1px solid #22303c; }
.triage-card .tc-title { font-size:1.15em; font-weight:600; line-height:1.4; margin-bottom:12px; }
.triage-card .tc-meta { display:flex; gap:16px; margin-bottom:16px; flex-wrap:wrap; font-size:0.85em; color:#8899a6; }
.triage-card .tc-meta span { display:flex; align-items:center; gap:4px; }
.triage-card .tc-text { font-size:0.92em; line-height:1.6; color:#ccd6dd; margin-bottom:16px; max-height:200px; overflow-y:auto; }
.triage-card .tc-scores { display:flex; gap:12px; margin-bottom:16px; flex-wrap:wrap; }
.triage-card .tc-score-item { background:#22303c; border-radius:8px; padding:8px 14px; text-align:center; min-width:70px; }
.triage-card .tc-score-item .val { font-size:1.3em; font-weight:bold; }
.triage-card .tc-score-item .lbl { font-size:0.72em; color:#8899a6; }
.triage-card .tc-actions { display:flex; gap:10px; justify-content:center; margin-top:16px; }
.triage-card .tc-actions .btn { padding:10px 24px; font-size:0.95em; }
.triage-counter { text-align:center; color:#8899a6; font-size:0.85em; margin-bottom:12px; }

/* Hotkey hints */
.hotkey-hint { display:inline-block; background:#22303c; border:1px solid #38444d; border-radius:4px; padding:1px 5px; font-size:0.72em; color:#8899a6; margin-left:4px; font-family:monospace; }

/* Storyline cards */
.storyline-card { background:#192734; border-radius:10px; padding:16px; margin-bottom:12px; border:1px solid #22303c; transition:border-color .2s; }
.storyline-card:hover { border-color:#1da1f2; }
.storyline-header { display:flex; justify-content:space-between; align-items:center; margin-bottom:10px; }
.storyline-phase { display:inline-block; padding:2px 8px; border-radius:10px; font-size:0.75em; font-weight:600; }
.storyline-phase.emerging { background:#2d4a3e; color:#17bf63; }
.storyline-phase.developing { background:#3d3a2e; color:#ffad1f; }
.storyline-phase.trending { background:#3d2e2e; color:#e0245e; }
.storyline-members { margin-top:8px; }
.storyline-member { padding:6px 0; border-bottom:1px solid #22303c; font-size:0.85em; display:flex; gap:8px; align-items:center; }
.storyline-member:last-child { border-bottom:none; }
.storyline-member .sm-source { color:#8899a6; min-width:80px; font-size:0.8em; }
.storyline-member .sm-score { color:#1da1f2; font-weight:600; min-width:35px; text-align:right; }
.storyline-stats { display:flex; gap:12px; font-size:0.82em; color:#8899a6; }

/* Health plus */
.health-rec { display:inline-block; padding:3px 8px; border-radius:6px; font-size:0.78em; margin:2px; }
.health-rec.error { background:#3d2e2e; color:#e0245e; }
.health-rec.warning { background:#3d3a2e; color:#ffad1f; }
.health-rec.info { background:#1e3a4e; color:#8899a6; }
.sparkline { display:inline-flex; align-items:flex-end; gap:2px; height:24px; vertical-align:middle; }
.sparkline-bar { width:8px; border-radius:2px 2px 0 0; background:#1da1f2; min-height:2px; transition:height .3s; }
.trend-arrow { font-weight:bold; font-size:0.9em; }
.trend-arrow.up { color:#17bf63; }
.trend-arrow.down { color:#e0245e; }
.trend-arrow.stable { color:#8899a6; }

/* Simulator */
.sim-stat { display:inline-block; background:#192734; border-radius:8px; padding:12px 20px; margin:6px; text-align:center; min-width:120px; }
.sim-stat .val { font-size:1.4em; font-weight:bold; color:#e1e8ed; }
.sim-stat .lbl { font-size:0.78em; color:#8899a6; display:block; margin-top:4px; }
.sim-bar { display:flex; height:28px; border-radius:6px; overflow:hidden; background:#22303c; margin:8px 0; }
.sim-bar div { display:flex; align-items:center; justify-content:center; font-size:0.72em; color:#fff; font-weight:600; transition:width .3s; }

/* Funnel */
.funnel-bar { display:flex; align-items:center; gap:10px; }
.funnel-bar .fb-label { min-width:100px; font-size:0.82em; color:#8899a6; text-align:right; }
.funnel-bar .fb-track { flex:1; height:24px; background:#22303c; border-radius:4px; overflow:hidden; position:relative; }
.funnel-bar .fb-fill { height:100%; border-radius:4px; transition:width .6s; display:flex; align-items:center; padding:0 8px; font-size:0.75em; font-weight:600; color:#fff; min-width:fit-content; }
.funnel-bar .fb-val { min-width:50px; font-size:0.85em; font-weight:600; }
</style>
</head>
<body>

<header>
  <div style="display:flex;align-items:center;gap:12px">
    <svg width="36" height="36" viewBox="0 0 100 100" style="flex-shrink:0">
      <rect width="100" height="100" rx="20" fill="#1da1f2"/>
      <text x="50" y="40" text-anchor="middle" font-size="30" font-family="sans-serif" font-weight="bold" fill="#fff">IGR</text>
      <text x="50" y="72" text-anchor="middle" font-size="22" font-family="sans-serif" fill="#fff" opacity="0.8">NEWS</text>
      <circle cx="85" cy="18" r="9" fill="#e0245e"/>
    </svg>
    <h1>IgroNews <span style="font-weight:300;font-size:0.7em;color:#8899a6">Admin</span></h1>
  </div>
  <span style="color:#8899a6;font-size:0.85em" id="clock"></span>
  <label style="display:inline-flex;align-items:center;gap:4px;margin-left:12px;cursor:pointer;font-size:0.82em;color:#8899a6" title="Браузерные уведомления">
    <input type="checkbox" id="notif-toggle" style="width:14px;height:14px;cursor:pointer" onchange="toggleNotifications(this.checked)">
    &#128276; Уведомления
  </label>
</header>

<div class="container">
  <div class="tabs">
    <div class="tab" data-tab="ops" id="tab-ops" style="display:none">&#9889; Обзор</div>
    <div class="tab active" data-tab="editorial">Редакция</div>
    <div class="tab" data-tab="news">Обогащённые</div>
    <div class="tab" data-tab="final">Финал</div>
    <div class="tab" data-tab="editor">Контент</div>
    <div class="tab" data-tab="viral">Виральность</div>
    <div class="tab" data-tab="analytics">Аналитика</div>
    <div class="tab" data-tab="queue">Очередь</div>
    <div class="tab" data-tab="health">Здоровье</div>
    <div class="tab" data-tab="storylines" id="tab-storylines" style="display:none">Сюжеты</div>
    <div class="tab" data-tab="settings">&#9881;</div>
    <div style="margin-left:auto"><a href="/logout" class="btn btn-secondary btn-sm">Выйти</a></div>
  </div>

  <!-- OPS DASHBOARD (Phase 1, behind feature flag dashboard_v2) -->
  <div class="panel" id="panel-ops" style="display:none">
    <h2>&#9889; Что делать сейчас</h2>
    <div id="ops-actions" style="margin:16px 0"></div>
    <div style="display:flex;gap:16px;flex-wrap:wrap;margin-bottom:20px">
      <div class="stat" id="ops-pending"><div class="num" id="ops-pending-num">—</div><div class="lbl">Ждут ревью</div></div>
      <div class="stat" id="ops-ready"><div class="num" id="ops-ready-num" style="color:#17bf63">—</div><div class="lbl">Готовы</div></div>
      <div class="stat" id="ops-errors"><div class="num" id="ops-errors-num" style="color:#e0245e">—</div><div class="lbl">Ошибки очереди</div></div>
      <div class="stat" id="ops-candidates"><div class="num" id="ops-candidates-num" style="color:#ffad1f">—</div><div class="lbl">Кандидаты ≥60</div></div>
      <div class="stat" id="ops-degraded"><div class="num" id="ops-degraded-num" style="color:#e0245e">—</div><div class="lbl">Источники &#9888;</div></div>
      <div class="stat" id="ops-cost"><div class="num" id="ops-cost-num" style="color:#8899a6;font-size:1.2em">—</div><div class="lbl">API сегодня ($)</div></div>
      <div class="stat" id="ops-drafts"><div class="num" id="ops-drafts-num" style="color:#8899a6">—</div><div class="lbl">Черновики</div></div>
    </div>
    <div id="ops-flags" style="margin-top:20px">
      <h2 style="margin-bottom:8px">&#9873; Feature Flags</h2>
      <div id="ops-flags-list"></div>
    </div>
  </div>

  <!-- EDITORIAL — единая рабочая вкладка -->
  <div class="panel active" id="panel-editorial">
    <!-- Triage Mode Switcher (Phase 1, behind newsroom_triage_v1 flag) -->
    <div class="triage-modes" id="triage-modes" style="display:none">
      <div class="triage-mode active" data-mode="table" onclick="setTriageMode('table')">&#9776; Таблица</div>
      <div class="triage-mode" data-mode="flow" onclick="setTriageMode('flow')">&#9654; Поток</div>
      <div class="triage-mode" data-mode="disputed" onclick="setTriageMode('disputed')">&#9888; Спорные</div>
    </div>

    <!-- Flow mode container (one card at a time) -->
    <div id="triage-flow" style="display:none">
      <div class="triage-counter" id="triage-counter"></div>
      <div class="triage-card" id="triage-card">
        <div class="tc-title" id="tc-title"></div>
        <div class="tc-meta" id="tc-meta"></div>
        <div class="tc-text" id="tc-text"></div>
        <div class="tc-scores" id="tc-scores"></div>
        <div class="tc-actions">
          <button class="btn btn-success" onclick="triageApprove()" title="Одобрить">&#10003; Одобрить <span class="hotkey-hint">A</span></button>
          <button class="btn btn-danger" onclick="triageReject()" title="Отклонить">&#10007; Отклонить <span class="hotkey-hint">R</span></button>
          <button class="btn btn-secondary" onclick="triageSkip()" title="Пропустить">&#10140; Далее <span class="hotkey-hint">S</span></button>
          <button class="btn btn-secondary" onclick="openExplainDrawer(_triageCurrent?.id, _triageCurrent?.title)" title="Почему?">? Почему</button>
        </div>
      </div>
    </div>

    <!-- Stat cards -->
    <div class="stats" id="ed-stats"></div>

    <!-- Filters -->
    <div class="dash-filters" style="margin-bottom:12px;display:flex;gap:8px;align-items:center;flex-wrap:wrap">
      <select id="ed-status" onchange="loadEditorial()" style="padding:4px 8px;background:#192734;color:#e1e8ed;border:1px solid #38444d;border-radius:6px">
        <option value="" selected>Активные</option>
        <option value="new">Новые</option>
        <option value="in_review">На проверке</option>
        <option value="moderation">Модерация</option>
        <option value="approved">Одобренные</option>
        <option value="processed">Обработанные</option>
        <option value="ready">Готовые</option>
        <option value="duplicate">Дубли</option>
        <option value="rejected">Отклонённые</option>
      </select>
      <select id="ed-source" onchange="loadEditorial()" style="padding:4px 8px;background:#192734;color:#e1e8ed;border:1px solid #38444d;border-radius:6px">
        <option value="">Все источники</option>
      </select>
      <select id="ed-viral" onchange="loadEditorial()" style="padding:4px 8px;background:#192734;color:#e1e8ed;border:1px solid #38444d;border-radius:6px">
        <option value="">Виральность</option>
        <option value="high">High</option>
        <option value="medium">Medium</option>
        <option value="low">Low</option>
      </select>
      <select id="ed-tier" onchange="loadEditorial()" style="padding:4px 8px;background:#192734;color:#e1e8ed;border:1px solid #38444d;border-radius:6px">
        <option value="">Тир</option>
        <option value="S">S-tier</option>
        <option value="A">A-tier</option>
        <option value="B">B-tier</option>
      </select>
      <input id="ed-min-score" type="number" value="0" min="0" max="100" placeholder="Мин. скор" onchange="loadEditorial()" style="width:70px;padding:4px 8px;background:#192734;color:#e1e8ed;border:1px solid #38444d;border-radius:6px">
      <input id="ed-search" placeholder="Поиск по заголовку..." oninput="debounceEdSearch()" style="padding:4px 8px;background:#192734;color:#e1e8ed;border:1px solid #38444d;border-radius:6px;min-width:180px">
      <button class="btn btn-sm btn-secondary" onclick="loadEditorial()">&#128269;</button>
      <button class="btn btn-sm btn-secondary" onclick="resetEdFilters()" title="Сбросить фильтры">&#10005;</button>
      <span style="color:#8899a6;font-size:0.85em" id="ed-count"></span>
    </div>

    <!-- Bulk actions -->
    <div style="display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap;align-items:center">
      <button class="btn btn-sm btn-success" onclick="edRunAutoReview()" id="ed-review-btn">&#9654; Проверить новые</button>
      <span style="color:#38444d">|</span>
      <button class="btn btn-sm btn-primary" onclick="edApproveSelected()">&#10003; Одобрить</button>
      <button class="btn btn-sm btn-danger" onclick="edRejectSelected()">&#10007; Отклонить</button>
      <button class="btn btn-sm btn-warning" onclick="edAutoApprove()">&#9889; Авто-одобрение (&gt;70)</button>
      <span style="color:#38444d">|</span>
      <button class="btn btn-sm btn-secondary" onclick="edExportSheets()">&#9776; Sheets</button>
      <button class="btn btn-sm btn-secondary" onclick="edBatchRewrite()">&#9998; Рерайт</button>
      <span style="color:#38444d">|</span>
      <span id="pipeline-controls">
        <button id="btn-full-auto" class="btn btn-sm" style="background:#1da1f2;color:#fff" onclick="runFullAuto()" title="Скор→>70 на LLM→Финальный скор→>60 на рерайт→Sheets">&#128640; Полный автомат</button>
        <button id="btn-no-llm" class="btn btn-sm" style="background:#794bc4;color:#fff" onclick="runNoLLM()">&#128203; Без LLM</button>
        <button id="btn-pipeline-stop" class="btn btn-sm btn-danger" style="display:none" onclick="stopPipeline()">&#9724; Стоп</button>
        <span id="pipeline-status" style="display:none;margin-left:8px;padding:4px 12px;border-radius:8px;background:#253341;font-size:0.85em;color:#ffad1f;border:1px solid #38444d"></span>
      </span>
      <span id="ed-selected-count" style="color:#8899a6;font-size:0.85em"></span>
    </div>

    <!-- Table -->
    <div style="background:#192734;border-radius:10px;overflow:hidden">
      <table>
        <thead><tr>
          <th style="width:30px"><input type="checkbox" onchange="edToggleAll(this)" style="width:16px;height:16px"></th>
          <th class="sortable" onclick="edSort('source')" style="width:90px">Источник</th>
          <th class="sortable" onclick="edSort('title')">Заголовок</th>
          <th class="sortable" onclick="edSort('status')" style="width:85px">Статус</th>
          <th class="sortable" onclick="edSort('total_score')" style="width:50px">Скор</th>
          <th style="width:60px">Кач/Рел</th>
          <th class="sortable" onclick="edSort('viral_score')" style="width:75px">Вирал.</th>
          <th style="width:65px">Свежесть</th>
          <th style="width:40px">Тон</th>
          <th style="width:120px">Теги</th>
          <th style="width:120px">Действия</th>
        </tr></thead>
        <tbody id="ed-table"></tbody>
      </table>
    </div>
    <div id="ed-pagination" style="margin-top:10px;display:flex;gap:8px;justify-content:center"></div>
  </div>

  <!-- MODERATION -->
  <!-- Moderation tab removed: use Редакция filter "Модерация" instead -->

  <!-- EDITOR -->
  <div class="panel" id="panel-editor">
    <style>
      .editor-list-card { background:#192734; border-radius:10px; padding:15px; min-height:600px; display:flex; flex-direction:column; }
      @media(max-width:900px) { .editor-list-card { min-height:300px; } }
      .art-card { background:#192734; border-radius:10px; padding:16px; margin-bottom:12px; transition:box-shadow .15s; cursor:pointer; border-left:3px solid transparent; }
      .art-card:hover { box-shadow:0 2px 12px rgba(0,0,0,0.2); }
      .art-card.selected { border-left-color:#1da1f2; background:#1da1f215; }
      .art-status { display:inline-block; padding:2px 8px; border-radius:10px; font-size:0.75em; font-weight:500; }
      .art-status-draft { background:#38444d; color:#8899a6; }
      .art-status-ready { background:#17bf6320; color:#17bf63; }
      .art-status-published { background:#1da1f220; color:#1da1f2; }
      .art-status-scheduled { background:#ffad1f20; color:#ffad1f; }
      .art-actions-bar { display:flex; gap:6px; flex-wrap:wrap; margin-bottom:12px; }
      .art-editor { background:#192734; border-radius:10px; padding:16px; }
      .art-field { margin-bottom:12px; }
      .art-field label { display:block; font-size:0.75em; color:#8899a6; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:4px; }
      .art-field input, .art-field textarea { width:100%; background:#22303c; border:1px solid #38444d; border-radius:6px; color:#e1e8ed; padding:8px 12px; font-size:0.9em; font-family:inherit; resize:vertical; }
      .art-field input:focus, .art-field textarea:focus { border-color:#1da1f2; outline:none; }
      .art-field .char-count { font-size:0.72em; color:#657786; text-align:right; margin-top:2px; }
      .art-improve-btn { padding:6px 12px; background:#22303c; border:1px solid #38444d; border-radius:6px; color:#e1e8ed; cursor:pointer; font-size:0.82em; transition:all .15s; }
      .art-improve-btn:hover { border-color:#1da1f2; color:#1da1f2; }
      .art-improve-btn.loading { opacity:0.5; pointer-events:none; }
    </style>

    <div style="display:grid;grid-template-columns:340px 1fr;gap:15px" id="articles-layout">
      <!-- LEFT: list panel with tab switcher -->
      <div class="editor-list-card">
        <!-- Tab switcher: Статьи / Новости -->
        <div style="display:flex;margin-bottom:10px;border-bottom:2px solid #22303c">
          <button id="cnt-tab-articles" class="cnt-tab active" onclick="switchContentList('articles')" style="flex:1;padding:8px;background:none;border:none;color:#1da1f2;font-size:0.9em;font-weight:600;cursor:pointer;border-bottom:2px solid #1da1f2;margin-bottom:-2px">Статьи <span id="art-count" style="font-size:0.8em;font-weight:normal;color:#8899a6"></span></button>
          <button id="cnt-tab-news" class="cnt-tab" onclick="switchContentList('news')" style="flex:1;padding:8px;background:none;border:none;color:#8899a6;font-size:0.9em;font-weight:500;cursor:pointer;border-bottom:2px solid transparent;margin-bottom:-2px">Новости <span id="cnt-news-count" style="font-size:0.8em;font-weight:normal"></span></button>
        </div>

        <!-- ARTICLES LIST -->
        <div id="cnt-articles-panel">
          <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
            <div style="display:flex;gap:4px">
              <button class="btn btn-sm btn-primary" onclick="downloadSelectedDocx()" id="art-bulk-docx-btn" disabled title="DOCX">&#128196;</button>
              <button class="btn btn-sm btn-danger" onclick="deleteSelectedArticles()" id="art-bulk-del-btn" disabled title="Удалить">&#128465;</button>
            </div>
          </div>
          <div style="display:flex;gap:6px;margin-bottom:8px">
            <input type="search" id="art-search" placeholder="Поиск..." oninput="filterArticles()" autocomplete="off" style="flex:1;padding:6px 10px;background:#22303c;border:1px solid #38444d;border-radius:6px;color:#e1e8ed;font-size:0.85em">
            <select id="art-status-filter" onchange="filterArticles()" style="padding:6px;background:#22303c;border:1px solid #38444d;border-radius:6px;color:#e1e8ed;font-size:0.85em">
              <option value="">Все</option>
              <option value="draft">Черновики</option>
              <option value="ready">Готовые</option>
              <option value="scheduled">Запланированные</option>
              <option value="published">Опубликованные</option>
            </select>
          </div>
          <div id="articles-list" style="flex:1;overflow-y:auto;font-size:0.85em;margin:0 -15px;padding:0 15px;max-height:calc(100vh - 300px)"></div>
          <div style="text-align:center;margin-top:8px;padding-top:8px;border-top:1px solid #22303c">
            <span id="art-selected-count" style="color:#1da1f2;font-size:0.82em;font-weight:500"></span>
          </div>
        </div>

        <!-- NEWS FOR REWRITE LIST -->
        <div id="cnt-news-panel" style="display:none">
          <div style="display:flex;gap:6px;margin-bottom:8px">
            <input type="search" id="cnt-news-search" placeholder="Поиск новостей..." oninput="filterContentNews()" autocomplete="off" style="flex:1;padding:6px 10px;background:#22303c;border:1px solid #38444d;border-radius:6px;color:#e1e8ed;font-size:0.85em">
            <select id="cnt-news-source" onchange="filterContentNews()" style="padding:6px;background:#22303c;border:1px solid #38444d;border-radius:6px;color:#e1e8ed;font-size:0.85em">
              <option value="">Все</option>
            </select>
          </div>
          <div style="display:flex;gap:4px;margin-bottom:8px;flex-wrap:wrap">
            <select id="cnt-rewrite-style" style="padding:5px 8px;background:#22303c;border:1px solid #38444d;border-radius:6px;color:#e1e8ed;font-size:0.82em">
              <option value="news">Новость</option>
              <option value="seo">SEO</option>
              <option value="review">Обзор</option>
              <option value="clickbait">Кликбейт</option>
              <option value="short">Кратко</option>
              <option value="social">Соцсети</option>
            </select>
            <button class="btn btn-sm btn-primary" onclick="cntRewriteSelected()" id="cnt-rewrite-btn" disabled>&#9998; Переписать выбранные</button>
          </div>
          <div id="cnt-news-list" style="flex:1;overflow-y:auto;font-size:0.85em;margin:0 -15px;padding:0 15px;max-height:calc(100vh - 330px)"></div>
          <div style="text-align:center;margin-top:8px;padding-top:8px;border-top:1px solid #22303c">
            <span id="cnt-news-selected" style="color:#1da1f2;font-size:0.82em;font-weight:500"></span>
          </div>
        </div>
      </div>

      <!-- RIGHT: editor -->
      <div class="art-editor" id="art-editor-panel">
        <div id="art-empty" style="text-align:center;padding:80px 20px;color:#8899a6">
          <div style="font-size:2em;margin-bottom:10px;opacity:0.3">&#128221;</div>
          <div>Выберите статью из списка или создайте новую в Редакторе</div>
        </div>

        <div id="art-edit-form" style="display:none">
          <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px">
            <div style="display:flex;align-items:center;gap:8px">
              <h2 style="margin:0;font-size:1em" id="art-edit-header">Редактирование</h2>
              <select id="art-edit-status" style="padding:4px 8px;background:#22303c;border:1px solid #38444d;border-radius:6px;color:#e1e8ed;font-size:0.82em">
                <option value="draft">Черновик</option>
                <option value="ready">Готово</option>
                <option value="scheduled">Запланировано</option>
                <option value="published">Опубликовано</option>
              </select>
            </div>
            <div style="display:flex;gap:6px">
              <button class="btn btn-sm btn-success" onclick="saveCurrentArticle()">Сохранить</button>
              <button class="btn btn-sm btn-primary" onclick="downloadArticleDocx()">DOCX</button>
              <button class="btn btn-sm btn-secondary" onclick="copyArticleText()">Копировать</button>
              <button class="btn btn-sm btn-danger" onclick="deleteCurrentArticle()">Удалить</button>
            </div>
          </div>

          <div class="art-field">
            <label>Заголовок</label>
            <input type="text" id="art-edit-title" oninput="artCharCount('art-edit-title', 100)">
            <div class="char-count" id="art-edit-title-count"></div>
          </div>

          <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">
            <div class="art-field">
              <label>SEO Title (до 60 симв.)</label>
              <input type="text" id="art-edit-seo-title" oninput="artCharCount('art-edit-seo-title', 60)">
              <div class="char-count" id="art-edit-seo-title-count"></div>
            </div>
            <div class="art-field">
              <label>Meta Description (до 155 симв.)</label>
              <input type="text" id="art-edit-seo-desc" oninput="artCharCount('art-edit-seo-desc', 155)">
              <div class="char-count" id="art-edit-seo-desc-count"></div>
            </div>
          </div>

          <div class="art-field">
            <label>Теги (через запятую)</label>
            <input type="text" id="art-edit-tags">
          </div>

          <div class="art-field">
            <label>Текст статьи</label>
            <textarea id="art-edit-text" rows="14" oninput="artCharCount('art-edit-text', 0)"></textarea>
            <div class="char-count" id="art-edit-text-count"></div>
          </div>

          <!-- AI Actions -->
          <div style="margin-top:8px;padding:12px;background:#22303c;border-radius:8px">
            <div style="font-size:0.75em;color:#8899a6;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px">AI-действия</div>
            <div style="display:flex;gap:6px;flex-wrap:wrap">
              <button class="art-improve-btn" onclick="improveArticle('improve')">&#9998; Улучшить стиль</button>
              <button class="art-improve-btn" onclick="improveArticle('expand')">&#128200; Расширить</button>
              <button class="art-improve-btn" onclick="improveArticle('shorten')">&#9986; Сократить</button>
              <button class="art-improve-btn" onclick="improveArticle('fix_grammar')">&#128221; Грамматика</button>
              <button class="art-improve-btn" onclick="improveArticle('add_seo')">&#128269; Добавить SEO</button>
              <button class="art-improve-btn" onclick="improveArticle('make_engaging')">&#128293; Вовлекающий</button>
              <span style="margin-left:8px;font-size:0.8em;color:#657786">|</span>
              <button class="art-improve-btn" onclick="rewriteArticleInStyle('news')">&#128240; Новость</button>
              <button class="art-improve-btn" onclick="rewriteArticleInStyle('seo')">&#128269; SEO</button>
              <button class="art-improve-btn" onclick="rewriteArticleInStyle('clickbait')">&#128293; Кликбейт</button>
              <button class="art-improve-btn" onclick="rewriteArticleInStyle('social')">&#128242; Соцсети</button>
              <button class="art-improve-btn" onclick="rewriteArticleInStyle('short')">&#9889; Кратко</button>
              <span style="margin-left:8px;font-size:0.8em;color:#657786">|</span>
              <button class="art-improve-btn" onclick="generateMultiOutput(['social','short','news'])" title="Telegram + Short + Новость">&#128172; Multi-output</button>
              <button class="art-improve-btn" onclick="regenerateField('title')" title="Перегенерировать заголовок">&#127922; Заголовок</button>
              <button class="art-improve-btn" onclick="regenerateField('seo_title')">&#128269; SEO Title</button>
              <button class="art-improve-btn" onclick="regenerateField('tags')">&#127991; Теги</button>
            </div>
            <div id="art-ai-loading" style="display:none;margin-top:8px;font-size:0.85em;color:#8899a6">
              <span class="spinner" style="width:14px;height:14px;border:2px solid #38444d;border-top-color:#1da1f2;border-radius:50%;animation:spin .8s linear infinite;display:inline-block;vertical-align:middle"></span>
              <span id="art-ai-loading-text">Обрабатываем...</span>
            </div>
            <div id="art-ai-changes" style="display:none;margin-top:8px;padding:8px 12px;background:#17bf6315;border-radius:6px;font-size:0.83em;color:#17bf63"></div>
          </div>

          <!-- Article Versions (Phase 2, behind content_versions_v1) -->
          <div id="art-versions-container" style="margin-top:8px"></div>

          <!-- SEO Analysis -->
          <div style="margin-top:8px;padding:12px;background:#22303c;border-radius:8px">
            <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px">
              <div style="font-size:0.75em;color:#8899a6;text-transform:uppercase;letter-spacing:0.5px">SEO-анализ</div>
              <button class="art-improve-btn" onclick="runSeoCheck()">&#128202; SEO-анализ</button>
              <span id="seo-score-badge" style="display:none;padding:2px 10px;border-radius:12px;font-size:0.85em;font-weight:600"></span>
            </div>
            <div id="seo-results" style="display:none;margin-top:6px"></div>
          </div>

          <!-- Schedule publication -->
          <div style="margin-top:8px;padding:12px;background:#22303c;border-radius:8px">
            <div style="font-size:0.75em;color:#8899a6;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px">Запланировать публикацию</div>
            <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
              <input type="datetime-local" id="art-schedule-datetime" style="padding:5px 8px;background:#192734;border:1px solid #38444d;border-radius:6px;color:#e1e8ed;font-size:0.85em">
              <button class="btn btn-sm btn-primary" onclick="scheduleArticle()">Запланировать</button>
              <button class="btn btn-sm btn-secondary" onclick="cancelScheduleArticle()" id="art-cancel-schedule-btn" style="display:none">Отменить</button>
              <span id="art-schedule-info" style="font-size:0.8em;color:#ffad1f;display:none"></span>
            </div>
          </div>

          <!-- Original text (collapsed) -->
          <details style="margin-top:12px" id="art-original-block">
            <summary style="cursor:pointer;font-size:0.82em;color:#8899a6">Оригинал</summary>
            <div id="art-original-text" style="margin-top:8px;padding:12px;background:#22303c;border-radius:8px;font-size:0.83em;color:#8899a6;max-height:300px;overflow-y:auto;white-space:pre-wrap;line-height:1.5"></div>
          </details>
        </div>
      </div>
    </div>
  </div>

  <!-- QUEUE -->
  <!-- queue panel moved to settings sub-tab -->

  <!-- ANALYTICS -->
  <!-- VIRAL -->
  <div class="panel" id="panel-viral">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;flex-wrap:wrap;gap:8px">
      <h2>Анализ виральности</h2>
      <div style="display:flex;gap:8px;align-items:center">
        <button class="btn btn-sm btn-secondary" onclick="loadViral()">Обновить</button>
      </div>
    </div>

    <!-- Viral stat cards -->
    <div class="stats" id="viral-stats"></div>

    <!-- Calendar event banner -->
    <div id="viral-calendar" style="display:none;margin-bottom:12px;padding:10px 16px;background:linear-gradient(90deg,#1da1f233,#e0245e22);border-radius:10px;border-left:3px solid #ffad1f"></div>

    <!-- Filters -->
    <div class="dash-filters" style="margin-bottom:12px">
      <span class="filter-label">Уровень:</span>
      <select id="viral-level" onchange="loadViral()">
        <option value="">Все</option>
        <option value="high">High (70+)</option>
        <option value="medium">Medium (40-69)</option>
        <option value="low">Low (20-39)</option>
        <option value="none">None (&lt;20)</option>
      </select>
      <span class="filter-sep"></span>
      <span class="filter-label">Категория:</span>
      <select id="viral-category" onchange="loadViral()">
        <option value="">Все</option>
        <option value="Скандалы">Скандалы</option>
        <option value="Утечки">Утечки</option>
        <option value="Shadow Drops">Shadow Drops</option>
        <option value="Плохие релизы">Плохие релизы</option>
        <option value="AI">AI</option>
        <option value="Ивенты">Ивенты</option>
        <option value="Деньги">Деньги</option>
        <option value="Культура">Культура</option>
        <option value="Персоны">Персоны</option>
        <option value="Скорость">Скорость</option>
        <option value="Базовые">Базовые</option>
      </select>
      <span class="filter-sep"></span>
      <span class="filter-label">Тональность:</span>
      <select id="viral-sentiment" onchange="loadViral()">
        <option value="">Все</option>
        <option value="positive">Позитив</option>
        <option value="neutral">Нейтрал</option>
        <option value="negative">Негатив</option>
      </select>
      <span class="filter-sep"></span>
      <span class="filter-label">Источник:</span>
      <select id="viral-source" onchange="loadViral()">
        <option value="">Все</option>
      </select>
      <span class="filter-sep"></span>
      <span class="filter-label">Мин. скор:</span>
      <input type="number" id="viral-min-score" value="0" min="0" max="100" style="width:60px" onchange="loadViral()" autocomplete="off">
      <span class="filter-sep"></span>
      <span class="filter-label">С:</span>
      <input type="date" id="viral-date-from" onchange="loadViral()">
      <span class="filter-sep"></span>
      <span class="filter-label">По:</span>
      <input type="date" id="viral-date-to" onchange="loadViral()">
    </div>

    <!-- Charts row -->
    <div class="grid-2" style="gap:12px;margin-bottom:16px">
      <div class="card">
        <h3 style="font-size:0.95em;margin-bottom:10px">Топ триггеры</h3>
        <div id="viral-top-triggers"></div>
      </div>
      <div class="card">
        <h3 style="font-size:0.95em;margin-bottom:10px">Категории триггеров</h3>
        <div id="viral-categories"></div>
      </div>
    </div>

    <div class="grid-2" style="gap:12px;margin-bottom:16px">
      <div class="card">
        <h3 style="font-size:0.95em;margin-bottom:10px">Тональность</h3>
        <div id="viral-sentiment-chart" style="display:flex;gap:8px;align-items:center;height:30px"></div>
      </div>
      <div class="card">
        <h3 style="font-size:0.95em;margin-bottom:10px">Средний скор по источникам</h3>
        <div id="viral-source-avg"></div>
      </div>
    </div>

    <!-- Table -->
    <div style="margin-bottom:8px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px">
      <span id="viral-count" style="color:#8899a6;font-size:0.85em"></span>
      <div style="display:flex;gap:6px">
        <button class="btn btn-sm" style="background:#64c8ff22;color:#64c8ff;border:1px solid #64c8ff44" onclick="sendViralToEditor('high')">HIGH &#8594; Редактор</button>
        <button class="btn btn-sm" style="background:#64c8ff22;color:#64c8ff;border:1px solid #64c8ff44" onclick="sendViralToEditor('medium')">MED &#8594; Редактор</button>
        <button class="btn btn-sm" style="background:#64c8ff22;color:#64c8ff;border:1px solid #64c8ff44" onclick="sendViralToEditor('all')">Все &#8594; Редактор</button>
        <span id="viral-picks-count" style="color:#64c8ff;font-size:0.85em;align-self:center;display:none"></span>
      </div>
    </div>
    <table>
      <thead><tr>
        <th class="sortable" data-sort="viral_score" onclick="sortViralTab('viral_score')">Скор <span class="sort-arrow">&#9660;</span></th>
        <th>Уровень</th>
        <th>Тональн.</th>
        <th class="sortable" data-sort="source" onclick="sortViralTab('source')">Источник <span class="sort-arrow">&#9650;</span></th>
        <th class="sortable" data-sort="title" onclick="sortViralTab('title')">Заголовок <span class="sort-arrow">&#9650;</span></th>
        <th>Триггеры</th>
        <th>Теги</th>
        <th class="sortable" data-sort="parsed_at" onclick="sortViralTab('parsed_at')">Дата <span class="sort-arrow">&#9650;</span></th>
        <th>Статус</th>
        <th style="width:40px"></th>
      </tr></thead>
      <tbody id="viral-table"></tbody>
    </table>
    <div id="viral-empty" style="display:none;text-align:center;padding:40px;color:#8899a6">Нет данных. Нажмите &laquo;Обновить&raquo;</div>
  </div>

  <div class="panel" id="panel-analytics">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
      <h2>Аналитика</h2>
      <div style="display:flex;gap:8px">
        <button class="btn btn-sm btn-secondary" onclick="loadAnalytics()">Обновить</button>
        <button class="btn btn-sm btn-primary" onclick="generateDigest('today')">&#128240; Дайджест за день</button>
        <button class="btn btn-sm btn-primary" onclick="generateDigest('week')">&#128240; Дайджест за неделю</button>
      </div>
    </div>

    <!-- Funnel (Phase 3, behind analytics_funnel_v1) -->
    <div id="analytics-funnel" style="display:none;margin-bottom:16px">
      <div class="card" style="padding:16px">
        <h3 style="font-size:0.95em;margin-bottom:12px">Воронка конверсии</h3>
        <div id="funnel-bars" style="display:flex;flex-direction:column;gap:6px"></div>
        <div style="margin-top:12px">
          <h3 style="font-size:0.95em;margin-bottom:8px">Конверсия по источникам</h3>
          <div id="funnel-sources" style="max-height:200px;overflow-y:auto"></div>
        </div>
      </div>
    </div>

    <!-- Summary cards -->
    <div class="grid-2" style="gap:12px;margin-bottom:16px" id="analytics-summary"></div>

    <!-- Charts row -->
    <div class="grid-2" style="gap:12px;margin-bottom:16px">
      <div class="card">
        <h3 style="font-size:0.95em;margin-bottom:10px">Топ источников (7 дней)</h3>
        <div id="analytics-top-sources"></div>
      </div>
      <div class="card">
        <h3 style="font-size:0.95em;margin-bottom:10px">Новости по дням (14 дней)</h3>
        <div id="analytics-daily" style="display:flex;align-items:flex-end;gap:3px;height:120px"></div>
      </div>
    </div>

    <div class="grid-2" style="gap:12px;margin-bottom:16px">
      <div class="card">
        <h3 style="font-size:0.95em;margin-bottom:10px">Пиковые часы</h3>
        <div id="analytics-peak-hours"></div>
      </div>
      <div class="card">
        <h3 style="font-size:0.95em;margin-bottom:10px">Топ ключевые слова</h3>
        <div id="analytics-bigrams" style="display:flex;flex-wrap:wrap;gap:4px"></div>
      </div>
    </div>

    <div class="grid-2" style="gap:12px;margin-bottom:16px">
      <div class="card">
        <h3 style="font-size:0.95em;margin-bottom:10px">Средний скор по дням</h3>
        <div id="analytics-score-trend" style="display:flex;align-items:flex-end;gap:3px;height:120px"></div>
      </div>
      <div class="card">
        <h3 style="font-size:0.95em;margin-bottom:10px">Конверсия по дням</h3>
        <div id="analytics-conversion" style="display:flex;align-items:flex-end;gap:3px;height:120px"></div>
        <div style="margin-top:6px;display:flex;gap:12px;font-size:0.75em;color:#8899a6">
          <span><span style="display:inline-block;width:10px;height:10px;background:#17bf63;border-radius:2px;margin-right:3px"></span>Одобрено</span>
          <span><span style="display:inline-block;width:10px;height:10px;background:#e0245e;border-radius:2px;margin-right:3px"></span>Отклонено</span>
        </div>
      </div>
    </div>

    <div class="grid-2" style="gap:12px;margin-bottom:16px">
      <div class="card">
        <h3 style="font-size:0.95em;margin-bottom:10px">Веса источников (Feedback)</h3>
        <div id="analytics-source-weights"></div>
      </div>
      <div class="card">
        <h3 style="font-size:0.95em;margin-bottom:10px">Версии промптов</h3>
        <div id="analytics-prompts"></div>
        <div style="margin-top:8px;display:flex;gap:8px">
          <select id="prompt-name-select" style="padding:4px 8px;background:#192734;border:1px solid #38444d;border-radius:6px;color:#e1e8ed">
            <option value="trend_forecast">trend_forecast</option>
            <option value="merge_analysis">merge_analysis</option>
            <option value="keyso_queries">keyso_queries</option>
            <option value="rewrite">rewrite</option>
          </select>
          <button class="btn btn-sm btn-success" onclick="saveCurrentPromptVersion()">Сохранить текущую версию</button>
        </div>
      </div>
    </div>

    <!-- Digest result (inline from old button) -->
    <div class="card" id="digest-result" style="display:none;margin-bottom:16px">
      <h3 style="font-size:0.95em;margin-bottom:10px">Дайджест</h3>
      <div id="digest-content"></div>
    </div>

    <!-- Saved Digests section -->
    <div class="card" style="margin-bottom:16px">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
        <h3 style="font-size:0.95em;margin:0">Дайджесты</h3>
        <div style="display:flex;gap:8px;align-items:center">
          <select id="digest-style-select" style="padding:4px 8px;background:#192734;border:1px solid #38444d;border-radius:6px;color:#e1e8ed;font-size:0.85em">
            <option value="brief">Краткий</option>
            <option value="detailed">Подробный</option>
            <option value="telegram">Telegram</option>
          </select>
          <button class="btn btn-sm btn-success" onclick="generateAndSaveDigest()">Сгенерировать</button>
          <button class="btn btn-sm btn-secondary" onclick="loadSavedDigests()">Обновить</button>
        </div>
      </div>
      <div id="saved-digests-list" style="color:#8899a6;font-size:0.85em">Нажмите «Обновить» для загрузки</div>
    </div>
  </div>

  <!-- QUEUE (standalone tab) -->
  <div class="panel" id="panel-queue">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;flex-wrap:wrap;gap:8px">
      <h2>Очередь задач</h2>
      <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
        <select id="q-filter-type" onchange="renderQueueStandalone()" style="padding:4px 8px;background:#192734;color:#e1e8ed;border:1px solid #38444d;border-radius:6px">
          <option value="">Все типы</option>
          <option value="rewrite">Рерайт</option>
          <option value="sheets">Sheets</option>
          <option value="full_auto">Полный автомат</option>
          <option value="no_llm">Без LLM</option>
          <option value="mod_rewrite">Рерайт (модерация)</option>
        </select>
        <select id="q-filter-status" onchange="renderQueueStandalone()" style="padding:4px 8px;background:#192734;color:#e1e8ed;border:1px solid #38444d;border-radius:6px">
          <option value="">Все статусы</option>
          <option value="pending">Ожидает</option>
          <option value="processing">Обработка</option>
          <option value="done">Готово</option>
          <option value="error">Ошибка</option>
          <option value="cancelled">Отменено</option>
          <option value="skipped">Пропущено</option>
        </select>
        <button class="btn btn-sm btn-secondary" onclick="loadQueueStandalone()">Обновить</button>
        <button class="btn btn-sm btn-danger" onclick="cancelAllQueue('')">Отменить ожидающие</button>
        <button class="btn btn-sm btn-secondary" onclick="clearDoneQueue()">Очистить завершённые</button>
      </div>
    </div>
    <div style="display:flex;gap:12px;margin-bottom:12px;flex-wrap:wrap" id="q-stats"></div>
    <div style="background:#192734;border-radius:10px;overflow:hidden">
      <table>
        <thead><tr>
          <th style="width:40px"><input type="checkbox" onchange="qToggleAll(this)" style="width:16px;height:16px"></th>
          <th>Тип</th><th>Новость</th><th>Стиль</th><th>Статус</th><th>Результат</th><th>Создано</th><th>Действия</th>
        </tr></thead>
        <tbody id="q-table"></tbody>
      </table>
    </div>
    <div style="margin-top:8px;display:flex;gap:8px">
      <span id="q-selected-count" style="color:#8899a6;font-size:0.85em;line-height:28px"></span>
      <button class="btn btn-sm btn-danger" onclick="cancelSelectedQueue()">Отменить выбранные</button>
      <button class="btn btn-sm btn-primary" onclick="retrySelectedQueue()">&#128260; Повторить выбранные</button>
      <button class="btn btn-sm btn-warning" onclick="retryAllErrors()" id="q-retry-errors-btn" style="display:none">&#128260; Повторить все ошибки</button>
    </div>
  </div>

  <!-- HEALTH -->
  <div class="panel" id="panel-health">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
      <h2>Здоровье источников</h2>
      <div>
        <span id="health-summary" style="color:#8899a6;font-size:0.85em;margin-right:12px"></span>
        <button class="btn btn-sm btn-secondary" onclick="loadHealth()">24ч</button>
        <button class="btn btn-sm btn-primary" onclick="loadHealthPlus()" id="btn-health-plus">7д расширенный</button>
      </div>
    </div>
    <div id="health-recs" style="display:none;margin-bottom:15px"></div>
    <table>
      <thead id="health-thead"><tr><th>Статус</th><th>Источник</th><th>Статей (24ч)</th><th style="min-width:120px">Активность</th><th>Последний парсинг</th><th>Минут назад</th></tr></thead>
      <tbody id="health-table"></tbody>
    </table>
  </div>

  <!-- STORYLINES -->
  <div class="panel" id="panel-storylines" style="display:none">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
      <h2>Сюжетные линии <span style="font-size:0.65em;color:#8899a6;font-weight:normal">кластеры похожих новостей за 3 дня</span></h2>
      <div>
        <span id="storylines-count" style="color:#8899a6;font-size:0.85em;margin-right:12px"></span>
        <button class="btn btn-sm btn-secondary" onclick="loadStorylines()">Обновить</button>
      </div>
    </div>
    <div id="storylines-list"></div>
  </div>

  <!-- NEWS -->
  <div class="panel" id="panel-news">
    <div class="dash-filters">
      <span class="filter-label">Поиск:</span>
      <input type="search" id="news-search" placeholder="По заголовку..." oninput="filterNewsTable()" autocomplete="off" name="news-search-nologin">
      <span class="filter-sep"></span>
      <span class="filter-label">Статус:</span>
      <select id="filter-status" onchange="loadNewsPage(0)">
        <option value="" selected>Все обогащённые</option>
        <option value="approved">Одобренные (ждут обогащения)</option>
        <option value="processed">Обогащённые</option>
        <option value="ready">Готовые</option>
      </select>
      <span class="filter-sep"></span>
      <span class="filter-label">Источник:</span>
      <select id="filter-source" onchange="loadNewsPage(0)">
        <option value="">Все источники</option>
      </select>
      <span class="filter-sep"></span>
      <span class="filter-label">LLM:</span>
      <select id="filter-llm" onchange="loadNewsPage(0)">
        <option value="">Все</option>
        <option value="publish_now">publish_now</option>
        <option value="schedule">schedule</option>
        <option value="skip">skip</option>
        <option value="has_rec">Есть рекомендация</option>
        <option value="no_rec">Нет рекомендации</option>
      </select>
      <span class="filter-sep"></span>
      <span class="filter-label">Кол-во:</span>
      <input type="number" id="filter-limit" value="100" min="10" max="500" style="width:80px" onchange="loadNewsPage(0)" autocomplete="off">
      <span class="filter-sep"></span>
      <span class="filter-label">С:</span>
      <input type="date" id="news-date-from" onchange="loadNewsPage(0)">
      <span class="filter-label">По:</span>
      <input type="date" id="news-date-to" onchange="loadNewsPage(0)">
      <span class="filter-sep"></span>
      <button class="btn btn-sm btn-secondary" onclick="setNewsDateRange('today')">Сегодня</button>
      <button class="btn btn-sm btn-secondary" onclick="setNewsDateRange('yesterday')">Вчера</button>
      <button class="btn btn-sm btn-secondary" onclick="setNewsDateRange('week')">Неделя</button>
      <button class="btn btn-sm btn-secondary" onclick="setNewsDateRange('month')">Месяц</button>
      <button class="btn btn-sm btn-secondary" onclick="setNewsDateRange('')">Все</button>
    </div>
    <div class="btn-group">
      <button class="btn btn-secondary btn-sm" onclick="loadNewsPage(0)">Обновить</button>
      <button class="btn btn-warning btn-sm" onclick="bulkStatusChange('approved')">Одобрить выбранные</button>
      <button class="btn btn-danger btn-sm" onclick="bulkStatusChange('rejected')">Отклонить выбранные</button>
      <button class="btn btn-danger btn-sm" onclick="deleteSelectedNews()" style="margin-left:4px">Удалить выбранные</button>
      <button class="btn btn-primary btn-sm" onclick="analyzeSelectedNews()">&#9654; Анализировать выбранные</button>
      <button class="btn btn-success btn-sm" onclick="exportSelectedToSheets()">&#9776; В Sheets</button>
      <button class="btn btn-primary btn-sm" onclick="sendSelectedToContent()" style="background:#9b59b6;border-color:#9b59b6" title="Отправить выбранные на рерайт в Контент">&#9998; В контент</button>
      <span id="news-selected-count" style="color:#1da1f2;font-size:0.85em;margin-left:8px"></span>
      <span id="news-count" style="color:#8899a6;font-size:0.85em;margin-left:auto"></span>
    </div>
    <table id="news-main-table">
      <thead><tr>
        <th style="width:30px"><input type="checkbox" id="news-check-all" onchange="toggleAllNews(this)"></th>
        <th class="sortable" data-sort="source" onclick="sortNewsTab('source')">Источник <span class="sort-arrow">&#9650;</span></th>
        <th class="sortable" data-sort="title" onclick="sortNewsTab('title')">Заголовок <span class="sort-arrow">&#9650;</span></th>
        <th class="sortable" data-sort="published_at" onclick="sortNewsTab('published_at')">Опубл. <span class="sort-arrow">&#9650;</span></th>
        <th class="sortable" data-sort="status" onclick="sortNewsTab('status')">Статус <span class="sort-arrow">&#9650;</span></th>
        <th>Биграммы</th>
        <th>Keys.so</th>
        <th>Похожие</th>
        <th>Trends</th>
        <th>LLM</th>
        <th class="sortable" data-sort="total_score" onclick="sortNewsTab('total_score')">Скор <span class="sort-arrow">&#9650;</span></th>
        <th>Дата</th>
        <th>Действия</th>
      </tr></thead>
      <tbody id="news-table"></tbody>
    </table>
    <div id="news-pagination"></div>
  </div>

  <!-- FINAL -->
  <div class="panel" id="panel-final">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;flex-wrap:wrap;gap:8px">
      <h2>Финал <span style="font-size:0.65em;color:#8899a6;font-weight:normal">только publish_now</span></h2>
      <div style="display:flex;gap:8px;align-items:center">
        <span class="filter-label">Источник:</span>
        <select id="fin-source" onchange="loadFinal(0)" style="padding:4px 8px;background:#192734;color:#e1e8ed;border:1px solid #38444d;border-radius:6px">
          <option value="">Все</option>
        </select>
        <button class="btn btn-sm btn-secondary" onclick="loadFinal(0)">Обновить</button>
        <button class="btn btn-sm" onclick="finSelectAbove60()" style="background:#17bf63;color:#fff" title="Отметить все новости с финальным скором >= 60">&#10003; Отметить &gt;60</button>
        <button class="btn btn-sm btn-primary" onclick="finSendSelected()" style="background:#9b59b6;border-color:#9b59b6">&#9998; В контент</button>
        <span id="fin-selected-count" style="color:#1da1f2;font-size:0.85em"></span>
      </div>
    </div>
    <div class="stats" id="fin-stats"></div>
    <div style="background:#192734;border-radius:10px;overflow-x:auto">
      <table style="font-size:0.85em">
        <thead><tr>
          <th style="width:28px"><input type="checkbox" onchange="finToggleAll(this)" style="width:15px;height:15px"></th>
          <th class="sortable" onclick="sortFinal('source')" style="width:80px">Источник</th>
          <th class="sortable" onclick="sortFinal('title')">Заголовок</th>
          <th class="sortable" onclick="sortFinal('total_score')" style="width:50px" title="Внутренний скор">Скор</th>
          <th class="sortable" onclick="sortFinal('viral_score')" style="width:50px">Вирал</th>
          <th class="sortable" onclick="sortFinal('freshness_hours')" style="width:55px">Свеж.</th>
          <th style="width:45px">Тон</th>
          <th style="width:110px">Теги</th>
          <th style="width:120px">Биграммы</th>
          <th style="width:55px">Keys.so</th>
          <th style="width:50px">Похож.</th>
          <th style="width:55px">Trends</th>
          <th class="sortable" onclick="sortFinal('final_score')" style="width:55px;color:#17bf63;font-weight:700" title="Финальный скор = внутренний + обогащение">Финал</th>
          <th style="width:70px">Действия</th>
        </tr></thead>
        <tbody id="fin-table"></tbody>
      </table>
    </div>
    <div id="fin-pagination" style="margin-top:10px;display:flex;gap:8px;justify-content:center"></div>
    <div id="fin-count" style="text-align:center;margin-top:6px;color:#8899a6;font-size:0.85em"></div>
  </div>

  <!-- LOGS -->
  <!-- logs, sources, prompts, tools, users panels moved to settings sub-tabs -->

  <!-- SETTINGS (unified with sub-tabs) -->
  <div class="panel" id="panel-settings">
    <div class="settings-nav">
      <div class="settings-tab active" data-stab="general">Общие</div>
      <div class="settings-tab" data-stab="sources">Источники</div>
      <div class="settings-tab" data-stab="prompts">Промпты</div>
      <div class="settings-tab" data-stab="viral_cfg">Виральность</div>
      <div class="settings-tab" data-stab="simulator">Симулятор</div>
      <div class="settings-tab" data-stab="tools">Инструменты</div>
      <div class="settings-tab" data-stab="logs">Логи</div>
      <div class="settings-tab" data-stab="users">Пользователи</div>
    </div>

    <!-- General -->
    <div class="settings-section active" id="stab-general">
      <div class="grid-2">
        <div class="card">
          <h2>Общие</h2>
          <div class="form-group"><label>Модель LLM</label>
            <select id="set-model">
              <option value="anthropic/claude-sonnet-4">Claude Sonnet 4 (anthropic)</option>
              <option value="openai/gpt-4o-mini">GPT-4o Mini (openai)</option>
              <option value="openai/gpt-4o">GPT-4o (openai)</option>
              <option value="google/gemini-2.0-flash-001">Gemini 2.0 Flash (google)</option>
              <option value="meta-llama/llama-3.1-70b-instruct">Llama 3.1 70B (meta)</option>
            </select>
          </div>
          <div class="form-group"><label>Регион Keys.so</label><input id="set-keyso-region"></div>
          <div class="form-group"><label>Название вкладки Sheets</label><input id="set-sheets-tab"></div>
          <button class="btn btn-primary" onclick="saveSettings()">Сохранить</button>
          <hr style="border-color:#38444d;margin:15px 0">
          <h3 style="margin-bottom:10px">Автоматизация</h3>
          <div class="form-group"><label>Порог авто-одобрения (0 = выкл)</label><input type="number" id="set-auto-approve" min="0" max="100" value="70"></div>
          <div class="form-group"><label style="display:flex;align-items:center;gap:8px"><input type="checkbox" id="set-auto-rewrite" checked> Авто-рерайт при LLM "publish_now"</label></div>
          <div class="form-group"><label>Стиль авто-рерайта</label>
            <select id="set-auto-rewrite-style">
              <option value="news">news</option>
              <option value="review">review</option>
              <option value="guide">guide</option>
              <option value="editorial">editorial</option>
            </select>
          </div>
          <button class="btn btn-primary" onclick="saveSettings()">Сохранить</button>
        </div>
        <div class="card">
          <h2>Статус API</h2>
          <div id="api-status"></div>
        </div>
      </div>
      <div class="grid-2" style="margin-top:15px">
        <div class="card">
          <h2>База данных</h2>
          <div id="db-info" style="color:#8899a6;font-size:0.9em">Загрузка...</div>
        </div>
        <div class="card">
          <h2>Быстрые действия</h2>
          <div style="display:flex;flex-direction:column;gap:8px">
            <button class="btn btn-primary" onclick="runProcess()">&#9654; Обогатить одобренные</button>
            <button class="btn btn-warning" onclick="reparseAll()">Парсить все источники</button>
            <button class="btn btn-secondary" onclick="setupHeaders()">Создать заголовки Sheets</button>
            <button class="btn btn-secondary" onclick="exportAllProcessed()">&#128196; Экспорт обработанных в Лист1</button>
            <button class="btn btn-primary" onclick="exportReadyAll()">&#128196; Экспорт переписанных в Ready</button>
          </div>
        </div>
      </div>
    </div>

    <!-- Sources (moved from separate tab) -->
    <div class="settings-section" id="stab-sources">
      <div class="grid-2">
        <div class="card">
          <h2>Активные источники</h2>
          <table>
            <thead><tr><th style="width:30px"></th><th>Имя</th><th>Тип</th><th>Статей</th><th>Последний</th><th>URL</th><th>Интервал</th><th>Действия</th></tr></thead>
            <tbody id="sources-table"></tbody>
          </table>
        </div>
        <div class="card">
          <h2>Добавить источник</h2>
          <div class="form-group"><label>Имя</label><input id="src-name" autocomplete="off"></div>
          <div class="form-group"><label>Тип</label>
            <select id="src-type" onchange="toggleSrcFields()">
              <option value="rss">RSS</option><option value="html">HTML</option><option value="dtf">DTF (SPA)</option><option value="sitemap">Sitemap XML</option>
            </select>
          </div>
          <div class="form-group"><label>URL</label><input id="src-url" autocomplete="off"></div>
          <div class="form-group"><label>Интервал (мин)</label><input type="number" id="src-interval" value="15" autocomplete="off"></div>
          <div class="form-group" id="src-selector-group" style="display:none"><label>CSS Селектор</label><input id="src-selector" placeholder=".news-item" autocomplete="off"></div>
          <div class="form-group" id="src-title-sel-group" style="display:none"><label>Title Селектор</label><input id="src-title-selector" placeholder="h3 a" autocomplete="off"></div>
          <div class="form-group" id="src-url-filter-group" style="display:none"><label>Фильтр URL</label><input id="src-url-filter" placeholder="/news/" autocomplete="off"></div>
          <button class="btn btn-primary" onclick="addSource()">Добавить</button>
        </div>
      </div>
    </div>

    <!-- Prompts (moved from separate tab) -->
    <div class="settings-section" id="stab-prompts">
      <div class="card" style="margin-bottom:15px">
        <h2>Промпт прогноза трендов</h2>
        <textarea id="prompt-trend" rows="10"></textarea>
      </div>
      <div class="card" style="margin-bottom:15px">
        <h2>Промпт анализа объединений</h2>
        <textarea id="prompt-merge" rows="8"></textarea>
      </div>
      <div class="card" style="margin-bottom:15px">
        <h2>Промпт запросов Keys.so</h2>
        <textarea id="prompt-keyso" rows="8"></textarea>
      </div>
      <button class="btn btn-primary" onclick="savePrompts()">Сохранить промпты</button>
    </div>

    <!-- Tools (moved from separate tab) -->
    <div class="settings-section" id="stab-viral_cfg">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:15px">
        <h2 style="margin:0">Триггеры виральности</h2>
        <div style="display:flex;gap:8px">
          <input id="vt-search" placeholder="Поиск..." oninput="filterViralTriggers()" style="padding:6px 10px;border-radius:6px;border:1px solid #38444d;background:#15202b;color:#d9d9d9;width:200px" autocomplete="off">
          <button class="btn btn-primary" onclick="showAddViralTrigger()">+ Новый триггер</button>
          <button class="btn btn-secondary" onclick="loadViralTriggers()">Обновить</button>
        </div>
      </div>
      <div id="vt-add-form" style="display:none;margin-bottom:15px;padding:15px;background:#192734;border-radius:8px;border:1px solid #38444d">
        <div style="display:grid;grid-template-columns:1fr 1fr 100px;gap:10px;margin-bottom:10px">
          <div><label style="color:#8899a6;font-size:0.8em">ID</label><input id="vt-new-id" placeholder="my_trigger" style="width:100%;padding:6px;border-radius:4px;border:1px solid #38444d;background:#15202b;color:#d9d9d9" autocomplete="off"></div>
          <div><label style="color:#8899a6;font-size:0.8em">Название</label><input id="vt-new-label" placeholder="My Trigger" style="width:100%;padding:6px;border-radius:4px;border:1px solid #38444d;background:#15202b;color:#d9d9d9" autocomplete="off"></div>
          <div><label style="color:#8899a6;font-size:0.8em">Скор</label><input id="vt-new-weight" type="number" value="30" min="0" max="100" style="width:100%;padding:6px;border-radius:4px;border:1px solid #38444d;background:#15202b;color:#d9d9d9" autocomplete="off"></div>
        </div>
        <div><label style="color:#8899a6;font-size:0.8em">Ключевые слова (через запятую)</label><textarea id="vt-new-keywords" rows="2" placeholder="keyword1, keyword2, ключ3" style="width:100%;padding:6px;border-radius:4px;border:1px solid #38444d;background:#15202b;color:#d9d9d9;resize:vertical" autocomplete="off"></textarea></div>
        <div style="margin-top:10px;display:flex;gap:8px">
          <button class="btn btn-success" onclick="saveNewViralTrigger()">Сохранить</button>
          <button class="btn btn-secondary" onclick="document.getElementById('vt-add-form').style.display='none'">Отмена</button>
        </div>
      </div>
      <div style="max-height:600px;overflow-y:auto">
        <table>
          <thead><tr>
            <th style="width:30px"></th>
            <th style="cursor:pointer" onclick="sortViralTriggers('label')">Название</th>
            <th style="width:70px;cursor:pointer" onclick="sortViralTriggers('weight')">Скор</th>
            <th>Ключевые слова</th>
            <th style="width:80px">Тип</th>
            <th style="width:120px">Действия</th>
          </tr></thead>
          <tbody id="vt-table"></tbody>
        </table>
      </div>
      <div id="vt-stats" style="margin-top:10px;color:#8899a6;font-size:0.85em"></div>
    </div>

    <!-- Threshold Simulator -->
    <div class="settings-section" id="stab-simulator">
      <div class="card">
        <h2>Симулятор порогов</h2>
        <p style="color:#8899a6;font-size:0.85em;margin-bottom:12px">Симулируйте влияние порогов на количество прошедших новостей (данные за 7 дней)</p>
        <div class="grid-2">
          <div>
            <div class="form-group"><label>Внутренний скор (мин)</label><input type="range" id="sim-score-min" min="0" max="100" value="70" oninput="updateSimLabel(this,'sim-score-min-val')"><span id="sim-score-min-val" style="color:#1da1f2;margin-left:8px">70</span></div>
            <div class="form-group"><label>Внутренний скор (макс)</label><input type="range" id="sim-score-max" min="0" max="100" value="100" oninput="updateSimLabel(this,'sim-score-max-val')"><span id="sim-score-max-val" style="color:#1da1f2;margin-left:8px">100</span></div>
          </div>
          <div>
            <div class="form-group"><label>Финальный скор (мин)</label><input type="range" id="sim-final-min" min="0" max="100" value="60" oninput="updateSimLabel(this,'sim-final-min-val')"><span id="sim-final-min-val" style="color:#1da1f2;margin-left:8px">60</span></div>
            <div class="form-group"><label>Финальный скор (макс)</label><input type="range" id="sim-final-max" min="0" max="100" value="100" oninput="updateSimLabel(this,'sim-final-max-val')"><span id="sim-final-max-val" style="color:#1da1f2;margin-left:8px">100</span></div>
          </div>
        </div>
        <button class="btn btn-primary" onclick="runSimulation()">Симулировать</button>
        <div id="sim-results" style="margin-top:15px"></div>
      </div>
    </div>

    <div class="settings-section" id="stab-tools">
      <div class="grid-2">
        <div class="card">
          <h2>Тест LLM</h2>
          <div class="form-group"><label>Промпт</label><textarea id="test-llm-prompt" rows="4">Ты аналитик. Ответь JSON: {"test": "ok", "model": "your_model"}</textarea></div>
          <button class="btn btn-primary" onclick="testLLM()">&#9654; Отправить</button>
          <pre id="test-llm-result" style="margin-top:10px;color:#8899a6;font-size:0.85em;white-space:pre-wrap"></pre>
        </div>
        <div class="card">
          <h2>Тест Keys.so</h2>
          <div class="form-group"><label>Ключевое слово</label><input id="test-keyso-kw" value="gta 6"></div>
          <button class="btn btn-primary" onclick="testKeyso()">&#9654; Проверить</button>
          <pre id="test-keyso-result" style="margin-top:10px;color:#8899a6;font-size:0.85em;white-space:pre-wrap"></pre>
        </div>
      </div>
      <div class="card" style="margin-top:15px">
        <h2>Тест Google Sheets</h2>
        <button class="btn btn-primary" onclick="testSheets()">Проверить соединение</button>
        <pre id="test-sheets-result" style="margin-top:10px;color:#8899a6;font-size:0.85em;white-space:pre-wrap"></pre>
      </div>
      <div class="card" style="margin-top:15px">
        <h2>Тест парсинга URL</h2>
        <div class="form-group"><label>URL статьи</label><input id="test-parse-url" placeholder="https://example.com/article" autocomplete="off"></div>
        <button class="btn btn-primary" onclick="testParse()">Парсить</button>
        <pre id="test-parse-result" style="margin-top:10px;color:#8899a6;font-size:0.85em;white-space:pre-wrap"></pre>
      </div>
    </div>

    <!-- Queue (moved from separate tab) -->
    <div class="settings-section" id="stab-queue">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;flex-wrap:wrap;gap:8px">
        <h2>Очередь задач</h2>
        <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
          <select id="queue-filter-type" onchange="renderQueueTable()" style="padding:4px 8px;background:#192734;color:#e1e8ed;border:1px solid #38444d;border-radius:6px">
            <option value="">Все типы</option>
            <option value="rewrite">Переписка</option>
            <option value="sheets">Sheets</option>
            <option value="full_auto">Полный автомат</option>
            <option value="no_llm">Без LLM</option>
            <option value="mod_rewrite">Рерайт (модерация)</option>
          </select>
          <select id="queue-filter-status" onchange="renderQueueTable()" style="padding:4px 8px;background:#192734;color:#e1e8ed;border:1px solid #38444d;border-radius:6px">
            <option value="">Все статусы</option>
            <option value="pending">Ожидает</option>
            <option value="processing">Обработка</option>
            <option value="done">Готово</option>
            <option value="error">Ошибка</option>
            <option value="cancelled">Отменено</option>
            <option value="skipped">Пропущено</option>
          </select>
          <button class="btn btn-sm btn-secondary" onclick="loadQueue()">Обновить</button>
          <button class="btn btn-sm btn-danger" onclick="cancelAllQueue('')">Отменить все ожидающие</button>
          <button class="btn btn-sm btn-secondary" onclick="clearDoneQueue()">Очистить завершённые</button>
        </div>
      </div>
      <div style="display:flex;gap:16px;margin-bottom:12px" id="queue-stats"></div>
      <table>
        <thead><tr>
          <th style="width:40px"><input type="checkbox" onchange="toggleAllQueue(this)" style="width:16px;height:16px"></th>
          <th>Тип</th><th>Новость</th><th>Стиль</th><th>Статус</th><th>Результат</th><th>Создано</th><th>Действия</th>
        </tr></thead>
        <tbody id="queue-table"></tbody>
      </table>
      <div style="margin-top:8px;display:flex;gap:8px">
        <span id="queue-selected-count" style="color:#8899a6;font-size:0.85em;line-height:28px"></span>
        <button class="btn btn-sm btn-danger" onclick="cancelSelectedQueue()">Отменить выбранные</button>
        <button class="btn btn-sm btn-primary" onclick="retrySelectedQueue()">&#128260; Повторить выбранные</button>
      </div>
    </div>

    <!-- Logs (moved from separate tab) -->
    <div class="settings-section" id="stab-logs">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;flex-wrap:wrap;gap:8px">
        <h2>Логи системы</h2>
        <div style="display:flex;gap:8px;align-items:center">
          <select id="log-level" onchange="loadLogs()" style="padding:4px 8px;background:#192734;color:#e1e8ed;border:1px solid #38444d;border-radius:6px">
            <option value="">Все уровни</option>
            <option value="ERROR">Ошибки</option>
            <option value="WARNING">Предупреждения</option>
            <option value="INFO">Информация</option>
          </select>
          <button class="btn btn-sm btn-secondary" onclick="loadLogs()">Обновить</button>
        </div>
      </div>
      <div style="display:flex;gap:16px;margin-bottom:12px" id="api-stats"></div>
      <div style="background:#192734;border-radius:10px;overflow:hidden;max-height:600px;overflow-y:auto">
        <table>
          <thead><tr><th style="width:160px">Время</th><th style="width:70px">Уровень</th><th style="width:140px">Модуль</th><th>Сообщение</th></tr></thead>
          <tbody id="logs-table"></tbody>
        </table>
      </div>
    </div>

    <!-- Users (moved from separate tab) -->
    <div class="settings-section" id="stab-users">
      <div class="grid-2">
        <div class="card">
          <h2>Пользователи</h2>
          <table>
            <thead><tr><th>Логин</th><th>Роль</th><th>Действия</th></tr></thead>
            <tbody id="users-table"></tbody>
          </table>
        </div>
        <div class="card">
          <h2>Добавить пользователя</h2>
          <div class="form-group"><label>Логин</label><input id="new-username" autocomplete="off"></div>
          <div class="form-group"><label>Пароль</label><input id="new-password" type="password" autocomplete="new-password"></div>
          <div class="form-group"><label>Роль</label>
            <select id="new-role">
              <option value="editor">Редактор</option>
              <option value="viewer">Читатель</option>
              <option value="admin">Админ</option>
            </select>
          </div>
          <button class="btn btn-primary" onclick="addUser()">Добавить</button>
          <hr style="border-color:#38444d;margin:15px 0">
          <h2>Сменить пароль</h2>
          <div class="form-group"><label>Пользователь</label>
            <select id="chpass-user"></select>
          </div>
          <div class="form-group"><label>Новый пароль</label><input id="chpass-password" type="password" autocomplete="new-password"></div>
          <button class="btn btn-warning" onclick="changePassword()">Сменить</button>
        </div>
      </div>
    </div>
  </div>
</div>

<div class="modal-overlay" id="edit-modal">
  <div class="modal">
    <h2>Редактировать источник</h2>
    <input type="hidden" id="edit-old-name">
    <div class="form-group"><label>Имя</label><input id="edit-name"></div>
    <div class="form-group"><label>Type</label>
      <select id="edit-type" onchange="document.getElementById('edit-selector-group').style.display=this.value==='html'?'block':'none'">
        <option value="rss">RSS</option><option value="html">HTML</option>
      </select>
    </div>
    <div class="form-group"><label>URL</label><input id="edit-url"></div>
    <div class="form-group"><label>Интервал (мин)</label><input type="number" id="edit-interval"></div>
    <div class="form-group" id="edit-selector-group" style="display:none"><label>CSS Селектор</label><input id="edit-selector"></div>
    <div class="modal-buttons">
      <button class="btn btn-secondary" onclick="closeEditModal()">Отмена</button>
      <button class="btn btn-primary" onclick="saveEditSource()">Сохранить</button>
    </div>
  </div>
</div>

<div class="toast" id="toast"></div>

<!-- Explain Drawer (Phase 1, behind explainability_v1 flag) -->
<div class="explain-drawer" id="explain-drawer">
  <button class="close-btn" onclick="closeExplainDrawer()">&times;</button>
  <h2 style="margin-bottom:12px">&#128269; Почему так?</h2>
  <div id="explain-title" style="font-weight:600;margin-bottom:12px;font-size:1.05em"></div>
  <div class="explain-section">
    <h3>Score Breakdown</h3>
    <div id="explain-scores"></div>
  </div>
  <div class="explain-section">
    <h3>Решения системы</h3>
    <div id="explain-trace"></div>
  </div>
  <div class="explain-section">
    <h3>Причина статуса</h3>
    <div id="explain-reason" style="font-size:0.9em;color:#8899a6"></div>
  </div>
</div>

<script>
// Tabs
document.querySelectorAll('.tab').forEach(t => t.addEventListener('click', () => {
  document.querySelectorAll('.tab').forEach(x => x.classList.remove('active'));
  document.querySelectorAll('.panel').forEach(x => x.classList.remove('active'));
  t.classList.add('active');
  document.getElementById('panel-' + t.dataset.tab).classList.add('active');
  // Refresh data when switching to key tabs
  if (t.dataset.tab === 'ops') { loadOpsDashboard(); loadFeatureFlags(); }
  if (t.dataset.tab === 'editorial') { loadEditorial(); }
  // moderation tab removed
  if (t.dataset.tab === 'news') { loadNewsPage(); }
  if (t.dataset.tab === 'final') { loadFinal(); }
  if (t.dataset.tab === 'editor') { loadArticles(); }
  if (t.dataset.tab === 'viral') { loadViral(); }
  if (t.dataset.tab === 'analytics') { loadAnalytics(); loadSavedDigests(); loadFunnelAnalytics(); }
  if (t.dataset.tab === 'queue') { loadQueueStandalone(); }
  if (t.dataset.tab === 'health') { loadHealth(); }
  if (t.dataset.tab === 'storylines') { loadStorylines(); }
  if (t.dataset.tab === 'settings') { loadSettings(); loadLogs(); loadQueue(); loadViralTriggers(); }
}));

// Settings sub-tabs
document.querySelectorAll('.settings-tab').forEach(t => t.addEventListener('click', () => {
  document.querySelectorAll('.settings-tab').forEach(x => x.classList.remove('active'));
  document.querySelectorAll('.settings-section').forEach(x => x.classList.remove('active'));
  t.classList.add('active');
  document.getElementById('stab-' + t.dataset.stab).classList.add('active');
  // Refresh sub-tab data
  const s = t.dataset.stab;
  if (s === 'sources') loadSources();
  if (s === 'prompts') loadPrompts();
  if (s === 'logs') loadLogs();
  if (s === 'queue') loadQueue();
  if (s === 'users') loadUsers();
  if (s === 'viral_cfg') loadViralTriggers();
}));

function toast(msg, isError) {
  const el = document.getElementById('toast');
  if (!el) return;
  el.textContent = msg;
  el.className = 'toast' + (isError ? ' error' : '');
  el.style.display = 'block';
  setTimeout(() => el.style.display = 'none', 3000);
}
function showToast(msg, type) {
  toast(msg, type === 'error' || type === 'warning');
}

async function api(url, body) {
  const opts = body ? {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)} : {};
  const r = await fetch(url, opts);
  return r.json();
}

// Clock
setInterval(() => {
  document.getElementById('clock').textContent = new Date().toLocaleString('ru-RU');
}, 1000);

// ---- Browser Notifications ----
let _notificationsEnabled = localStorage.getItem('notif_enabled') === 'true';
let _prevFinalIds = new Set();
let _prevQueueStatuses = {};

function requestNotificationPermission() {
  if (!('Notification' in window)) return;
  if (Notification.permission === 'default') {
    Notification.requestPermission();
  }
}

function showNotification(title, body) {
  if (!_notificationsEnabled) return;
  if (!('Notification' in window) || Notification.permission !== 'granted') return;
  try {
    const n = new Notification(title, { body, icon: '/favicon.ico', tag: title.slice(0, 20) });
    setTimeout(() => n.close(), 8000);
  } catch(e) {}
}

function toggleNotifications(on) {
  _notificationsEnabled = on;
  localStorage.setItem('notif_enabled', on ? 'true' : 'false');
  if (on) {
    if (!('Notification' in window)) { alert('Браузер не поддерживает уведомления'); return; }
    if (Notification.permission === 'default') {
      Notification.requestPermission().then(p => {
        if (p !== 'granted') {
          _notificationsEnabled = false;
          localStorage.setItem('notif_enabled', 'false');
          document.getElementById('notif-toggle').checked = false;
        }
      });
    } else if (Notification.permission === 'denied') {
      alert('Уведомления заблокированы в настройках браузера');
      _notificationsEnabled = false;
      localStorage.setItem('notif_enabled', 'false');
      document.getElementById('notif-toggle').checked = false;
    }
  }
}

// Restore toggle state on load
document.addEventListener('DOMContentLoaded', () => {
  const toggle = document.getElementById('notif-toggle');
  if (toggle) toggle.checked = _notificationsEnabled;
  if (_notificationsEnabled) requestNotificationPermission();
});

// News tab state (used by Обогащённые panel)
let _allNews = [];

let _newsTotal = 0;
let _newsOffset = 0;
let _newsPageSize = 100;

async function loadNewsPage(offset) {
  if (offset !== undefined) _newsOffset = offset;
  const limit = parseInt(document.getElementById('filter-limit')?.value) || 100;
  const status = document.getElementById('filter-status')?.value || '';
  const source = document.getElementById('filter-source')?.value || '';
  const llmFilter = document.getElementById('filter-llm')?.value || '';
  const dateFrom = document.getElementById('news-date-from')?.value || '';
  const dateTo = document.getElementById('news-date-to')?.value || '';
  let url = `/api/news?limit=${limit}&offset=${_newsOffset}`;
  if (status) url += `&status=${status}`;
  if (source) url += `&source=${encodeURIComponent(source)}`;
  if (llmFilter) url += `&llm=${encodeURIComponent(llmFilter)}`;
  if (dateFrom) url += `&date_from=${dateFrom}`;
  if (dateTo) url += `&date_to=${dateTo}`;
  const resp = await api(url);
  const news = resp.news || resp;
  _allNews = news;
  _newsTotal = resp.total || news.length;
  _newsPageSize = limit;
  renderNewsFiltered();
  renderNewsPagination();
}

function renderNewsPagination() {
  let el = document.getElementById('news-pagination');
  if (!el) return;
  const totalPages = Math.ceil(_newsTotal / _newsPageSize);
  const currentPage = Math.floor(_newsOffset / _newsPageSize) + 1;
  if (totalPages <= 1) { el.innerHTML = ''; return; }
  let html = '<div style="display:flex;gap:4px;align-items:center;margin-top:10px;justify-content:center">';
  if (currentPage > 1) html += `<button class="btn btn-sm btn-secondary" onclick="loadNewsPage(${(_newsOffset - _newsPageSize)})">&#9664; Назад</button>`;
  html += `<span style="color:#8899a6;font-size:0.85em;margin:0 8px">Стр. ${currentPage} из ${totalPages} (всего ${_newsTotal})</span>`;
  if (currentPage < totalPages) html += `<button class="btn btn-sm btn-secondary" onclick="loadNewsPage(${(_newsOffset + _newsPageSize)})">Далее &#9654;</button>`;
  html += '</div>';
  el.innerHTML = html;
}

const STATUS_LABELS = {new:'Новая',in_review:'Проверка',approved:'Одобр.',processed:'Обогащ.',duplicate:'Дубль',rejected:'Откл.',ready:'Готова'};

function esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }
function fmtDate(d) { if (!d) return '-'; return d.replace('T',' ').slice(0,16); }

function renderSimilarTooltip(count, items) {
  const rows = items.map((it, i) => {
    const word = typeof it === 'string' ? it : (it.word || '');
    const ws = typeof it === 'object' ? (it.ws || '') : '';
    return `<div class="tip-row"><span class="tip-word">${esc(word)}</span>${ws ? `<span class="tip-ws">${ws}</span>` : ''}<button class="tip-copy" onclick="event.stopPropagation();navigator.clipboard.writeText('${esc(word).replace(/'/g,"\\'")}');this.textContent='✓';setTimeout(()=>this.textContent='⎘',800)" title="Копировать">⎘</button></div>`;
  }).join('');
  const allWords = items.map(it => typeof it === 'string' ? it : (it.word || '')).join('\\n');
  return `<div class="tip-wrap"><span class="tip-count">${count}</span><div class="tip-box"><div class="tip-box-inner"><div class="tip-header"><span>Похожие запросы</span><button class="tip-copy-all" onclick="event.stopPropagation();navigator.clipboard.writeText('${allWords.replace(/'/g,"\\'")}');this.textContent='Скопировано!';setTimeout(()=>this.textContent='Копировать все',1000)">Копировать все</button></div>${rows}</div></div></div>`;
}

function switchToTab(tabName) {
  // Check if this tab was moved into settings as a sub-tab
  const settingsSubTabs = ['sources', 'prompts', 'tools', 'queue', 'logs', 'users'];
  if (settingsSubTabs.includes(tabName)) {
    // Switch to settings panel first
    document.querySelectorAll('.tab').forEach(x => x.classList.remove('active'));
    document.querySelectorAll('.panel').forEach(x => x.classList.remove('active'));
    document.querySelector('.tab[data-tab="settings"]').classList.add('active');
    document.getElementById('panel-settings').classList.add('active');
    // Then switch to the sub-tab
    document.querySelectorAll('.settings-tab').forEach(x => x.classList.remove('active'));
    document.querySelectorAll('.settings-section').forEach(x => x.classList.remove('active'));
    const stab = document.querySelector(`.settings-tab[data-stab="${tabName}"]`);
    if (stab) stab.classList.add('active');
    const sect = document.getElementById('stab-' + tabName);
    if (sect) sect.classList.add('active');
    return;
  }
  document.querySelectorAll('.tab').forEach(x => x.classList.remove('active'));
  document.querySelectorAll('.panel').forEach(x => x.classList.remove('active'));
  document.querySelector(`.tab[data-tab="${tabName}"]`).classList.add('active');
  document.getElementById('panel-' + tabName).classList.add('active');
}

// Actions
async function runProcess() {
  toast('Обработка запущена...');
  const r = await api('/api/process', {});
  toast(r.message || 'Готово');
  setTimeout(loadAll, 5000);
}

async function processOne(id) {
  toast('Анализ...');
  const r = await api('/api/process_one', {news_id: id});
  if (r.status === 'ok') toast('Проанализировано!');
  else toast(r.message, true);
  loadAll();
}

async function exportOne(id) {
  const r = await api('/api/queue/sheets', {news_ids: [id]});
  if (r.status === 'ok') { toast('Добавлено в очередь Sheets'); loadQueue(); }
  else toast(r.message, true);
}

// Sources
let _sources = [];
let _healthData = [];
async function loadSources() {
  const [sources, health] = await Promise.all([api('/api/sources'), api('/api/health')]);
  _sources = sources;
  _healthData = health;
  const hMap = {};
  health.forEach(h => { hMap[h.source] = h; });
  document.getElementById('sources-table').innerHTML = _sources.map(s => {
    const h = hMap[s.name] || {};
    const icon = h.status==='healthy'?'&#9989;':h.status==='low'?'&#128993;':h.status==='warning'?'&#9888;&#65039;':h.status==='dead'?'&#10060;':'&#9898;';
    const cnt = h.count_24h ?? '-';
    const lastP = h.last_parsed ? fmtDate(h.last_parsed) : '-';
    const minAgo = h.minutes_ago >= 0 ? h.minutes_ago + ' мин' : '';
    return `<tr>
      <td>${icon}</td>
      <td><b>${s.name}</b></td>
      <td><span class="badge" style="background:#22303c;color:#8899a6">${s.type}</span></td>
      <td>${cnt}</td>
      <td style="font-size:0.8em;color:#8899a6">${lastP}${minAgo ? '<br>'+minAgo+' назад' : ''}</td>
      <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis" title="${esc(s.url)}">${esc(s.url)}</td>
      <td>${s.interval} мин</td>
      <td style="white-space:nowrap">
        <button class="btn btn-sm btn-primary" onclick="reparseSource('${esc(s.name)}')">Парсить</button>
        <button class="btn btn-sm btn-secondary" onclick="openEditModal('${esc(s.name)}')">Ред.</button>
        <button class="btn btn-sm btn-danger" onclick="deleteSource('${esc(s.name)}')">&#10005;</button>
      </td>
    </tr>`;
  }).join('');
}

function openEditModal(name) {
  const s = _sources.find(x => x.name === name);
  if (!s) return;
  document.getElementById('edit-old-name').value = s.name;
  document.getElementById('edit-name').value = s.name;
  document.getElementById('edit-type').value = s.type;
  document.getElementById('edit-url').value = s.url;
  document.getElementById('edit-interval').value = s.interval;
  document.getElementById('edit-selector').value = s.selector || '';
  document.getElementById('edit-selector-group').style.display = s.type === 'html' ? 'block' : 'none';
  document.getElementById('edit-modal').classList.add('show');
}

function closeEditModal() {
  document.getElementById('edit-modal').classList.remove('show');
}

async function saveEditSource() {
  const data = {
    old_name: document.getElementById('edit-old-name').value,
    name: document.getElementById('edit-name').value,
    type: document.getElementById('edit-type').value,
    url: document.getElementById('edit-url').value,
    interval: document.getElementById('edit-interval').value,
    selector: document.getElementById('edit-selector').value,
  };
  await api('/api/sources/edit', data);
  toast('Источник обновлён');
  closeEditModal();
  loadSources();
}

async function addSource() {
  const data = {
    name: document.getElementById('src-name').value,
    type: document.getElementById('src-type').value,
    url: document.getElementById('src-url').value,
    interval: document.getElementById('src-interval').value,
    selector: document.getElementById('src-selector').value,
  };
  if (!data.name || !data.url) { toast('Заполните имя и URL', true); return; }
  const r = await api('/api/sources/add', data);
  toast('Источник добавлен');
  loadSources();
}

async function deleteSource(name) {
  if (!confirm('Удалить ' + name + '?')) return;
  await api('/api/sources/delete', {name});
  toast('Удалено');
  loadSources();
}

async function reparseSource(name) {
  toast('Парсинг ' + name + '...');
  const r = await api('/api/reparse', {name});
  if (r.status === 'ok') toast(name + ': ' + r.new_articles + ' новых статей');
  else toast(r.message, true);
  loadAll();
}

// Prompts
async function loadPrompts() {
  const p = await api('/api/prompts');
  document.getElementById('prompt-trend').value = p.trend_forecast || '';
  document.getElementById('prompt-merge').value = p.merge_analysis || '';
  document.getElementById('prompt-keyso').value = p.keyso_queries || '';
}

async function savePrompts() {
  await api('/api/prompts/save', {
    trend_forecast: document.getElementById('prompt-trend').value,
    merge_analysis: document.getElementById('prompt-merge').value,
    keyso_queries: document.getElementById('prompt-keyso').value,
  });
  toast('Промпты сохранены');
}

// Settings
async function loadSettings() {
  const s = await api('/api/settings');
  document.getElementById('set-model').value = s.llm_model || '';
  document.getElementById('set-keyso-region').value = s.keyso_region || '';
  document.getElementById('set-sheets-tab').value = s.sheets_tab || '';
  document.getElementById('set-auto-approve').value = s.auto_approve_threshold ?? 70;
  document.getElementById('set-auto-rewrite').checked = s.auto_rewrite_on_publish_now !== false;
  document.getElementById('set-auto-rewrite-style').value = s.auto_rewrite_style || 'news';
  document.getElementById('api-status').innerHTML =
    `<p style="margin:8px 0">OpenAI API: ${s.openai_key_set ? '<span style="color:#17bf63">Connected</span>' : '<span style="color:#e0245e">Not set</span>'}</p>` +
    `<p style="margin:8px 0">Keys.so API: ${s.keyso_key_set ? '<span style="color:#17bf63">Connected</span>' : '<span style="color:#e0245e">Not set</span>'}</p>` +
    `<p style="margin:8px 0">Google Sheets: ${s.google_sa_set ? '<span style="color:#17bf63">Connected</span>' : '<span style="color:#e0245e">Not set</span>'}</p>` +
    `<p style="margin:8px 0">Sheets ID: <code>${s.sheets_id || 'not set'}</code></p>`;
}

async function saveSettings() {
  await api('/api/settings/save', {
    llm_model: document.getElementById('set-model').value,
    keyso_region: document.getElementById('set-keyso-region').value,
    sheets_tab: document.getElementById('set-sheets-tab').value,
    auto_approve_threshold: parseInt(document.getElementById('set-auto-approve').value) || 0,
    auto_rewrite_on_publish_now: document.getElementById('set-auto-rewrite').checked,
    auto_rewrite_style: document.getElementById('set-auto-rewrite-style').value,
  });
  toast('Настройки сохранены');
}

// Tools
async function testLLM() {
  document.getElementById('test-llm-result').textContent = 'Загрузка...';
  const r = await api('/api/test_llm', {prompt: document.getElementById('test-llm-prompt').value});
  document.getElementById('test-llm-result').textContent = JSON.stringify(r, null, 2);
}

async function testKeyso() {
  document.getElementById('test-keyso-result').textContent = 'Загрузка...';
  const r = await api('/api/test_keyso', {keyword: document.getElementById('test-keyso-kw').value});
  document.getElementById('test-keyso-result').textContent = JSON.stringify(r, null, 2);
}

// Users
async function loadUsers() {
  const users = await api('/api/users');
  const roleLabels = {admin:'Админ',editor:'Редактор',viewer:'Читатель'};
  const roleColors = {admin:'#e0245e',editor:'#1da1f2',viewer:'#8899a6'};
  document.getElementById('users-table').innerHTML = users.map(u => {
    const rl = roleLabels[u.role]||u.role;
    const rc = roleColors[u.role]||'#8899a6';
    return `<tr><td>${esc(u.username)}</td><td style="color:${rc};font-weight:600">${rl}</td><td>${u.username==='admin'?'':'<button class="btn btn-sm btn-danger" onclick="deleteUser(\''+u.username+'\')">Удалить</button>'}</td></tr>`;
  }).join('');
  const sel = document.getElementById('chpass-user');
  if (sel) {
    sel.innerHTML = users.map(u => `<option value="${u.username}">${esc(u.username)}</option>`).join('');
  }
}
async function addUser() {
  const username = document.getElementById('new-username').value;
  const password = document.getElementById('new-password').value;
  const role = document.getElementById('new-role').value;
  if (!username || !password) { toast('Заполните логин и пароль', true); return; }
  const r = await api('/api/users/add', {username, password, role});
  if (r.status === 'error') { toast(r.message, true); return; }
  toast('Пользователь добавлен');
  loadUsers();
}
async function deleteUser(username) {
  if (!confirm('Удалить пользователя ' + username + '?')) return;
  await api('/api/users/delete', {username});
  toast('Пользователь удалён');
  loadUsers();
}

async function testSheets() {
  document.getElementById('test-sheets-result').textContent = 'Загрузка...';
  const r = await api('/api/test_sheets', {});
  document.getElementById('test-sheets-result').textContent = JSON.stringify(r, null, 2);
}

async function testParse() {
  const url = document.getElementById('test-parse-url').value;
  if (!url) { toast('Введите URL', true); return; }
  document.getElementById('test-parse-result').textContent = 'Загрузка...';
  const r = await api('/api/test_parse', {url});
  document.getElementById('test-parse-result').textContent = JSON.stringify(r, null, 2);
}

// Health
async function loadHealth() {
  const data = await api('/api/health');
  const maxCount = Math.max(...data.map(h => h.count_24h), 1);
  let healthy=0, warn=0, dead=0;
  document.getElementById('health-table').innerHTML = data.map(h => {
    const icon = h.status==='healthy'?'&#9989;':h.status==='low'?'&#128993;':h.status==='warning'?'&#9888;&#65039;':'&#10060;';
    if (h.status==='healthy') healthy++; else if (h.status==='dead') dead++; else warn++;
    const pct = Math.round((h.count_24h / maxCount) * 100);
    const barColor = h.status==='healthy'?'#17bf63':h.status==='low'?'#ffad1f':'#e0245e';
    return `<tr>
      <td>${icon} ${h.status}</td>
      <td>${h.source}</td>
      <td>${h.count_24h}</td>
      <td><div style="background:#22303c;border-radius:4px;overflow:hidden;height:18px"><div style="background:${barColor};width:${pct}%;height:100%;border-radius:4px;transition:width .5s"></div></div></td>
      <td>${fmtDate(h.last_parsed)}</td>
      <td>${h.minutes_ago >= 0 ? h.minutes_ago + ' мин' : '?'}</td>
    </tr>`;
  }).join('');
  const el = document.getElementById('health-summary');
  if (el) el.textContent = `${healthy} ок / ${warn} внимание / ${dead} мертв`;
}

// Generic sort for news/viral arrays
function sortNews(arr, field, dir) {
  const numFields = ['total_score','viral_score','momentum_score','headline_score','quality_score','relevance_score','sentiment_score','freshness_hours','final_score'];
  const isNum = numFields.includes(field);
  return [...arr].sort((a, b) => {
    let va = a[field], vb = b[field];
    if (isNum) { va = Number(va) || 0; vb = Number(vb) || 0; return dir === 'asc' ? va - vb : vb - va; }
    va = String(va || '').toLowerCase(); vb = String(vb || '').toLowerCase();
    if (va < vb) return dir === 'asc' ? -1 : 1;
    if (va > vb) return dir === 'asc' ? 1 : -1;
    return 0;
  });
}

// News tab sorting & filtering
let _newsSortField = 'parsed_at';
let _newsSortDir = 'desc';
let _newsFiltered = [];

function sortNewsTab(field) {
  if (_newsSortField === field) {
    _newsSortDir = _newsSortDir === 'asc' ? 'desc' : 'asc';
  } else {
    _newsSortField = field;
    _newsSortDir = 'asc';
  }
  document.querySelectorAll('#news-main-table th.sortable').forEach(th => {
    const arrow = th.querySelector('.sort-arrow');
    if (th.dataset.sort === field) {
      th.classList.add('sort-active');
      arrow.innerHTML = _newsSortDir === 'asc' ? '&#9650;' : '&#9660;';
    } else {
      th.classList.remove('sort-active');
      arrow.innerHTML = '&#9650;';
    }
  });
  renderNewsFiltered();
}

function filterNewsTable() {
  renderNewsFiltered();
}

function renderNewsFiltered() {
  const search = (document.getElementById('news-search')?.value || '').toLowerCase();
  const status = document.getElementById('filter-status')?.value || '';
  const source = document.getElementById('filter-source')?.value || '';
  const llmFilter = document.getElementById('filter-llm')?.value || '';
  const limit = parseInt(document.getElementById('filter-limit')?.value) || 100;
  let filtered = _allNews;
  if (status) filtered = filtered.filter(n => n.status === status);
  if (source) filtered = filtered.filter(n => n.source === source);
  if (search) filtered = filtered.filter(n => (n.title||'').toLowerCase().includes(search));
  if (llmFilter === 'has_rec') filtered = filtered.filter(n => n.llm_recommendation && n.llm_recommendation !== '-' && n.llm_recommendation.trim());
  else if (llmFilter === 'no_rec') filtered = filtered.filter(n => !n.llm_recommendation || n.llm_recommendation === '-' || !n.llm_recommendation.trim());
  else if (llmFilter) filtered = filtered.filter(n => (n.llm_recommendation||'').toLowerCase().includes(llmFilter));
  _newsFiltered = filtered;
  const sorted = sortNews(filtered, _newsSortField, _newsSortDir).slice(0, limit);
  const newsTb = document.getElementById('news-table');
  if (!newsTb) return;
  newsTb.innerHTML = sorted.map(n => {
    let bigrams = '';
    try { bigrams = JSON.parse(n.bigrams||'[]').map(b=>b[0]).join(', '); } catch(e){}
    const statusLabel = STATUS_LABELS[n.status] || n.status;
    // Keys.so data
    let keysoFreq = '-', keysoSimilar = 0, keysoSimilarItems = [];
    try {
      const kd = JSON.parse(n.keyso_data||'{}');
      keysoFreq = kd.freq || kd.ws || '-';
      keysoSimilarItems = kd.similar || [];
      keysoSimilar = keysoSimilarItems.length;
    } catch(e){}
    // Trends
    let trendsLabel = '-';
    try {
      const td = JSON.parse(n.trends_data||'{}');
      if (td && typeof td === 'object') {
        const vals = Object.values(td);
        if (vals.length > 0 && typeof vals[0] === 'number') trendsLabel = vals[vals.length-1];
        else if (td.interest_over_time) trendsLabel = 'есть';
        else if (vals.length > 0) trendsLabel = 'есть';
      }
    } catch(e){}
    const analyzed = n.processed_at ? fmtDate(n.processed_at) : '-';
    return `<tr>
      <td><input type="checkbox" class="news-tab-check" data-id="${n.id}" onchange="updateNewsSelectedCount()"></td>
      <td>${n.source}</td>
      <td class="td-title"><a href="${n.url}" target="_blank" title="${esc(n.description||'')}">${esc(n.title||'')}</a>${n.h1 && n.h1 !== n.title ? `<div style="font-size:0.8em;color:#1da1f2;margin-top:2px">&#127760; ${esc(n.h1)}</div>` : ''}</td>
      <td>${fmtDate(n.published_at)}</td>
      <td><span class="badge badge-${n.status}">${statusLabel}</span></td>
      <td title="${esc(bigrams)}" style="max-width:160px;font-size:0.82em">${bigrams.slice(0,50)||'-'}</td>
      <td style="font-size:0.82em">${keysoFreq}</td>
      <td class="td-tip" style="font-size:0.82em">${keysoSimilar ? renderSimilarTooltip(keysoSimilar, keysoSimilarItems) : '-'}</td>
      <td style="font-size:0.82em">${trendsLabel}</td>
      <td style="font-size:0.82em">${esc(n.llm_recommendation||'-')}</td>
      <td>${n.llm_trend_forecast||'-'}</td>
      <td style="font-size:0.82em;color:${analyzed!=='-'?'#17bf63':'#8899a6'}">${analyzed}</td>
      <td style="white-space:nowrap">
        <button class="btn btn-sm btn-primary" onclick="processOne('${n.id}')" title="Анализ API">&#9654;</button>
        <button class="btn btn-sm btn-success" onclick="exportOne('${n.id}')" title="В Google Sheets">&#9776;</button>
        <button class="btn btn-sm btn-secondary" onclick="translateTitle('${n.id}')" title="Перевод" style="padding:4px 6px">&#127760;</button>
      </td>
    </tr>`;
  }).join('');
  document.getElementById('news-count').textContent = sorted.length + ' из ' + filtered.length;
}

function getNewsSelectedIds() {
  return [...document.querySelectorAll('.news-tab-check:checked')].map(c => c.dataset.id);
}
function updateNewsSelectedCount() {
  const cnt = getNewsSelectedIds().length;
  document.getElementById('news-selected-count').textContent = cnt ? cnt + ' выбрано' : '';
}
function toggleAllNews(el) {
  document.querySelectorAll('.news-tab-check').forEach(c => c.checked = el.checked);
  updateNewsSelectedCount();
}

async function analyzeSelectedNews() {
  const ids = getNewsSelectedIds();
  if (!ids.length) { toast('Сначала выберите новости', true); return; }
  if (!confirm(`Анализировать ${ids.length} новостей? Это расходует API (Keys.so, Trends, LLM).`)) return;
  toast(`Запуск анализа ${ids.length} новостей...`);
  let ok = 0, fail = 0;
  for (const id of ids) {
    try {
      const r = await api('/api/process_one', {news_id: id});
      if (r.status === 'ok') ok++; else fail++;
    } catch(e) { fail++; }
  }
  toast(`Анализ завершён: ${ok} успешно, ${fail} ошибок`);
  loadNewsPage();
}

async function exportSelectedToSheets() {
  const ids = getNewsSelectedIds();
  if (!ids.length) { toast('Сначала выберите новости', true); return; }
  if (!confirm('Экспортировать ' + ids.length + ' новостей в Google Sheets через очередь?')) return;
  const r = await api('/api/queue/sheets', {news_ids: ids});
  if (r.status === 'ok') { toast(`${r.queued} задач добавлено в очередь Sheets`); loadQueue(); }
  else toast(r.message, true);
}



async function sendSelectedToContent() {
  const ids = getNewsSelectedIds();
  if (!ids.length) { toast('Сначала выберите новости', true); return; }
  const style = prompt('Стиль рерайта (news / seo / review / clickbait / short / social):', 'news');
  if (!style) return;
  const validStyles = ['news','seo','review','clickbait','short','social'];
  if (!validStyles.includes(style)) { toast('Неизвестный стиль: ' + style, true); return; }
  if (!confirm('Отправить ' + ids.length + ' новостей на рерайт в стиле "' + style + '"? Используется LLM API.')) return;
  toast('Отправка ' + ids.length + ' новостей в очередь рерайта...');
  const r = await api('/api/queue/rewrite', {news_ids: ids, style: style, language: 'русский'});
  if (r.status === 'ok') {
    toast(r.queued + ' задач добавлено в очередь рерайта');
    switchToTab('queue'); loadQueueStandalone();
  } else toast(r.message || 'Ошибка', true);
}

async function deleteSelectedNews() {
  const ids = getNewsSelectedIds();
  if (!ids.length) { toast('Сначала выберите новости', true); return; }
  if (!confirm('Удалить ' + ids.length + ' новостей? Это необратимо!')) return;
  const r = await api('/api/news/delete', {news_ids: ids});
  if (r.status === 'ok') toast('Удалено: ' + r.deleted);
  else toast(r.message, true);
  loadAll();
}

async function bulkStatusChange(newStatus) {
  const ids = getNewsSelectedIds();
  if (!ids.length) { toast('Сначала выберите новости', true); return; }
  if (!confirm('Изменить статус ' + ids.length + ' новостей на "' + newStatus + '"?')) return;
  const r = await api('/api/news/bulk_status', {news_ids: ids, status: newStatus});
  if (r.status === 'ok') toast('Обновлено: ' + r.updated);
  else toast(r.message, true);
  loadAll();
}

// Source type field toggles
function toggleSrcFields() {
  const t = document.getElementById('src-type').value;
  document.getElementById('src-selector-group').style.display = (t==='html')?'block':'none';
  document.getElementById('src-title-sel-group').style.display = (t==='html')?'block':'none';
  document.getElementById('src-url-filter-group').style.display = (t==='sitemap')?'block':'none';
}

// Change password
async function changePassword() {
  const username = document.getElementById('chpass-user').value;
  const password = document.getElementById('chpass-password').value;
  if (!password) { toast('Введите новый пароль', true); return; }
  const r = await api('/api/users/change_password', {username, password});
  if (r.status === 'ok') { toast('Пароль изменён'); document.getElementById('chpass-password').value = ''; }
  else toast(r.message, true);
}

// DB info
async function loadDbInfo() {
  const info = await api('/api/db_info');
  document.getElementById('db-info').innerHTML =
    `<p style="margin:6px 0">Тип: <b>${info.type}</b></p>` +
    `<p style="margin:6px 0">Всего новостей: <b>${info.total_news}</b></p>` +
    `<p style="margin:6px 0">Проанализировано: <b>${info.total_analyzed}</b></p>` +
    `<p style="margin:6px 0">Новые: ${info.status_new} | Одобрены: ${info.status_approved} | Обогащены: ${info.status_processed} | Отклонены: ${info.status_rejected}</p>` +
    `<p style="margin:6px 0;font-size:0.85em;color:#8899a6">Период: ${fmtDate(info.oldest)} — ${fmtDate(info.newest)}</p>`;
}

// Reparse all
async function reparseAll() {
  if (!confirm('Парсить все источники? Это может занять несколько минут.')) return;
  toast('Парсинг всех источников...');
  const r = await api('/api/reparse_all', {});
  if (r.status === 'ok') toast('Готово! Новых: ' + r.new_articles);
  else toast(r.message, true);
  loadAll();
}

// Setup Sheets headers
async function setupHeaders() {
  const r = await api('/api/setup_headers', {});
  if (r.status === 'ok') toast('Заголовки Sheets созданы');
  else toast(r.message, true);
}

async function exportAllProcessed() {
  if (!confirm('Экспортировать ВСЕ обработанные новости в Sheets (Лист1)? Дубликаты будут пропущены.')) return;
  toast('Запуск экспорта...');
  const r = await api('/api/export_all_processed', {});
  if (r.status === 'ok') {
    toast(r.queued + ' задач в очереди экспорта');
    _switchToQueueTab();
  } else {
    toast(r.message || 'Ошибка', true);
  }
}

async function exportReadyAll() {
  if (!confirm('Экспортировать ВСЕ переписанные статьи в Sheets (Ready)? Дубликаты будут пропущены.')) return;
  toast('Запуск экспорта в Ready...');
  const r = await api('/api/export_ready_all', {});
  if (r.status === 'ok') {
    toast(r.message || (r.queued + ' статей в очереди'));
  } else {
    toast(r.message || 'Ошибка', true);
  }
}

function _switchToQueueTab() {
  switchToTab('queue'); loadQueueStandalone();
}

// ===== CONTENT TAB: News list for rewrite =====
let _cntNews = [];
let _cntSelectedIds = new Set();
let _cntActiveList = 'articles'; // 'articles' or 'news'

function switchContentList(which) {
  _cntActiveList = which;
  document.getElementById('cnt-articles-panel').style.display = which === 'articles' ? '' : 'none';
  document.getElementById('cnt-news-panel').style.display = which === 'news' ? '' : 'none';
  // Tab styles
  const artTab = document.getElementById('cnt-tab-articles');
  const newsTab = document.getElementById('cnt-tab-news');
  if (which === 'articles') {
    artTab.style.color = '#1da1f2'; artTab.style.borderBottomColor = '#1da1f2'; artTab.style.fontWeight = '600';
    newsTab.style.color = '#8899a6'; newsTab.style.borderBottomColor = 'transparent'; newsTab.style.fontWeight = '500';
  } else {
    newsTab.style.color = '#1da1f2'; newsTab.style.borderBottomColor = '#1da1f2'; newsTab.style.fontWeight = '600';
    artTab.style.color = '#8899a6'; artTab.style.borderBottomColor = 'transparent'; artTab.style.fontWeight = '500';
    loadContentNews();
  }
}

async function loadContentNews() {
  const r = await api('/api/news?limit=100&status=approved');
  const r2 = await api('/api/news?limit=100&status=processed');
  const news1 = r.news || r || [];
  const news2 = r2.news || r2 || [];
  _cntNews = [...news1, ...news2].sort((a, b) => (b.parsed_at || '').localeCompare(a.parsed_at || ''));
  // Populate source filter
  const srcSel = document.getElementById('cnt-news-source');
  if (srcSel.options.length <= 1) {
    const sources = [...new Set(_cntNews.map(n => n.source))].sort();
    sources.forEach(s => { const o = document.createElement('option'); o.value = s; o.textContent = s; srcSel.appendChild(o); });
  }
  const cntEl = document.getElementById('cnt-news-count');
  if (cntEl) cntEl.textContent = _cntNews.length;
  filterContentNews();
}

function filterContentNews() {
  const search = (document.getElementById('cnt-news-search')?.value || '').toLowerCase();
  const source = document.getElementById('cnt-news-source')?.value || '';
  let filtered = _cntNews;
  if (search) filtered = filtered.filter(n => (n.title||'').toLowerCase().includes(search));
  if (source) filtered = filtered.filter(n => n.source === source);
  renderContentNewsList(filtered.slice(0, 60));
}

function renderContentNewsList(news) {
  const el = document.getElementById('cnt-news-list');
  if (!news.length) {
    el.innerHTML = '<div style="text-align:center;padding:40px;color:#8899a6"><div style="font-size:2em;margin-bottom:10px;opacity:0.3">&#128240;</div>Нет одобренных новостей<br><span style="font-size:0.85em">Одобрите новости в Редакции</span></div>';
    return;
  }
  el.innerHTML = news.map(n => {
    const isChecked = _cntSelectedIds.has(n.id);
    const score = n.total_score || 0;
    const scoreColor = score >= 70 ? '#17bf63' : score >= 40 ? '#ffad1f' : '#e0245e';
    const dateStr = fmtDate(n.published_at || n.parsed_at);
    const statusBadge = n.status === 'processed'
      ? '<span style="background:#794bc420;color:#794bc4;padding:1px 6px;border-radius:8px;font-size:0.8em">обогащена</span>'
      : '<span style="background:#17bf6320;color:#17bf63;padding:1px 6px;border-radius:8px;font-size:0.8em">одобрена</span>';
    return `<div class="art-card" style="padding:10px 12px;margin-bottom:6px" onclick="cntPreviewNews('${n.id}')">
      <div style="display:flex;gap:8px;align-items:start">
        <input type="checkbox" ${isChecked?'checked':''} onclick="event.stopPropagation();cntToggleNews('${n.id}',this.checked)" style="margin-top:2px;cursor:pointer;width:16px;height:16px;min-width:16px;flex-shrink:0">
        <div style="flex:1;min-width:0">
          <div style="font-size:0.88em;line-height:1.3;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden">${esc(n.title||'')}</div>
          <div style="font-size:0.72em;color:#8899a6;margin-top:3px;display:flex;align-items:center;gap:6px;flex-wrap:wrap">
            <span style="font-weight:500;color:#657786">${n.source}</span>
            <span style="color:${scoreColor};font-weight:bold">${score}</span>
            <span>${dateStr}</span>
            ${statusBadge}
          </div>
        </div>
      </div>
    </div>`;
  }).join('');
}

function cntToggleNews(id, checked) {
  if (checked) _cntSelectedIds.add(id); else _cntSelectedIds.delete(id);
  const cnt = _cntSelectedIds.size;
  document.getElementById('cnt-news-selected').textContent = cnt > 0 ? cnt + ' выбрано' : '';
  document.getElementById('cnt-rewrite-btn').disabled = cnt < 1;
}

async function cntPreviewNews(id) {
  // Show preview in right panel
  document.getElementById('art-empty').style.display = 'none';
  document.getElementById('art-edit-form').style.display = 'none';
  // Create or reuse preview div
  let preview = document.getElementById('cnt-news-preview');
  if (!preview) {
    preview = document.createElement('div');
    preview.id = 'cnt-news-preview';
    document.getElementById('art-editor-panel').appendChild(preview);
  }
  preview.style.display = 'block';
  preview.innerHTML = '<div style="text-align:center;padding:30px;color:#8899a6">Загрузка...</div>';

  const r = await api('/api/news/detail', {news_id: id});
  if (r.status !== 'ok') { toast(r.message, true); return; }
  const n = r.news;
  const a = r.analysis;

  let html = `<div style="margin-bottom:12px">`;
  html += `<div style="color:#1da1f2;font-size:1.1em;font-weight:600;line-height:1.3;margin-bottom:6px">${esc(n.title||'')}</div>`;
  html += `<div style="display:flex;gap:10px;align-items:center;font-size:0.82em;color:#8899a6;flex-wrap:wrap">`;
  html += `<span style="font-weight:500;color:#657786">${n.source}</span>`;
  html += `<span>${fmtDate(n.published_at)}</span>`;
  html += `<a href="${n.url}" target="_blank" style="color:#1da1f2">Оригинал &#8599;</a>`;
  if (a && a.total_score) html += `<span style="font-weight:bold;color:${a.total_score>=70?'#17bf63':a.total_score>=40?'#ffad1f':'#e0245e'}">Скор: ${a.total_score}</span>`;
  html += `</div></div>`;

  if (n.h1 && n.h1 !== n.title) html += `<div style="margin-bottom:6px;padding:6px 10px;background:#22303c;border-radius:6px;font-size:0.85em"><span style="color:#8899a6">H1:</span> ${esc(n.h1)}</div>`;
  if (n.description) html += `<div style="margin-bottom:6px;padding:6px 10px;background:#22303c;border-radius:6px;font-size:0.85em"><span style="color:#8899a6">Desc:</span> ${esc(n.description).slice(0,300)}</div>`;

  // Tags & scores
  if (a) {
    html += `<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:10px;font-size:0.82em">`;
    if (a.viral_score) html += `<span style="padding:3px 10px;background:#e0245e18;border:1px solid #e0245e40;border-radius:12px;color:#e0245e">Вирал: ${a.viral_score}</span>`;
    if (a.llm_recommendation) html += `<span style="padding:3px 10px;background:#22303c;border-radius:12px">${esc(a.llm_recommendation)}</span>`;
    if (a.llm_trend_forecast) html += `<span style="padding:3px 10px;background:#ffad1f20;border:1px solid #ffad1f40;border-radius:12px;color:#ffad1f">LLM: ${a.llm_trend_forecast}</span>`;
    html += `</div>`;
  }

  // Rewrite action bar
  html += `<div style="margin:12px 0;padding:12px;background:#22303c;border-radius:8px">`;
  html += `<div style="font-size:0.75em;color:#8899a6;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px">Быстрый рерайт</div>`;
  html += `<div style="display:flex;gap:6px;flex-wrap:wrap">`;
  html += `<button class="art-improve-btn" onclick="cntQuickRewrite('${n.id}','news')">&#128240; Новость</button>`;
  html += `<button class="art-improve-btn" onclick="cntQuickRewrite('${n.id}','seo')">&#128269; SEO</button>`;
  html += `<button class="art-improve-btn" onclick="cntQuickRewrite('${n.id}','review')">&#128196; Обзор</button>`;
  html += `<button class="art-improve-btn" onclick="cntQuickRewrite('${n.id}','clickbait')">&#128293; Кликбейт</button>`;
  html += `<button class="art-improve-btn" onclick="cntQuickRewrite('${n.id}','short')">&#9889; Кратко</button>`;
  html += `<button class="art-improve-btn" onclick="cntQuickRewrite('${n.id}','social')">&#128242; Соцсети</button>`;
  html += `</div>`;
  html += `<div id="cnt-rewrite-loading" style="display:none;margin-top:8px;font-size:0.85em;color:#8899a6"></div>`;
  html += `</div>`;

  // Text
  const textLen = (n.plain_text||'').length;
  html += `<div style="font-size:0.75em;color:#8899a6;margin-bottom:4px">Текст (${textLen} симв.)</div>`;
  html += `<div style="padding:12px;background:#22303c;border-radius:8px;font-size:0.85em;max-height:350px;overflow-y:auto;white-space:pre-wrap;line-height:1.55;color:#d9d9d9">${esc(n.plain_text||'Текст не загружен')}</div>`;

  preview.innerHTML = html;
}

async function cntQuickRewrite(newsId, style) {
  const loadEl = document.getElementById('cnt-rewrite-loading');
  if (loadEl) {
    loadEl.style.display = 'block';
    loadEl.innerHTML = '<span class="spinner" style="width:14px;height:14px;border:2px solid #38444d;border-top-color:#1da1f2;border-radius:50%;animation:spin .8s linear infinite;display:inline-block;vertical-align:middle"></span> Отправка в очередь...';
  }
  const r = await api('/api/queue/rewrite', {news_ids: [newsId], style: style});
  if (loadEl) {
    if (r.status === 'ok') {
      loadEl.innerHTML = '<span style="color:#17bf63">&#10003; В очереди! Статья появится в списке слева после завершения.</span>';
    } else {
      loadEl.innerHTML = '<span style="color:#e0245e">&#10007; ' + (r.message || 'Ошибка') + '</span>';
    }
  }
  if (r.status === 'ok') toast('Рерайт добавлен в очередь (' + style + ')');
}

async function cntRewriteSelected() {
  if (_cntSelectedIds.size < 1) { toast('Выберите новости', true); return; }
  const style = document.getElementById('cnt-rewrite-style').value;
  if (!confirm('Переписать ' + _cntSelectedIds.size + ' новостей в стиле "' + style + '"? Используется LLM API.')) return;
  const r = await api('/api/queue/rewrite', {news_ids: [..._cntSelectedIds], style: style, language: 'русский'});
  if (r.status === 'ok') {
    toast(r.queued + ' задач добавлено в очередь');
    _cntSelectedIds.clear();
    filterContentNews();
    document.getElementById('cnt-news-selected').textContent = '';
    document.getElementById('cnt-rewrite-btn').disabled = true;
  } else toast(r.message, true);
}

// ===== ARTICLES TAB =====
let _articles = [];
let _currentArticleId = null;

async function loadArticles() {
  const data = await api('/api/articles');
  if (Array.isArray(data)) _articles = data;
  const badge = document.getElementById('articles-badge');
  if (badge) {
    if (_articles.length) { badge.style.display = 'inline'; badge.textContent = _articles.length; }
    else badge.style.display = 'none';
  }
  filterArticles();
}

function filterArticles() {
  const search = (document.getElementById('art-search')?.value || '').toLowerCase();
  const status = document.getElementById('art-status-filter')?.value || '';
  let filtered = _articles;
  if (search) filtered = filtered.filter(a => (a.title||'').toLowerCase().includes(search));
  if (status) filtered = filtered.filter(a => a.status === status);
  renderArticlesList(filtered);
  const cnt = document.getElementById('art-count');
  if (cnt) cnt.textContent = filtered.length + ' из ' + _articles.length;
}

let _artSelectedIds = new Set();

function renderArticlesList(articles) {
  const el = document.getElementById('articles-list');
  if (!articles.length) {
    el.innerHTML = '<div style="text-align:center;padding:40px;color:#8899a6"><div style="font-size:2em;margin-bottom:10px;opacity:0.3">&#128221;</div>Нет статей<br><span style="font-size:0.85em">Создайте статью в Редакторе</span></div>';
    return;
  }
  el.innerHTML = articles.map(a => {
    const isSel = _currentArticleId === a.id;
    const isChecked = _artSelectedIds.has(a.id);
    const statusCls = 'art-status art-status-' + (a.status || 'draft');
    const date = a.updated_at ? fmtDate(a.updated_at) : '';
    const textLen = (a.text||'').length;
    return `<div class="art-card${isSel?' selected':''}" onclick="selectArticle('${a.id}')">
      <div style="display:flex;gap:8px;align-items:start">
        <input type="checkbox" ${isChecked?'checked':''} onclick="event.stopPropagation();toggleArtSelect('${a.id}',this.checked)" style="margin-top:2px;cursor:pointer;width:16px;height:16px;min-width:16px;flex-shrink:0">
        <div style="flex:1;min-width:0">
          <div style="font-size:0.92em;font-weight:500;line-height:1.3;margin-bottom:4px;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden">${esc(a.title||'Без заголовка')}</div>
          <div style="font-size:0.75em;color:#8899a6;display:flex;gap:8px;align-items:center;flex-wrap:wrap">
            <span class="${statusCls}">${{draft:'Черновик',ready:'Готово',scheduled:'Запланировано',published:'Опубликовано'}[a.status]||a.status}</span>
            <span>${a.style||''}</span>
            <span>${textLen} симв.</span>
            <span>${date}</span>
          </div>${a.scheduled_at ? '<div style="font-size:0.7em;color:#ffad1f;margin-top:2px">&#128197; ' + new Date(a.scheduled_at).toLocaleString('ru-RU') + '</div>' : ''}
        </div>
      </div>
    </div>`;
  }).join('');
}

function toggleArtSelect(id, checked) {
  if (checked) _artSelectedIds.add(id); else _artSelectedIds.delete(id);
  updateArtBulkButtons();
}

function updateArtBulkButtons() {
  const cnt = _artSelectedIds.size;
  document.getElementById('art-bulk-docx-btn').disabled = cnt < 1;
  document.getElementById('art-bulk-del-btn').disabled = cnt < 1;
  const el = document.getElementById('art-selected-count');
  el.textContent = cnt > 0 ? cnt + ' выбрано' : '';
}

async function selectArticle(id) {
  _currentArticleId = id;
  filterArticles();
  document.getElementById('art-empty').style.display = 'none';
  document.getElementById('art-edit-form').style.display = 'block';
  document.getElementById('art-ai-changes').style.display = 'none';

  const r = await api('/api/articles/detail', {id});
  if (r.status !== 'ok') { toast(r.message, true); return; }
  const a = r.article;
  document.getElementById('art-edit-title').value = a.title || '';
  document.getElementById('art-edit-seo-title').value = a.seo_title || '';
  document.getElementById('art-edit-seo-desc').value = a.seo_description || '';
  let tags = [];
  try { tags = JSON.parse(a.tags || '[]'); } catch(e){}
  document.getElementById('art-edit-tags').value = Array.isArray(tags) ? tags.join(', ') : '';
  document.getElementById('art-edit-text').value = a.text || '';
  document.getElementById('art-edit-status').value = a.status || 'draft';
  document.getElementById('art-edit-header').textContent = (a.style ? a.style + ' | ' : '') + (a.language || '');

  // Schedule info
  _updateScheduleInfo(a.scheduled_at || null);
  if (a.scheduled_at) {
    try {
      const local = new Date(a.scheduled_at);
      const pad = n => String(n).padStart(2,'0');
      const localStr = local.getFullYear()+'-'+pad(local.getMonth()+1)+'-'+pad(local.getDate())+'T'+pad(local.getHours())+':'+pad(local.getMinutes());
      document.getElementById('art-schedule-datetime').value = localStr;
    } catch(e){}
  } else {
    document.getElementById('art-schedule-datetime').value = '';
  }

  // Original
  const origBlock = document.getElementById('art-original-block');
  const origText = document.getElementById('art-original-text');
  if (a.original_text || a.original_title) {
    origBlock.style.display = 'block';
    origText.textContent = (a.original_title ? a.original_title + '\n\n' : '') + (a.original_text || '');
  } else {
    origBlock.style.display = 'none';
  }

  // Load article versions (Phase 2)
  loadArticleVersions(id);

  // Char counts
  artCharCount('art-edit-title', 100);
  artCharCount('art-edit-seo-title', 60);
  artCharCount('art-edit-seo-desc', 155);
  artCharCount('art-edit-text', 0);
}

function artCharCount(id, max) {
  const el = document.getElementById(id);
  const cnt = document.getElementById(id + '-count');
  if (!el || !cnt) return;
  const len = (el.value||'').length;
  if (max > 0) {
    cnt.textContent = len + '/' + max;
    cnt.style.color = len > max ? '#e0245e' : '#657786';
  } else {
    cnt.textContent = len + ' симв.';
  }
}

async function saveCurrentArticle() {
  if (!_currentArticleId) return;
  const tags = document.getElementById('art-edit-tags').value.split(',').map(t => t.trim()).filter(Boolean);
  const r = await api('/api/articles/update', {
    id: _currentArticleId,
    title: document.getElementById('art-edit-title').value,
    text: document.getElementById('art-edit-text').value,
    seo_title: document.getElementById('art-edit-seo-title').value,
    seo_description: document.getElementById('art-edit-seo-desc').value,
    tags: tags,
    status: document.getElementById('art-edit-status').value,
  });
  if (r.status === 'ok') { toast('Сохранено!'); loadArticles(); }
  else toast(r.message, true);
}

async function deleteCurrentArticle() {
  if (!_currentArticleId) return;
  if (!confirm('Удалить эту статью?')) return;
  const r = await api('/api/articles/delete', {id: _currentArticleId});
  if (r.status === 'ok') {
    _currentArticleId = null;
    document.getElementById('art-edit-form').style.display = 'none';
    document.getElementById('art-empty').style.display = 'block';
    toast('Удалено');
    loadArticles();
  } else toast(r.message, true);
}

function downloadArticleDocx() {
  if (!_currentArticleId) return;
  window.open('/api/articles/docx?id=' + _currentArticleId, '_blank');
}

function copyArticleText() {
  const title = document.getElementById('art-edit-title').value;
  const text = document.getElementById('art-edit-text').value;
  navigator.clipboard.writeText(title + '\n\n' + text);
  toast('Скопировано!');
}

async function scheduleArticle() {
  if (!_currentArticleId) { toast('Сначала выберите статью', true); return; }
  const dt = document.getElementById('art-schedule-datetime').value;
  if (!dt) { toast('Укажите дату и время', true); return; }
  const scheduled_at = new Date(dt).toISOString();
  const r = await api('/api/articles/schedule', {article_id: _currentArticleId, scheduled_at});
  if (r.status === 'ok') {
    toast('Публикация запланирована!');
    document.getElementById('art-edit-status').value = 'scheduled';
    _updateScheduleInfo(scheduled_at);
    loadArticles();
  } else {
    toast(r.message || 'Ошибка', true);
  }
}

async function cancelScheduleArticle() {
  if (!_currentArticleId) return;
  const r = await api('/api/articles/update', {
    id: _currentArticleId,
    title: document.getElementById('art-edit-title').value,
    text: document.getElementById('art-edit-text').value,
    seo_title: document.getElementById('art-edit-seo-title').value,
    seo_description: document.getElementById('art-edit-seo-desc').value,
    tags: document.getElementById('art-edit-tags').value.split(',').map(t=>t.trim()).filter(Boolean),
    status: 'draft',
  });
  if (r.status === 'ok') {
    toast('Планирование отменено');
    document.getElementById('art-edit-status').value = 'draft';
    _updateScheduleInfo(null);
    loadArticles();
  }
}

function _updateScheduleInfo(scheduled_at) {
  const info = document.getElementById('art-schedule-info');
  const cancelBtn = document.getElementById('art-cancel-schedule-btn');
  if (scheduled_at) {
    const d = new Date(scheduled_at);
    info.textContent = 'Запланировано: ' + d.toLocaleString('ru-RU');
    info.style.display = 'inline';
    cancelBtn.style.display = 'inline-block';
  } else {
    info.style.display = 'none';
    cancelBtn.style.display = 'none';
  }
}

async function runSeoCheck() {
  const title = document.getElementById('art-edit-title').value;
  const seo_title = document.getElementById('art-edit-seo-title').value;
  const seo_description = document.getElementById('art-edit-seo-desc').value;
  const text = document.getElementById('art-edit-text').value;
  const tagsRaw = document.getElementById('art-edit-tags').value;
  const tags = tagsRaw ? tagsRaw.split(',').map(t => t.trim()).filter(Boolean) : [];
  const badge = document.getElementById('seo-score-badge');
  const panel = document.getElementById('seo-results');
  badge.style.display = 'none';
  panel.style.display = 'none';
  panel.innerHTML = '<span style="color:#8899a6;font-size:0.85em">Анализируем...</span>';
  panel.style.display = 'block';
  try {
    const r = await api('/api/seo_check', {title, seo_title, seo_description, text, tags});
    if (r.status !== 'ok') { panel.innerHTML = '<span style="color:#e0245e">Ошибка анализа</span>'; return; }
    const score = r.score;
    const color = score >= 70 ? '#17bf63' : score >= 40 ? '#ffad1f' : '#e0245e';
    badge.textContent = score + '/100';
    badge.style.background = color + '22';
    badge.style.color = color;
    badge.style.display = 'inline-block';
    let html = '';
    for (const c of r.checks) {
      const icon = c.status === 'pass' ? '&#9989;' : c.status === 'warn' ? '&#9888;&#65039;' : '&#10060;';
      const clr = c.status === 'pass' ? '#17bf63' : c.status === 'warn' ? '#ffad1f' : '#e0245e';
      html += '<div style="display:flex;align-items:center;gap:6px;padding:4px 0;font-size:0.83em;border-bottom:1px solid #38444d22">';
      html += '<span>' + icon + '</span>';
      html += '<span style="color:#e1e8ed;min-width:140px;font-weight:500">' + c.name + '</span>';
      html += '<span style="color:' + clr + '">' + c.message + '</span>';
      html += '</div>';
    }
    panel.innerHTML = html;
  } catch(e) {
    panel.innerHTML = '<span style="color:#e0245e">Ошибка: ' + e.message + '</span>';
  }
}

async function improveArticle(action) {
  if (!_currentArticleId) return;
  // Save current edits first
  await saveCurrentArticle();
  const loadEl = document.getElementById('art-ai-loading');
  const loadText = document.getElementById('art-ai-loading-text');
  const changesEl = document.getElementById('art-ai-changes');
  const labels = {improve:'Улучшаем стиль',expand:'Расширяем',shorten:'Сокращаем',fix_grammar:'Исправляем грамматику',add_seo:'Добавляем SEO',make_engaging:'Делаем вовлекающим'};
  loadText.textContent = (labels[action]||'Обрабатываем') + '...';
  loadEl.style.display = 'block';
  changesEl.style.display = 'none';
  document.querySelectorAll('.art-improve-btn').forEach(b => b.classList.add('loading'));

  const r = await api('/api/articles/improve', {id: _currentArticleId, action});
  loadEl.style.display = 'none';
  document.querySelectorAll('.art-improve-btn').forEach(b => b.classList.remove('loading'));

  if (r.status === 'ok' && r.result) {
    document.getElementById('art-edit-title').value = r.result.title || document.getElementById('art-edit-title').value;
    document.getElementById('art-edit-text').value = r.result.text || document.getElementById('art-edit-text').value;
    if (r.result.seo_title) document.getElementById('art-edit-seo-title').value = r.result.seo_title;
    if (r.result.seo_description) document.getElementById('art-edit-seo-desc').value = r.result.seo_description;
    artCharCount('art-edit-title', 100);
    artCharCount('art-edit-seo-title', 60);
    artCharCount('art-edit-seo-desc', 155);
    artCharCount('art-edit-text', 0);
    if (r.result.changes_summary) {
      changesEl.textContent = r.result.changes_summary;
      changesEl.style.display = 'block';
    }
    toast('Текст обновлён! Не забудьте сохранить.');
  } else {
    toast(r.message || 'Ошибка', true);
  }
}

async function rewriteArticleInStyle(style) {
  if (!_currentArticleId) return;
  await saveCurrentArticle();
  const loadEl = document.getElementById('art-ai-loading');
  const loadText = document.getElementById('art-ai-loading-text');
  loadText.textContent = 'Переписываем в стиле ' + style + '...';
  loadEl.style.display = 'block';
  document.querySelectorAll('.art-improve-btn').forEach(b => b.classList.add('loading'));

  const r = await api('/api/articles/rewrite', {id: _currentArticleId, style});
  loadEl.style.display = 'none';
  document.querySelectorAll('.art-improve-btn').forEach(b => b.classList.remove('loading'));

  if (r.status === 'ok' && r.result) {
    document.getElementById('art-edit-title').value = r.result.title || '';
    document.getElementById('art-edit-text').value = r.result.text || '';
    if (r.result.seo_title) document.getElementById('art-edit-seo-title').value = r.result.seo_title;
    if (r.result.seo_description) document.getElementById('art-edit-seo-desc').value = r.result.seo_description;
    if (r.result.tags) document.getElementById('art-edit-tags').value = r.result.tags.join(', ');
    artCharCount('art-edit-title', 100);
    artCharCount('art-edit-seo-title', 60);
    artCharCount('art-edit-seo-desc', 155);
    artCharCount('art-edit-text', 0);
    toast('Переписано! Не забудьте сохранить.');
  } else {
    toast(r.message || 'Ошибка', true);
  }
}

// Bulk DOCX download (ZIP)
function downloadSelectedDocx() {
  if (_artSelectedIds.size < 1) return;
  const ids = [..._artSelectedIds].join(',');
  window.open('/api/articles/docx_bulk?ids=' + encodeURIComponent(ids), '_blank');
  toast('Скачивание ZIP...');
}

// Phase 2: Multi-output generation
async function generateMultiOutput(formats) {
  if (!_currentArticleId) { showToast('Выберите статью', 'warning'); return; }
  if (!ffEnabled('content_versions_v1')) { showToast('Включите флаг content_versions_v1', 'warning'); return; }
  showToast('Генерация форматов...', 'info');
  const r = await api('/api/articles/multi_output', {article_id: _currentArticleId, formats: formats || ['social', 'short']});
  if (r && r.outputs) {
    let msg = 'Сгенерировано: ';
    const parts = [];
    for (const [fmt, data] of Object.entries(r.outputs)) {
      parts.push(fmt);
    }
    msg += parts.join(', ');
    showToast(msg, 'success');
    // Show in a simple modal
    let html = '<div style="max-height:500px;overflow-y:auto">';
    for (const [fmt, data] of Object.entries(r.outputs)) {
      html += `<div style="background:#22303c;border-radius:8px;padding:12px;margin-bottom:12px">
        <h3 style="color:#1da1f2;margin-bottom:8px">${esc(fmt)}</h3>
        <div style="font-weight:600;margin-bottom:6px">${esc(data.title || '')}</div>
        <div style="font-size:0.9em;white-space:pre-wrap">${esc(data.text || '')}</div>
        <button class="btn btn-sm btn-secondary" style="margin-top:8px" onclick="navigator.clipboard.writeText(${JSON.stringify(data.text||'').replace(/'/g,"\\'")});showToast('Скопировано')">Копировать</button>
      </div>`;
    }
    html += '</div>';
    // Reuse explain drawer as generic side panel
    const drawer = document.getElementById('explain-drawer');
    if (drawer) {
      document.getElementById('explain-title').textContent = 'Multi-output';
      document.getElementById('explain-scores').innerHTML = html;
      document.getElementById('explain-trace').innerHTML = '';
      document.getElementById('explain-reason').innerHTML = '';
      drawer.classList.add('open');
    }
  } else {
    showToast(r?.error || 'Ошибка', 'error');
  }
}

// Phase 2: Regenerate single field
async function regenerateField(field) {
  if (!_currentArticleId) return;
  showToast('Генерация ' + field + '...', 'info');
  const r = await api('/api/articles/regenerate_field', {article_id: _currentArticleId, field});
  if (r && r.value !== undefined) {
    const fieldMap = {title: 'art-edit-title', seo_title: 'art-edit-seo-title', seo_description: 'art-edit-seo-desc'};
    const el = document.getElementById(fieldMap[field]);
    if (el) {
      el.value = typeof r.value === 'string' ? r.value : JSON.stringify(r.value);
      showToast(field + ' обновлён', 'success');
    } else if (field === 'tags' && Array.isArray(r.value)) {
      document.getElementById('art-edit-tags').value = r.value.join(', ');
      showToast('Теги обновлены', 'success');
    }
  } else {
    showToast(r?.error || 'Ошибка генерации', 'error');
  }
}

// Phase 2: Get article versions
async function loadArticleVersions(articleId) {
  if (!ffEnabled('content_versions_v1')) return;
  const r = await api('/api/articles/versions', {article_id: articleId});
  if (r && r.versions && r.versions.length > 0) {
    let html = '<div style="margin-top:12px"><h3 style="color:#1da1f2;margin-bottom:8px">История версий (' + r.versions.length + ')</h3>';
    html += r.versions.slice(0, 10).map(v => `
      <div style="background:#22303c;border-radius:6px;padding:8px;margin-bottom:4px;font-size:0.85em;display:flex;justify-content:space-between">
        <span>v${v.version} — ${v.change_type} (${v.changed_by})</span>
        <span style="color:#8899a6">${v.created_at ? new Date(v.created_at).toLocaleString('ru') : ''}</span>
      </div>
    `).join('');
    html += '</div>';
    const container = document.getElementById('art-versions-container');
    if (container) container.innerHTML = html;
  }
}

// Bulk delete articles
async function deleteSelectedArticles() {
  const cnt = _artSelectedIds.size;
  if (cnt < 1) return;
  if (!confirm(`Удалить ${cnt} статей?`)) return;
  let ok = 0;
  for (const id of _artSelectedIds) {
    const r = await api('/api/articles/delete', {id});
    if (r.status === 'ok') ok++;
  }
  _artSelectedIds.clear();
  updateArtBulkButtons();
  if (_currentArticleId && !_articles.find(a => a.id === _currentArticleId)) {
    _currentArticleId = null;
    document.getElementById('art-edit-form').style.display = 'none';
    document.getElementById('art-empty').style.display = 'block';
  }
  toast(`Удалено: ${ok}`);
  loadArticles();
}

// ---- Analytics ----
async function loadAnalytics() {
  const r = await api('/api/analytics');
  if (r.status !== 'ok') return;

  // Summary cards
  document.getElementById('analytics-summary').innerHTML = `
    <div class="card" style="text-align:center">
      <div style="font-size:2em;font-weight:700;color:#1da1f2">${r.total_news||0}</div>
      <div style="font-size:0.82em;color:#8899a6">Всего новостей</div>
      <div style="font-size:0.82em;color:#17bf63;margin-top:4px">Конверсия: ${r.approval_rate||0}%</div>
    </div>
    <div class="card" style="text-align:center">
      <div style="font-size:2em;font-weight:700;color:#17bf63">${r.total_articles||0}</div>
      <div style="font-size:0.82em;color:#8899a6">Статей</div>
      <div style="font-size:0.82em;color:#8899a6;margin-top:4px">${Object.entries(r.article_stats||{}).map(([k,v])=>k+':'+v).join(' | ')}</div>
    </div>
  `;

  // Top sources
  const maxSrc = Math.max(...(r.top_sources||[]).map(s=>s.count), 1);
  document.getElementById('analytics-top-sources').innerHTML = (r.top_sources||[]).map(s =>
    `<div style="display:flex;align-items:center;gap:8px;margin-bottom:4px">
      <span style="width:100px;font-size:0.82em;text-align:right">${s.source}</span>
      <div style="flex:1;background:#22303c;border-radius:4px;height:18px;overflow:hidden">
        <div style="width:${s.count/maxSrc*100}%;background:#1da1f2;height:100%;border-radius:4px;transition:width .3s"></div>
      </div>
      <span style="font-size:0.82em;color:#8899a6;width:40px">${s.count}</span>
    </div>`
  ).join('');

  // Daily chart
  const maxD = Math.max(...(r.daily||[]).map(d=>d.count), 1);
  document.getElementById('analytics-daily').innerHTML = (r.daily||[]).map(d => {
    const h = Math.max(4, d.count/maxD*110);
    const day = d.date ? d.date.slice(5) : '';
    return `<div style="flex:1;display:flex;flex-direction:column;align-items:center;gap:2px">
      <span style="font-size:0.7em;color:#8899a6">${d.count}</span>
      <div style="width:100%;height:${h}px;background:#1da1f2;border-radius:3px 3px 0 0"></div>
      <span style="font-size:0.65em;color:#657786">${day}</span>
    </div>`;
  }).join('');

  // Peak hours
  document.getElementById('analytics-peak-hours').innerHTML = (r.peak_hours||[]).map((h,i) =>
    `<div style="display:flex;align-items:center;gap:8px;margin-bottom:3px">
      <span style="font-size:0.85em;font-weight:${i===0?'700':'400'};color:${i===0?'#ffad1f':'#e1e8ed'}">${h.hour}:00</span>
      <span style="font-size:0.82em;color:#8899a6">${h.count} новостей</span>
    </div>`
  ).join('');

  // Bigrams
  document.getElementById('analytics-bigrams').innerHTML = (r.top_bigrams||[]).map(([term, cnt]) =>
    `<span style="padding:3px 8px;background:#1da1f218;border:1px solid #1da1f2;border-radius:12px;font-size:0.78em;color:#1da1f2">${term} <span style="color:#8899a6">${cnt}</span></span>`
  ).join('');

  // Score trend (line chart via bars with dots)
  const scoreTrend = r.score_trend || [];
  const maxSc = Math.max(...scoreTrend.map(d => d.avg_score), 1);
  document.getElementById('analytics-score-trend').innerHTML = scoreTrend.map(d => {
    const h = Math.max(4, d.avg_score / maxSc * 100);
    const day = d.date ? d.date.slice(5) : '';
    const color = d.avg_score >= 50 ? '#17bf63' : d.avg_score >= 30 ? '#ffad1f' : '#e0245e';
    return `<div style="flex:1;display:flex;flex-direction:column;align-items:center;gap:2px" title="${day}: ${d.avg_score} (${d.count} шт)">
      <span style="font-size:0.7em;color:${color};font-weight:500">${d.avg_score}</span>
      <div style="width:100%;height:${h}px;background:${color};border-radius:3px 3px 0 0;opacity:0.8"></div>
      <span style="font-size:0.65em;color:#657786">${day}</span>
    </div>`;
  }).join('');

  // Conversion daily (stacked bars)
  const convDaily = r.conversion_daily || [];
  const maxConv = Math.max(...convDaily.map(d => d.approved + d.rejected), 1);
  document.getElementById('analytics-conversion').innerHTML = convDaily.map(d => {
    const total = d.approved + d.rejected;
    const hA = Math.max(0, d.approved / maxConv * 100);
    const hR = Math.max(0, d.rejected / maxConv * 100);
    const day = d.date ? d.date.slice(5) : '';
    const pct = total > 0 ? Math.round(d.approved / total * 100) : 0;
    return `<div style="flex:1;display:flex;flex-direction:column;align-items:center;gap:0" title="${day}: ${d.approved} одобр / ${d.rejected} откл (${pct}%)">
      <span style="font-size:0.65em;color:#8899a6">${pct}%</span>
      <div style="width:100%;display:flex;flex-direction:column;gap:1px">
        <div style="width:100%;height:${hR}px;background:#e0245e;border-radius:3px 3px 0 0;opacity:0.7"></div>
        <div style="width:100%;height:${hA}px;background:#17bf63;border-radius:0 0 3px 3px;opacity:0.8"></div>
      </div>
      <span style="font-size:0.65em;color:#657786">${day}</span>
    </div>`;
  }).join('');

  // Source weights
  document.getElementById('analytics-source-weights').innerHTML = (r.source_stats||[]).map(s => {
    const rate = s.approval_rate||0;
    const rateColor = rate>=70?'#17bf63':rate>=40?'#ffad1f':'#e0245e';
    return `<div style="display:flex;align-items:center;gap:8px;margin-bottom:3px;font-size:0.82em">
      <span style="width:100px">${s.source}</span>
      <span style="color:#8899a6">${s.total} шт</span>
      <span style="color:${rateColor}">${rate}% одобр.</span>
      <span style="color:#1da1f2;font-weight:500">x${s.weight}</span>
    </div>`;
  }).join('') || '<span style="color:#8899a6;font-size:0.82em">Нет данных</span>';

  // Load prompt versions
  loadPromptVersions();
}

async function loadPromptVersions() {
  const r = await api('/api/prompt_versions');
  if (r.status !== 'ok') return;
  const el = document.getElementById('analytics-prompts');
  if (!r.versions || !r.versions.length) {
    el.innerHTML = '<span style="color:#8899a6;font-size:0.82em">Нет сохранённых версий</span>';
    return;
  }
  el.innerHTML = r.versions.map(v =>
    `<div style="display:flex;align-items:center;gap:8px;margin-bottom:3px;font-size:0.82em;padding:4px 8px;background:${v.is_active?'#17bf6318':'transparent'};border-radius:6px">
      <span style="font-weight:500">${v.prompt_name}</span>
      <span style="color:#8899a6">v${v.version}</span>
      <span style="color:#8899a6">${v.notes||''}</span>
      ${v.is_active ? '<span style="color:#17bf63">ACTIVE</span>' : `<button class="btn btn-sm" style="padding:1px 6px;font-size:0.8em" onclick="activatePromptVersion('${v.id}')">Активировать</button>`}
    </div>`
  ).join('');
}

async function saveCurrentPromptVersion() {
  const name = document.getElementById('prompt-name-select').value;
  const notes = prompt('Заметка к версии (необязательно):') || '';
  // Get current prompt content
  const prompts = await api('/api/prompts');
  const content = prompts[name] || '';
  if (!content) { toast('Промпт не найден', true); return; }
  const r = await api('/api/prompt_versions/save', {prompt_name: name, content, notes});
  if (r.status === 'ok') { toast('Версия v' + r.version + ' сохранена'); loadPromptVersions(); }
  else toast(r.message, true);
}

async function activatePromptVersion(id) {
  const r = await api('/api/prompt_versions/activate', {id});
  if (r.status === 'ok') { toast('Активировано'); loadPromptVersions(); }
  else toast(r.message, true);
}

async function generateDigest(period) {
  if (!confirm(`$ Сгенерировать дайджест за ${period === 'week' ? 'неделю' : 'день'}? Это вызов LLM API.`)) return;
  toast('Генерация дайджеста...');
  const r = await api('/api/generate_digest', {period});
  const el = document.getElementById('digest-result');
  const content = document.getElementById('digest-content');
  if (r.status === 'ok' && r.digest) {
    el.style.display = 'block';
    const d = r.digest;
    content.innerHTML = `
      <div style="font-size:1.1em;font-weight:600;color:#1da1f2;margin-bottom:8px">${d.title||'Дайджест'}</div>
      <div style="margin-bottom:10px;line-height:1.6">${d.summary||''}</div>
      ${d.top_news ? '<div style="margin-bottom:8px"><b>Главные новости:</b><ul style="margin:4px 0">' + d.top_news.map(n=>'<li>'+n+'</li>').join('') + '</ul></div>' : ''}
      ${d.trends ? '<div><b>Тренды:</b> ' + d.trends.join(', ') + '</div>' : ''}
      <div style="margin-top:8px;font-size:0.82em;color:#8899a6">Использовано ${r.news_count} новостей</div>
    `;
  } else {
    el.style.display = 'block';
    content.innerHTML = '<span style="color:#e0245e">' + (r.message||'Ошибка') + '</span>';
  }
}

// ---- Saved Digests ----
async function loadSavedDigests() {
  const r = await api('/api/digests');
  const el = document.getElementById('saved-digests-list');
  if (r.status === 'ok' && r.digests && r.digests.length > 0) {
    const styleLabels = {brief: 'Краткий', detailed: 'Подробный', telegram: 'Telegram'};
    el.innerHTML = r.digests.map(d => `
      <div class="card" style="margin-bottom:8px;padding:10px">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">
          <span style="font-weight:600;color:#1da1f2;font-size:0.95em">${d.title||'Дайджест'}</span>
          <span style="font-size:0.78em;color:#8899a6">${d.digest_date||''} &middot; ${styleLabels[d.style]||d.style} &middot; ${d.news_count||0} новостей</span>
        </div>
        <div style="white-space:pre-wrap;line-height:1.5;font-size:0.88em;color:#e1e8ed">${(d.text||'').replace(/</g,'&lt;')}</div>
        <div style="margin-top:6px;font-size:0.75em;color:#657786">${d.created_at||''}</div>
      </div>
    `).join('');
  } else {
    el.innerHTML = '<div style="text-align:center;padding:20px;color:#657786">Нет сохранённых дайджестов</div>';
  }
}

async function generateAndSaveDigest() {
  const style = document.getElementById('digest-style-select').value;
  const styleLabels = {brief: 'Краткий', detailed: 'Подробный', telegram: 'Telegram'};
  if (!confirm('Сгенерировать и сохранить дайджест (' + styleLabels[style] + ')? Это вызов LLM API.')) return;
  toast('Генерация дайджеста...');
  const r = await api('/api/digest/generate', {style});
  if (r.status === 'ok' && r.digest) {
    toast('Дайджест сохранён!');
    loadSavedDigests();
  } else {
    toast(r.message || 'Ошибка генерации', true);
  }
}

// ---- Queue ----
let _queueTasks = [];

async function loadQueue() {
  const r = await api('/api/queue');
  if (r.status === 'ok') {
    _queueTasks = r.tasks || [];
    renderQueueTable();
    updateQueueBadge();
  }
}

function updateQueueBadge() {
  const pending = _queueTasks.filter(t => t.status === 'pending' || t.status === 'processing').length;
  const badge = document.getElementById('queue-badge');
  if (!badge) return;
  if (pending > 0) { badge.textContent = pending; badge.style.display = 'inline'; }
  else { badge.style.display = 'none'; }
}

function renderQueueTable() {
  const typeF = document.getElementById('queue-filter-type').value;
  const statusF = document.getElementById('queue-filter-status').value;
  let tasks = _queueTasks;
  if (typeF) tasks = tasks.filter(t => t.task_type === typeF);
  if (statusF) tasks = tasks.filter(t => t.status === statusF);

  // Stats
  const stats = {};
  _queueTasks.forEach(t => { stats[t.status] = (stats[t.status] || 0) + 1; });
  const statLabels = {pending:'Ожидает',processing:'Обработка',running:'Работает',done:'Готово',error:'Ошибка',cancelled:'Отменено',skipped:'Пропущено'};
  const statColors = {pending:'#f5a623',processing:'#1da1f2',running:'#1da1f2',done:'#17bf63',error:'#e0245e',cancelled:'#71767b',skipped:'#8899a6'};
  const curStatusF = document.getElementById('queue-filter-status')?.value || '';
  document.getElementById('queue-stats').innerHTML = Object.entries(stats).map(([k,v]) => {
    const isActive = curStatusF === k;
    return `<span onclick="document.getElementById('queue-filter-status').value=document.getElementById('queue-filter-status').value==='${k}'?'':'${k}';renderQueueTable()" style="padding:4px 10px;background:${isActive?statColors[k]||'#38444d':(statColors[k]||'#38444d')+'22'};border:1px solid ${statColors[k]||'#38444d'};border-radius:12px;font-size:0.82em;color:${isActive?'#fff':statColors[k]||'#8899a6'};cursor:pointer">${statLabels[k]||k}: ${v}</span>`;
  }).join('');

  const typeLabels = {rewrite:'Переписка', sheets:'Sheets', full_auto:'Полный автомат', no_llm:'Без LLM', mod_rewrite:'Рерайт (мод.)'};
  const typeIcons = {rewrite:'&#9998;', sheets:'&#128196;', full_auto:'&#128640;', no_llm:'&#128203;', mod_rewrite:'&#9998;'};
  const statusIcons = {pending:'&#9203;',processing:'&#9881;',running:'&#9881;',done:'&#9989;',error:'&#10060;',cancelled:'&#128683;',skipped:'&#8594;'};

  document.getElementById('queue-table').innerHTML = tasks.map(t => {
    const canCancel = t.status === 'pending';
    const canRetry = ['error', 'cancelled', 'skipped', 'done'].includes(t.status);
    const canCheck = canCancel || canRetry;
    const timeAgo = t.created_at ? new Date(t.created_at).toLocaleString('ru') : '';
    let resultText = '';
    if (t.result) {
      try {
        const rr = JSON.parse(t.result);
        if (rr.stage) {
          const stageLabels = {scoring:'Скоринг',score_filter:'Фильтр скора',enriching:'Обогащение',final_score:'Финальный скор',rewriting:'Рерайт',exporting:'Экспорт',complete:'Готово',filtered:'Отфильтровано',init:'Инициализация'};
          resultText = (stageLabels[rr.stage] || rr.stage);
          if (rr.score !== undefined) resultText += ` (скор:${rr.score})`;
          if (rr.final_score !== undefined) resultText += ` (финал:${rr.final_score})`;
          if (rr.reason) resultText += ` — ${rr.reason}`;
          if (rr.sheet_row) resultText += ` → строка ${rr.sheet_row}`;
          if (rr.rewrite_title) resultText += ` | ${rr.rewrite_title}`;
          if (rr.error) resultText += ` ❌ ${rr.error}`;
        } else {
          resultText = rr.title || rr.row || rr.article_id || t.result;
        }
      } catch(e) { resultText = t.result; }
    }
    if (resultText.length > 60) resultText = resultText.substring(0, 60) + '...';
    return `<tr style="opacity:${t.status==='cancelled'?'0.5':'1'}">
      <td><input type="checkbox" class="queue-check" value="${t.id}" data-status="${t.status}" style="width:16px;height:16px" ${canCheck?'':'disabled'}></td>
      <td>${typeIcons[t.task_type]||''} ${typeLabels[t.task_type]||t.task_type}</td>
      <td style="max-width:300px"><div style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:300px" title="${(t.news_title||'').replace(/"/g,'&quot;')}">${t.news_title||t.news_id}</div></td>
      <td>${t.style||'—'}</td>
      <td><span style="color:${statColors[t.status]||'#8899a6'}">${statusIcons[t.status]||''} ${statLabels[t.status]||t.status}</span></td>
      <td style="max-width:200px;font-size:0.82em;color:#8899a6"><div style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:200px">${resultText}</div></td>
      <td style="font-size:0.82em;color:#8899a6;white-space:nowrap">${timeAgo}</td>
      <td style="white-space:nowrap">${canCancel ? `<button class="btn btn-sm btn-danger" onclick="cancelQueueTask('${t.id}')">Отменить</button>` : ''}${canRetry ? `<button class="btn btn-sm btn-primary" onclick="retryQueueTask('${t.id}')">Повторить</button>` : ''}</td>
    </tr>`;
  }).join('') || '<tr><td colspan="8" style="text-align:center;color:#8899a6;padding:20px">Очередь пуста</td></tr>';
  updateQueueSelectedCount();
}

function _refreshQueues() { loadQueue(); loadQueueStandalone(); }

async function cancelQueueTask(id) {
  const r = await api('/api/queue/cancel', {task_id: id});
  if (r.status === 'ok') { toast('Задача отменена'); _refreshQueues(); }
  else toast(r.message, true);
}

async function cancelAllQueue(type) {
  if (!confirm('Отменить все ожидающие задачи' + (type ? ` (${type})` : '') + '?')) return;
  const r = await api('/api/queue/cancel_all', {task_type: type});
  if (r.status === 'ok') { toast('Все ожидающие отменены'); _refreshQueues(); }
  else toast(r.message, true);
}

async function cancelSelectedQueue() {
  const checks = document.querySelectorAll('.queue-check:checked');
  if (!checks.length) { toast('Выберите задачи', true); return; }
  for (const c of checks) {
    await api('/api/queue/cancel', {task_id: c.value});
  }
  toast(`Отменено: ${checks.length}`);
  _refreshQueues();
}

async function clearDoneQueue() {
  if (!confirm('Удалить завершённые, отменённые и пропущенные из очереди?')) return;
  // We use cancel_all with a trick — but we need a dedicated endpoint. For now just reload.
  const r = await fetch('/api/queue/clear_done', {method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});
  const d = await r.json();
  if (d.status === 'ok') { toast('Очищено'); _refreshQueues(); }
  else toast(d.message||'Ошибка', true);
}

async function retryQueueTask(id) {
  const r = await api('/api/queue/retry', {task_ids: [id]});
  if (r.status === 'ok') { toast('Задача перезапущена'); _refreshQueues(); }
  else toast(r.message, true);
}

async function retrySelectedQueue() {
  const checks = [...document.querySelectorAll('.queue-check:checked')];
  if (!checks.length) { toast('Выберите задачи', true); return; }
  const ids = checks.map(c => c.value);
  const r = await api('/api/queue/retry', {task_ids: ids});
  if (r.status === 'ok') { toast(`Перезапущено: ${r.retried}`); _refreshQueues(); }
  else toast(r.message, true);
}

function toggleAllQueue(el) {
  document.querySelectorAll('.queue-check:not(:disabled)').forEach(c => c.checked = el.checked);
  updateQueueSelectedCount();
}
function updateQueueSelectedCount() {
  const cnt = document.querySelectorAll('.queue-check:checked').length;
  const el = document.getElementById('queue-selected-count');
  if (el) el.textContent = cnt > 0 ? cnt + ' выбрано' : '';
}
document.addEventListener('change', e => { if (e.target.classList.contains('queue-check')) updateQueueSelectedCount(); });

async function retryAllErrors() {
  const errorIds = _queueTasks.filter(t => t.status === 'error').map(t => t.id);
  if (!errorIds.length) { showToast('Нет задач с ошибками', 'warning'); return; }
  const r = await api('/api/queue/retry', {task_ids: errorIds});
  if (r.status === 'ok') { showToast('Перезапущено ошибок: ' + r.retried, 'success'); loadQueueStandalone(); }
  else showToast(r.message || 'Ошибка', 'error');
}

// ---- Standalone Queue tab ----
async function loadQueueStandalone() {
  const r = await api('/api/queue');
  if (r.status === 'ok') {
    const newTasks = r.tasks || [];
    // Notify on status changes to done/error
    if (Object.keys(_prevQueueStatuses).length > 0) {
      let doneCount = 0, errCount = 0;
      newTasks.forEach(t => {
        const prev = _prevQueueStatuses[t.id];
        if (prev && prev !== t.status) {
          if (t.status === 'done') doneCount++;
          else if (t.status === 'error') errCount++;
        }
      });
      if (doneCount > 0) showNotification('Очередь: готово', doneCount + ' задач завершено');
      if (errCount > 0) showNotification('Очередь: ошибка', errCount + ' задач с ошибкой');
    }
    newTasks.forEach(t => { _prevQueueStatuses[t.id] = t.status; });
    _queueTasks = newTasks;
    renderQueueStandalone();
    updateQueueBadge();
  }
}

function renderQueueStandalone() {
  const typeF = document.getElementById('q-filter-type')?.value || '';
  const statusF = document.getElementById('q-filter-status')?.value || '';
  let tasks = _queueTasks;
  if (typeF) tasks = tasks.filter(t => t.task_type === typeF);
  if (statusF) tasks = tasks.filter(t => t.status === statusF);

  // Stats — clickable
  const stats = {};
  _queueTasks.forEach(t => { stats[t.status] = (stats[t.status] || 0) + 1; });
  const statLabels = {pending:'Ожидает',processing:'Обработка',running:'Работает',done:'Готово',error:'Ошибка',cancelled:'Отменено',skipped:'Пропущено'};
  const statColors = {pending:'#f5a623',processing:'#1da1f2',running:'#1da1f2',done:'#17bf63',error:'#e0245e',cancelled:'#71767b',skipped:'#8899a6'};
  const qStatsEl = document.getElementById('q-stats');
  if (qStatsEl) {
    qStatsEl.innerHTML = Object.entries(stats).map(([k,v]) => {
      const isActive = statusF === k;
      return `<span onclick="qFilterByStatus('${k}')" style="padding:4px 12px;background:${isActive ? statColors[k]||'#38444d' : (statColors[k]||'#38444d')+'22'};border:1px solid ${statColors[k]||'#38444d'};border-radius:12px;font-size:0.85em;color:${isActive?'#fff':statColors[k]||'#8899a6'};cursor:pointer;user-select:none;transition:all .15s">${statLabels[k]||k}: ${v}</span>`;
    }).join('');
  }

  const typeLabels = {rewrite:'Переписка', sheets:'Sheets', full_auto:'Полный автомат', no_llm:'Без LLM', mod_rewrite:'Рерайт (мод.)'};
  const typeIcons = {rewrite:'&#9998;', sheets:'&#128196;', full_auto:'&#128640;', no_llm:'&#128203;', mod_rewrite:'&#9998;'};
  const statusIcons = {pending:'&#9203;',processing:'&#9881;',running:'&#9881;',done:'&#9989;',error:'&#10060;',cancelled:'&#128683;',skipped:'&#8594;'};

  const tb = document.getElementById('q-table');
  if (!tb) return;
  tb.innerHTML = tasks.map(t => {
    const canCancel = t.status === 'pending';
    const canRetry = ['error', 'cancelled', 'skipped', 'done'].includes(t.status);
    const canCheck = canCancel || canRetry;
    const timeAgo = t.created_at ? new Date(t.created_at).toLocaleString('ru') : '';
    let resultText = '';
    if (t.result) {
      try {
        const rr = JSON.parse(t.result);
        if (rr.stage) {
          const stageLabels = {scoring:'Скоринг',score_filter:'Фильтр скора',enriching:'Обогащение',final_score:'Финальный скор',rewriting:'Рерайт',exporting:'Экспорт',complete:'Готово',filtered:'Отфильтровано',init:'Инициализация'};
          resultText = (stageLabels[rr.stage] || rr.stage);
          if (rr.score !== undefined) resultText += ` (скор:${rr.score})`;
          if (rr.final_score !== undefined) resultText += ` (финал:${rr.final_score})`;
          if (rr.reason) resultText += ` — ${rr.reason}`;
          if (rr.sheet_row) resultText += ` → строка ${rr.sheet_row}`;
          if (rr.rewrite_title) resultText += ` | ${rr.rewrite_title}`;
          if (rr.error) resultText += ` ❌ ${rr.error}`;
        } else {
          resultText = rr.title || rr.row || rr.article_id || t.result;
        }
      } catch(e) { resultText = t.result; }
    }
    if (resultText.length > 60) resultText = resultText.substring(0, 60) + '...';
    return `<tr style="opacity:${t.status==='cancelled'?'0.5':'1'}">
      <td><input type="checkbox" class="queue-check" value="${t.id}" data-status="${t.status}" style="width:16px;height:16px" ${canCheck?'':'disabled'}></td>
      <td>${typeIcons[t.task_type]||''} ${typeLabels[t.task_type]||t.task_type}</td>
      <td style="max-width:300px"><div style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:300px" title="${(t.news_title||'').replace(/"/g,'&quot;')}">${t.news_title||t.news_id}</div></td>
      <td>${t.style||'—'}</td>
      <td><span style="color:${statColors[t.status]||'#8899a6'}">${statusIcons[t.status]||''} ${statLabels[t.status]||t.status}</span></td>
      <td style="max-width:200px;font-size:0.82em;color:#8899a6"><div style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:200px">${resultText}</div></td>
      <td style="font-size:0.82em;color:#8899a6;white-space:nowrap">${timeAgo}</td>
      <td style="white-space:nowrap">${canCancel ? `<button class="btn btn-sm btn-danger" onclick="cancelQueueTask('${t.id}')">Отменить</button>` : ''}${canRetry ? `<button class="btn btn-sm btn-primary" onclick="retryQueueTask('${t.id}')">Повторить</button>` : ''}</td>
    </tr>`;
  }).join('') || '<tr><td colspan="8" style="text-align:center;color:#8899a6;padding:20px">Очередь пуста</td></tr>';

  const cnt = document.querySelectorAll('#q-table .queue-check:checked').length;
  const cntEl = document.getElementById('q-selected-count');
  if (cntEl) cntEl.textContent = cnt > 0 ? cnt + ' выбрано' : '';

  // Show/hide retry all errors button
  const retryErrBtn = document.getElementById('q-retry-errors-btn');
  if (retryErrBtn) retryErrBtn.style.display = (stats.error || 0) > 0 ? '' : 'none';
}

function qFilterByStatus(status) {
  const sel = document.getElementById('q-filter-status');
  if (sel) {
    sel.value = sel.value === status ? '' : status;
    renderQueueStandalone();
  }
}

function qToggleAll(el) {
  document.querySelectorAll('#q-table .queue-check:not(:disabled)').forEach(c => c.checked = el.checked);
  const cnt = document.querySelectorAll('#q-table .queue-check:checked').length;
  const cntEl = document.getElementById('q-selected-count');
  if (cntEl) cntEl.textContent = cnt > 0 ? cnt + ' выбрано' : '';
}

// Date range quick filters (News tab)
function setNewsDateRange(range) {
  const fromEl = document.getElementById('news-date-from');
  const toEl = document.getElementById('news-date-to');
  if (!range) { fromEl.value = ''; toEl.value = ''; loadNewsPage(0); return; }
  const now = new Date();
  const fmt = d => d.toISOString().slice(0,10);
  toEl.value = fmt(now);
  if (range === 'today') fromEl.value = fmt(now);
  else if (range === 'yesterday') { const y = new Date(now); y.setDate(y.getDate()-1); fromEl.value = fmt(y); toEl.value = fmt(y); }
  else if (range === 'week') { const w = new Date(now); w.setDate(w.getDate()-7); fromEl.value = fmt(w); }
  else if (range === 'month') { const m = new Date(now); m.setMonth(m.getMonth()-1); fromEl.value = fmt(m); }
  loadNewsPage(0);
}

// Logs
async function loadLogs() {
  const level = document.getElementById('log-level')?.value || '';
  const resp = await api(`/api/logs?limit=200&level=${level}`);
  const logs = resp.logs || [];
  const tb = document.getElementById('logs-table');
  if (!tb) return;
  const levelColors = {ERROR:'#e0245e', WARNING:'#ffad1f', INFO:'#17bf63', DEBUG:'#8899a6'};
  tb.innerHTML = logs.reverse().map(l => `<tr>
    <td style="font-size:0.8em;color:#8899a6;white-space:nowrap">${(l.time||'').replace('T',' ').slice(0,19)}</td>
    <td style="font-size:0.78em;font-weight:600;color:${levelColors[l.level]||'#8899a6'}">${l.level}</td>
    <td style="font-size:0.78em;color:#8899a6">${esc(l.logger||'')}</td>
    <td style="font-size:0.82em;white-space:pre-wrap;word-break:break-word">${esc(l.message||'')}</td>
  </tr>`).join('');

  // API stats
  const rates = await api('/api/rate_stats');
  const cache = await api('/api/cache_stats');
  const statsEl = document.getElementById('api-stats');
  if (statsEl) {
    let html = '';
    for (const [svc, data] of Object.entries(rates)) {
      const pct = data.limit > 0 ? Math.round(data.used / data.limit * 100) : 0;
      const color = pct > 80 ? '#e0245e' : pct > 50 ? '#ffad1f' : '#17bf63';
      html += `<div style="background:#192734;border-radius:8px;padding:10px 14px;min-width:120px">
        <div style="font-size:0.78em;color:#8899a6;text-transform:uppercase">${svc}</div>
        <div style="font-size:1.3em;font-weight:bold;color:${color}">${data.used}<span style="font-size:0.5em;color:#8899a6">/${data.limit}</span></div>
        <div style="font-size:0.75em;color:#8899a6">осталось: ${data.remaining}</div>
      </div>`;
    }
    html += `<div style="background:#192734;border-radius:8px;padding:10px 14px;min-width:120px">
      <div style="font-size:0.78em;color:#8899a6;text-transform:uppercase">Cache</div>
      <div style="font-size:1.3em;font-weight:bold;color:#1da1f2">${cache.alive||0}<span style="font-size:0.5em;color:#8899a6">/${cache.max||0}</span></div>
      <div style="font-size:0.75em"><button class="btn btn-sm btn-secondary" onclick="clearApiCache()" style="padding:2px 8px;font-size:0.85em">Очистить</button></div>
    </div>`;
    statsEl.innerHTML = html;
  }
}

async function clearApiCache() {
  await api('/api/cache/clear', {});
  toast('Кэш очищен');
  loadLogs();
}

// ─── Viral Triggers Management ───
let _vtData = [];
let _vtSortField = 'weight';
let _vtSortDir = 'desc';

async function loadViralTriggers() {
  const r = await (await fetch('/api/viral_triggers')).json();
  _vtData = r.triggers || [];
  renderViralTriggers();
  document.getElementById('vt-stats').textContent = `Всего триггеров: ${_vtData.length}, активных: ${_vtData.filter(t=>t.is_active).length}`;
}

function filterViralTriggers() {
  renderViralTriggers();
}

function sortViralTriggers(field) {
  if (_vtSortField === field) _vtSortDir = _vtSortDir === 'asc' ? 'desc' : 'asc';
  else { _vtSortField = field; _vtSortDir = field === 'label' ? 'asc' : 'desc'; }
  renderViralTriggers();
}

function renderViralTriggers() {
  const search = (document.getElementById('vt-search').value || '').toLowerCase();
  let data = _vtData;
  if (search) data = data.filter(t => t.label.toLowerCase().includes(search) || t.id.toLowerCase().includes(search) || t.keywords.some(k => k.includes(search)));
  data.sort((a,b) => {
    let va = a[_vtSortField], vb = b[_vtSortField];
    if (typeof va === 'string') { va = va.toLowerCase(); vb = (vb||'').toLowerCase(); }
    if (va < vb) return _vtSortDir === 'asc' ? -1 : 1;
    if (va > vb) return _vtSortDir === 'asc' ? 1 : -1;
    return 0;
  });

  const tbody = document.getElementById('vt-table');
  tbody.innerHTML = data.map(t => {
    const wColor = t.weight >= 50 ? '#e0245e' : t.weight >= 30 ? '#ffad1f' : '#1da1f2';
    const typeLabel = t.is_custom ? '<span style="color:#794bc4">Кастомный</span>' : (t.modified ? '<span style="color:#ffad1f">Изменён</span>' : '<span style="color:#657786">Стандартный</span>');
    const kwShort = t.keywords.slice(0,5).join(', ') + (t.keywords.length > 5 ? ` +${t.keywords.length-5}` : '');
    const opacity = t.is_active ? '1' : '0.4';
    return `<tr style="opacity:${opacity}">
      <td><input type="checkbox" ${t.is_active?'checked':''} onchange="toggleViralTrigger('${t.id}',this.checked)" title="Вкл/выкл" style="width:16px;height:16px"></td>
      <td><b style="color:#d9d9d9">${t.label}</b><br><span style="color:#657786;font-size:0.75em">${t.id}</span></td>
      <td style="text-align:center"><span style="color:${wColor};font-weight:bold;font-size:1.1em">${t.weight}</span></td>
      <td style="font-size:0.82em;color:#8899a6;max-width:300px"><div style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:300px" title="${t.keywords.join(', ')}">${kwShort}</div></td>
      <td style="font-size:0.82em">${typeLabel}</td>
      <td style="white-space:nowrap">
        <button class="btn btn-sm btn-secondary" onclick="editViralTrigger('${t.id}')">&#9998;</button>
        ${t.is_custom ? `<button class="btn btn-sm btn-danger" onclick="deleteViralTrigger('${t.id}')">&#10005;</button>` : (t.modified ? `<button class="btn btn-sm btn-secondary" onclick="resetViralTrigger('${t.id}')" title="Сбросить">&#8634;</button>` : '')}
      </td>
    </tr>`;
  }).join('') || '<tr><td colspan="6" style="text-align:center;color:#8899a6;padding:20px">Нет триггеров</td></tr>';
}

function showAddViralTrigger() {
  document.getElementById('vt-add-form').style.display = 'block';
  document.getElementById('vt-new-id').value = '';
  document.getElementById('vt-new-label').value = '';
  document.getElementById('vt-new-weight').value = '30';
  document.getElementById('vt-new-keywords').value = '';
  document.getElementById('vt-new-id').readOnly = false;
  document.getElementById('vt-add-form').dataset.mode = 'add';
}

function editViralTrigger(id) {
  const t = _vtData.find(x => x.id === id);
  if (!t) return;
  document.getElementById('vt-add-form').style.display = 'block';
  document.getElementById('vt-new-id').value = t.id;
  document.getElementById('vt-new-label').value = t.label;
  document.getElementById('vt-new-weight').value = t.weight;
  document.getElementById('vt-new-keywords').value = t.keywords.join(', ');
  document.getElementById('vt-new-id').readOnly = true;
  document.getElementById('vt-add-form').dataset.mode = 'edit';
}

async function saveNewViralTrigger() {
  const mode = document.getElementById('vt-add-form').dataset.mode || 'add';
  const id = document.getElementById('vt-new-id').value.trim();
  const label = document.getElementById('vt-new-label').value.trim();
  const weight = parseInt(document.getElementById('vt-new-weight').value) || 0;
  const kwText = document.getElementById('vt-new-keywords').value;
  const keywords = kwText.split(',').map(k => k.trim().toLowerCase()).filter(k => k);
  if (!id || !label) { toast('Заполните ID и название', true); return; }
  if (!keywords.length) { toast('Добавьте хотя бы одно ключевое слово', true); return; }
  const isCustom = mode === 'add' && !_vtData.find(x => x.id === id && !x.is_custom);
  const r = await api('/api/viral_triggers/save', {trigger_id: id, label, weight, keywords, is_active: true, is_custom: isCustom});
  if (r.status === 'ok') {
    toast('Триггер сохранён');
    document.getElementById('vt-add-form').style.display = 'none';
    loadViralTriggers();
  } else toast(r.message, true);
}

async function toggleViralTrigger(id, active) {
  const t = _vtData.find(x => x.id === id);
  if (!t) return;
  await api('/api/viral_triggers/save', {trigger_id: id, label: t.label, weight: t.weight, keywords: t.keywords, is_active: active, is_custom: t.is_custom});
  loadViralTriggers();
}

async function deleteViralTrigger(id) {
  if (!confirm(`Удалить кастомный триггер "${id}"?`)) return;
  const r = await api('/api/viral_triggers/delete', {trigger_id: id});
  if (r.status === 'ok') { toast('Триггер удалён'); loadViralTriggers(); }
  else toast(r.message, true);
}

async function resetViralTrigger(id) {
  if (!confirm(`Сбросить триггер "${id}" к стандартным значениям?`)) return;
  const r = await api('/api/viral_triggers/delete', {trigger_id: id});
  if (r.status === 'ok') { toast('Сброшено к стандартным'); loadViralTriggers(); }
  else toast(r.message, true);
}

// Translate title
async function translateTitle(newsId) {
  toast('Перевод...');
  const r = await api('/api/translate_title', {news_id: newsId});
  if (r.status === 'ok') {
    if (r.is_russian) { toast('Заголовок уже на русском'); return; }
    toast(`Переведено с ${r.source_lang}: ${r.translated}`);
    // Update title in the current row visually
    const row = document.querySelector(`tr[data-id="${newsId}"] td:nth-child(2) a`) || document.querySelector(`tr[data-id="${newsId}"] td:nth-child(2)`);
    if (row && r.translated) row.textContent = r.translated;
    loadEditorial();
  } else toast(r.message, true);
}

// AI recommendation
async function aiRecommend(newsId) {
  toast('AI анализирует...');
  const r = await api('/api/ai_recommend', {news_id: newsId});
  if (r.status === 'ok') {
    const rec = r.recommendation;
    const verdictColors = {publish:'#17bf63', rewrite:'#ffad1f', skip:'#e0245e'};
    const verdictLabels = {publish:'Публиковать', rewrite:'Переписать', skip:'Пропустить'};
    const html = `<div style="padding:15px;background:#192734;border-radius:10px;border-left:4px solid ${verdictColors[rec.verdict]||'#8899a6'}">
      <div style="font-size:1.1em;font-weight:bold;color:${verdictColors[rec.verdict]||'#8899a6'};margin-bottom:6px">${verdictLabels[rec.verdict]||rec.verdict} <span style="font-size:0.7em;color:#8899a6">(${Math.round((rec.confidence||0)*100)}%)</span></div>
      <div style="margin-bottom:6px">${esc(rec.reason||'')}</div>
      ${rec.suggested_angle ? `<div style="color:#1da1f2;font-size:0.9em">Ракурс: ${esc(rec.suggested_angle)}</div>` : ''}
      <div style="margin-top:6px;font-size:0.8em;color:#8899a6">Приоритет: ${rec.priority || '-'}</div>
    </div>`;
    // Show in a toast-like popup
    let popup = document.getElementById('ai-rec-popup');
    if (!popup) { popup = document.createElement('div'); popup.id = 'ai-rec-popup'; popup.style.cssText = 'position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);z-index:200;max-width:460px;width:90vw;box-shadow:0 12px 40px rgba(0,0,0,0.6);border-radius:12px;background:#192734;padding:20px'; document.body.appendChild(popup); }
    popup.innerHTML = `<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px"><h3 style="color:#1da1f2;margin:0;font-size:1em">AI Рекомендация</h3><button onclick="this.parentElement.parentElement.remove()" style="background:none;border:none;color:#8899a6;font-size:1.3em;cursor:pointer">&times;</button></div>${html}`;
  } else toast(r.message, true);
}

// Init
function loadAll() { loadEditorial(); }

// ===== VIRAL TAB =====
let _viralData = [];
let _viralSortField = 'viral_score';
let _viralSortDir = 'desc';
let _viralPicks = new Set(); // IDs sent to editor from viral tab

async function loadViral() {
  const level = document.getElementById('viral-level')?.value || '';
  const category = document.getElementById('viral-category')?.value || '';
  const sentiment = document.getElementById('viral-sentiment')?.value || '';
  const source = document.getElementById('viral-source')?.value || '';
  const minScore = document.getElementById('viral-min-score')?.value || '0';
  const dateFrom = document.getElementById('viral-date-from')?.value || '';
  const dateTo = document.getElementById('viral-date-to')?.value || '';
  let url = '/api/viral?limit=200';
  if (level) url += '&level=' + level;
  if (category) url += '&category=' + encodeURIComponent(category);
  if (sentiment) url += '&sentiment=' + sentiment;
  if (source) url += '&source=' + encodeURIComponent(source);
  if (parseInt(minScore) > 0) url += '&min_score=' + minScore;
  if (dateFrom) url += '&date_from=' + dateFrom;
  if (dateTo) url += '&date_to=' + dateTo;

  const r = await api(url);
  _viralData = r.items || [];

  // Stat cards
  const s = r.stats || {};
  const statItems = [
    {num: s.total||0, lbl: 'Всего', cls: ''},
    {num: s.high||0, lbl: 'High', cls: 'high', color: '#e0245e'},
    {num: s.medium||0, lbl: 'Medium', cls: 'med', color: '#ffad1f'},
    {num: s.low||0, lbl: 'Low', cls: 'low', color: '#1da1f2'},
    {num: s.none||0, lbl: 'None', cls: 'none', color: '#38444d'},
  ];
  document.getElementById('viral-stats').innerHTML = statItems.map(i =>
    `<div class="stat" style="${i.color ? 'border-bottom:3px solid '+i.color : ''}" title="Нажмите для фильтрации" onclick="document.getElementById('viral-level').value='${i.cls==='high'?'high':i.cls==='med'?'medium':i.cls==='low'?'low':i.cls==='none'?'none':''}';loadViral()">
      <div class="num">${i.num}</div><div class="lbl">${i.lbl}</div>
    </div>`
  ).join('');

  // Calendar banner
  const calEl = document.getElementById('viral-calendar');
  if (r.calendar && r.calendar.event) {
    calEl.innerHTML = '<span style="font-size:1.1em;margin-right:8px">&#128197;</span> <b>' + esc(r.calendar.event) + '</b> <span style="color:#8899a6;margin-left:8px">(+' + r.calendar.boost + ' ко всем скорам)</span>';
    calEl.style.display = 'block';
  } else {
    calEl.style.display = 'none';
  }

  // Populate source filter
  const srcSel = document.getElementById('viral-source');
  if (srcSel && srcSel.options.length <= 1 && r.source_avg) {
    r.source_avg.forEach(s => { const o = document.createElement('option'); o.value = s.source; o.textContent = s.source; srcSel.appendChild(o); });
  }

  // Top triggers bar chart
  const tt = r.top_triggers || [];
  const maxTrig = tt.length ? tt[0][1] : 1;
  document.getElementById('viral-top-triggers').innerHTML = tt.slice(0,12).map(([label, count]) =>
    `<div style="display:flex;align-items:center;gap:8px;margin-bottom:4px">
      <span style="min-width:140px;font-size:0.82em;color:#8899a6;text-align:right">${esc(label)}</span>
      <div style="flex:1;background:#192734;border-radius:4px;height:18px;overflow:hidden">
        <div style="width:${(count/maxTrig*100).toFixed(1)}%;height:100%;background:linear-gradient(90deg,#e0245e,#ffad1f);border-radius:4px;transition:width .3s"></div>
      </div>
      <span style="font-size:0.82em;color:#e1e8ed;min-width:24px">${count}</span>
    </div>`
  ).join('') || '<span style="color:#38444d">Нет триггеров</span>';

  // Categories chart
  const cats = r.top_categories || [];
  const maxCat = cats.length ? cats[0][1] : 1;
  const catColors = {'Скандалы':'#e0245e','Утечки':'#794bc4','Shadow Drops':'#17bf63','Плохие релизы':'#ff6300','AI':'#1da1f2','Ивенты':'#ffad1f','Деньги':'#00bcd4','Культура':'#e8598b','Персоны':'#8bc34a','Скорость':'#ff9800','Базовые':'#38444d','Прочее':'#455a64'};
  document.getElementById('viral-categories').innerHTML = cats.map(([cat, count]) =>
    `<div style="display:flex;align-items:center;gap:8px;margin-bottom:4px">
      <span style="min-width:120px;font-size:0.82em;color:${catColors[cat]||'#8899a6'};text-align:right;font-weight:500">${esc(cat)}</span>
      <div style="flex:1;background:#192734;border-radius:4px;height:18px;overflow:hidden">
        <div style="width:${(count/maxCat*100).toFixed(1)}%;height:100%;background:${catColors[cat]||'#38444d'};border-radius:4px;opacity:0.7"></div>
      </div>
      <span style="font-size:0.82em;color:#e1e8ed;min-width:24px">${count}</span>
    </div>`
  ).join('') || '<span style="color:#38444d">Нет данных</span>';

  // Sentiment bar
  const sent = r.sentiment || {};
  const sentTotal = (sent.positive||0) + (sent.neutral||0) + (sent.negative||0) || 1;
  const pPct = ((sent.positive||0)/sentTotal*100).toFixed(1);
  const nPct = ((sent.neutral||0)/sentTotal*100).toFixed(1);
  const negPct = ((sent.negative||0)/sentTotal*100).toFixed(1);
  document.getElementById('viral-sentiment-chart').innerHTML =
    `<div style="flex:1;display:flex;height:24px;border-radius:6px;overflow:hidden">
      <div style="width:${pPct}%;background:#17bf63" title="Позитив: ${sent.positive||0} (${pPct}%)"></div>
      <div style="width:${nPct}%;background:#8899a6" title="Нейтрал: ${sent.neutral||0} (${nPct}%)"></div>
      <div style="width:${negPct}%;background:#e0245e" title="Негатив: ${sent.negative||0} (${negPct}%)"></div>
    </div>
    <div style="font-size:0.8em;color:#8899a6;min-width:200px;text-align:right">
      <span style="color:#17bf63">&#9679; ${sent.positive||0}</span>
      <span style="margin:0 6px">&#9679; ${sent.neutral||0}</span>
      <span style="color:#e0245e">&#9679; ${sent.negative||0}</span>
    </div>`;

  // Source avg scores
  const srcAvg = r.source_avg || [];
  const maxAvg = srcAvg.length ? srcAvg[0].avg : 1;
  document.getElementById('viral-source-avg').innerHTML = srcAvg.slice(0,10).map(s =>
    `<div style="display:flex;align-items:center;gap:8px;margin-bottom:3px">
      <span style="min-width:100px;font-size:0.82em;color:#8899a6;text-align:right;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(s.source)}</span>
      <div style="flex:1;background:#192734;border-radius:4px;height:16px;overflow:hidden">
        <div style="width:${(s.avg/100*100).toFixed(1)}%;height:100%;background:${s.avg>=50?'#e0245e':s.avg>=25?'#ffad1f':'#1da1f2'};border-radius:4px;opacity:0.7"></div>
      </div>
      <span style="font-size:0.82em;color:#e1e8ed;min-width:50px">${s.avg} (${s.count})</span>
    </div>`
  ).join('') || '<span style="color:#38444d">Нет данных</span>';

  renderViralTable();
}

function renderViralTable() {
  const items = sortNews(_viralData, _viralSortField, _viralSortDir);
  const tb = document.getElementById('viral-table');
  const emptyEl = document.getElementById('viral-empty');
  const countEl = document.getElementById('viral-count');

  if (!items.length) {
    tb.innerHTML = '';
    emptyEl.style.display = 'block';
    countEl.textContent = '';
    return;
  }
  emptyEl.style.display = 'none';
  countEl.textContent = items.length + ' новостей';

  const levelColors = {high:'#e0245e',medium:'#ffad1f',low:'#1da1f2',none:'#38444d'};
  const levelLabels = {high:'HIGH',medium:'MED',low:'LOW',none:'-'};
  const sentIcons = {positive:'&#9650;',negative:'&#9660;',neutral:'&#9679;'};
  const sentColors = {positive:'#17bf63',negative:'#e0245e',neutral:'#8899a6'};

  tb.innerHTML = items.map(n => {
    const triggers = (n.triggers||[]).map(t =>
      `<span style="display:inline-block;background:#192734;border:1px solid ${levelColors[n.viral_level]||'#38444d'}33;border-radius:4px;padding:1px 5px;font-size:0.75em;margin:1px;color:${levelColors[n.viral_level]||'#8899a6'}" title="Вес: ${t.weight}">${esc(t.label)}</span>`
    ).join('');
    const tags = (n.tags||[]).map(t =>
      `<span class="tag tag-${t.id}">${t.label}</span>`
    ).join('');
    const statusLabel = STATUS_LABELS[n.status] || n.status;
    const isPick = _viralPicks.has(n.id);
    const rowBg = isPick ? 'background:rgba(100,200,255,0.06)' : '';
    const pickBtn = isPick
      ? `<button class="btn btn-sm" style="background:#64c8ff33;color:#64c8ff;border:none;padding:3px 6px;font-size:0.8em" disabled title="Уже в редакторе">&#10003;</button>`
      : `<button class="btn btn-sm" style="background:#64c8ff22;color:#64c8ff;border:1px solid #64c8ff44;padding:3px 6px;font-size:0.8em" onclick="sendOneToEditor('${n.id}')" title="В редактор">&#9998;</button>`;
    return `<tr style="${rowBg}">
      <td style="text-align:center"><span style="font-weight:700;font-size:1.1em;color:${levelColors[n.viral_level]||'#8899a6'}">${n.viral_score}</span></td>
      <td><span style="padding:2px 8px;border-radius:4px;font-size:0.8em;font-weight:600;background:${levelColors[n.viral_level]||'#38444d'}22;color:${levelColors[n.viral_level]||'#8899a6'}">${levelLabels[n.viral_level]||'-'}</span></td>
      <td style="text-align:center"><span style="color:${sentColors[n.sentiment]||'#8899a6'};font-size:1.1em" title="${n.sentiment} (${n.sentiment_score})">${sentIcons[n.sentiment]||''}</span></td>
      <td style="font-size:0.85em">${esc(n.source)}</td>
      <td class="td-title"><a href="${n.url}" target="_blank">${esc(n.title||'')}</a>${isPick ? '<span class="viral-pick-badge">VIRAL</span>' : ''}</td>
      <td style="max-width:250px">${triggers || '<span style="color:#38444d">-</span>'}</td>
      <td>${tags || '-'}</td>
      <td style="font-size:0.82em;white-space:nowrap">${fmtDate(n.parsed_at)}</td>
      <td><span class="badge badge-${n.status}">${statusLabel}</span></td>
      <td>${pickBtn}</td>
    </tr>`;
  }).join('');
}

function sendOneToEditor(id) {
  _viralPicks.add(id);
  updateViralPicksCount();
  renderViralTable();
  switchToTab('editor');
  loadArticles();
  toast('Отправлено в контент');
}

function sendViralToEditor(level) {
  let items;
  if (level === 'all') items = _viralData;
  else if (level === 'high') items = _viralData.filter(n => n.viral_level === 'high');
  else if (level === 'medium') items = _viralData.filter(n => n.viral_level === 'medium' || n.viral_level === 'high');
  else items = _viralData.filter(n => n.viral_level === level);

  if (!items.length) { toast('Нет подходящих новостей', true); return; }
  items.forEach(n => _viralPicks.add(n.id));
  updateViralPicksCount();
  renderViralTable();
  switchToTab('editor');
  loadArticles();
  toast('Отправлено ' + items.length + ' новостей в контент');
}

function updateViralPicksCount() {
  const el = document.getElementById('viral-picks-count');
  if (!el) return;
  if (_viralPicks.size > 0) {
    el.textContent = _viralPicks.size + ' в редакторе';
    el.style.display = 'inline';
  } else {
    el.style.display = 'none';
  }
}

function sortViralTab(field) {
  if (_viralSortField === field) {
    _viralSortDir = _viralSortDir === 'asc' ? 'desc' : 'asc';
  } else {
    _viralSortField = field;
    _viralSortDir = field === 'viral_score' ? 'desc' : 'asc';
  }
  // Update arrows
  document.querySelectorAll('#panel-viral .sortable').forEach(th => {
    const arrow = th.querySelector('.sort-arrow');
    if (th.dataset.sort === _viralSortField) {
      th.classList.add('sort-active');
      arrow.innerHTML = _viralSortDir === 'asc' ? '&#9650;' : '&#9660;';
    } else {
      th.classList.remove('sort-active');
      arrow.innerHTML = '&#9650;';
    }
  });
  renderViralTable();
}

// === FINAL TAB ===
let _finData = [];
let _finSortField = 'final_score';
let _finSortDir = 'desc';
let _finTotal = 0;
let _finPage = 0;
const _finLimit = 50;

function calcFinalScore(n) {
  // Финальный скор: internal(40%) + viral(20%) + keyso_bonus(15%) + trends_bonus(10%) + headline(15%)
  const internal = Number(n.total_score) || 0;
  const viral = Number(n.viral_score) || 0;
  const headline = Number(n.headline_score) || 0;
  let keysoBonus = 0;
  try {
    const kd = JSON.parse(n.keyso_data || '{}');
    const freq = Number(kd.freq || kd.ws) || 0;
    if (freq >= 10000) keysoBonus = 100;
    else if (freq >= 5000) keysoBonus = 80;
    else if (freq >= 1000) keysoBonus = 60;
    else if (freq >= 100) keysoBonus = 40;
    else if (freq > 0) keysoBonus = 20;
  } catch(e){}
  let trendsBonus = 0;
  try {
    const td = JSON.parse(n.trends_data || '{}');
    const maxT = Math.max(...Object.values(td).map(Number).filter(v => !isNaN(v)), 0);
    if (maxT >= 80) trendsBonus = 100;
    else if (maxT >= 50) trendsBonus = 70;
    else if (maxT >= 20) trendsBonus = 40;
    else if (maxT > 0) trendsBonus = 20;
  } catch(e){}
  return Math.round(internal * 0.4 + viral * 0.2 + keysoBonus * 0.15 + trendsBonus * 0.1 + headline * 0.15);
}

async function loadFinal(page) {
  if (page !== undefined) _finPage = page;
  const source = document.getElementById('fin-source')?.value || '';
  const offset = _finPage * _finLimit;
  let url = `/api/final?limit=${_finLimit}&offset=${offset}`;
  if (source) url += '&source=' + encodeURIComponent(source);
  const resp = await api(url);
  _finData = (resp.news || []).map(n => ({...n, final_score: calcFinalScore(n)}));
  _finTotal = resp.total || _finData.length;

  // Notify about new high-score items
  if (_prevFinalIds.size > 0) {
    const newHigh = _finData.filter(n => n.final_score >= 60 && !_prevFinalIds.has(n.id));
    if (newHigh.length === 1) {
      showNotification('Новая топ-новость!', newHigh[0].title + ' (скор: ' + newHigh[0].final_score + ')');
    } else if (newHigh.length > 1) {
      showNotification('Новые топ-новости: ' + newHigh.length, newHigh.slice(0, 3).map(n => n.title.slice(0, 50)).join('; '));
    }
  }
  _prevFinalIds = new Set(_finData.map(n => n.id));

  // Populate source filter
  const srcEl = document.getElementById('fin-source');
  const curVal = srcEl.value;
  const sources = [...new Set(_finData.map(n => n.source))].sort();
  srcEl.innerHTML = '<option value="">Все</option>' + sources.map(s => `<option value="${s}" ${s===curVal?'selected':''}>${s}</option>`).join('');

  // Stats
  const avg = _finData.length ? Math.round(_finData.reduce((s,n) => s + n.final_score, 0) / _finData.length) : 0;
  const high = _finData.filter(n => n.final_score >= 60).length;
  document.getElementById('fin-stats').innerHTML = `
    <div class="stat"><div class="stat-value">${_finTotal}</div><div class="stat-label">Всего publish_now</div></div>
    <div class="stat"><div class="stat-value" style="color:#17bf63">${high}</div><div class="stat-label">Финал &ge; 60</div></div>
    <div class="stat"><div class="stat-value">${avg}</div><div class="stat-label">Средний финал</div></div>
  `;

  renderFinalTable();
  renderFinalPagination(_finTotal);
}

function renderFinalPagination(total) {
  const pages = Math.ceil(total / _finLimit);
  const el = document.getElementById('fin-pagination');
  if (!el) return;
  if (pages <= 1) { el.innerHTML = ''; return; }
  let html = '';
  if (_finPage > 0) {
    html += `<button onclick="loadFinal(${_finPage - 1})" style="padding:4px 10px;border:1px solid #38444d;border-radius:4px;cursor:pointer;background:#192734;color:#8899a6">&laquo;</button>`;
  }
  const maxVisible = 7;
  let start = Math.max(0, _finPage - Math.floor(maxVisible / 2));
  let end = Math.min(pages, start + maxVisible);
  if (end - start < maxVisible) start = Math.max(0, end - maxVisible);
  if (start > 0) {
    html += `<button onclick="loadFinal(0)" style="padding:4px 10px;border:1px solid #38444d;border-radius:4px;cursor:pointer;background:#192734;color:#8899a6">1</button>`;
    if (start > 1) html += `<span style="color:#657786;padding:4px">...</span>`;
  }
  for (let i = start; i < end; i++) {
    const active = i === _finPage ? 'background:#1da1f2;color:#fff' : 'background:#192734;color:#8899a6';
    html += `<button onclick="loadFinal(${i})" style="padding:4px 10px;border:1px solid #38444d;border-radius:4px;cursor:pointer;${active}">${i+1}</button>`;
  }
  if (end < pages) {
    if (end < pages - 1) html += `<span style="color:#657786;padding:4px">...</span>`;
    html += `<button onclick="loadFinal(${pages - 1})" style="padding:4px 10px;border:1px solid #38444d;border-radius:4px;cursor:pointer;background:#192734;color:#8899a6">${pages}</button>`;
  }
  if (_finPage < pages - 1) {
    html += `<button onclick="loadFinal(${_finPage + 1})" style="padding:4px 10px;border:1px solid #38444d;border-radius:4px;cursor:pointer;background:#192734;color:#8899a6">&raquo;</button>`;
  }
  el.innerHTML = html;
}

function renderFinalTable() {
  const sorted = sortNews(_finData, _finSortField, _finSortDir);
  const tb = document.getElementById('fin-table');
  if (!tb) return;
  tb.innerHTML = sorted.map(n => {
    let bigrams = '';
    try { bigrams = JSON.parse(n.bigrams||'[]').map(b=>b[0]).join(', '); } catch(e){}
    let keysoFreq = '-', keysoSimilar = 0;
    try {
      const kd = JSON.parse(n.keyso_data||'{}');
      keysoFreq = kd.freq || kd.ws || '-';
      keysoSimilar = (kd.similar || []).length;
    } catch(e){}
    let trendsLabel = '-';
    try {
      const td = JSON.parse(n.trends_data||'{}');
      const vals = Object.entries(td).filter(([k,v]) => typeof v === 'number' && v > 0);
      if (vals.length) trendsLabel = vals.map(([k,v]) => `${k}:${v}`).join(' ');
    } catch(e){}
    let tags = '';
    try { tags = JSON.parse(n.tags_data||'[]').map(t => `<span class="tag" style="background:#1da1f233;color:#1da1f2">${t.label||t.id}</span>`).join(''); } catch(e){}
    const sc = n.total_score || 0;
    const fs = n.final_score || 0;
    const scColor = sc >= 70 ? '#17bf63' : sc >= 40 ? '#ffad1f' : '#e0245e';
    const fsColor = fs >= 60 ? '#17bf63' : fs >= 35 ? '#ffad1f' : '#e0245e';
    const viralBadge = n.viral_level === 'high' ? 'background:#e0245e33;color:#e0245e' : n.viral_level === 'medium' ? 'background:#ffad1f33;color:#ffad1f' : 'background:#38444d;color:#8899a6';
    let viralTriggers = '';
    try {
      const vd = JSON.parse(n.viral_data || '[]');
      if (Array.isArray(vd) && vd.length) viralTriggers = vd.map(t => typeof t === 'string' ? t : (t.label || t.trigger || t)).join(', ');
    } catch(e){}
    const viralTip = viralTriggers ? `${n.viral_level || 'low'} (${n.viral_score||0})\n${viralTriggers}` : `${n.viral_level || 'low'} (${n.viral_score||0})`;
    const freshBadge = n.freshness_status === 'fresh' ? 'color:#17bf63' : n.freshness_status === 'aging' ? 'color:#ffad1f' : 'color:#e0245e';
    const sentIcon = n.sentiment_label === 'positive' ? '&#128994;' : n.sentiment_label === 'negative' ? '&#128308;' : '&#9898;';
    const hrs = n.freshness_hours >= 0 ? Math.round(n.freshness_hours) + 'ч' : '-';
    return `<tr>
      <td><input type="checkbox" class="fin-check" data-id="${n.id}" data-score="${fs}" onchange="finUpdateCount()"></td>
      <td style="font-size:0.8em">${n.source}</td>
      <td><a href="${n.url}" target="_blank" style="color:#e1e8ed" title="${esc(n.h1||'')}">${esc((n.title||'').slice(0,80))}</a></td>
      <td style="text-align:center;font-weight:700;color:${scColor}">${sc}</td>
      <td style="text-align:center" class="td-tip"><span style="padding:2px 6px;border-radius:8px;font-size:0.8em;${viralBadge};cursor:help" title="${esc(viralTip)}">${n.viral_score||0}</span></td>
      <td style="text-align:center;${freshBadge};font-size:0.85em">${hrs}</td>
      <td style="text-align:center;font-size:0.9em">${sentIcon}</td>
      <td style="font-size:0.78em">${tags||'-'}</td>
      <td style="font-size:0.78em;max-width:120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(bigrams)}">${bigrams.slice(0,40)||'-'}</td>
      <td style="text-align:center;font-size:0.85em">${keysoFreq}</td>
      <td style="text-align:center;font-size:0.85em">${keysoSimilar||'-'}</td>
      <td style="font-size:0.78em">${trendsLabel}</td>
      <td style="text-align:center;font-weight:700;font-size:1.05em;color:${fsColor}">${fs}</td>
      <td style="white-space:nowrap">
        <button class="btn btn-sm" style="background:#9b59b6;color:#fff;padding:3px 7px" onclick="finToContent('${n.id}')" title="В контент">&#9998;</button>
        <button class="btn btn-sm btn-secondary" style="padding:3px 7px" onclick="exportOne('${n.id}')" title="В Sheets">&#9776;</button>
        <button class="btn btn-sm btn-secondary" style="padding:3px 7px;font-size:0.75em" onclick="openExplainDrawer('${n.id}','${esc((n.title||'').slice(0,60)).replace(/'/g,"\\'")}')" title="Почему?">?</button>
      </td>
    </tr>`;
  }).join('');
  document.getElementById('fin-count').textContent = sorted.length + ' из ' + _finTotal;
}

function sortFinal(field) {
  if (_finSortField === field) {
    _finSortDir = _finSortDir === 'asc' ? 'desc' : 'asc';
  } else {
    _finSortField = field;
    _finSortDir = field === 'freshness_hours' ? 'asc' : 'desc';
  }
  document.querySelectorAll('#panel-final .sortable').forEach(th => {
    const col = th.getAttribute('onclick')?.match(/'(\w+)'/)?.[1];
    const arrow = th.querySelector('.sort-arrow');
    if (col === field) {
      th.classList.add('sort-active');
      if (arrow) arrow.innerHTML = _finSortDir === 'asc' ? '&#9650;' : '&#9660;';
    } else {
      th.classList.remove('sort-active');
      if (arrow) arrow.innerHTML = '&#9650;';
    }
  });
  renderFinalTable();
}

function finToggleAll(el) {
  document.querySelectorAll('.fin-check').forEach(c => c.checked = el.checked);
  finUpdateCount();
}
function finSelectAbove60() {
  document.querySelectorAll('.fin-check').forEach(c => {
    c.checked = Number(c.dataset.score) >= 60;
  });
  finUpdateCount();
  const cnt = [...document.querySelectorAll('.fin-check:checked')].length;
  toast(`Отмечено ${cnt} новостей с финалом >= 60`);
}
function finUpdateCount() {
  const cnt = [...document.querySelectorAll('.fin-check:checked')].length;
  document.getElementById('fin-selected-count').textContent = cnt ? cnt + ' выбрано' : '';
}
function finGetSelectedIds() {
  return [...document.querySelectorAll('.fin-check:checked')].map(c => c.dataset.id);
}
async function finToContent(newsId) {
  const r = await api('/api/queue/add', {news_id: newsId, style: 'news', task_type: 'rewrite'});
  if (r.status === 'ok') toast('Отправлено в контент');
  else toast(r.message || 'Ошибка', true);
}
async function finSendSelected() {
  const ids = finGetSelectedIds();
  if (!ids.length) { toast('Выберите новости', true); return; }
  let ok = 0;
  for (const id of ids) {
    const r = await api('/api/queue/add', {news_id: id, style: 'news', task_type: 'rewrite'});
    if (r.status === 'ok') ok++;
  }
  toast(`Отправлено в контент: ${ok} из ${ids.length}`);
}

// === EDITORIAL TAB ===
let _edData = [];
let _edSortField = 'parsed_at';
let _edSortDir = 'desc';
let _edPage = 0;
let _edTotalAll = 0;
const _edLimit = 100;

loadAll();
loadSources();
loadPrompts();
loadSettings();
loadUsers();
loadHealth();
loadDbInfo();
loadArticles();
loadQueue();
loadAnalytics();
loadLogs();
// Smart polling — pause when tab is hidden
let _tabVisible = true;
document.addEventListener('visibilitychange', () => { _tabVisible = !document.hidden; });

function smartInterval(fn, ms) {
  setInterval(() => { if (_tabVisible) fn(); }, ms);
}
smartInterval(loadHealth, 60000);
smartInterval(() => { loadQueue(); if (document.getElementById('panel-queue')?.classList.contains('active')) loadQueueStandalone(); }, 15000);

let _edSearchTimer;
function debounceEdSearch() {
  clearTimeout(_edSearchTimer);
  _edSearchTimer = setTimeout(() => loadEditorial(0), 400);
}

function resetEdFilters() {
  document.getElementById('ed-status').value = '';
  document.getElementById('ed-source').value = '';
  document.getElementById('ed-viral').value = '';
  document.getElementById('ed-tier').value = '';
  document.getElementById('ed-min-score').value = '0';
  document.getElementById('ed-search').value = '';
  loadEditorial(0);
}

async function loadEditorial(page) {
  if (page !== undefined) _edPage = page;
  const status = document.getElementById('ed-status').value;
  const source = document.getElementById('ed-source').value;
  const viral = document.getElementById('ed-viral').value;
  const tier = document.getElementById('ed-tier').value;
  const minScore = document.getElementById('ed-min-score').value || 0;
  const search = document.getElementById('ed-search').value;
  const offset = _edPage * _edLimit;

  let url = `/api/editorial?limit=${_edLimit}&offset=${offset}`;
  if (status) url += `&status=${status}`;
  if (source) url += `&source=${encodeURIComponent(source)}`;
  if (viral) url += `&viral_level=${viral}`;
  if (tier) url += `&tier=${tier}`;
  if (minScore > 0) url += `&min_score=${minScore}`;
  if (search) url += `&q=${encodeURIComponent(search)}`;

  const r = await (await fetch(url)).json();
  _edData = r.news || [];
  const total = r.total || 0;
  const stats = r.stats || {};

  // Render stats
  const statsEl = document.getElementById('ed-stats');
  const sColors = {new:'#ffad1f',in_review:'#1da1f2',moderation:'#f5a623',approved:'#17bf63',processed:'#794bc4',duplicate:'#657786',rejected:'#e0245e',ready:'#17bf63'};
  const sLabels = {new:'Новые',in_review:'Проверка',moderation:'Модерация',approved:'Одобрены',processed:'Обработаны',duplicate:'Дубли',rejected:'Отклонены',ready:'Готовы'};
  const totalAll = Object.values(stats).reduce((a,b) => a + (b||0), 0);
  _edTotalAll = totalAll;
  let statsHtml = `<div class="stat ${!status?'active-filter':''}" title="Нажмите для фильтрации" onclick="document.getElementById('ed-status').value='';loadEditorial(0)">
    <div class="num" style="color:#e1e8ed">${totalAll}</div><div class="lbl">Всего</div></div>`;
  statsHtml += Object.entries(sLabels).map(([k,v]) => {
    const cnt = stats[k] || 0;
    const isActive = status === k;
    return `<div class="stat ${isActive?'active-filter':''}" title="Нажмите для фильтрации" onclick="document.getElementById('ed-status').value='${isActive?'':k}';loadEditorial(0)">
      <div class="num" style="color:${sColors[k]}">${cnt}</div><div class="lbl">${v}</div></div>`;
  }).join('');
  statsEl.innerHTML = statsHtml;

  // Populate source filter (once)
  const srcSel = document.getElementById('ed-source');
  if (srcSel.options.length <= 1) {
    const sources = [...new Set(_edData.map(n => n.source))].sort();
    sources.forEach(s => { const o = document.createElement('option'); o.value=s; o.text=s; srcSel.add(o); });
  }

  document.getElementById('ed-count').textContent = `${total} новостей`;
  renderEdTable();
  renderEdPagination(total);
}

function renderEdTable() {
  // Sort
  const data = [..._edData].sort((a, b) => {
    let va = a[_edSortField], vb = b[_edSortField];
    if (typeof va === 'string') va = va.toLowerCase();
    if (typeof vb === 'string') vb = vb.toLowerCase();
    if (va < vb) return _edSortDir === 'asc' ? -1 : 1;
    if (va > vb) return _edSortDir === 'asc' ? 1 : -1;
    return 0;
  });

  const tbody = document.getElementById('ed-table');
  if (!data.length) {
    tbody.innerHTML = `<tr><td colspan="11" style="text-align:center;padding:40px;color:#8899a6">
      <div style="font-size:1.3em;margin-bottom:12px">Нет новостей для отображения</div>
      <div style="margin-bottom:8px">Всего в базе: <b>${_edTotalAll}</b> новостей</div>
      <div style="margin-bottom:12px">${_edTotalAll === 0
        ? 'База пуста. Нажмите «Парсить» чтобы загрузить новости из источников.'
        : 'Нажмите «Проверить новые» или выберите другой фильтр статуса'}</div>
      ${_edTotalAll === 0
        ? '<button class="btn btn-primary" onclick="edForceParse()" style="padding:8px 24px;font-size:1em">&#128229; Парсить источники</button>'
        : '<button class="btn btn-success" onclick="edRunAutoReview()" style="padding:8px 24px;font-size:1em">&#9654; Проверить новые</button>'}
    </td></tr>`;
    return;
  }
  tbody.innerHTML = data.map(n => {
    const sc = n.total_score || 0;
    const scColor = sc >= 70 ? '#17bf63' : sc >= 40 ? '#ffad1f' : '#e0245e';
    const stColor = {new:'#ffad1f',in_review:'#1da1f2',moderation:'#f5a623',approved:'#17bf63',processed:'#794bc4',duplicate:'#657786',rejected:'#e0245e',ready:'#17bf63'}[n.status] || '#8899a6';
    const stLabel = {new:'Новая',in_review:'Проверка',moderation:'Модерация',approved:'Одобрена',processed:'Обработана',duplicate:'Дубль',rejected:'Отклонена',ready:'Готова'}[n.status] || n.status;

    // Viral
    const vl = n.viral_level || '';
    const vs = n.viral_score || 0;
    const vlColor = {high:'#e0245e',medium:'#ffad1f',low:'#1da1f2'}[vl] || '#657786';

    // Freshness
    const fh = n.freshness_hours;
    const fs = n.freshness_status || '';
    const fLabel = fh >= 0 ? (fh < 1 ? '<1ч' : Math.round(fh)+'ч') : '-';
    const fColor = fs === 'fresh' ? '#17bf63' : fs === 'aging' ? '#ffad1f' : '#e0245e';

    // Sentiment
    const sl = n.sentiment_label || '';
    const sEmoji = sl === 'positive' ? '⊕' : sl === 'negative' ? '⊖' : '⊘';
    const slColor = sl === 'positive' ? '#17bf63' : sl === 'negative' ? '#e0245e' : '#8899a6';

    // Tags
    let tags = [];
    try { tags = typeof n.tags_data === 'string' ? JSON.parse(n.tags_data || '[]') : (n.tags_data || []); } catch(e) {}
    if (!Array.isArray(tags)) tags = [];
    const tagsHtml = tags.slice(0,3).map(t => {
      const label = typeof t === 'object' ? (t.label || t.id || '') : String(t);
      const tid = typeof t === 'object' ? (t.id || '') : '';
      return `<span class="tag tag-${tid}" style="font-size:0.7em;padding:1px 5px">${label}</span>`;
    }).join(' ');

    // Entities
    let ents = [];
    try { ents = JSON.parse(n.entity_names || '[]'); } catch(e) {}
    const entTier = n.entity_best_tier || '';
    const entHtml = entTier ? `<span style="color:${entTier==='S'?'#e0245e':entTier==='A'?'#ffad1f':'#8899a6'};font-size:0.7em;font-weight:bold">${entTier}</span>` : '';

    // Viral triggers tooltip
    let vTriggers = [];
    try {
      const vRaw = typeof n.viral_data === 'string' ? JSON.parse(n.viral_data || '[]') : (n.viral_data || []);
      vTriggers = Array.isArray(vRaw) ? vRaw : [];
    } catch(e) {}
    const vTriggersTooltip = (vTriggers.map(t => `${(t.label||'?').replace(/"/g,'&quot;')} (${t.weight||0})`).join('&#10;') || 'Нет триггеров');

    // Quality / Relevance
    const qs = n.quality_score || 0;
    const rs = n.relevance_score || 0;

    // Status buttons
    const canApprove = ['new','in_review'].includes(n.status);
    const approveBtn = canApprove ? `<button class="btn btn-sm btn-primary" onclick="edApprove('${n.id}')" title="Одобрить" style="padding:2px 6px">&#10003;</button>` : '';
    const rejectBtn = canApprove ? `<button class="btn btn-sm btn-danger" onclick="edReject('${n.id}')" title="Отклонить" style="padding:2px 6px">&#10007;</button>` : '';
    const editorBtn = `<button class="btn btn-sm btn-secondary" onclick="edToEditor('${n.id}')" title="В редактор" style="padding:2px 6px">&#9998;</button>`;
    const explainBtn = `<button class="btn btn-sm btn-secondary" onclick="openExplainDrawer('${n.id}','${esc(n.title).replace(/'/g,"\\'")}') " title="Почему?" style="padding:2px 6px;font-size:0.75em">?</button>`;

    return `<tr class="ed-row" data-id="${n.id}">
      <td><input type="checkbox" class="ed-cb" value="${n.id}" style="width:15px;height:15px"></td>
      <td style="font-size:0.8em;color:#8899a6">${n.source} ${entHtml}</td>
      <td>
        <div style="cursor:pointer" onclick="edToggleDetail('${n.id}')">
          <div style="font-size:0.9em;line-height:1.3;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden">${n.title}</div>
        </div>
      </td>
      <td><span style="color:${stColor};font-size:0.75em;font-weight:500">${stLabel}</span></td>
      <td style="text-align:center"><span style="color:${scColor};font-weight:bold;font-size:1.1em">${sc}</span></td>
      <td style="text-align:center;font-size:0.8em;color:#8899a6">${qs}/${rs}</td>
      <td style="text-align:center;cursor:help" title="${vTriggersTooltip}"><span style="color:${vlColor};font-weight:bold">${vs}</span><br><span style="font-size:0.7em;color:${vlColor}">${vl}</span></td>
      <td style="text-align:center"><span style="color:${fColor};font-size:0.85em">${fLabel}</span></td>
      <td style="text-align:center"><span style="color:${slColor}" title="${sl}">${sEmoji}</span></td>
      <td>${tagsHtml}</td>
      <td style="white-space:nowrap">${approveBtn} ${rejectBtn} ${editorBtn} ${explainBtn}</td>
    </tr>
    <tr class="ed-detail" id="ed-detail-${n.id}" style="display:none">
      <td colspan="11" style="padding:12px 20px;background:#15202b;border-left:3px solid #1da1f2">
        ${_edRenderDetail(n, ents, tags)}
      </td>
    </tr>`;
  }).join('');

  // Update selected count
  edUpdateSelected();
}

function _edRenderDetail(n, ents, tags) {
  // Viral triggers
  let triggers = [];
  try { const trRaw = typeof n.viral_data === 'string' ? JSON.parse(n.viral_data || '[]') : (n.viral_data || []); triggers = Array.isArray(trRaw) ? trRaw : []; } catch(e) {}
  const trigHtml = triggers.map(t =>
    `<span style="display:inline-block;padding:2px 6px;border-radius:4px;font-size:0.75em;margin:1px;background:${(t.weight||0)>=40?'#e0245e33':(t.weight||0)>=20?'#ffad1f33':'#1da1f233'};color:${(t.weight||0)>=40?'#e0245e':(t.weight||0)>=20?'#ffad1f':'#1da1f2'}">${t.label||'?'} +${t.weight||0}</span>`
  ).join(' ');

  // Entities
  const entHtml = ents.length > 0 ? ents.join(', ') : '<span style="color:#657786">нет</span>';

  // Enrichment data (if available)
  let keysoHtml = '-', trendsHtml = '-', llmHtml = '-', bigramsHtml = '-';
  try {
    const kd = JSON.parse(n.keyso_data || '{}');
    if (kd.freq) keysoHtml = `ws=${kd.freq}`;
    if (kd.similar) keysoHtml += ` (${typeof kd.similar === 'object' ? (Array.isArray(kd.similar) ? kd.similar.length : 0) : 0} similar)`;
  } catch(e) {}
  try {
    const td = JSON.parse(n.trends_data || '{}');
    const tvals = Object.entries(td).map(([k,v]) => `${k}:${v}`).join(' ');
    if (tvals) trendsHtml = tvals;
  } catch(e) {}
  if (n.llm_recommendation) llmHtml = n.llm_recommendation;
  try {
    const bg = JSON.parse(n.bigrams || '[]');
    if (bg.length) bigramsHtml = bg.slice(0,5).map(b => Array.isArray(b) ? b[0] : b).join(', ');
  } catch(e) {}

  const fh = n.freshness_hours;
  const freshLabel = fh >= 0 ? (fh < 1 ? 'менее часа' : Math.round(fh) + ' ч. назад') : 'н/д';

  return `
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">
      <div>
        <div style="margin-bottom:8px"><b style="color:#1da1f2">Проверки:</b>
          Качество: <b>${n.quality_score}</b> &middot;
          Релевантность: <b>${n.relevance_score}</b> &middot;
          Headline: <b>${n.headline_score}</b> &middot;
          Momentum: <b>${n.momentum_score}</b> &middot;
          Свежесть: <b>${freshLabel}</b>
        </div>
        <div style="margin-bottom:8px"><b style="color:#1da1f2">Сущности:</b> ${entHtml}</div>
        <div style="margin-bottom:8px"><b style="color:#1da1f2">Вирал. тригеры:</b> ${trigHtml || '<span style="color:#657786">нет</span>'}</div>
        <div><b style="color:#1da1f2">Биграммы:</b> <span style="color:#8899a6">${bigramsHtml}</span></div>
        <div id="chain-${n.id}" style="margin-top:8px"><span style="color:#657786;font-size:0.8em">Цепочка: загрузка...</span></div>
      </div>
      <div>
        <div style="margin-bottom:8px"><b style="color:#1da1f2">Keys.so:</b> <span style="color:#8899a6">${keysoHtml}</span></div>
        <div style="margin-bottom:8px"><b style="color:#1da1f2">Trends:</b> <span style="color:#8899a6">${trendsHtml}</span></div>
        <div style="margin-bottom:8px"><b style="color:#1da1f2">LLM:</b> <span style="color:#8899a6">${llmHtml}</span></div>
        <div style="margin-bottom:8px">
          <a href="${n.url}" target="_blank" style="color:#1da1f2;font-size:0.85em">Открыть оригинал &#8599;</a>
          &middot; <span style="color:#657786;font-size:0.8em">${n.published_at || ''}</span>
        </div>
        <div style="margin-top:8px;display:flex;gap:6px;flex-wrap:wrap">
          ${['new','in_review'].includes(n.status) ? `<button class="btn btn-sm btn-primary" onclick="edApprove('${n.id}')">&#10003; Одобрить</button><button class="btn btn-sm btn-danger" onclick="edReject('${n.id}')">&#10007; Отклонить</button>` : ''}
          <button class="btn btn-sm btn-secondary" onclick="edToEditor('${n.id}')">&#9998; В редактор</button>
          ${n.status === 'processed' ? `<button class="btn btn-sm btn-secondary" onclick="edExportOne('${n.id}')">&#9776; В Sheets</button>` : ''}
          <button class="btn btn-sm btn-secondary" onclick="edQuickRewrite('${n.id}','news')">&#128221; Рерайт (новость)</button>
          <button class="btn btn-sm btn-secondary" onclick="edQuickRewrite('${n.id}','seo')">SEO</button>
          <button class="btn btn-sm btn-secondary" onclick="edQuickRewrite('${n.id}','short')">Коротко</button>
        </div>
      </div>
    </div>`;
}

function edToggleDetail(id) {
  const el = document.getElementById('ed-detail-' + id);
  if (!el) return;
  const wasHidden = el.style.display === 'none';
  el.style.display = wasHidden ? '' : 'none';
  if (wasHidden) {
    const chainEl = document.getElementById('chain-' + id);
    if (chainEl && !chainEl.dataset.loaded) {
      chainEl.dataset.loaded = '1';
      fetch('/api/event_chain?news_id=' + encodeURIComponent(id))
        .then(r => r.json())
        .then(data => {
          if (!data || data.phase === 'single' || !data.chain || data.chain.length === 0) {
            chainEl.innerHTML = '<b style="color:#1da1f2">Цепочка:</b> <span style="color:#657786;font-size:0.85em">нет связанных</span>';
            return;
          }
          const phaseColors = {emerging:'#ffad1f',developing:'#1da1f2',trending:'#e0245e'};
          const phaseLabels = {emerging:'Зарождение',developing:'Развитие',trending:'Тренд'};
          const pc = phaseColors[data.phase] || '#8899a6';
          const pl = phaseLabels[data.phase] || data.phase;
          const badge = '<span style="display:inline-block;padding:1px 8px;border-radius:10px;font-size:0.75em;font-weight:600;background:' + pc + '22;color:' + pc + ';border:1px solid ' + pc + '44">' + pl + ' (' + data.chain_length + ')</span>';
          const items = data.chain.slice(0, 10).map(c => {
            const d = c.published_at ? c.published_at.substring(0, 10) : '';
            return '<div style="font-size:0.8em;padding:2px 0;color:#d9d9d9"><span style="color:#657786">' + d + '</span> <span style="color:#8899a6">[' + (c.source||'') + ']</span> ' + (c.title||'') + '</div>';
          }).join('');
          const srcInfo = data.unique_sources > 1 ? ' &middot; ' + data.unique_sources + ' источн.' : '';
          const spanInfo = data.days_span > 0 ? ' &middot; ' + data.days_span + ' дн.' : '';
          chainEl.innerHTML = '<b style="color:#1da1f2">Цепочка:</b> ' + badge + srcInfo + spanInfo + '<div style="margin-top:4px;padding-left:8px;border-left:2px solid ' + pc + '44">' + items + '</div>';
        })
        .catch(() => {
          chainEl.innerHTML = '<b style="color:#1da1f2">Цепочка:</b> <span style="color:#657786">ошибка загрузки</span>';
        });
    }
  }
}

function edSort(field) {
  if (_edSortField === field) {
    _edSortDir = _edSortDir === 'asc' ? 'desc' : 'asc';
  } else {
    _edSortField = field;
    _edSortDir = field === 'title' || field === 'source' ? 'asc' : 'desc';
  }
  renderEdTable();
}

function edToggleAll(cb) {
  document.querySelectorAll('.ed-cb').forEach(c => c.checked = cb.checked);
  edUpdateSelected();
}

function edUpdateSelected() {
  const cnt = document.querySelectorAll('.ed-cb:checked').length;
  const el = document.getElementById('ed-selected-count');
  el.textContent = cnt > 0 ? `Выбрано: ${cnt}` : '';
}

// Delegation for checkbox changes
document.getElementById('ed-table')?.addEventListener('change', (e) => {
  if (e.target.classList.contains('ed-cb')) edUpdateSelected();
});

function _edGetSelected() {
  return [...document.querySelectorAll('.ed-cb:checked')].map(c => c.value);
}

async function edApprove(id) {
  const r = await api('/api/approve', {news_ids: [id]});
  if (r.status === 'ok') { toast('Одобрено + обогащение запущено'); loadEditorial(); } else toast(r.message, true);
}

async function edReject(id) {
  const r = await api('/api/reject', {news_ids: [id]});
  if (r.status === 'ok') { toast('Отклонено'); loadEditorial(); } else toast(r.message, true);
}

async function edApproveSelected() {
  const ids = _edGetSelected();
  if (!ids.length) { toast('Выберите новости', true); return; }
  const r = await api('/api/approve', {news_ids: ids});
  if (r.status === 'ok') { toast(`Одобрено: ${ids.length} — обогащение запущено`); loadEditorial(); } else toast(r.message, true);
}

async function edRejectSelected() {
  const ids = _edGetSelected();
  if (!ids.length) { toast('Выберите новости', true); return; }
  const r = await api('/api/reject', {news_ids: ids});
  if (r.status === 'ok') { toast(`Отклонено: ${ids.length}`); loadEditorial(); } else toast(r.message, true);
}

async function edAutoApprove() {
  const ids = _edData.filter(n => (n.total_score || 0) >= 70 && ['new','in_review'].includes(n.status)).map(n => n.id);
  if (!ids.length) { toast('Нет новостей со скором >= 70', true); return; }
  const r = await api('/api/approve', {news_ids: ids});
  if (r.status === 'ok') { toast(`Авто-одобрено: ${ids.length}`); loadEditorial(); } else toast(r.message, true);
}

function edToEditor(id) {
  // Switch to Контент tab and load articles
  switchToTab('editor');
  loadArticles();
}

function renderEdPagination(total) {
  const pages = Math.ceil(total / _edLimit);
  if (pages <= 1) { document.getElementById('ed-pagination').innerHTML = ''; return; }
  let html = '';
  for (let i = 0; i < pages; i++) {
    const active = i === _edPage ? 'background:#1da1f2;color:#fff' : 'background:#192734;color:#8899a6';
    html += `<button onclick="loadEditorial(${i})" style="padding:4px 10px;border:1px solid #38444d;border-radius:4px;cursor:pointer;${active}">${i+1}</button>`;
  }
  document.getElementById('ed-pagination').innerHTML = html;
}

// Bulk Sheets export
async function edExportSheets() {
  let ids = _edGetSelected();
  if (!ids.length) {
    ids = _edData.filter(n => n.status === 'processed').map(n => n.id);
    if (!ids.length) { toast('Нет обработанных для экспорта', true); return; }
  }
  const btn = event && event.target ? event.target : null;
  const origText = btn ? btn.innerHTML : '';
  if (btn) { btn.disabled = true; btn.innerHTML = '&#9203; Экспорт...'; }
  try {
    toast(`Экспорт ${ids.length} в Sheets...`);
    const r = await api('/api/queue/sheets', {news_ids: ids});
    if (r.status === 'ok') { toast(`${r.queued || ids.length} задач добавлено в очередь Sheets`); } else toast(r.message, true);
  } finally {
    if (btn) { btn.disabled = false; btn.innerHTML = origText; }
  }
}

// Batch rewrite from editorial
async function edBatchRewrite() {
  let ids = _edGetSelected();
  if (!ids.length) {
    ids = _edData.filter(n => ['approved','processed'].includes(n.status)).map(n => n.id);
    if (!ids.length) { toast('Нет одобренных для рерайта', true); return; }
  }
  const btn = event && event.target ? event.target : null;
  const origText = btn ? btn.innerHTML : '';
  if (btn) { btn.disabled = true; btn.innerHTML = '&#9203; Рерайт...'; }
  try {
    toast(`Рерайт ${ids.length} в очередь...`);
    const r = await api('/api/queue/rewrite', {news_ids: ids, style: 'news'});
    if (r.status === 'ok') { toast(`${r.queued || ids.length} задач добавлено`); } else toast(r.message, true);
  } finally {
    if (btn) { btn.disabled = false; btn.innerHTML = origText; }
  }
}

// Force parse all sources from editorial empty state
// Content tab switcher (Рерайт / Статьи)
// Content tab: articles loaded directly (no sub-tabs)

async function edForceParse() {
  const btn = event && event.target ? event.target : null;
  const origText = btn ? btn.innerHTML : '';
  if (btn) { btn.disabled = true; btn.innerHTML = '&#9203; Парсинг...'; }
  try {
    toast('Парсинг всех источников...');
    const r = await api('/api/reparse_all', {});
    if (r.status === 'ok') {
      toast('Спарсено: ' + (r.new_articles || 0) + ' новостей. Запуск проверки...');
      if (r.new_articles > 0) {
        await edRunAutoReview();
      }
      loadEditorial(0);
    } else {
      toast(r.message || 'Ошибка парсинга', true);
    }
  } finally {
    if (btn) { btn.disabled = false; btn.innerHTML = origText; }
  }
}

// Run auto-review for all unchecked news
async function edRunAutoReview() {
  const btn = document.getElementById('ed-review-btn');
  btn.disabled = true;
  btn.innerHTML = '&#8987; Проверка...';
  toast('Запуск проверки (батч 20)...');
  try {
    const r = await api('/api/run_auto_review', {});
    if (r.status === 'ok') {
      toast(r.message);
      loadEditorial(0);
      if (r.remaining > 0) {
        btn.innerHTML = `&#9654; Проверить ещё (${r.remaining})`;
      }
    } else {
      toast(r.message || 'Ошибка', true);
    }
  } catch(e) {
    toast('Ошибка: ' + e, true);
  }
  btn.disabled = false;
  if (!btn.innerHTML.includes('ещё')) btn.innerHTML = '&#9654; Проверить новые';
}

// Quick rewrite single news from editorial detail panel
async function edQuickRewrite(id, style) {
  toast(`Рерайт (${style})...`);
  const r = await api('/api/queue/rewrite', {news_ids: [id], style: style});
  if (r.status === 'ok') { toast('Задача рерайта добавлена в очередь'); } else toast(r.message || 'Ошибка', true);
}

// Export single news to Sheets from editorial detail panel
async function edExportOne(id) {
  toast('Экспорт в Sheets...');
  const r = await api('/api/queue/sheets', {news_ids: [id]});
  if (r.status === 'ok') { toast('Экспорт добавлен в очередь'); } else toast(r.message || 'Ошибка', true);
}

// Auto-refresh editorial every 30s (shows enrichment progress)
smartInterval(() => {
  const edPanel = document.getElementById('panel-editorial');
  if (edPanel && edPanel.classList.contains('active')) {
    loadEditorial();
  }
}, 30000);

// Initial editorial load + auto-review if empty
loadEditorial().then(() => {
  const stats = document.getElementById('ed-stats');
  if (_edData.length === 0 && stats && stats.textContent.match(/Новые.*[1-9]/)) {
    edRunAutoReview();
  }
});

// Check if pipeline is already running on page load
(async () => {
  try {
    const r = await api('/api/pipeline/status');
    if (r.running && r.active_type) {
      setPipelineActive(r.active_type);
    }
  } catch(e) {}
})();

// ═══════════════════════════════════════════
// PIPELINE BUTTONS (Редакция)
// ═══════════════════════════════════════════

let _pipelinePolling = null;

function setPipelineActive(type) {
  const btnFull = document.getElementById('btn-full-auto');
  const btnNoLLM = document.getElementById('btn-no-llm');
  const btnStop = document.getElementById('btn-pipeline-stop');
  const statusEl = document.getElementById('pipeline-status');

  if (type) {
    btnFull.disabled = true;
    btnNoLLM.disabled = true;
    btnFull.style.opacity = type === 'full_auto' ? '1' : '0.4';
    btnNoLLM.style.opacity = type === 'no_llm' ? '1' : '0.4';
    if (type === 'full_auto') btnFull.style.boxShadow = '0 0 8px #1da1f2';
    if (type === 'no_llm') btnNoLLM.style.boxShadow = '0 0 8px #794bc4';
    btnStop.style.display = 'inline-block';
    statusEl.style.display = 'inline-block';
    statusEl.innerHTML = '';
    // Start polling
    if (!_pipelinePolling) {
      _pipelinePolling = setInterval(pollPipelineStatus, 3000);
    }
  } else {
    btnFull.disabled = false;
    btnNoLLM.disabled = false;
    btnFull.style.opacity = '1';
    btnNoLLM.style.opacity = '1';
    btnFull.style.boxShadow = '';
    btnNoLLM.style.boxShadow = '';
    btnStop.style.display = 'none';
    statusEl.style.display = 'none';
    statusEl.innerHTML = '';
    if (_pipelinePolling) {
      clearInterval(_pipelinePolling);
      _pipelinePolling = null;
    }
  }
}

async function pollPipelineStatus() {
  try {
    const r = await api('/api/pipeline/status');
    const statusEl = document.getElementById('pipeline-status');
    if (r.running) {
      statusEl.style.display = 'inline-block';
      const typeLabel = r.active_type === 'full_auto' ? 'Автомат' : 'Без LLM';
      const done = r.total_done || 0;
      const totalAll = done + (r.running_count || 0) + (r.pending_count || 0);
      let elapsed = '';
      if (r.started_at) {
        const diffSec = Math.floor((Date.now() - new Date(r.started_at).getTime()) / 1000);
        if (diffSec > 0) {
          const m = Math.floor(diffSec / 60);
          const s = diffSec % 60;
          elapsed = m > 0 ? ` | ${m}м ${s}с` : ` | ${s}с`;
        }
      }
      statusEl.innerHTML = `<b>${typeLabel}</b> &middot; ${done}/${totalAll} обработано${elapsed}`;
      statusEl.style.color = '#ffad1f';
      statusEl.style.borderColor = r.active_type === 'full_auto' ? '#1da1f2' : '#794bc4';
    } else {
      statusEl.style.display = 'inline-block';
      statusEl.innerHTML = '<b style="color:#17bf63">Завершено</b>';
      statusEl.style.color = '#17bf63';
      statusEl.style.borderColor = '#17bf63';
      setPipelineActive(null);
      loadEditorial();
      setTimeout(() => { statusEl.style.display = 'none'; statusEl.innerHTML = ''; }, 3000);
    }
  } catch(e) {}
}

async function runFullAuto() {
  let ids = getEdSelectedIds();
  let allNew = false;
  if (!ids.length) {
    if (!confirm('Выбранных нет. Запустить полный автомат для ВСЕХ новых?\\n\\nПайплайн: Скор → >70 на LLM → Финальный скор → >60 на рерайт → Sheets/Ready')) return;
    allNew = true;
  } else {
    if (!confirm('Запустить полный автомат для ' + ids.length + ' новостей?\\n\\nПайплайн: Скор → >70 на LLM → Финальный скор → >60 на рерайт → Sheets/Ready')) return;
  }
  toast('Запуск полного автомата...');
  const r = await api('/api/pipeline/full_auto', {news_ids: ids, all_new: allNew});
  if (r.status === 'ok') {
    toast(r.queued + ' задач в очереди');
    setPipelineActive('full_auto');
  } else {
    toast(r.message || 'Ошибка', true);
  }
}

async function runNoLLM() {
  let ids = getEdSelectedIds();
  let allNew = false;
  if (!ids.length) {
    if (!confirm('Выбранных нет. Запустить "Без LLM" для ВСЕХ новых?')) return;
    allNew = true;
  } else {
    if (!confirm('Запустить "Без LLM" для ' + ids.length + ' новостей? Только локальный анализ + Sheets NotReady.')) return;
  }
  toast('Запуск анализа без LLM...');
  const r = await api('/api/pipeline/no_llm', {news_ids: ids, all_new: allNew});
  if (r.status === 'ok') {
    toast(r.queued + ' задач в очереди');
    setPipelineActive('no_llm');
  } else {
    toast(r.message || 'Ошибка', true);
  }
}

async function stopPipeline() {
  if (!confirm('Остановить текущий пайплайн?')) return;
  const r = await api('/api/pipeline/stop', {});
  if (r.status === 'ok') {
    toast('Пайплайн остановлен');
    // Immediately update UI
    const btnStop = document.getElementById('btn-pipeline-stop');
    btnStop.disabled = true;
    btnStop.textContent = '⏳ Останавливается...';
    document.getElementById('pipeline-status').textContent = 'Останавливается...';
    document.getElementById('pipeline-status').style.color = '#e0245e';
    // Force check and reset after short delay
    setTimeout(async () => {
      setPipelineActive(null);
      btnStop.disabled = false;
      btnStop.textContent = '⏹ Стоп';
      loadEditorial();
      loadQueue();
    }, 2000);
  } else {
    toast(r.message || 'Ошибка', true);
  }
}

function getEdSelectedIds() {
  return [...document.querySelectorAll('.ed-cb:checked')].map(cb => cb.value);
}

// ═══════════════════════════════════════════
// MODERATION TAB
// ═══════════════════════════════════════════

// Moderation tab removed — use Редакция filter "Модерация" instead

// ═══════════════════════════════════════════
// PHASE 0: OPS DASHBOARD & FEATURE FLAGS
// ═══════════════════════════════════════════

let _featureFlags = {};

async function loadFeatureFlags() {
  try {
    const data = await api('/api/feature_flags');
    if (data && data.flags) {
      _featureFlags = {};
      data.flags.forEach(f => { _featureFlags[f.flag_id] = f.enabled; });
      applyFeatureFlags();
      renderFlagsList(data.flags);
    }
  } catch(e) { console.warn('Feature flags load error:', e); }
}

function ffEnabled(flagId) {
  return !!_featureFlags[flagId];
}

// applyFeatureFlags defined below after triage mode

function renderFlagsList(flags) {
  const el = document.getElementById('ops-flags-list');
  if (!el) return;
  el.innerHTML = flags.map(f => `
    <div class="flag-row">
      <div style="flex:1">
        <span class="flag-name">${esc(f.flag_id)}</span>
        <span class="flag-phase">P${f.phase}</span>
        <div class="flag-desc">${esc(f.description)}</div>
      </div>
      <label class="flag-toggle">
        <input type="checkbox" ${f.enabled ? 'checked' : ''} onchange="toggleFlag('${esc(f.flag_id)}', this.checked)">
        <span class="slider"></span>
      </label>
    </div>
  `).join('');
}

async function toggleFlag(flagId, enabled) {
  const data = await api('/api/feature_flags/toggle', {flag_id: flagId, enabled});
  if (data && data.status === 'ok') {
    _featureFlags[flagId] = enabled;
    applyFeatureFlags();
    showToast((enabled ? 'Включен' : 'Выключен') + ': ' + flagId, enabled ? 'success' : 'warning');
  }
}

async function loadOpsDashboard() {
  if (!ffEnabled('dashboard_v2')) return;
  try {
    const data = await api('/api/ops_dashboard');
    if (!data || data.error) return;

    // Update counters
    const set = (id, val) => { const el = document.getElementById(id); if(el) el.textContent = val; };
    set('ops-pending-num', data.pending_review || 0);
    set('ops-ready-num', data.ready_to_publish || 0);
    set('ops-errors-num', data.queue_errors || 0);
    set('ops-candidates-num', data.high_score_candidates || 0);
    set('ops-degraded-num', data.degraded_sources || 0);
    set('ops-cost-num', '$' + (data.api_cost_today || 0).toFixed(3));
    set('ops-drafts-num', data.draft_articles || 0);

    // Render action cards
    const actEl = document.getElementById('ops-actions');
    if (actEl && data.actions) {
      const typeColors = {review:'#1da1f2', error:'#e0245e', opportunity:'#ffad1f', warning:'#f5a623', publish:'#17bf63', draft:'#8899a6'};
      actEl.innerHTML = data.actions.length === 0
        ? '<div style="color:#8899a6;padding:12px">Всё под контролем &#10004;</div>'
        : data.actions.map(a => `
          <div class="ops-action-card" data-type="${a.type}" onclick="switchToTab('${a.tab}')">
            <div class="ops-action-count" style="color:${typeColors[a.type] || '#8899a6'}">${a.count}</div>
            <div class="ops-action-title">${esc(a.title)}</div>
          </div>
        `).join('');
    }
  } catch(e) { console.warn('Ops dashboard error:', e); }
}

// ═══════════════════════════════════════════
// PHASE 1: EXPLAIN DRAWER (EXPLAINABILITY)
// ═══════════════════════════════════════════

function openExplainDrawer(newsId, title) {
  if (!ffEnabled('explainability_v1')) {
    showToast('Включите флаг explainability_v1 в настройках', 'warning');
    return;
  }
  const drawer = document.getElementById('explain-drawer');
  if (!drawer) return;
  document.getElementById('explain-title').textContent = title || '';
  document.getElementById('explain-scores').innerHTML = '<div style="color:#8899a6">Загрузка...</div>';
  document.getElementById('explain-trace').innerHTML = '';
  document.getElementById('explain-reason').innerHTML = '';
  drawer.classList.add('open');
  loadExplainData(newsId);
}

function closeExplainDrawer() {
  const drawer = document.getElementById('explain-drawer');
  if (drawer) drawer.classList.remove('open');
}

async function loadExplainData(newsId) {
  try {
    // Load decision trace
    const traceData = await api('/api/decision_trace', {news_id: newsId});

    // Load news detail for score breakdown
    const detail = await api('/api/news/detail', {id: newsId});

    // Render score breakdown
    const scoresEl = document.getElementById('explain-scores');
    if (detail && detail.analysis) {
      const a = detail.analysis;
      let breakdown = {};
      if (a.score_breakdown) {
        try { breakdown = typeof a.score_breakdown === 'string' ? JSON.parse(a.score_breakdown) : a.score_breakdown; } catch(e) {}
      }

      const bars = [
        {label: 'Качество', val: breakdown.quality || a.quality_score || 0, color: '#1da1f2'},
        {label: 'Релевантность', val: breakdown.relevance || a.relevance_score || 0, color: '#17bf63'},
        {label: 'Свежесть', val: breakdown.freshness || 0, color: '#ffad1f'},
        {label: 'Виральность', val: breakdown.viral || a.viral_score || 0, color: '#e0245e'},
        {label: 'Заголовок', val: a.headline_score || 0, color: '#794bc4'},
        {label: 'Momentum', val: breakdown.momentum_bonus || a.momentum_score || 0, max: 20, color: '#f5a623'},
        {label: 'Source Weight', val: Math.round((breakdown.source_weight || 1) * 100), max: 150, color: '#8899a6'},
        {label: 'Feedback', val: breakdown.feedback_adj || 0, max: 20, min: -10, color: '#1da1f2'},
      ];

      scoresEl.innerHTML = bars.map(b => {
        const max = b.max || 100;
        const pct = Math.max(0, Math.min(100, ((b.val - (b.min||0)) / (max - (b.min||0))) * 100));
        const barColor = b.val < 30 ? '#e0245e' : b.val < 60 ? '#ffad1f' : b.color;
        return `<div class="score-bar">
          <div class="bar-label">${b.label}</div>
          <div class="bar-track"><div class="bar-fill" style="width:${pct}%;background:${barColor}"></div></div>
          <div class="bar-val">${b.val}</div>
        </div>`;
      }).join('') + `<div style="margin-top:10px;font-size:0.95em"><strong>Total Score: ${a.total_score || 0}</strong></div>`;
    } else {
      scoresEl.innerHTML = '<div style="color:#8899a6">Нет данных о скоринге</div>';
    }

    // Render trace
    const traceEl = document.getElementById('explain-trace');
    if (traceData && traceData.trace && traceData.trace.length > 0) {
      traceEl.innerHTML = traceData.trace.map(t => `
        <div class="trace-step" data-decision="${esc(t.decision)}">
          <div style="display:flex;justify-content:space-between;margin-bottom:4px">
            <strong>${esc(t.step)}</strong>
            <span class="reason-badge ${t.decision === 'rejected' || t.decision === 'auto_rejected' || t.decision === 'duplicate' ? 'negative' : t.decision === 'approved' || t.decision === 'in_review' ? 'positive' : 'neutral'}">${esc(t.decision)}</span>
          </div>
          <div style="color:#8899a6">${esc(t.reason)}</div>
          ${t.score_after ? '<div style="margin-top:4px;font-size:0.8em">Score: ' + t.score_after + '</div>' : ''}
        </div>
      `).join('');
    } else {
      traceEl.innerHTML = '<div style="color:#8899a6">Нет записей трассировки</div>';
    }

    // Reason summary
    const reasonEl = document.getElementById('explain-reason');
    if (detail && detail.news) {
      const status = detail.news.status;
      const reasons = {
        'new': 'Новость только что распарсена, ещё не проверена',
        'in_review': 'Прошла авто-ревью, ждёт модерации редактора',
        'approved': 'Одобрена для обогащения (Keys.so + Trends + LLM)',
        'rejected': 'Отклонена: низкий скор или ручное решение',
        'duplicate': 'Обнаружен дубликат по TF-IDF или entity overlap',
        'processed': 'Обогащена данными API, готова к оценке',
        'moderation': 'Экспортирована без LLM, ждёт ручную проверку',
        'ready': 'Прошла все этапы, готова к публикации',
      };
      reasonEl.textContent = reasons[status] || 'Статус: ' + status;
    }

  } catch(e) {
    console.warn('Explain data error:', e);
    document.getElementById('explain-scores').innerHTML = '<div style="color:#e0245e">Ошибка загрузки</div>';
  }
}

// ═══════════════════════════════════════════
// PHASE 1: TRIAGE MODE (newsroom_triage_v1)
// ═══════════════════════════════════════════

let _triageMode = 'table';
let _triageIndex = 0;
let _triageCurrent = null;
let _triageList = [];

function setTriageMode(mode) {
  _triageMode = mode;
  document.querySelectorAll('.triage-mode').forEach(m => m.classList.toggle('active', m.dataset.mode === mode));

  const flow = document.getElementById('triage-flow');
  const stats = document.getElementById('ed-stats');
  const filters = document.querySelector('.dash-filters');
  const bulk = filters ? filters.nextElementSibling : null;
  const table = document.querySelector('#panel-editorial .table, #panel-editorial table')?.parentElement;
  const pagination = document.getElementById('ed-pagination');

  if (mode === 'flow') {
    if (flow) flow.style.display = '';
    if (stats) stats.style.display = 'none';
    if (filters) filters.style.display = 'none';
    if (bulk) bulk.style.display = 'none';
    if (table) table.style.display = 'none';
    if (pagination) pagination.style.display = 'none';
    startTriageFlow();
  } else {
    if (flow) flow.style.display = 'none';
    if (stats) stats.style.display = '';
    if (filters) filters.style.display = '';
    if (bulk) bulk.style.display = '';
    if (table) table.style.display = '';
    if (pagination) pagination.style.display = '';
    if (mode === 'disputed') {
      // Filter to disputed: score 30-70, not duplicate/rejected
      document.getElementById('ed-status').value = 'in_review';
      document.getElementById('ed-min-score').value = 30;
      loadEditorial();
    } else if (mode === 'table') {
      loadEditorial();
    }
  }
}

async function startTriageFlow() {
  // Load in_review items for sequential review
  const data = await api('/api/editorial?status=in_review&limit=100');
  _triageList = (data && data.news) ? data.news : [];
  _triageIndex = 0;
  renderTriageCard();
}

function renderTriageCard() {
  if (_triageIndex >= _triageList.length) {
    document.getElementById('triage-card').innerHTML = '<div style="text-align:center;padding:40px;color:#8899a6"><div style="font-size:1.3em;margin-bottom:12px">Все новости проверены</div><button class="btn btn-primary" onclick="setTriageMode(\'table\')">Вернуться к таблице</button></div>';
    document.getElementById('triage-counter').textContent = '';
    return;
  }
  const n = _triageList[_triageIndex];
  _triageCurrent = n;
  document.getElementById('triage-counter').textContent = `${_triageIndex + 1} из ${_triageList.length}`;

  const sc = n.total_score || 0;
  const scColor = sc >= 70 ? '#17bf63' : sc >= 40 ? '#ffad1f' : '#e0245e';
  const vs = n.viral_score || 0;
  const fh = n.freshness_hours;
  const fLabel = fh >= 0 ? (fh < 1 ? '<1ч' : Math.round(fh)+'ч') : '-';

  document.getElementById('tc-title').textContent = n.title || '';
  document.getElementById('tc-meta').innerHTML = `
    <span style="color:#1da1f2">${esc(n.source)}</span>
    <span>&#9201; ${fLabel}</span>
    <span>${n.sentiment_label === 'positive' ? '&#8853;' : n.sentiment_label === 'negative' ? '&#8854;' : '&#8856;'} ${n.sentiment_label || ''}</span>
    ${n.entity_best_tier ? '<span style="font-weight:bold;color:' + (n.entity_best_tier==='S'?'#e0245e':n.entity_best_tier==='A'?'#ffad1f':'#8899a6') + '">' + n.entity_best_tier + '-tier</span>' : ''}
  `;
  document.getElementById('tc-text').textContent = n.description || n.title || '';
  document.getElementById('tc-scores').innerHTML = `
    <div class="tc-score-item"><div class="val" style="color:${scColor}">${sc}</div><div class="lbl">Total</div></div>
    <div class="tc-score-item"><div class="val">${n.quality_score||0}</div><div class="lbl">Качество</div></div>
    <div class="tc-score-item"><div class="val">${n.relevance_score||0}</div><div class="lbl">Релев.</div></div>
    <div class="tc-score-item"><div class="val" style="color:${vs>=60?'#e0245e':vs>=30?'#ffad1f':'#8899a6'}">${vs}</div><div class="lbl">Вирал.</div></div>
    <div class="tc-score-item"><div class="val">${n.headline_score||0}</div><div class="lbl">Заголовок</div></div>
  `;
}

async function triageApprove() {
  if (!_triageCurrent) return;
  await api('/api/approve', {id: _triageCurrent.id});
  showToast('Одобрено', 'success');
  _triageIndex++;
  renderTriageCard();
}

async function triageReject() {
  if (!_triageCurrent) return;
  await api('/api/reject', {id: _triageCurrent.id});
  showToast('Отклонено', 'warning');
  _triageIndex++;
  renderTriageCard();
}

function triageSkip() {
  _triageIndex++;
  renderTriageCard();
}

// Keyboard shortcuts for editorial (global, check focus)
document.addEventListener('keydown', function(e) {
  if (e.key === 'Escape') { closeExplainDrawer(); return; }

  // Only process hotkeys when no input/textarea is focused
  const tag = document.activeElement?.tagName;
  if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return;

  // Triage flow hotkeys
  if (_triageMode === 'flow' && ffEnabled('newsroom_triage_v1')) {
    if (e.key === 'a' || e.key === 'A' || e.key === 'ф' || e.key === 'Ф') { e.preventDefault(); triageApprove(); }
    if (e.key === 'r' || e.key === 'R' || e.key === 'к' || e.key === 'К') { e.preventDefault(); triageReject(); }
    if (e.key === 's' || e.key === 'S' || e.key === 'ы' || e.key === 'Ы') { e.preventDefault(); triageSkip(); }
  }
});

// ═══════════════════════════════════════════
// PHASE 3: ANALYTICS FUNNEL
// ═══════════════════════════════════════════

async function loadFunnelAnalytics() {
  if (!ffEnabled('analytics_funnel_v1')) {
    const el = document.getElementById('analytics-funnel');
    if (el) el.style.display = 'none';
    return;
  }
  const el = document.getElementById('analytics-funnel');
  if (el) el.style.display = '';

  const data = await api('/api/analytics/funnel');
  if (!data || data.error) return;

  const stages = [
    {key: 'parsed', label: 'Распарсено', color: '#8899a6'},
    {key: 'reviewed', label: 'Проверено', color: '#1da1f2'},
    {key: 'in_review', label: 'На модерации', color: '#ffad1f'},
    {key: 'approved', label: 'Одобрено', color: '#17bf63'},
    {key: 'processed', label: 'Обогащено', color: '#794bc4'},
    {key: 'ready', label: 'Готово', color: '#17bf63'},
    {key: 'rewritten', label: 'Рерайтнуто', color: '#1da1f2'},
    {key: 'published', label: 'Опубликовано', color: '#e0245e'},
  ];

  const maxVal = data.parsed || 1;
  const barsEl = document.getElementById('funnel-bars');
  if (barsEl) {
    barsEl.innerHTML = stages.map(s => {
      const val = data[s.key] || 0;
      const pct = Math.max(2, (val / maxVal) * 100);
      return `<div class="funnel-bar">
        <div class="fb-label">${s.label}</div>
        <div class="fb-track"><div class="fb-fill" style="width:${pct}%;background:${s.color}">${val}</div></div>
        <div class="fb-val">${val}</div>
      </div>`;
    }).join('');
  }

  // Source conversion table
  const srcEl = document.getElementById('funnel-sources');
  if (srcEl && data.by_source) {
    srcEl.innerHTML = '<table style="width:100%;font-size:0.82em"><thead><tr><th style="text-align:left">Источник</th><th>Всего</th><th>Ready</th><th>Rejected</th><th>Дубли</th><th>Конверсия</th></tr></thead><tbody>' +
      data.by_source.map(s => {
        const conv = s.total > 0 ? ((s.ready_count / s.total) * 100).toFixed(1) + '%' : '0%';
        const convColor = s.ready_count / s.total > 0.1 ? '#17bf63' : s.ready_count / s.total > 0.03 ? '#ffad1f' : '#e0245e';
        return `<tr><td>${esc(s.source)}</td><td style="text-align:center">${s.total}</td><td style="text-align:center;color:#17bf63">${s.ready_count}</td><td style="text-align:center;color:#e0245e">${s.rejected_count}</td><td style="text-align:center;color:#8899a6">${s.dup_count}</td><td style="text-align:center;color:${convColor};font-weight:bold">${conv}</td></tr>`;
      }).join('') + '</tbody></table>';
  }
}

// ═══════════════════════════════════════════
// FEATURE FLAG UI HOOKS
// ═══════════════════════════════════════════

// ═══════════════════════════════════════════
// Phase 4: Storylines
// ═══════════════════════════════════════════
async function loadStorylines() {
  const el = document.getElementById('storylines-list');
  const countEl = document.getElementById('storylines-count');
  if (!el) return;
  el.innerHTML = '<div style="color:#8899a6;padding:20px">Загрузка кластеров...</div>';
  try {
    const data = await api('/api/storylines');
    const stories = data.storylines || [];
    if (countEl) countEl.textContent = stories.length + ' сюжетов из ' + (data.total_news||0) + ' новостей';
    if (!stories.length) {
      el.innerHTML = '<div style="color:#8899a6;padding:20px">Нет кластеров (менее 2 похожих новостей за 3 дня)</div>';
      return;
    }
    el.innerHTML = stories.map(s => {
      const phaseClass = s.phase;
      const phaseLabel = s.phase === 'trending' ? 'Тренд' : s.phase === 'developing' ? 'Развивается' : 'Новый';
      const membersHtml = s.members.map(m =>
        '<div class="storyline-member">' +
          '<span class="sm-source">' + esc(m.source) + '</span>' +
          '<span style="flex:1">' + esc(m.title) + '</span>' +
          '<span class="sm-score">' + (m.total_score||0) + '</span>' +
          '<span style="color:#8899a6;font-size:0.75em">' + fmtDate(m.published_at) + '</span>' +
        '</div>'
      ).join('');
      return '<div class="storyline-card">' +
        '<div class="storyline-header">' +
          '<div><span class="storyline-phase ' + phaseClass + '">' + phaseLabel + '</span> ' +
            '<strong style="margin-left:8px">' + s.count + ' статей</strong></div>' +
          '<div class="storyline-stats">' +
            '<span>Ср.скор: <b>' + s.avg_score + '</b></span>' +
            '<span>Вирал: <b>' + s.max_viral + '</b></span>' +
            '<span>' + s.sources.length + ' источников</span>' +
          '</div>' +
        '</div>' +
        '<div style="font-size:0.8em;color:#8899a6;margin-bottom:6px">Источники: ' + s.sources.join(', ') + '</div>' +
        '<div class="storyline-members">' + membersHtml + '</div>' +
      '</div>';
    }).join('');
  } catch(e) {
    el.innerHTML = '<div style="color:#e0245e;padding:20px">Ошибка: ' + (e.message||e) + '</div>';
  }
}

// ═══════════════════════════════════════════
// Phase 4: Source Health Plus
// ═══════════════════════════════════════════
async function loadHealthPlus() {
  if (!ffEnabled('source_health_plus_v1')) { showToast('Source Health Plus не активен', 'warning'); return; }
  const el = document.getElementById('health-table');
  const thead = document.getElementById('health-thead');
  const recsEl = document.getElementById('health-recs');
  if (!el) return;
  el.innerHTML = '<tr><td colspan="8" style="color:#8899a6;padding:20px">Загрузка расширенных данных...</td></tr>';
  try {
    const data = await api('/api/source_health_plus');
    const sources = data.sources || [];

    // Update header
    if (thead) thead.innerHTML = '<tr><th>Статус</th><th>Источник</th><th>24ч</th><th>7д тренд</th><th>Ср.скор</th><th>Конверсия</th><th>Направление</th><th>Рекомендации</th></tr>';

    // Collect all recommendations
    const allRecs = [];
    sources.forEach(s => (s.recommendations||[]).forEach(r => allRecs.push({source: s.source, ...r})));

    if (recsEl && allRecs.length) {
      recsEl.style.display = 'block';
      recsEl.innerHTML = '<div style="background:#192734;border-radius:8px;padding:12px;margin-bottom:8px"><strong>Рекомендации:</strong><div style="margin-top:6px">' +
        allRecs.map(r => '<span class="health-rec ' + r.type + '">' + esc(r.source) + ': ' + esc(r.text) + '</span>').join('') +
      '</div></div>';
    } else if (recsEl) {
      recsEl.style.display = 'none';
    }

    el.innerHTML = sources.map(s => {
      const icon = s.status==='healthy'?'&#9989;':s.status==='low'?'&#128993;':s.status==='warning'?'&#9888;&#65039;':'&#10060;';
      const maxT = Math.max(...(s.trend_7d||[1]), 1);
      const sparkHtml = '<span class="sparkline">' +
        (s.trend_7d||[]).map(v => '<span class="sparkline-bar" style="height:' + Math.max(2, Math.round(v/maxT*24)) + 'px"></span>').join('') +
      '</span>';
      const arrClass = s.trend_direction === 'up' ? 'up' : s.trend_direction === 'down' ? 'down' : 'stable';
      const arrSymbol = s.trend_direction === 'up' ? '&#9650;' : s.trend_direction === 'down' ? '&#9660;' : '&#8212;';
      const convColor = s.conversion_pct >= 30 ? '#17bf63' : s.conversion_pct >= 15 ? '#ffad1f' : '#e0245e';
      const recsHtml = (s.recommendations||[]).map(r => '<span class="health-rec ' + r.type + '" title="' + esc(r.text) + '">' + r.type + '</span>').join('');
      return '<tr>' +
        '<td>' + icon + ' ' + s.status + '</td>' +
        '<td>' + esc(s.source) + '</td>' +
        '<td>' + s.count_24h + '</td>' +
        '<td>' + sparkHtml + ' <span style="color:#8899a6;font-size:0.8em">' + s.total_7d + '</span></td>' +
        '<td style="color:#1da1f2;font-weight:600">' + s.avg_score + '</td>' +
        '<td style="color:' + convColor + ';font-weight:600">' + s.conversion_pct + '%</td>' +
        '<td><span class="trend-arrow ' + arrClass + '">' + arrSymbol + '</span></td>' +
        '<td>' + (recsHtml || '<span style="color:#17bf63">OK</span>') + '</td>' +
      '</tr>';
    }).join('');

    const sumEl = document.getElementById('health-summary');
    if (sumEl) {
      const ok = sources.filter(s => s.status === 'healthy').length;
      const warn = sources.filter(s => ['low','warning'].includes(s.status)).length;
      const dead = sources.filter(s => ['dead','down'].includes(s.status)).length;
      sumEl.textContent = ok + ' ок / ' + warn + ' внимание / ' + dead + ' мертв / ' + allRecs.length + ' рекомендаций';
    }
  } catch(e) {
    el.innerHTML = '<tr><td colspan="8" style="color:#e0245e">Ошибка: ' + (e.message||e) + '</td></tr>';
  }
}

// ═══════════════════════════════════════════
// Phase 4: Threshold Simulator
// ═══════════════════════════════════════════
function updateSimLabel(input, labelId) {
  document.getElementById(labelId).textContent = input.value;
}

async function runSimulation() {
  const resEl = document.getElementById('sim-results');
  if (!resEl) return;
  resEl.innerHTML = '<div style="color:#8899a6">Симуляция...</div>';
  try {
    const data = await api('/api/simulate_thresholds', {
      score_min: parseInt(document.getElementById('sim-score-min').value),
      score_max: parseInt(document.getElementById('sim-score-max').value),
      final_min: parseInt(document.getElementById('sim-final-min').value),
      final_max: parseInt(document.getElementById('sim-final-max').value),
    });
    if (data.error) { resEl.innerHTML = '<div style="color:#e0245e">' + data.error + '</div>'; return; }

    // Stats cards
    let html = '<div style="display:flex;flex-wrap:wrap;gap:4px">';
    html += '<div class="sim-stat"><div class="val">' + data.total + '</div><div class="lbl">Всего (7д)</div></div>';
    html += '<div class="sim-stat"><div class="val" style="color:#1da1f2">' + data.pass_score + '</div><div class="lbl">По скору (' + data.pct_score + '%)</div></div>';
    html += '<div class="sim-stat"><div class="val" style="color:#17bf63">' + data.pass_final + '</div><div class="lbl">По финалу (' + data.pct_final + '%)</div></div>';
    html += '<div class="sim-stat"><div class="val" style="color:#ffad1f">' + data.pass_both + '</div><div class="lbl">Оба фильтра</div></div>';
    html += '</div>';

    // Score distribution bar
    const dist = data.score_distribution || {};
    const total = data.total || 1;
    const colors = {'0-19':'#e0245e','20-39':'#ffad1f','40-59':'#8899a6','60-79':'#1da1f2','80-100':'#17bf63'};
    html += '<div style="margin-top:12px"><strong style="font-size:0.85em">Распределение скоров:</strong></div>';
    html += '<div class="sim-bar">';
    for (const [k,v] of Object.entries(dist)) {
      const pct = Math.round(v/total*100);
      if (pct > 0) html += '<div style="width:' + pct + '%;background:' + colors[k] + '">' + k + ' (' + v + ')</div>';
    }
    html += '</div>';

    // Final distribution
    const fdist = data.final_distribution || {};
    html += '<div style="margin-top:8px"><strong style="font-size:0.85em">Распределение финальных:</strong></div>';
    html += '<div class="sim-bar">';
    for (const [k,v] of Object.entries(fdist)) {
      const pct = Math.round(v/total*100);
      if (pct > 0) html += '<div style="width:' + pct + '%;background:' + colors[k] + '">' + k + ' (' + v + ')</div>';
    }
    html += '</div>';

    // By source table
    const bySource = data.by_source || {};
    const srcEntries = Object.entries(bySource).sort((a,b) => b[1].total - a[1].total);
    if (srcEntries.length) {
      html += '<div style="margin-top:12px"><strong style="font-size:0.85em">По источникам:</strong></div>';
      html += '<table style="font-size:0.82em;margin-top:6px"><thead><tr><th>Источник</th><th>Всего</th><th>Прошли скор</th><th>Прошли финал</th></tr></thead><tbody>';
      srcEntries.forEach(([src, d]) => {
        html += '<tr><td>' + esc(src) + '</td><td>' + d.total + '</td><td style="color:#1da1f2">' + d.pass_score + '</td><td style="color:#17bf63">' + d.pass_final + '</td></tr>';
      });
      html += '</tbody></table>';
    }

    resEl.innerHTML = html;
  } catch(e) {
    resEl.innerHTML = '<div style="color:#e0245e">Ошибка: ' + (e.message||e) + '</div>';
  }
}

function applyFeatureFlags() {
  // Ops dashboard
  const opsTab = document.getElementById('tab-ops');
  if (opsTab) opsTab.style.display = ffEnabled('dashboard_v2') ? '' : 'none';
  const opsPanel = document.getElementById('panel-ops');
  if (opsPanel) opsPanel.style.display = ffEnabled('dashboard_v2') ? '' : 'none';

  // Triage modes
  const triageModes = document.getElementById('triage-modes');
  if (triageModes) triageModes.style.display = ffEnabled('newsroom_triage_v1') ? '' : 'none';

  // Analytics funnel
  const funnelEl = document.getElementById('analytics-funnel');
  if (funnelEl) funnelEl.style.display = ffEnabled('analytics_funnel_v1') ? '' : 'none';

  // Storylines tab
  const slTab = document.getElementById('tab-storylines');
  if (slTab) slTab.style.display = ffEnabled('storyline_mode_v1') ? '' : 'none';
  const slPanel = document.getElementById('panel-storylines');
  if (slPanel) slPanel.style.display = ffEnabled('storyline_mode_v1') ? '' : 'none';

  // Health plus button
  const hpBtn = document.getElementById('btn-health-plus');
  if (hpBtn) hpBtn.style.display = ffEnabled('source_health_plus_v1') ? '' : 'none';
}

// ═══════════════════════════════════════════
// INIT: Load feature flags on startup
// ═══════════════════════════════════════════

(function() {
  setTimeout(function() {
    loadFeatureFlags().then(function() {
      if (ffEnabled('dashboard_v2')) {
        loadOpsDashboard();
      }
    });
  }, 500);
})();

</script>
</body>
</html>"""


def _load_active_prompts():
    """Загружает активные версии промптов из БД при старте."""
    try:
        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT prompt_name, content FROM prompt_versions WHERE is_active = 1")
            import apis.llm as llm
            prompt_map = {
                "trend_forecast": "PROMPT_TREND_FORECAST",
                "merge_analysis": "PROMPT_MERGE_ANALYSIS",
                "keyso_queries": "PROMPT_KEYSO_QUERIES",
                "rewrite": "PROMPT_REWRITE",
            }
            if _is_postgres():
                for row in cur.fetchall():
                    attr = prompt_map.get(row[0])
                    if attr and hasattr(llm, attr):
                        setattr(llm, attr, row[1])
                        logger.info("Loaded active prompt: %s", row[0])
            else:
                for row in cur.fetchall():
                    r = dict(row)
                    attr = prompt_map.get(r["prompt_name"])
                    if attr and hasattr(llm, attr):
                        setattr(llm, attr, r["content"])
                        logger.info("Loaded active prompt: %s", r["prompt_name"])
        finally:
            cur.close()
    except Exception as e:
        logger.debug("Could not load active prompts: %s", e)


def start_web():
    _load_active_prompts()
    server = HTTPServer(("0.0.0.0", PORT), AdminHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info("Admin panel running on port %d", PORT)
    return server
