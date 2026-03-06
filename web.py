import json
import os
import hashlib
import secrets
import threading
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
from http.cookies import SimpleCookie
from urllib.parse import urlparse, parse_qs

from storage.database import get_connection, _is_postgres, get_unprocessed_news, update_news_status, init_db

logger = logging.getLogger(__name__)
PORT = int(os.getenv("PORT", 8080))

# Users: {username: password_hash}
USERS = {
    "admin": hashlib.sha256("admin123".encode()).hexdigest(),
}
# Active sessions: {token: username}
SESSIONS = {}


class AdminHandler(BaseHTTPRequestHandler):

    def _get_session_user(self):
        cookie_header = self.headers.get("Cookie", "")
        cookie = SimpleCookie(cookie_header)
        token = cookie.get("session")
        if token:
            return SESSIONS.get(token.value)
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
        if path == "/login":
            self._serve_login()
            return
        if path == "/logout":
            self._do_logout()
            return
        if not self._require_auth():
            return

        routes = {
            "/": self._serve_dashboard,
            "/api/stats": lambda: self._json(self._get_stats()),
            "/api/news": lambda: self._json(self._get_news()),
            "/api/sources": lambda: self._json(self._get_sources()),
            "/api/prompts": lambda: self._json(self._get_prompts()),
            "/api/settings": lambda: self._json(self._get_settings()),
            "/api/users": lambda: self._json(self._get_users()),
        }
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
            "/api/users/add": lambda: self._add_user(body),
            "/api/users/delete": lambda: self._delete_user(body),
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
        self.end_headers()
        self.wfile.write(body)

    # --- Auth ---
    def _do_login(self, body):
        username = body.get("username", "")
        password = body.get("password", "")
        pw_hash = hashlib.sha256(password.encode()).hexdigest()
        if USERS.get(username) == pw_hash:
            token = secrets.token_hex(32)
            SESSIONS[token] = username
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Set-Cookie", f"session={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age=86400")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "ok"}).encode())
        else:
            self._json({"status": "error", "message": "Invalid credentials"}, 401)

    def _do_logout(self):
        cookie_header = self.headers.get("Cookie", "")
        cookie = SimpleCookie(cookie_header)
        token = cookie.get("session")
        if token and token.value in SESSIONS:
            del SESSIONS[token.value]
        self.send_response(302)
        self.send_header("Set-Cookie", "session=; Path=/; Max-Age=0")
        self.send_header("Location", "/login")
        self.end_headers()

    def _get_users(self):
        return [{"username": u} for u in USERS.keys()]

    def _add_user(self, body):
        username = body.get("username", "")
        password = body.get("password", "")
        if not username or not password:
            self._json({"status": "error", "message": "Username and password required"})
            return
        USERS[username] = hashlib.sha256(password.encode()).hexdigest()
        self._json({"status": "ok", "users": [{"username": u} for u in USERS.keys()]})

    def _delete_user(self, body):
        username = body.get("username", "")
        if username == "admin":
            self._json({"status": "error", "message": "Cannot delete admin"})
            return
        USERS.pop(username, None)
        # Remove their sessions
        to_remove = [t for t, u in SESSIONS.items() if u == username]
        for t in to_remove:
            del SESSIONS[t]
        self._json({"status": "ok", "users": [{"username": u} for u in USERS.keys()]})

    def _serve_login(self):
        html = """<!DOCTYPE html>
<html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>IgroNews Login</title>
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

    # --- Data ---
    def _get_stats(self):
        conn = get_connection()
        cur = conn.cursor()
        ph = "%s" if _is_postgres() else "?"
        stats = {}
        for status in ["new", "processed", "approved", "rejected"]:
            cur.execute(f"SELECT COUNT(*) FROM news WHERE status = {ph}", (status,))
            stats[status] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM news")
        stats["total"] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM news_analysis")
        stats["analyzed"] = cur.fetchone()[0]
        return stats

    def _get_news(self):
        conn = get_connection()
        cur = conn.cursor()
        qs = parse_qs(urlparse(self.path).query)
        limit = int(qs.get("limit", [100])[0])
        status_filter = qs.get("status", [None])[0]
        source_filter = qs.get("source", [None])[0]

        ph = "%s" if _is_postgres() else "?"
        conditions = []
        params = []
        if status_filter:
            conditions.append(f"n.status = {ph}")
            params.append(status_filter)
        if source_filter:
            conditions.append(f"n.source = {ph}")
            params.append(source_filter)

        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        query = f"""
            SELECT n.id, n.source, n.title, n.url, n.h1, n.description,
                   n.published_at, n.parsed_at, n.status,
                   a.bigrams, a.llm_recommendation, a.llm_trend_forecast, a.sheets_row
            FROM news n
            LEFT JOIN news_analysis a ON n.id = a.news_id
            {where}
            ORDER BY n.parsed_at DESC LIMIT {ph}
        """
        params.append(limit)
        cur.execute(query, params)

        if _is_postgres():
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
        else:
            return [dict(row) for row in cur.fetchall()]

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
            "keyso_region": config.KEYSO_REGION,
            "regions": config.REGIONS,
            "sheets_id": config.GOOGLE_SHEETS_ID,
            "sheets_tab": config.SHEETS_TAB,
            "openai_key_set": bool(config.OPENAI_API_KEY),
            "keyso_key_set": bool(config.KEYSO_API_KEY),
            "google_sa_set": bool(config.GOOGLE_SERVICE_ACCOUNT_JSON),
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
        import config
        if "llm_model" in body:
            config.LLM_MODEL = body["llm_model"]
        if "keyso_region" in body:
            config.KEYSO_REGION = body["keyso_region"]
        if "sheets_tab" in body:
            config.SHEETS_TAB = body["sheets_tab"]
        self._json({"status": "ok"})

    def _test_llm(self, body):
        try:
            import config
            from openai import OpenAI
            import json as _json
            prompt = body.get("prompt", "Ответь JSON: {\"test\": \"ok\"}")
            client = OpenAI(api_key=config.OPENAI_API_KEY)
            response = client.chat.completions.create(
                model=config.LLM_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                response_format={"type": "json_object"},
            )
            text = response.choices[0].message.content
            self._json({"status": "ok", "model": config.LLM_MODEL, "raw": text, "result": _json.loads(text)})
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
            else:
                from parsers.html_parser import parse_html_source
                count = parse_html_source(source)
            self._json({"status": "ok", "new_articles": count})
        except Exception as e:
            self._json({"status": "error", "message": str(e)})

    # --- Dashboard HTML ---
    def _serve_dashboard(self):
        html = DASHBOARD_HTML
        body = html.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
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
<style>
* { margin:0; padding:0; box-sizing:border-box; }
body { font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; background:#0f1923; color:#e1e8ed; }
.container { max-width:1400px; margin:0 auto; padding:15px; }
h1 { color:#1da1f2; font-size:1.5em; }
h2 { color:#1da1f2; font-size:1.1em; margin-bottom:10px; }
header { background:#192734; padding:12px 20px; display:flex; align-items:center; justify-content:space-between; border-bottom:1px solid #22303c; }

/* Tabs */
.tabs { display:flex; gap:0; background:#192734; border-radius:8px; margin:15px 0; overflow:hidden; }
.tab { padding:10px 20px; cursor:pointer; color:#8899a6; border:none; background:none; font-size:0.9em; transition:all .2s; }
.tab:hover { color:#e1e8ed; background:#22303c; }
.tab.active { color:#1da1f2; background:#22303c; border-bottom:2px solid #1da1f2; }
.panel { display:none; }
.panel.active { display:block; }

/* Stats */
.stats { display:flex; gap:12px; margin-bottom:20px; flex-wrap:wrap; }
.stat { background:#192734; border-radius:10px; padding:15px 20px; min-width:120px; }
.stat .num { font-size:1.8em; font-weight:bold; color:#1da1f2; }
.stat .lbl { color:#8899a6; font-size:0.8em; }
.stat.new .num { color:#ffad1f; }
.stat.proc .num { color:#17bf63; }

/* Buttons */
.btn { padding:8px 16px; border:none; border-radius:6px; cursor:pointer; font-size:0.85em; transition:all .2s; }
.btn-primary { background:#1da1f2; color:#fff; }
.btn-primary:hover { background:#1a91da; }
.btn-success { background:#17bf63; color:#fff; }
.btn-success:hover { background:#14a857; }
.btn-danger { background:#e0245e; color:#fff; }
.btn-danger:hover { background:#c81e52; }
.btn-secondary { background:#38444d; color:#e1e8ed; }
.btn-secondary:hover { background:#4a5568; }
.btn-sm { padding:4px 10px; font-size:0.8em; }
.btn-group { display:flex; gap:8px; margin-bottom:15px; flex-wrap:wrap; }

/* Table */
table { width:100%; border-collapse:collapse; background:#192734; border-radius:10px; overflow:hidden; font-size:0.85em; }
th { background:#22303c; text-align:left; padding:10px 12px; color:#8899a6; font-size:0.8em; white-space:nowrap; }
td { padding:8px 12px; border-bottom:1px solid #22303c; max-width:300px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
tr:hover { background:#22303c; }
a { color:#1da1f2; text-decoration:none; }
.badge { padding:2px 8px; border-radius:10px; font-size:0.75em; }
.badge-new { background:#ffad1f22; color:#ffad1f; }
.badge-processed { background:#17bf6322; color:#17bf63; }
.badge-approved { background:#1da1f222; color:#1da1f2; }

/* Forms */
.form-group { margin-bottom:12px; }
.form-group label { display:block; color:#8899a6; font-size:0.85em; margin-bottom:4px; }
input, select { background:#22303c; border:1px solid #38444d; color:#e1e8ed; padding:8px 12px; border-radius:6px; width:100%; font-size:0.9em; }
textarea { background:#22303c; border:1px solid #38444d; color:#e1e8ed; padding:10px; border-radius:6px; width:100%; font-size:0.85em; font-family:monospace; min-height:150px; resize:vertical; }
input:focus, textarea:focus, select:focus { outline:none; border-color:#1da1f2; }

/* Grid */
.grid-2 { display:grid; grid-template-columns:1fr 1fr; gap:15px; }
.card { background:#192734; border-radius:10px; padding:15px; }

/* Toast */
.toast { position:fixed; bottom:20px; right:20px; background:#17bf63; color:#fff; padding:12px 20px; border-radius:8px; z-index:999; display:none; font-size:0.9em; }
.toast.error { background:#e0245e; }

/* Filters */
.filters { display:flex; gap:10px; margin-bottom:15px; align-items:center; flex-wrap:wrap; }
.filters select, .filters input { width:auto; min-width:150px; }

/* Modal */
.modal-overlay { display:none; position:fixed; top:0;left:0;right:0;bottom:0; background:rgba(0,0,0,0.7); z-index:100; justify-content:center; align-items:center; }
.modal-overlay.show { display:flex; }
.modal { background:#192734; border-radius:12px; padding:25px; width:450px; max-width:90vw; }
.modal h2 { margin-bottom:15px; }
.modal-buttons { display:flex; gap:10px; margin-top:15px; justify-content:flex-end; }

/* Responsive */
@media(max-width:768px) {
  .grid-2 { grid-template-columns:1fr; }
  .stats { gap:8px; }
  .stat { min-width:80px; padding:10px; }
  .stat .num { font-size:1.4em; }
}
</style>
</head>
<body>

<header>
  <h1>IgroNews Admin</h1>
  <span style="color:#8899a6;font-size:0.85em" id="clock"></span>
</header>

<div class="container">
  <div class="tabs">
    <div class="tab active" data-tab="dashboard">Dashboard</div>
    <div class="tab" data-tab="news">News</div>
    <div class="tab" data-tab="sources">Sources</div>
    <div class="tab" data-tab="prompts">Prompts</div>
    <div class="tab" data-tab="tools">Tools</div>
    <div class="tab" data-tab="settings">Settings</div>
    <div class="tab" data-tab="users">Users</div>
    <div style="margin-left:auto"><a href="/logout" class="btn btn-secondary btn-sm">Logout</a></div>
  </div>

  <!-- DASHBOARD -->
  <div class="panel active" id="panel-dashboard">
    <div class="stats" id="stats"></div>
    <div class="btn-group">
      <button class="btn btn-primary" onclick="runProcess()">Run Process (Analyze All New)</button>
      <button class="btn btn-secondary" onclick="loadNews()">Refresh</button>
    </div>
    <table>
      <thead><tr><th>Source</th><th>Title</th><th>Published</th><th>Parsed</th><th>Status</th><th>Score</th><th>Actions</th></tr></thead>
      <tbody id="dash-news"></tbody>
    </table>
  </div>

  <!-- NEWS -->
  <div class="panel" id="panel-news">
    <div class="filters">
      <select id="filter-status" onchange="loadNews()">
        <option value="">All statuses</option>
        <option value="new">New</option>
        <option value="processed">Processed</option>
        <option value="approved">Approved</option>
        <option value="rejected">Rejected</option>
      </select>
      <select id="filter-source" onchange="loadNews()">
        <option value="">All sources</option>
      </select>
      <input type="number" id="filter-limit" value="100" min="10" max="500" style="width:80px" onchange="loadNews()">
      <button class="btn btn-secondary btn-sm" onclick="loadNews()">Filter</button>
    </div>
    <table>
      <thead><tr><th>Source</th><th>Title</th><th>H1</th><th>Published</th><th>Status</th><th>Bigrams</th><th>LLM</th><th>Score</th><th>Sheet</th><th>Actions</th></tr></thead>
      <tbody id="news-table"></tbody>
    </table>
  </div>

  <!-- SOURCES -->
  <div class="panel" id="panel-sources">
    <div class="grid-2">
      <div class="card">
        <h2>Active Sources</h2>
        <table>
          <thead><tr><th>Name</th><th>Type</th><th>URL</th><th>Interval</th><th>Selector</th><th>Actions</th></tr></thead>
          <tbody id="sources-table"></tbody>
        </table>
      </div>
      <div class="card">
        <h2>Add Source</h2>
        <div class="form-group"><label>Name</label><input id="src-name"></div>
        <div class="form-group"><label>Type</label>
          <select id="src-type" onchange="document.getElementById('src-selector-group').style.display=this.value==='html'?'block':'none'">
            <option value="rss">RSS</option><option value="html">HTML</option>
          </select>
        </div>
        <div class="form-group"><label>URL</label><input id="src-url"></div>
        <div class="form-group"><label>Interval (min)</label><input type="number" id="src-interval" value="15"></div>
        <div class="form-group" id="src-selector-group" style="display:none"><label>CSS Selector</label><input id="src-selector" placeholder=".news-item"></div>
        <button class="btn btn-primary" onclick="addSource()">Add Source</button>
      </div>
    </div>
  </div>

  <!-- PROMPTS -->
  <div class="panel" id="panel-prompts">
    <div class="card" style="margin-bottom:15px">
      <h2>Trend Forecast Prompt</h2>
      <textarea id="prompt-trend" rows="10"></textarea>
    </div>
    <div class="card" style="margin-bottom:15px">
      <h2>Merge Analysis Prompt</h2>
      <textarea id="prompt-merge" rows="8"></textarea>
    </div>
    <div class="card" style="margin-bottom:15px">
      <h2>Keys.so Queries Prompt</h2>
      <textarea id="prompt-keyso" rows="8"></textarea>
    </div>
    <button class="btn btn-primary" onclick="savePrompts()">Save Prompts</button>
  </div>

  <!-- TOOLS -->
  <div class="panel" id="panel-tools">
    <div class="grid-2">
      <div class="card">
        <h2>Test LLM (OpenAI)</h2>
        <div class="form-group"><label>Prompt</label><textarea id="test-llm-prompt" rows="4">Ты аналитик. Ответь JSON: {"test": "ok", "model": "your_model"}</textarea></div>
        <button class="btn btn-primary" onclick="testLLM()">Send</button>
        <pre id="test-llm-result" style="margin-top:10px;color:#8899a6;font-size:0.85em;white-space:pre-wrap"></pre>
      </div>
      <div class="card">
        <h2>Test Keys.so</h2>
        <div class="form-group"><label>Keyword</label><input id="test-keyso-kw" value="gta 6"></div>
        <button class="btn btn-primary" onclick="testKeyso()">Check</button>
        <pre id="test-keyso-result" style="margin-top:10px;color:#8899a6;font-size:0.85em;white-space:pre-wrap"></pre>
      </div>
    </div>
    <div class="card" style="margin-top:15px">
      <h2>Test Google Sheets</h2>
      <button class="btn btn-primary" onclick="testSheets()">Test Connection</button>
      <pre id="test-sheets-result" style="margin-top:10px;color:#8899a6;font-size:0.85em;white-space:pre-wrap"></pre>
    </div>
  </div>

  <!-- USERS -->
  <div class="panel" id="panel-users">
    <div class="grid-2">
      <div class="card">
        <h2>Users</h2>
        <table>
          <thead><tr><th>Username</th><th>Actions</th></tr></thead>
          <tbody id="users-table"></tbody>
        </table>
      </div>
      <div class="card">
        <h2>Add User</h2>
        <div class="form-group"><label>Username</label><input id="new-username"></div>
        <div class="form-group"><label>Password</label><input id="new-password" type="password"></div>
        <button class="btn btn-primary" onclick="addUser()">Add User</button>
      </div>
    </div>
  </div>

  <!-- SETTINGS -->
  <div class="panel" id="panel-settings">
    <div class="grid-2">
      <div class="card">
        <h2>General</h2>
        <div class="form-group"><label>LLM Model</label><input id="set-model"></div>
        <div class="form-group"><label>Keys.so Region</label><input id="set-keyso-region"></div>
        <div class="form-group"><label>Sheets Tab Name</label><input id="set-sheets-tab"></div>
        <button class="btn btn-primary" onclick="saveSettings()">Save</button>
      </div>
      <div class="card">
        <h2>API Status</h2>
        <div id="api-status"></div>
      </div>
    </div>
  </div>
</div>

<div class="modal-overlay" id="edit-modal">
  <div class="modal">
    <h2>Edit Source</h2>
    <input type="hidden" id="edit-old-name">
    <div class="form-group"><label>Name</label><input id="edit-name"></div>
    <div class="form-group"><label>Type</label>
      <select id="edit-type" onchange="document.getElementById('edit-selector-group').style.display=this.value==='html'?'block':'none'">
        <option value="rss">RSS</option><option value="html">HTML</option>
      </select>
    </div>
    <div class="form-group"><label>URL</label><input id="edit-url"></div>
    <div class="form-group"><label>Interval (min)</label><input type="number" id="edit-interval"></div>
    <div class="form-group" id="edit-selector-group" style="display:none"><label>CSS Selector</label><input id="edit-selector"></div>
    <div class="modal-buttons">
      <button class="btn btn-secondary" onclick="closeEditModal()">Cancel</button>
      <button class="btn btn-primary" onclick="saveEditSource()">Save</button>
    </div>
  </div>
</div>

<div class="toast" id="toast"></div>

<script>
// Tabs
document.querySelectorAll('.tab').forEach(t => t.addEventListener('click', () => {
  document.querySelectorAll('.tab').forEach(x => x.classList.remove('active'));
  document.querySelectorAll('.panel').forEach(x => x.classList.remove('active'));
  t.classList.add('active');
  document.getElementById('panel-' + t.dataset.tab).classList.add('active');
}));

function toast(msg, isError) {
  const el = document.getElementById('toast');
  el.textContent = msg;
  el.className = 'toast' + (isError ? ' error' : '');
  el.style.display = 'block';
  setTimeout(() => el.style.display = 'none', 3000);
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

// Stats
async function loadStats() {
  const s = await api('/api/stats');
  document.getElementById('stats').innerHTML =
    `<div class="stat"><div class="num">${s.total}</div><div class="lbl">Total</div></div>`+
    `<div class="stat new"><div class="num">${s.new}</div><div class="lbl">New</div></div>`+
    `<div class="stat proc"><div class="num">${s.processed}</div><div class="lbl">Processed</div></div>`+
    `<div class="stat"><div class="num">${s.analyzed||0}</div><div class="lbl">Analyzed</div></div>`+
    `<div class="stat"><div class="num">${s.approved||0}</div><div class="lbl">Approved</div></div>`;
}

// News
async function loadNews() {
  const status = document.getElementById('filter-status')?.value || '';
  const source = document.getElementById('filter-source')?.value || '';
  const limit = document.getElementById('filter-limit')?.value || 100;
  let url = `/api/news?limit=${limit}`;
  if (status) url += `&status=${status}`;
  if (source) url += `&source=${encodeURIComponent(source)}`;

  const news = await api(url);

  // Dashboard table
  const dashTb = document.getElementById('dash-news');
  if (dashTb) {
    dashTb.innerHTML = news.slice(0, 50).map(n => `<tr>
      <td>${n.source}</td>
      <td><a href="${n.url}" target="_blank" title="${esc(n.description||'')}">${esc(n.title||'')}</a></td>
      <td>${fmtDate(n.published_at)}</td>
      <td>${fmtDate(n.parsed_at)}</td>
      <td><span class="badge badge-${n.status}">${n.status}</span></td>
      <td>${n.llm_trend_forecast||'-'}</td>
      <td>
        <button class="btn btn-sm btn-primary" onclick="processOne('${n.id}')">Analyze</button>
        <button class="btn btn-sm btn-success" onclick="exportOne('${n.id}')">Sheets</button>
      </td>
    </tr>`).join('');
  }

  // News tab table
  const newsTb = document.getElementById('news-table');
  if (newsTb) {
    newsTb.innerHTML = news.map(n => {
      let bigrams = '';
      try { bigrams = JSON.parse(n.bigrams||'[]').map(b=>b[0]).join(', '); } catch(e){}
      return `<tr>
        <td>${n.source}</td>
        <td><a href="${n.url}" target="_blank">${esc(n.title||'')}</a></td>
        <td>${esc(n.h1||'')}</td>
        <td>${fmtDate(n.published_at)}</td>
        <td><span class="badge badge-${n.status}">${n.status}</span></td>
        <td title="${esc(bigrams)}">${bigrams.slice(0,40)}</td>
        <td>${esc(n.llm_recommendation||'-')}</td>
        <td>${n.llm_trend_forecast||'-'}</td>
        <td>${n.sheets_row||'-'}</td>
        <td>
          <button class="btn btn-sm btn-primary" onclick="processOne('${n.id}')">Analyze</button>
          <button class="btn btn-sm btn-success" onclick="exportOne('${n.id}')">Sheets</button>
        </td>
      </tr>`;
    }).join('');
  }

  // Populate source filter
  const srcFilter = document.getElementById('filter-source');
  if (srcFilter && srcFilter.options.length <= 1) {
    const sources = [...new Set(news.map(n => n.source))].sort();
    sources.forEach(s => { const o = document.createElement('option'); o.value = s; o.textContent = s; srcFilter.appendChild(o); });
  }
}

function esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }
function fmtDate(d) { if (!d) return '-'; return d.replace('T',' ').slice(0,16); }

// Actions
async function runProcess() {
  toast('Processing started...');
  const r = await api('/api/process', {});
  toast(r.message || 'Done');
  setTimeout(loadAll, 5000);
}

async function processOne(id) {
  toast('Analyzing...');
  const r = await api('/api/process_one', {news_id: id});
  if (r.status === 'ok') toast('Analyzed!');
  else toast(r.message, true);
  loadAll();
}

async function exportOne(id) {
  const r = await api('/api/export_sheets', {news_id: id});
  if (r.status === 'ok') toast('Exported to row ' + r.row);
  else toast(r.message, true);
}

// Sources
let _sources = [];
async function loadSources() {
  _sources = await api('/api/sources');
  document.getElementById('sources-table').innerHTML = _sources.map(s =>
    `<tr>
      <td>${s.name}</td>
      <td>${s.type}</td>
      <td style="max-width:250px;overflow:hidden;text-overflow:ellipsis" title="${esc(s.url)}">${esc(s.url)}</td>
      <td>${s.interval}min</td>
      <td>${s.selector||'-'}</td>
      <td style="white-space:nowrap">
        <button class="btn btn-sm btn-secondary" onclick="openEditModal('${esc(s.name)}')">Edit</button>
        <button class="btn btn-sm btn-primary" onclick="reparseSource('${esc(s.name)}')">Reparse</button>
        <button class="btn btn-sm btn-danger" onclick="deleteSource('${esc(s.name)}')">Delete</button>
      </td>
    </tr>`
  ).join('');
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
  toast('Source updated');
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
  if (!data.name || !data.url) { toast('Fill name and URL', true); return; }
  const r = await api('/api/sources/add', data);
  toast('Source added');
  loadSources();
}

async function deleteSource(name) {
  if (!confirm('Delete ' + name + '?')) return;
  await api('/api/sources/delete', {name});
  toast('Deleted');
  loadSources();
}

async function reparseSource(name) {
  toast('Reparsing ' + name + '...');
  const r = await api('/api/reparse', {name});
  if (r.status === 'ok') toast(name + ': ' + r.new_articles + ' new articles');
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
  toast('Prompts saved');
}

// Settings
async function loadSettings() {
  const s = await api('/api/settings');
  document.getElementById('set-model').value = s.llm_model || '';
  document.getElementById('set-keyso-region').value = s.keyso_region || '';
  document.getElementById('set-sheets-tab').value = s.sheets_tab || '';
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
  });
  toast('Settings saved');
}

// Tools
async function testLLM() {
  document.getElementById('test-llm-result').textContent = 'Loading...';
  const r = await api('/api/test_llm', {prompt: document.getElementById('test-llm-prompt').value});
  document.getElementById('test-llm-result').textContent = JSON.stringify(r, null, 2);
}

async function testKeyso() {
  document.getElementById('test-keyso-result').textContent = 'Loading...';
  const r = await api('/api/test_keyso', {keyword: document.getElementById('test-keyso-kw').value});
  document.getElementById('test-keyso-result').textContent = JSON.stringify(r, null, 2);
}

// Users
async function loadUsers() {
  const users = await api('/api/users');
  document.getElementById('users-table').innerHTML = users.map(u =>
    `<tr><td>${u.username}</td><td>${u.username==='admin'?'':'<button class="btn btn-sm btn-danger" onclick="deleteUser(\''+u.username+'\')">Delete</button>'}</td></tr>`
  ).join('');
}
async function addUser() {
  const username = document.getElementById('new-username').value;
  const password = document.getElementById('new-password').value;
  if (!username || !password) { toast('Fill username and password', true); return; }
  await api('/api/users/add', {username, password});
  toast('User added');
  loadUsers();
}
async function deleteUser(username) {
  if (!confirm('Delete user ' + username + '?')) return;
  await api('/api/users/delete', {username});
  toast('User deleted');
  loadUsers();
}

async function testSheets() {
  document.getElementById('test-sheets-result').textContent = 'Loading...';
  const r = await api('/api/test_sheets', {});
  document.getElementById('test-sheets-result').textContent = JSON.stringify(r, null, 2);
}

// Init
function loadAll() { loadStats(); loadNews(); }
loadAll();
loadSources();
loadPrompts();
loadSettings();
loadUsers();
setInterval(loadAll, 30000);
</script>
</body>
</html>"""


def start_web():
    server = HTTPServer(("0.0.0.0", PORT), AdminHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info("Admin panel running on port %d", PORT)
    return server
