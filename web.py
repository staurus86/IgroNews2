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

# Users: {username: password_hash}
USERS = {
    "admin": hashlib.sha256("admin123".encode()).hexdigest(),
}


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
            "/api/health": lambda: self._json(self._get_health()),
            "/api/dashboard_groups": lambda: self._dashboard_groups(),
            "/api/sources_stats": lambda: self._json(self._get_sources_stats()),
            "/api/db_info": lambda: self._json(self._get_db_info()),
            "/api/articles": lambda: self._json(self._get_articles()),
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
            "/api/articles/save": lambda: self._save_article(body),
            "/api/articles/update": lambda: self._update_article(body),
            "/api/articles/delete": lambda: self._delete_article(body),
            "/api/articles/rewrite": lambda: self._rewrite_article(body),
            "/api/articles/improve": lambda: self._improve_article(body),
            "/api/articles/detail": lambda: self._article_detail(body),
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
            signed = _sign_cookie(username)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Set-Cookie", f"session={signed}; Path=/; HttpOnly; SameSite=Lax; Max-Age=604800")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "ok"}).encode())
        else:
            self._json({"status": "error", "message": "Invalid credentials"}, 401)

    def _do_logout(self):
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

    def _get_health(self):
        from checks.health import get_sources_health
        return get_sources_health()

    # --- Data ---
    def _get_stats(self):
        conn = get_connection()
        cur = conn.cursor()
        ph = "%s" if _is_postgres() else "?"
        stats = {}
        for status in ["new", "in_review", "duplicate", "approved", "processed", "rejected", "ready"]:
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
        except Exception as e:
            self._json({"status": "error", "message": str(e), "type": type(e).__name__})

    def _dashboard_groups(self):
        """Возвращает теги и группы для всех new новостей."""
        try:
            conn = get_connection()
            cur = conn.cursor()
            ph = "%s" if _is_postgres() else "?"
            cur.execute(f"SELECT id, title, description, plain_text FROM news WHERE status = {ph} ORDER BY parsed_at DESC LIMIT 200", ("new",))
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
        except Exception as e:
            self._json({"status": "error", "message": str(e), "type": type(e).__name__})

    def _review_batch(self, body):
        """Проверяет новости по статусу (batch, без изменения статуса)."""
        status = body.get("status", "new")
        limit = int(body.get("limit", 50))
        try:
            conn = get_connection()
            cur = conn.cursor()
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
        except Exception as e:
            self._json({"status": "error", "message": str(e), "type": type(e).__name__})

    def _approve_news(self, body):
        """Одобряет новости для обогащения."""
        news_ids = body.get("news_ids", [])
        if not news_ids:
            self._json({"status": "error", "message": "No news selected"})
            return
        try:
            from checks.pipeline import approve_for_enrichment
            approve_for_enrichment(news_ids)
            self._json({"status": "ok", "approved": len(news_ids)})
        except Exception as e:
            self._json({"status": "error", "message": str(e)})

    def _reject_news(self, body):
        """Отклоняет новость."""
        news_id = body.get("news_id")
        if not news_id:
            self._json({"status": "error", "message": "news_id required"})
            return
        try:
            update_news_status(news_id, "rejected")
            self._json({"status": "ok"})
        except Exception as e:
            self._json({"status": "error", "message": str(e)})

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
        news_ids = body.get("news_ids", [])
        if not news_ids:
            self._json({"status": "error", "message": "news_ids required"})
            return
        conn = get_connection()
        cur = conn.cursor()
        ph = "%s" if _is_postgres() else "?"
        for nid in news_ids:
            cur.execute(f"DELETE FROM news_analysis WHERE news_id = {ph}", (nid,))
            cur.execute(f"DELETE FROM news WHERE id = {ph}", (nid,))
        conn.commit()
        self._json({"status": "ok", "deleted": len(news_ids)})

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
        cur.execute("SELECT source, COUNT(*) as cnt, MAX(parsed_at) as last_parsed FROM news GROUP BY source ORDER BY cnt DESC")
        if _is_postgres():
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
        else:
            return [dict(row) for row in cur.fetchall()]

    def _get_db_info(self):
        conn = get_connection()
        cur = conn.cursor()
        info = {"type": "PostgreSQL" if _is_postgres() else "SQLite"}
        cur.execute("SELECT COUNT(*) FROM news")
        info["total_news"] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM news_analysis")
        info["total_analyzed"] = cur.fetchone()[0]
        for status in ["new", "approved", "processed", "rejected"]:
            ph = "%s" if _is_postgres() else "?"
            cur.execute(f"SELECT COUNT(*) FROM news WHERE status = {ph}", (status,))
            info[f"status_{status}"] = cur.fetchone()[0]
        cur.execute("SELECT MIN(parsed_at), MAX(parsed_at) FROM news")
        row = cur.fetchone()
        info["oldest"] = str(row[0]) if row[0] else "-"
        info["newest"] = str(row[1]) if row[1] else "-"
        return info

    def _export_sheets_bulk(self, body):
        news_ids = body.get("news_ids", [])
        if not news_ids:
            self._json({"status": "error", "message": "news_ids required"})
            return
        try:
            from storage.sheets import write_news_row
            conn = get_connection()
            cur = conn.cursor()
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
        except Exception as e:
            self._json({"status": "error", "message": str(e)})

    def _rewrite_news(self, body):
        news_id = body.get("news_id")
        style = body.get("style", "news")
        language = body.get("language", "русский")
        try:
            conn = get_connection()
            cur = conn.cursor()
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
        except Exception as e:
            self._json({"status": "error", "message": str(e)})

    def _news_detail(self, body):
        news_id = body.get("news_id")
        if not news_id:
            self._json({"status": "error", "message": "news_id required"})
            return
        conn = get_connection()
        cur = conn.cursor()
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

    def _analyze_news(self, body):
        """Полный анализ одной новости: viral, freshness, quality, relevance, sentiment, tags, trends, keyso."""
        news_id = body.get("news_id")
        if not news_id:
            self._json({"status": "error", "message": "news_id required"})
            return
        conn = get_connection()
        cur = conn.cursor()
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

    # --- Articles ---
    def _get_articles(self):
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM articles ORDER BY updated_at DESC")
        if _is_postgres():
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
        return [dict(row) for row in cur.fetchall()]

    def _save_article(self, body):
        import uuid
        from datetime import datetime, timezone
        aid = str(uuid.uuid4())[:12]
        now = datetime.now(timezone.utc).isoformat()
        conn = get_connection()
        cur = conn.cursor()
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

    def _update_article(self, body):
        from datetime import datetime, timezone
        aid = body.get("id")
        if not aid:
            self._json({"status": "error", "message": "id required"})
            return
        now = datetime.now(timezone.utc).isoformat()
        conn = get_connection()
        cur = conn.cursor()
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

    def _delete_article(self, body):
        aid = body.get("id")
        if not aid:
            self._json({"status": "error", "message": "id required"})
            return
        conn = get_connection()
        cur = conn.cursor()
        ph = "%s" if _is_postgres() else "?"
        cur.execute(f"DELETE FROM articles WHERE id = {ph}", (aid,))
        if not _is_postgres():
            conn.commit()
        self._json({"status": "ok"})

    def _article_detail(self, body):
        aid = body.get("id")
        if not aid:
            self._json({"status": "error", "message": "id required"})
            return
        conn = get_connection()
        cur = conn.cursor()
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

    def _rewrite_article(self, body):
        """Переписать существующую статью в другом стиле."""
        aid = body.get("id")
        style = body.get("style", "news")
        language = body.get("language", "русский")
        conn = get_connection()
        cur = conn.cursor()
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

    def _improve_article(self, body):
        """Улучшить текст статьи через LLM (грамматика, стиль, SEO)."""
        aid = body.get("id")
        action = body.get("action", "improve")  # improve, expand, shorten, fix_grammar, add_seo
        conn = get_connection()
        cur = conn.cursor()
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

    def _serve_docx(self, article_id):
        """Генерация и отдача DOCX файла."""
        conn = get_connection()
        cur = conn.cursor()
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
header { background:linear-gradient(135deg,#192734 0%,#1a3a4a 100%); padding:12px 20px; display:flex; align-items:center; justify-content:space-between; border-bottom:1px solid #22303c; box-shadow:0 2px 8px rgba(0,0,0,0.3); }

/* Tabs */
.tabs { display:flex; gap:0; background:#192734; border-radius:8px; margin:15px 0; overflow:hidden; box-shadow:0 1px 4px rgba(0,0,0,0.2); }
.tab { padding:10px 20px; cursor:pointer; color:#8899a6; border:none; background:none; font-size:0.9em; transition:all .2s; position:relative; }
.tab:hover { color:#e1e8ed; background:#22303c; }
.tab.active { color:#1da1f2; background:#22303c; border-bottom:2px solid #1da1f2; }

.panel { display:none; animation:fadeIn .3s; }
.panel.active { display:block; }
@keyframes fadeIn { from{opacity:0;transform:translateY(6px)} to{opacity:1;transform:translateY(0)} }

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
@keyframes spin { to { transform:rotate(360deg); } }
.btn-group { display:flex; gap:8px; margin-bottom:15px; flex-wrap:wrap; align-items:center; }
.btn-warning { background:#ffad1f; color:#000; }
.btn-warning:hover { background:#e69d1c; }

/* Table */
table { width:100%; border-collapse:collapse; background:#192734; border-radius:10px; overflow:hidden; font-size:0.85em; }
th { background:#22303c; text-align:left; padding:10px 12px; color:#8899a6; font-size:0.8em; white-space:nowrap; position:sticky; top:0; z-index:2; user-select:none; }
th.sortable { cursor:pointer; transition:color .2s; }
th.sortable:hover { color:#1da1f2; }
th.sortable .sort-arrow { margin-left:3px; font-size:0.75em; opacity:0.4; }
th.sortable.sort-active .sort-arrow { opacity:1; color:#1da1f2; }
td { padding:8px 12px; border-bottom:1px solid #22303c; max-width:300px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
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
</style>
</head>
<body>

<header>
  <h1>IgroNews Admin</h1>
  <span style="color:#8899a6;font-size:0.85em" id="clock"></span>
</header>

<div class="container">
  <div class="tabs">
    <div class="tab active" data-tab="dashboard">Дашборд</div>
    <div class="tab" data-tab="review">Проверка <span id="review-badge" class="badge badge-new" style="display:none">0</span></div>
    <div class="tab" data-tab="news">Новости</div>
    <div class="tab" data-tab="sources">Источники</div>
    <div class="tab" data-tab="prompts">Промпты</div>
    <div class="tab" data-tab="tools">Инструменты</div>
    <div class="tab" data-tab="editor">Редактор</div>
    <div class="tab" data-tab="articles">Статьи <span id="articles-badge" class="badge badge-new" style="display:none">0</span></div>
    <div class="tab" data-tab="health">Здоровье</div>
    <div class="tab" data-tab="settings">Настройки</div>
    <div class="tab" data-tab="users">Пользователи</div>
    <div style="margin-left:auto"><a href="/logout" class="btn btn-secondary btn-sm">Выйти</a></div>
  </div>

  <!-- DASHBOARD -->
  <div class="panel active" id="panel-dashboard">
    <div class="stats" id="stats"></div>

    <!-- Dashboard Filters -->
    <div class="dash-filters">
      <span class="filter-label">Поиск:</span>
      <input type="search" id="dash-search" placeholder="По заголовку..." oninput="applyDashFilters()" autocomplete="off" name="dash-search-nologin">
      <span class="filter-sep"></span>
      <span class="filter-label">Источник:</span>
      <select id="dash-source" onchange="applyDashFilters()">
        <option value="">Все</option>
      </select>
      <span class="filter-sep"></span>
      <span class="filter-label">Статус:</span>
      <select id="dash-status" onchange="applyDashFilters()">
        <option value="">Все</option>
        <option value="new">Новые</option>
        <option value="in_review">На проверке</option>
        <option value="approved">Одобрены</option>
        <option value="processed">Обогащены</option>
        <option value="duplicate">Дубликаты</option>
        <option value="rejected">Отклонены</option>
      </select>
      <span class="filter-sep"></span>
      <span class="filter-label">Тег:</span>
      <select id="dash-tag" onchange="applyDashFilters()">
        <option value="">Все</option>
        <option value="release">Release</option>
        <option value="update">Update/Patch</option>
        <option value="announcement">Announcement</option>
        <option value="esports">Esports</option>
        <option value="hardware">Hardware</option>
        <option value="controversy">Controversy</option>
        <option value="rumor">Rumor/Leak</option>
        <option value="review">Review</option>
        <option value="industry">Industry</option>
      </select>
      <span class="filter-sep"></span>
      <span class="filter-label">С:</span>
      <input type="date" id="dash-date-from" onchange="applyDashFilters()">
      <span class="filter-label">По:</span>
      <input type="date" id="dash-date-to" onchange="applyDashFilters()">
      <button class="btn btn-sm btn-secondary" onclick="resetDashFilters()">Сбросить</button>
    </div>
    <div class="active-filters" id="active-filters"></div>

    <!-- Action buttons -->
    <div class="btn-group">
      <button class="btn btn-primary" onclick="sendToReview()">Отправить на проверку</button>
      <button class="btn btn-success" onclick="runProcess()">Обогатить одобренные</button>
      <button class="btn btn-warning" onclick="loadDashboardGroups()">Найти группы</button>
      <button class="btn btn-secondary" onclick="selectAll()">Выбрать все</button>
      <button class="btn btn-secondary" onclick="deselectAll()">Снять выбор</button>
      <button class="btn btn-secondary" onclick="selectGroup()">Выбрать группу</button>
      <button class="btn btn-success btn-sm" onclick="exportSelectedToSheetsDash()" title="Экспорт выбранных в Google Sheets">В Sheets</button>
      <span id="selected-count" style="color:#1da1f2;font-size:0.9em;font-weight:500;margin-left:6px"></span>
    </div>
    <div id="groups-summary" style="display:none;margin-bottom:12px"></div>

    <div class="table-info">
      <span id="dash-table-count"></span>
      <span id="dash-showing"></span>
    </div>
    <table id="dash-table">
      <thead><tr>
        <th style="width:30px"><input type="checkbox" id="check-all" onchange="toggleAll(this)"></th>
        <th class="sortable" data-sort="source" onclick="sortDash('source')">Источник <span class="sort-arrow">&#9650;</span></th>
        <th class="sortable" data-sort="title" onclick="sortDash('title')">Заголовок <span class="sort-arrow">&#9650;</span></th>
        <th>Теги</th>
        <th class="sortable" data-sort="group" onclick="sortDash('group')">Группа <span class="sort-arrow">&#9650;</span></th>
        <th class="sortable" data-sort="published_at" onclick="sortDash('published_at')">Опубл. <span class="sort-arrow">&#9650;</span></th>
        <th class="sortable" data-sort="parsed_at" onclick="sortDash('parsed_at')">Собр. <span class="sort-arrow">&#9650;</span></th>
        <th class="sortable" data-sort="status" onclick="sortDash('status')">Статус <span class="sort-arrow">&#9650;</span></th>
        <th class="sortable" data-sort="score" onclick="sortDash('score')">Скор <span class="sort-arrow">&#9650;</span></th>
        <th>Действия</th>
      </tr></thead>
      <tbody id="dash-news"></tbody>
    </table>
    <div id="dash-empty" class="empty-state" style="display:none">
      <div class="empty-icon">&#128270;</div>
      <div>Нет новостей по заданным фильтрам</div>
    </div>

  </div>

  <!-- REVIEW -->
  <div class="panel" id="panel-review">
    <div class="dash-filters" style="margin-bottom:12px">
      <span class="filter-label">Статус:</span>
      <select id="rev-status" onchange="loadReviewTab()">
        <option value="new" selected>Новые</option>
        <option value="in_review">На проверке</option>
        <option value="">Все</option>
        <option value="approved">Одобрены</option>
        <option value="duplicate">Дубликаты</option>
        <option value="rejected">Отклонены</option>
      </select>
      <span class="filter-sep"></span>
      <span class="filter-label">Кол-во:</span>
      <select id="rev-limit" onchange="loadReviewTab()">
        <option value="50" selected>50</option>
        <option value="100">100</option>
        <option value="200">200</option>
      </select>
      <span class="filter-sep"></span>
      <button class="btn btn-sm btn-primary" onclick="loadReviewTab()">Проверить</button>
      <span class="filter-sep"></span>
      <span class="filter-label">Поиск:</span>
      <input type="search" id="rev-search" placeholder="По заголовку..." oninput="filterReviewTable()" autocomplete="off" name="rev-search-nologin">
      <span class="filter-sep"></span>
      <span class="filter-label">Тональность:</span>
      <select id="rev-sentiment" onchange="filterReviewTable()">
        <option value="">Все</option>
        <option value="positive">Позитивная</option>
        <option value="neutral">Нейтральная</option>
        <option value="negative">Негативная</option>
      </select>
      <span class="filter-sep"></span>
      <span class="filter-label">Мин. скор:</span>
      <input type="number" id="rev-min-score" placeholder="0" min="0" max="100" style="width:70px" oninput="filterReviewTable()" autocomplete="off">
      <span id="rev-loading" style="color:#8899a6;font-size:0.85em;margin-left:8px"></span>
    </div>
    <div class="btn-group">
      <button class="btn btn-success" onclick="approveSelected()">Одобрить выбранные</button>
      <button class="btn btn-danger" onclick="rejectSelected()">Отклонить выбранные</button>
      <button class="btn btn-secondary" onclick="toggleApproveAllPassed()">Выбрать прошедшие</button>
      <span id="review-count" style="color:#8899a6;font-size:0.9em;margin-left:10px"></span>
    </div>
    <div id="review-groups" style="margin-bottom:12px"></div>
    <table id="review-main-table">
      <thead><tr>
        <th><input type="checkbox" id="approve-all" onchange="toggleApproveAll(this)"></th>
        <th class="sortable" data-sort="title" onclick="sortReview('title')">Заголовок <span class="sort-arrow">&#9650;</span></th>
        <th class="sortable" data-sort="source" onclick="sortReview('source')">Источник <span class="sort-arrow">&#9650;</span></th>
        <th>Дедуп</th>
        <th class="sortable" data-sort="quality" onclick="sortReview('quality')">Качество <span class="sort-arrow">&#9650;</span></th>
        <th class="sortable" data-sort="relevance" onclick="sortReview('relevance')">Релев. <span class="sort-arrow">&#9650;</span></th>
        <th class="sortable" data-sort="freshness" onclick="sortReview('freshness')">Свежесть <span class="sort-arrow">&#9650;</span></th>
        <th class="sortable" data-sort="viral" onclick="sortReview('viral')">Вирал. <span class="sort-arrow">&#9650;</span></th>
        <th>Тональн.</th>
        <th>Теги</th>
        <th class="sortable" data-sort="total_score" onclick="sortReview('total_score')">Итог <span class="sort-arrow">&#9650;</span></th>
        <th>Ок</th>
        <th>Действия</th>
      </tr></thead>
      <tbody id="review-table"></tbody>
    </table>
    <div id="review-empty" class="empty-state" style="display:none">
      <div class="empty-icon">&#128203;</div>
      <div>Нажмите «Проверить» чтобы загрузить и оценить новости</div>
    </div>
  </div>

  <!-- EDITOR -->
  <div class="panel" id="panel-editor">
    <style>
      .editor-layout { display:grid; grid-template-columns:340px 1fr; gap:15px; }
      .editor-list-card { background:#192734; border-radius:10px; padding:15px; min-height:600px; display:flex; flex-direction:column; }
      .editor-main { display:flex; flex-direction:column; gap:15px; }
      .editor-toolbar { background:#192734; border-radius:10px; padding:15px; }
      .editor-preview-card { background:#192734; border-radius:10px; padding:15px; flex:1; }
      .editor-result-card { background:#192734; border-radius:10px; padding:15px; }
      .editor-news-item { padding:10px 12px; border-bottom:1px solid #22303c; cursor:pointer; display:flex; gap:8px; align-items:start; transition:background .15s; }
      .editor-news-item input[type="checkbox"] { width:16px; height:16px; min-width:16px; flex-shrink:0; }
      .editor-news-item:hover { background:#22303c; }
      .editor-news-item.selected { background:rgba(29,161,242,0.1); border-left:3px solid #1da1f2; }
      .editor-news-item.merge { background:rgba(255,173,31,0.1); border-left:3px solid #ffad1f; }
      .editor-news-item.selected.merge { border-left:3px solid #1da1f2; box-shadow:inset -3px 0 0 #ffad1f; }
      .style-option { display:flex; align-items:center; gap:10px; padding:8px 12px; border-radius:8px; cursor:pointer; border:2px solid #22303c; background:#192734; color:#e1e8ed; transition:all .15s; font-family:inherit; }
      .style-option:hover { border-color:#38444d; background:#22303c; }
      .style-option.active { border-color:#1da1f2; background:rgba(29,161,242,0.12); }
      .style-icon { font-size:1.3em; width:32px; text-align:center; }
      .style-label { font-size:0.85em; color:#e1e8ed; font-weight:500; }
      .style-desc { font-size:0.75em; color:#8899a6; }
      .merge-counter { display:inline-flex; align-items:center; gap:6px; padding:4px 12px; background:#22303c; border-radius:20px; font-size:0.85em; color:#ffad1f; }
      .rw-field { margin-bottom:10px; padding:10px 14px; background:#22303c; border-radius:8px; position:relative; }
      .rw-field:hover .rw-copy-btn { opacity:1; }
      .rw-field-label { font-size:0.75em; color:#8899a6; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:4px; }
      .rw-field-value { color:#e1e8ed; font-size:0.9em; line-height:1.5; }
      .rw-copy-btn { position:absolute; top:8px; right:8px; opacity:0; transition:opacity .15s; background:#1da1f2; border:none; color:#fff; padding:3px 8px; border-radius:4px; font-size:0.75em; cursor:pointer; }
      .rw-copy-btn:hover { background:#1a91da; }
      @media(max-width:900px) { .editor-layout { grid-template-columns:1fr; } .editor-list-card { min-height:300px; } }
    </style>

    <div class="editor-layout">
      <!-- LEFT: news list -->
      <div class="editor-list-card">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px">
          <h2 style="margin:0;font-size:1em">Новости</h2>
          <div class="merge-counter" id="merge-counter" style="display:none">
            <span id="merge-count-text">0 для слияния</span>
            <button onclick="clearMergeSelection()" style="background:none;border:none;color:#ffad1f;cursor:pointer;font-size:1em;padding:0" title="Очистить выбор">&#10005;</button>
          </div>
        </div>
        <div style="display:flex;gap:6px;margin-bottom:10px">
          <input type="search" id="editor-search" placeholder="Поиск..." oninput="filterEditorNews()" autocomplete="off" name="editor-search-nologin" style="flex:1;padding:6px 10px;background:#22303c;border:1px solid #38444d;border-radius:6px;color:#e1e8ed;font-size:0.85em">
          <select id="editor-source-filter" onchange="filterEditorNews()" style="padding:6px 8px;background:#22303c;border:1px solid #38444d;border-radius:6px;color:#e1e8ed;font-size:0.85em">
            <option value="">Все</option>
          </select>
          <select id="editor-status-filter" onchange="filterEditorNews()" style="padding:6px 8px;background:#22303c;border:1px solid #38444d;border-radius:6px;color:#e1e8ed;font-size:0.85em">
            <option value="">Статус</option>
            <option value="new">Новые</option>
            <option value="approved">Одобренные</option>
            <option value="rejected">Отклонённые</option>
          </select>
        </div>
        <div id="editor-news-list" style="flex:1;overflow-y:auto;font-size:0.85em;margin:0 -15px;padding:0 15px"></div>
        <div id="editor-list-count" style="text-align:center;color:#8899a6;font-size:0.75em;margin-top:8px;padding-top:8px;border-top:1px solid #22303c"></div>
      </div>

      <!-- RIGHT: toolbar + preview + result -->
      <div class="editor-main">
        <!-- Toolbar -->
        <div class="editor-toolbar">
          <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap">
            <div style="display:flex;gap:6px;flex-wrap:wrap;flex:1" id="style-buttons">
              <button class="style-option active" data-style="news" onclick="selectStyle(this)" title="Факты, без эмоций, кратко">
                <span class="style-icon">&#128240;</span>
                <div><div class="style-label">Новость</div></div>
              </button>
              <button class="style-option" data-style="seo" onclick="selectStyle(this)" title="Ключевые слова, структура, подзаголовки">
                <span class="style-icon">&#128269;</span>
                <div><div class="style-label">SEO</div></div>
              </button>
              <button class="style-option" data-style="review" onclick="selectStyle(this)" title="С мнением автора, подробный анализ">
                <span class="style-icon">&#128221;</span>
                <div><div class="style-label">Обзор</div></div>
              </button>
              <button class="style-option" data-style="clickbait" onclick="selectStyle(this)" title="Яркий заголовок, интрига, эмоции">
                <span class="style-icon">&#128293;</span>
                <div><div class="style-label">Кликбейт</div></div>
              </button>
              <button class="style-option" data-style="short" onclick="selectStyle(this)" title="2-3 предложения, только суть">
                <span class="style-icon">&#9889;</span>
                <div><div class="style-label">Кратко</div></div>
              </button>
              <button class="style-option" data-style="social" onclick="selectStyle(this)" title="Неформальный, с эмодзи, короткий">
                <span class="style-icon">&#128242;</span>
                <div><div class="style-label">Соцсети</div></div>
              </button>
            </div>
            <select id="rewrite-lang" style="padding:6px 10px;background:#22303c;border:1px solid #38444d;border-radius:6px;color:#e1e8ed;font-size:0.85em">
              <option value="русский">RU</option>
              <option value="английский">EN</option>
            </select>
            <button class="btn btn-primary" onclick="rewriteNews()" id="rewrite-btn" disabled style="white-space:nowrap">
              &#9998; Переписать
            </button>
            <button class="btn btn-warning" onclick="mergeSelected()" id="merge-btn" disabled style="white-space:nowrap">
              &#128279; Объединить
            </button>
            <button class="btn btn-secondary" onclick="analyzeEditorNews()" id="analyze-btn" disabled style="white-space:nowrap">
              &#128202; Анализ
            </button>
            <span id="rewrite-loading" style="color:#8899a6;font-size:0.85em"></span>
          </div>
        </div>

        <!-- Preview + Result in same card with tabs -->
        <div class="editor-preview-card" style="display:flex;flex-direction:column">
          <div style="display:flex;align-items:center;gap:0;margin-bottom:12px;border-bottom:1px solid #22303c;padding-bottom:0">
            <button class="editor-view-tab active" id="tab-preview-btn" onclick="switchEditorView('preview')" style="padding:8px 16px;background:none;border:none;border-bottom:2px solid #1da1f2;color:#1da1f2;cursor:pointer;font-size:0.9em;font-weight:500">Оригинал</button>
            <button class="editor-view-tab" id="tab-result-btn" onclick="switchEditorView('result')" style="padding:8px 16px;background:none;border:none;border-bottom:2px solid transparent;color:#8899a6;cursor:pointer;font-size:0.9em">Результат</button>
            <div style="flex:1"></div>
            <div id="rw-copy-buttons" style="display:none;gap:6px">
              <button class="btn btn-sm btn-success" onclick="saveRewriteAsArticle()" title="Сохранить в Статьи">&#128190; В статьи</button>
              <button class="btn btn-sm btn-secondary" onclick="copyRewrite()" title="Заголовок + текст">&#128203; Текст</button>
              <button class="btn btn-sm btn-secondary" onclick="copyRewriteSeo()" title="SEO-поля">SEO</button>
              <button class="btn btn-sm btn-secondary" onclick="copyRewriteJson()" title="Весь JSON">{}</button>
              <button class="btn btn-sm btn-secondary" onclick="copyRewriteHtml()" title="Как HTML">&lt;/&gt;</button>
            </div>
          </div>

          <!-- Preview view -->
          <div id="editor-view-preview" style="flex:1;overflow-y:auto;color:#8899a6;font-size:0.9em">
            <div style="text-align:center;padding:60px 20px">
              <div style="font-size:2em;margin-bottom:10px;opacity:0.3">&#128196;</div>
              <div>Выберите новость из списка слева</div>
              <div style="font-size:0.85em;margin-top:6px">Используйте чекбоксы для выбора нескольких новостей на объединение</div>
            </div>
          </div>

          <!-- Result view -->
          <div id="editor-view-result" style="display:none;flex:1;overflow-y:auto">
            <div id="rw-empty" style="text-align:center;padding:60px 20px;color:#8899a6">
              <div style="font-size:2em;margin-bottom:10px;opacity:0.3">&#9998;</div>
              <div>Нажмите «Переписать» или «Объединить»</div>
            </div>
            <div id="rewrite-result" style="display:none">
              <div class="rw-field">
                <div class="rw-field-label">Заголовок</div>
                <div class="rw-field-value" id="rw-title" style="color:#1da1f2;font-size:1.05em;font-weight:500"></div>
                <button class="rw-copy-btn" onclick="copyField('rw-title')">Копировать</button>
              </div>

              <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">
                <div class="rw-field">
                  <div class="rw-field-label">SEO Title <span id="rw-seo-title-len" style="color:#657786"></span></div>
                  <div class="rw-field-value" id="rw-seo-title" style="color:#17bf63"></div>
                  <button class="rw-copy-btn" onclick="copyField('rw-seo-title')">Копировать</button>
                </div>
                <div class="rw-field">
                  <div class="rw-field-label">Meta Description <span id="rw-seo-desc-len" style="color:#657786"></span></div>
                  <div class="rw-field-value" id="rw-seo-desc" style="color:#8899a6"></div>
                  <button class="rw-copy-btn" onclick="copyField('rw-seo-desc')">Копировать</button>
                </div>
              </div>

              <div class="rw-field" id="rw-tags-wrap">
                <div class="rw-field-label">Теги</div>
                <div class="rw-field-value" id="rw-tags"></div>
                <button class="rw-copy-btn" onclick="copyField('rw-tags')">Копировать</button>
              </div>

              <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:4px">
                <div>
                  <div style="font-size:0.75em;color:#8899a6;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:6px">Переписанный текст</div>
                  <div id="rw-text" style="white-space:pre-wrap;color:#e1e8ed;font-size:0.88em;line-height:1.6;padding:14px;background:#22303c;border-radius:8px;max-height:400px;overflow-y:auto"></div>
                </div>
                <div>
                  <div style="font-size:0.75em;color:#8899a6;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:6px">Оригинал</div>
                  <div id="rw-original" style="white-space:pre-wrap;color:#8899a6;font-size:0.83em;line-height:1.5;padding:14px;background:#22303c;border-radius:8px;max-height:400px;overflow-y:auto"></div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>

  <!-- ARTICLES -->
  <div class="panel" id="panel-articles">
    <style>
      .art-card { background:#192734; border-radius:10px; padding:16px; margin-bottom:12px; transition:box-shadow .15s; cursor:pointer; border-left:3px solid transparent; }
      .art-card:hover { box-shadow:0 2px 12px rgba(0,0,0,0.2); }
      .art-card.selected { border-left-color:#1da1f2; background:#1da1f215; }
      .art-status { display:inline-block; padding:2px 8px; border-radius:10px; font-size:0.75em; font-weight:500; }
      .art-status-draft { background:#38444d; color:#8899a6; }
      .art-status-ready { background:#17bf6320; color:#17bf63; }
      .art-status-published { background:#1da1f220; color:#1da1f2; }
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

    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px">
      <h2 style="margin:0">Мои статьи</h2>
      <div style="display:flex;gap:8px;align-items:center">
        <input type="search" id="art-search" placeholder="Поиск..." oninput="filterArticles()" autocomplete="off" style="padding:6px 10px;background:#22303c;border:1px solid #38444d;border-radius:6px;color:#e1e8ed;font-size:0.85em;width:200px">
        <select id="art-status-filter" onchange="filterArticles()" style="padding:6px;background:#22303c;border:1px solid #38444d;border-radius:6px;color:#e1e8ed;font-size:0.85em">
          <option value="">Все</option>
          <option value="draft">Черновики</option>
          <option value="ready">Готовые</option>
          <option value="published">Опубликованные</option>
        </select>
        <span id="art-count" style="color:#8899a6;font-size:0.82em"></span>
      </div>
    </div>

    <div style="display:grid;grid-template-columns:360px 1fr;gap:15px" id="articles-layout">
      <!-- Left: list -->
      <div>
        <div id="articles-list" style="max-height:calc(100vh - 200px);overflow-y:auto"></div>
      </div>

      <!-- Right: editor -->
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
                <option value="published">Опубликовано</option>
              </select>
            </div>
            <div style="display:flex;gap:6px">
              <button class="btn btn-sm btn-success" onclick="saveCurrentArticle()">Сохранить</button>
              <button class="btn btn-sm btn-primary" onclick="downloadArticleDocx()">DOCX</button>
              <button class="btn btn-sm btn-secondary" onclick="copyArticleText()">Копировать</button>
              <button class="btn btn-sm" style="background:#e0245e;color:#fff" onclick="deleteCurrentArticle()">Удалить</button>
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
              <button class="art-improve-btn" onclick="rewriteArticleInStyle('news')">Новость</button>
              <button class="art-improve-btn" onclick="rewriteArticleInStyle('seo')">SEO</button>
              <button class="art-improve-btn" onclick="rewriteArticleInStyle('clickbait')">Кликбейт</button>
              <button class="art-improve-btn" onclick="rewriteArticleInStyle('social')">Соцсети</button>
              <button class="art-improve-btn" onclick="rewriteArticleInStyle('short')">Кратко</button>
            </div>
            <div id="art-ai-loading" style="display:none;margin-top:8px;font-size:0.85em;color:#8899a6">
              <span class="spinner" style="width:14px;height:14px;border:2px solid #38444d;border-top-color:#1da1f2;border-radius:50%;animation:spin .8s linear infinite;display:inline-block;vertical-align:middle"></span>
              <span id="art-ai-loading-text">Обрабатываем...</span>
            </div>
            <div id="art-ai-changes" style="display:none;margin-top:8px;padding:8px 12px;background:#17bf6315;border-radius:6px;font-size:0.83em;color:#17bf63"></div>
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

  <!-- HEALTH -->
  <div class="panel" id="panel-health">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
      <h2>Здоровье источников (24ч)</h2>
      <div>
        <span id="health-summary" style="color:#8899a6;font-size:0.85em;margin-right:12px"></span>
        <button class="btn btn-sm btn-secondary" onclick="loadHealth()">Обновить</button>
      </div>
    </div>
    <table>
      <thead><tr><th>Статус</th><th>Источник</th><th>Статей (24ч)</th><th style="min-width:120px">Активность</th><th>Последний парсинг</th><th>Минут назад</th></tr></thead>
      <tbody id="health-table"></tbody>
    </table>
  </div>

  <!-- NEWS -->
  <div class="panel" id="panel-news">
    <div class="dash-filters">
      <span class="filter-label">Поиск:</span>
      <input type="search" id="news-search" placeholder="По заголовку..." oninput="filterNewsTable()" autocomplete="off" name="news-search-nologin">
      <span class="filter-sep"></span>
      <span class="filter-label">Статус:</span>
      <select id="filter-status" onchange="loadNews()">
        <option value="">Все статусы</option>
        <option value="new">Новые</option>
        <option value="in_review">На проверке</option>
        <option value="duplicate">Дубликаты</option>
        <option value="approved">Одобрены</option>
        <option value="processed">Обогащены</option>
        <option value="rejected">Отклонены</option>
        <option value="ready">Готовы</option>
      </select>
      <span class="filter-sep"></span>
      <span class="filter-label">Источник:</span>
      <select id="filter-source" onchange="loadNews()">
        <option value="">Все источники</option>
      </select>
      <span class="filter-sep"></span>
      <span class="filter-label">Кол-во:</span>
      <input type="number" id="filter-limit" value="100" min="10" max="500" style="width:80px" onchange="loadNews()" autocomplete="off">
    </div>
    <div class="btn-group">
      <button class="btn btn-secondary btn-sm" onclick="loadNews()">Обновить</button>
      <button class="btn btn-warning btn-sm" onclick="bulkStatusChange('approved')">Одобрить выбранные</button>
      <button class="btn btn-danger btn-sm" onclick="bulkStatusChange('rejected')">Отклонить выбранные</button>
      <button class="btn btn-danger btn-sm" onclick="deleteSelectedNews()" style="margin-left:4px">Удалить выбранные</button>
      <button class="btn btn-success btn-sm" onclick="exportSelectedToSheets()">В Sheets</button>
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
        <th>LLM</th>
        <th class="sortable" data-sort="score" onclick="sortNewsTab('score')">Скор <span class="sort-arrow">&#9650;</span></th>
        <th>Лист</th>
        <th>Действия</th>
      </tr></thead>
      <tbody id="news-table"></tbody>
    </table>
  </div>

  <!-- SOURCES -->
  <div class="panel" id="panel-sources">
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

  <!-- PROMPTS -->
  <div class="panel" id="panel-prompts">
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

  <!-- TOOLS -->
  <div class="panel" id="panel-tools">
    <div class="grid-2">
      <div class="card">
        <h2>Тест LLM</h2>
        <div class="form-group"><label>Промпт</label><textarea id="test-llm-prompt" rows="4">Ты аналитик. Ответь JSON: {"test": "ok", "model": "your_model"}</textarea></div>
        <button class="btn btn-primary" onclick="testLLM()">Отправить</button>
        <pre id="test-llm-result" style="margin-top:10px;color:#8899a6;font-size:0.85em;white-space:pre-wrap"></pre>
      </div>
      <div class="card">
        <h2>Тест Keys.so</h2>
        <div class="form-group"><label>Ключевое слово</label><input id="test-keyso-kw" value="gta 6"></div>
        <button class="btn btn-primary" onclick="testKeyso()">Проверить</button>
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

  <!-- USERS -->
  <div class="panel" id="panel-users">
    <div class="grid-2">
      <div class="card">
        <h2>Пользователи</h2>
        <table>
          <thead><tr><th>Логин</th><th>Действия</th></tr></thead>
          <tbody id="users-table"></tbody>
        </table>
      </div>
      <div class="card">
        <h2>Добавить пользователя</h2>
        <div class="form-group"><label>Логин</label><input id="new-username" autocomplete="off"></div>
        <div class="form-group"><label>Пароль</label><input id="new-password" type="password" autocomplete="new-password"></div>
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

  <!-- SETTINGS -->
  <div class="panel" id="panel-settings">
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
          <button class="btn btn-primary" onclick="runProcess()">Обогатить одобренные</button>
          <button class="btn btn-warning" onclick="reparseAll()">Парсить все источники</button>
          <button class="btn btn-secondary" onclick="setupHeaders()">Создать заголовки Sheets</button>
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

// Stats — clickable cards filter by status
async function loadStats() {
  const s = await api('/api/stats');
  const items = [
    {key:'',     num:s.total,         lbl:'Всего',       cls:''},
    {key:'new',  num:s.new,           lbl:'Новые',       cls:'new'},
    {key:'in_review', num:s.in_review||0, lbl:'На проверке', cls:''},
    {key:'duplicate', num:s.duplicate||0, lbl:'Дубликаты',   cls:''},
    {key:'approved',  num:s.approved||0,  lbl:'Одобрены',    cls:''},
    {key:'processed', num:s.processed||0, lbl:'Обогащены',   cls:'proc'},
    {key:'ready',     num:s.ready||0,     lbl:'Готовы',      cls:''},
  ];
  const activeStatus = document.getElementById('dash-status')?.value || '';
  document.getElementById('stats').innerHTML = items.map(i =>
    `<div class="stat ${i.cls} ${activeStatus===i.key?'active-filter':''}" onclick="filterByStatus('${i.key}')">
      <div class="num">${i.num}</div><div class="lbl">${i.lbl}</div>
    </div>`
  ).join('');
}

function filterByStatus(status) {
  document.getElementById('dash-status').value = status;
  applyDashFilters();
  loadStats();
}

// Dashboard groups data
let _dashTags = {};
let _dashGroups = [];
let _dashIdToGroup = {};
const GROUP_COLORS = ['#e0245e','#1da1f2','#17bf63','#ffad1f','#794bc4','#ff6300','#e8598b','#00bcd4','#8bc34a','#ff9800'];

async function loadDashboardGroups() {
  toast('Анализ групп...');
  const r = await api('/api/dashboard_groups');
  if (r.status !== 'ok') { toast(r.message, true); return; }
  _dashTags = r.tags || {};
  _dashGroups = r.groups || [];
  _dashIdToGroup = r.id_to_group || {};

  // Show groups summary
  const gs = document.getElementById('groups-summary');
  if (_dashGroups.length > 0) {
    gs.innerHTML = '<h2 style="margin-bottom:8px">Группы похожих новостей</h2>' +
      _dashGroups.map(g => {
        const color = GROUP_COLORS[(g.group - 1) % GROUP_COLORS.length];
        return `<div class="card" style="margin-bottom:6px;padding:8px;border-left:3px solid ${color}">
          <b style="color:${color}">Группа ${g.group}</b> (${g.count} шт):
          ${g.titles.map(t => '<span style="display:block;font-size:0.85em;color:#8899a6;margin:2px 0">' + esc(t) + '</span>').join('')}
          <button class="btn btn-sm btn-secondary" style="margin-top:4px" onclick="selectGroupById(${g.group})">Выбрать группу</button>
        </div>`;
      }).join('');
    gs.style.display = 'block';
  } else {
    gs.innerHTML = '<div class="card" style="padding:10px">Похожих новостей не найдено</div>';
    gs.style.display = 'block';
  }

  // Re-render dashboard
  loadNews();
  toast('Найдено ' + _dashGroups.length + ' групп');
}

function selectGroupById(gid) {
  const ids = (_dashGroups.find(g => g.group === gid) || {}).ids || [];
  document.querySelectorAll('.news-check').forEach(c => {
    c.checked = ids.includes(c.dataset.id);
  });
  updateSelectedCount();
}

function selectGroup() {
  // Select the group of the first checked item
  const checked = document.querySelector('.news-check:checked');
  if (!checked) { toast('Сначала выберите новость из группы', true); return; }
  const gid = _dashIdToGroup[checked.dataset.id];
  if (!gid) { toast('Эта новость не в группе', true); return; }
  selectGroupById(gid);
}

// renderTags and renderGroup replaced by renderTagsClickable and renderGroupClickable

// News
let _allNews = []; // full news array for client-side filtering

async function loadNews() {
  let url = `/api/news?limit=500`;

  const news = await api(url);
  _allNews = news;

  // Populate source filters
  const sources = [...new Set(news.map(n => n.source))].sort();
  const dashSrc = document.getElementById('dash-source');
  if (dashSrc && dashSrc.options.length <= 1) {
    sources.forEach(s => { const o = document.createElement('option'); o.value = s; o.textContent = s; dashSrc.appendChild(o); });
  }
  const srcFilter = document.getElementById('filter-source');
  if (srcFilter && srcFilter.options.length <= 1) {
    sources.forEach(s => { const o = document.createElement('option'); o.value = s; o.textContent = s; srcFilter.appendChild(o); });
  }

  applyDashFilters();
  renderNewsTab(news);
  initEditorSourceFilter();
}

function applyDashFilters() {
  const search = (document.getElementById('dash-search')?.value || '').toLowerCase();
  const source = document.getElementById('dash-source')?.value || '';
  const status = document.getElementById('dash-status')?.value || '';
  const tag = document.getElementById('dash-tag')?.value || '';
  const dateFrom = document.getElementById('dash-date-from')?.value || '';
  const dateTo = document.getElementById('dash-date-to')?.value || '';

  let filtered = _allNews;

  if (search) filtered = filtered.filter(n => (n.title||'').toLowerCase().includes(search) || (n.description||'').toLowerCase().includes(search));
  if (source) filtered = filtered.filter(n => n.source === source);
  if (status) filtered = filtered.filter(n => n.status === status);
  if (tag) filtered = filtered.filter(n => {
    const tags = _dashTags[n.id] || [];
    return tags.some(t => t.id === tag);
  });
  if (dateFrom) filtered = filtered.filter(n => {
    const d = (n.published_at || n.parsed_at || '').slice(0,10);
    return d >= dateFrom;
  });
  if (dateTo) filtered = filtered.filter(n => {
    const d = (n.published_at || n.parsed_at || '').slice(0,10);
    return d <= dateTo;
  });

  renderDashboard(filtered);
  renderActiveFilters(search, source, status, tag, dateFrom, dateTo);
}

function renderActiveFilters(search, source, status, tag, dateFrom, dateTo) {
  const chips = [];
  if (search) chips.push({label: 'Поиск: ' + search, clear: () => { document.getElementById('dash-search').value = ''; applyDashFilters(); }});
  if (source) chips.push({label: 'Источник: ' + source, clear: () => { document.getElementById('dash-source').value = ''; applyDashFilters(); }});
  if (status) chips.push({label: 'Статус: ' + status, clear: () => { document.getElementById('dash-status').value = ''; applyDashFilters(); loadStats(); }});
  if (tag) chips.push({label: 'Тег: ' + tag, clear: () => { document.getElementById('dash-tag').value = ''; applyDashFilters(); }});
  if (dateFrom) chips.push({label: 'С: ' + dateFrom, clear: () => { document.getElementById('dash-date-from').value = ''; applyDashFilters(); }});
  if (dateTo) chips.push({label: 'По: ' + dateTo, clear: () => { document.getElementById('dash-date-to').value = ''; applyDashFilters(); }});

  const container = document.getElementById('active-filters');
  container.innerHTML = '';
  chips.forEach((chip, i) => {
    const el = document.createElement('span');
    el.className = 'active-filter-chip';
    el.innerHTML = chip.label + ' <span class="chip-x">&times;</span>';
    el.onclick = chip.clear;
    container.appendChild(el);
  });
}

function resetDashFilters() {
  document.getElementById('dash-search').value = '';
  document.getElementById('dash-source').value = '';
  document.getElementById('dash-status').value = '';
  document.getElementById('dash-tag').value = '';
  document.getElementById('dash-date-from').value = '';
  document.getElementById('dash-date-to').value = '';
  applyDashFilters();
  loadStats();
}

function filterByTag(tagId) {
  document.getElementById('dash-tag').value = tagId;
  applyDashFilters();
}

function filterBySource(source) {
  document.getElementById('dash-source').value = source;
  applyDashFilters();
}

const STATUS_LABELS = {new:'Новая',in_review:'Проверка',approved:'Одобр.',processed:'Обогащ.',duplicate:'Дубль',rejected:'Откл.',ready:'Готова'};

// Sorting state
let _sortField = 'parsed_at';
let _sortDir = 'desc'; // 'asc' or 'desc'
let _lastFiltered = [];

function sortDash(field) {
  if (_sortField === field) {
    _sortDir = _sortDir === 'asc' ? 'desc' : 'asc';
  } else {
    _sortField = field;
    _sortDir = 'asc';
  }
  // Update header arrows
  document.querySelectorAll('#dash-table th.sortable').forEach(th => {
    const arrow = th.querySelector('.sort-arrow');
    if (th.dataset.sort === field) {
      th.classList.add('sort-active');
      arrow.innerHTML = _sortDir === 'asc' ? '&#9650;' : '&#9660;';
    } else {
      th.classList.remove('sort-active');
      arrow.innerHTML = '&#9650;';
    }
  });
  // Re-sort and render
  const sorted = sortNews(_lastFiltered, field, _sortDir);
  renderDashboardRows(sorted);
}

function sortNews(news, field, dir) {
  const arr = [...news];
  const mult = dir === 'asc' ? 1 : -1;
  arr.sort((a, b) => {
    let va, vb;
    if (field === 'source') { va = (a.source||'').toLowerCase(); vb = (b.source||'').toLowerCase(); }
    else if (field === 'title') { va = (a.title||'').toLowerCase(); vb = (b.title||'').toLowerCase(); }
    else if (field === 'published_at') { va = a.published_at||''; vb = b.published_at||''; }
    else if (field === 'parsed_at') { va = a.parsed_at||''; vb = b.parsed_at||''; }
    else if (field === 'status') { va = a.status||''; vb = b.status||''; }
    else if (field === 'score') {
      va = parseFloat(a.llm_trend_forecast) || 0;
      vb = parseFloat(b.llm_trend_forecast) || 0;
    }
    else if (field === 'group') {
      va = _dashIdToGroup[a.id] || 9999;
      vb = _dashIdToGroup[b.id] || 9999;
    }
    else { va = ''; vb = ''; }
    if (va < vb) return -1 * mult;
    if (va > vb) return 1 * mult;
    return 0;
  });
  return arr;
}

function renderDashboard(news) {
  _lastFiltered = news;
  const emptyEl = document.getElementById('dash-empty');
  const infoEl = document.getElementById('dash-table-count');
  const showEl = document.getElementById('dash-showing');

  if (!news.length) {
    document.getElementById('dash-news').innerHTML = '';
    emptyEl.style.display = 'block';
    infoEl.textContent = '';
    showEl.textContent = '';
    return;
  }
  emptyEl.style.display = 'none';
  infoEl.textContent = `${news.length} новостей`;
  showEl.textContent = news.length > 200 ? '(показано 200)' : '';

  const sorted = sortNews(news, _sortField, _sortDir);
  renderDashboardRows(sorted);
}

function renderDashboardRows(news) {
  const shown = news.slice(0, 200);
  document.getElementById('dash-news').innerHTML = shown.map(n => {
    const gid = _dashIdToGroup[n.id];
    const rowStyle = gid ? `border-left:3px solid ${GROUP_COLORS[(gid-1)%GROUP_COLORS.length]}` : '';
    const statusLabel = STATUS_LABELS[n.status] || n.status;
    return `<tr style="${rowStyle}">
      <td><input type="checkbox" class="news-check" data-id="${n.id}" onchange="updateSelectedCount()"></td>
      <td><span style="cursor:pointer" onclick="filterBySource('${esc(n.source)}')" title="Фильтр по источнику">${n.source}</span></td>
      <td><a href="${n.url}" target="_blank" title="${esc(n.description||'')}">${esc(n.title||'')}</a></td>
      <td>${renderTagsClickable(n.id)}</td>
      <td>${renderGroupClickable(n.id)}</td>
      <td>${fmtDate(n.published_at)}</td>
      <td>${fmtDate(n.parsed_at)}</td>
      <td><span class="badge badge-${n.status}" style="cursor:pointer" onclick="filterByStatus('${n.status}')">${statusLabel}</span></td>
      <td>${n.llm_trend_forecast||'-'}</td>
      <td style="white-space:nowrap">
        <button class="btn btn-sm btn-primary" onclick="processOne('${n.id}')">Анализ</button>
        <button class="btn btn-sm btn-success" onclick="exportOne('${n.id}')">Sheets</button>
      </td>
    </tr>`;
  }).join('');
}

function renderTagsClickable(newsId) {
  const tags = _dashTags[newsId] || [];
  if (!tags.length) return '<span style="color:#38444d">-</span>';
  return tags.map(t => `<span class="tag tag-${t.id}" onclick="filterByTag('${t.id}')" title="Фильтр по тегу">${t.label}</span>`).join('');
}

function renderGroupClickable(newsId) {
  const gid = _dashIdToGroup[newsId];
  if (!gid) return '';
  const color = GROUP_COLORS[(gid - 1) % GROUP_COLORS.length];
  const g = _dashGroups.find(x => x.group === gid);
  return `<span class="group-marker" style="background:${color}33;color:${color}" title="${g ? g.count + ' шт — нажми чтобы выбрать' : ''}" onclick="selectGroupById(${gid})">G${gid}</span>`;
}

function renderNewsTab(news) {
  renderNewsFiltered();
}

function esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }
function fmtDate(d) { if (!d) return '-'; return d.replace('T',' ').slice(0,16); }

// Selection
function getSelectedIds() {
  return [...document.querySelectorAll('.news-check:checked')].map(c => c.dataset.id);
}
function updateSelectedCount() {
  const cnt = getSelectedIds().length;
  document.getElementById('selected-count').textContent = cnt ? cnt + ' выбрано' : '';
}
function selectAll() { document.querySelectorAll('.news-check').forEach(c => c.checked = true); updateSelectedCount(); }
function deselectAll() { document.querySelectorAll('.news-check').forEach(c => c.checked = false); updateSelectedCount(); }
function toggleAll(el) { document.querySelectorAll('.news-check').forEach(c => c.checked = el.checked); updateSelectedCount(); }

// Review
let _reviewResults = [];
let _revSortField = 'total_score';
let _revSortDir = 'desc';

function switchToTab(tabName) {
  document.querySelectorAll('.tab').forEach(x => x.classList.remove('active'));
  document.querySelectorAll('.panel').forEach(x => x.classList.remove('active'));
  document.querySelector(`.tab[data-tab="${tabName}"]`).classList.add('active');
  document.getElementById('panel-' + tabName).classList.add('active');
}

// Load review tab — batch check by status
async function loadReviewTab() {
  const status = document.getElementById('rev-status').value;
  const limit = document.getElementById('rev-limit').value;
  document.getElementById('rev-loading').textContent = 'Загрузка и проверка...';
  const r = await api('/api/review_batch', {status, limit: parseInt(limit)});
  document.getElementById('rev-loading').textContent = '';
  if (r.status !== 'ok') { toast(r.message, true); return; }
  _reviewResults = r.results || [];
  renderReviewResults(r);
}

// Send selected from dashboard to review (with status change)
async function sendToReview() {
  const ids = getSelectedIds();
  if (!ids.length) { toast('Сначала выберите новости', true); return; }
  toast('Запуск проверки...');
  const r = await api('/api/review', {news_ids: ids});
  if (r.status !== 'ok') { toast(r.message, true); return; }
  _reviewResults = r.results || [];
  renderReviewResults(r);
  switchToTab('review');
  loadAll();
}

function renderReviewResults(r) {
  // Groups
  const groupsHtml = (r.groups||[]).filter(g => g.members.length >= 2).map(g => {
    const icon = g.status === 'trending' ? '&#9889;' : g.status === 'popular' ? '&#128293;' : '&#128994;';
    const titles = g.members.map(m => esc(m.title)).join('<br>');
    return `<div class="card" style="margin-bottom:6px;padding:8px"><b>${icon} ${g.status.toUpperCase()}</b> (${g.members.length} шт):<div style="font-size:0.85em;color:#8899a6;margin-top:3px">${titles}</div></div>`;
  }).join('');
  document.getElementById('review-groups').innerHTML = groupsHtml;

  // Badge
  const badge = document.getElementById('review-badge');
  badge.textContent = _reviewResults.length;
  badge.style.display = 'inline';
  document.getElementById('review-count').textContent = _reviewResults.length + ' новостей';

  if (!_reviewResults.length) {
    document.getElementById('review-table').innerHTML = '';
    document.getElementById('review-empty').style.display = 'block';
    return;
  }
  document.getElementById('review-empty').style.display = 'none';

  // Sort and render
  const sorted = sortReviewData(_reviewResults, _revSortField, _revSortDir);
  renderReviewRows(sorted);
  toast('Проверено: ' + _reviewResults.length + ' новостей');
}

function sortReview(field) {
  if (_revSortField === field) {
    _revSortDir = _revSortDir === 'asc' ? 'desc' : 'asc';
  } else {
    _revSortField = field;
    _revSortDir = field === 'total_score' || field === 'quality' || field === 'relevance' || field === 'freshness' || field === 'viral' ? 'desc' : 'asc';
  }
  document.querySelectorAll('#review-main-table th.sortable').forEach(th => {
    const arrow = th.querySelector('.sort-arrow');
    if (th.dataset.sort === field) {
      th.classList.add('sort-active');
      arrow.innerHTML = _revSortDir === 'asc' ? '&#9650;' : '&#9660;';
    } else {
      th.classList.remove('sort-active');
      arrow.innerHTML = '&#9650;';
    }
  });
  const sorted = sortReviewData(_reviewResults, field, _revSortDir);
  renderReviewRows(sorted);
}

function sortReviewData(results, field, dir) {
  const arr = [...results];
  const mult = dir === 'asc' ? 1 : -1;
  arr.sort((a, b) => {
    let va, vb;
    if (field === 'title') { va = (a.title||'').toLowerCase(); vb = (b.title||'').toLowerCase(); }
    else if (field === 'source') { va = a.source||''; vb = b.source||''; }
    else if (field === 'total_score') { va = a.total_score||0; vb = b.total_score||0; }
    else if (field === 'quality') { va = a.checks?.quality?.score||0; vb = b.checks?.quality?.score||0; }
    else if (field === 'relevance') { va = a.checks?.relevance?.score||0; vb = b.checks?.relevance?.score||0; }
    else if (field === 'freshness') { va = a.checks?.freshness?.score||0; vb = b.checks?.freshness?.score||0; }
    else if (field === 'viral') { va = a.checks?.viral?.score||0; vb = b.checks?.viral?.score||0; }
    else { va = ''; vb = ''; }
    if (va < vb) return -1 * mult;
    if (va > vb) return 1 * mult;
    return 0;
  });
  return arr;
}

function renderReviewRows(results) {
  document.getElementById('review-table').innerHTML = results.map(r => {
    const q = r.checks.quality, rel = r.checks.relevance, f = r.checks.freshness, v = r.checks.viral;
    const sent = r.sentiment || {};
    const tags = (r.tags||[]).map(t => `<span class="tag tag-${t.id}">${t.label}</span>`).join('') || '-';
    const sentColor = sent.label==='positive'?'#17bf63':sent.label==='negative'?'#e0245e':'#8899a6';
    const passIcon = r.overall_pass ? '&#9989;' : '&#10060;';
    const dup = r.is_duplicate ? '&#128308; DUP' : (r.dedup_status||'unique');
    const statusBadge = r.status ? `<span class="badge badge-${r.status}" style="margin-left:4px">${STATUS_LABELS[r.status]||r.status}</span>` : '';
    return `<tr id="review-row-${r.id}">
      <td><input type="checkbox" class="approve-check" data-id="${r.id}" ${r.overall_pass && !r.is_duplicate ? 'checked' : ''}></td>
      <td><a href="${r.url}" target="_blank" title="${esc(r.title||'')}">${esc((r.title||'').slice(0,50))}</a>${statusBadge}</td>
      <td>${r.source}</td>
      <td>${dup}</td>
      <td style="color:${q.pass?'#17bf63':'#e0245e'}">${q.score}</td>
      <td style="color:${rel.pass?'#17bf63':'#e0245e'}">${rel.score}</td>
      <td>${f.score} <span style="color:#8899a6;font-size:0.8em">${f.status||''}</span></td>
      <td>${v.score} <span style="color:#8899a6;font-size:0.8em">${v.level||''}</span>${(v.triggers||[]).length?'<div style="margin-top:2px;display:flex;flex-wrap:wrap;gap:2px">'+v.triggers.map(t=>{const c=t.weight>=40?'#e0245e':t.weight>=20?'#ffad1f':'#1da1f2';return `<span style="font-size:0.7em;padding:1px 5px;background:${c}18;border-radius:8px;color:${c}" title="+${t.weight}">${t.label}</span>`;}).join('')+'</div>':''}</td>
      <td style="color:${sentColor}">${sent.score||0} ${sent.label||''}</td>
      <td style="max-width:120px;overflow:hidden;text-overflow:ellipsis">${tags}</td>
      <td><b style="color:${r.total_score>=60?'#17bf63':r.total_score>=30?'#ffad1f':'#e0245e'}">${r.total_score}</b></td>
      <td>${passIcon}</td>
      <td style="white-space:nowrap">
        <button class="btn btn-sm btn-success" onclick="approveOne('${r.id}')">&#10004;</button>
        <button class="btn btn-sm btn-danger" onclick="rejectOne('${r.id}')">&#10008;</button>
      </td>
    </tr>`;
  }).join('');
}

function toggleApproveAll(el) { document.querySelectorAll('.approve-check').forEach(c => c.checked = el.checked); }

function toggleApproveAllPassed() {
  document.querySelectorAll('.approve-check').forEach(c => {
    const r = _reviewResults.find(x => x.id === c.dataset.id);
    c.checked = r && r.overall_pass && !r.is_duplicate;
  });
}

async function approveOne(id) {
  const r = await api('/api/approve', {news_ids: [id]});
  if (r.status === 'ok') {
    toast('Одобрено');
    const row = document.getElementById('review-row-' + id);
    if (row) row.style.opacity = '0.4';
  } else toast(r.message, true);
}

async function rejectOne(id) {
  const r = await api('/api/reject', {news_id: id});
  if (r.status === 'ok') {
    toast('Отклонено');
    const row = document.getElementById('review-row-' + id);
    if (row) row.style.opacity = '0.4';
  } else toast(r.message, true);
}

async function approveSelected() {
  const ids = [...document.querySelectorAll('.approve-check:checked')].map(c => c.dataset.id);
  if (!ids.length) { toast('Сначала выберите новости', true); return; }
  const r = await api('/api/approve', {news_ids: ids});
  if (r.status === 'ok') {
    toast('Одобрено: ' + r.approved + ' новостей');
    ids.forEach(id => { const row = document.getElementById('review-row-' + id); if (row) row.style.opacity = '0.4'; });
  }
  else toast(r.message, true);
  loadAll();
}

async function rejectSelected() {
  const ids = [...document.querySelectorAll('.approve-check:checked')].map(c => c.dataset.id);
  if (!ids.length) { toast('Сначала выберите новости', true); return; }
  for (const id of ids) {
    await api('/api/reject', {news_id: id});
    const row = document.getElementById('review-row-' + id); if (row) row.style.opacity = '0.4';
  }
  toast('Отклонено: ' + ids.length + ' новостей');
  loadAll();
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
  const r = await api('/api/export_sheets', {news_id: id});
  if (r.status === 'ok') toast('Экспортировано в строку ' + r.row);
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
  document.getElementById('users-table').innerHTML = users.map(u =>
    `<tr><td>${u.username}</td><td>${u.username==='admin'?'':'<button class="btn btn-sm btn-danger" onclick="deleteUser(\''+u.username+'\')">Удалить</button>'}</td></tr>`
  ).join('');
  const sel = document.getElementById('chpass-user');
  if (sel) {
    sel.innerHTML = users.map(u => `<option value="${u.username}">${u.username}</option>`).join('');
  }
}
async function addUser() {
  const username = document.getElementById('new-username').value;
  const password = document.getElementById('new-password').value;
  if (!username || !password) { toast('Заполните логин и пароль', true); return; }
  await api('/api/users/add', {username, password});
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

// Review filter
function filterReviewTable() {
  const search = (document.getElementById('rev-search')?.value || '').toLowerCase();
  const sentiment = document.getElementById('rev-sentiment')?.value || '';
  const minScore = parseInt(document.getElementById('rev-min-score')?.value) || 0;
  let filtered = _reviewResults;
  if (search) filtered = filtered.filter(r => (r.title||'').toLowerCase().includes(search));
  if (sentiment) filtered = filtered.filter(r => (r.sentiment?.label||'') === sentiment);
  if (minScore > 0) filtered = filtered.filter(r => (r.total_score||0) >= minScore);
  const sorted = sortReviewData(filtered, _revSortField, _revSortDir);
  renderReviewRows(sorted);
  document.getElementById('review-count').textContent = filtered.length + ' / ' + _reviewResults.length + ' новостей';
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
  const limit = parseInt(document.getElementById('filter-limit')?.value) || 100;
  let filtered = _allNews;
  if (status) filtered = filtered.filter(n => n.status === status);
  if (source) filtered = filtered.filter(n => n.source === source);
  if (search) filtered = filtered.filter(n => (n.title||'').toLowerCase().includes(search));
  _newsFiltered = filtered;
  const sorted = sortNews(filtered, _newsSortField, _newsSortDir).slice(0, limit);
  const newsTb = document.getElementById('news-table');
  if (!newsTb) return;
  newsTb.innerHTML = sorted.map(n => {
    let bigrams = '';
    try { bigrams = JSON.parse(n.bigrams||'[]').map(b=>b[0]).join(', '); } catch(e){}
    const statusLabel = STATUS_LABELS[n.status] || n.status;
    return `<tr>
      <td><input type="checkbox" class="news-tab-check" data-id="${n.id}" onchange="updateNewsSelectedCount()"></td>
      <td>${n.source}</td>
      <td><a href="${n.url}" target="_blank" title="${esc(n.description||'')}">${esc(n.title||'')}</a></td>
      <td>${fmtDate(n.published_at)}</td>
      <td><span class="badge badge-${n.status}">${statusLabel}</span></td>
      <td title="${esc(bigrams)}">${bigrams.slice(0,40)}</td>
      <td>${esc(n.llm_recommendation||'-')}</td>
      <td>${n.llm_trend_forecast||'-'}</td>
      <td>${n.sheets_row||'-'}</td>
      <td style="white-space:nowrap">
        <button class="btn btn-sm btn-primary" onclick="processOne('${n.id}')">Анализ</button>
        <button class="btn btn-sm btn-success" onclick="exportOne('${n.id}')">Sheets</button>
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

async function exportSelectedToSheets() {
  const ids = getNewsSelectedIds();
  if (!ids.length) { toast('Сначала выберите новости', true); return; }
  if (!confirm('Экспортировать ' + ids.length + ' новостей в Google Sheets?')) return;
  toast('Экспорт в Sheets...');
  const r = await api('/api/export_sheets_bulk', {news_ids: ids});
  if (r.status === 'ok') toast('Экспортировано: ' + r.exported + (r.skipped ? ', дубликатов: ' + r.skipped : '') + (r.errors ? ', ошибок: ' + r.errors : ''));
  else toast(r.message, true);
}

async function exportSelectedToSheetsDash() {
  const ids = getSelectedIds();
  if (!ids.length) { toast('Сначала выберите новости', true); return; }
  if (!confirm('Экспортировать ' + ids.length + ' новостей в Google Sheets?')) return;
  toast('Экспорт в Sheets...');
  const r = await api('/api/export_sheets_bulk', {news_ids: ids});
  if (r.status === 'ok') toast('Экспортировано: ' + r.exported + (r.skipped ? ', дубликатов: ' + r.skipped : '') + (r.errors ? ', ошибок: ' + r.errors : ''));
  else toast(r.message, true);
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

// Editor
let _editorNewsId = null;
let _editorMergeIds = new Set();
let _lastRewrite = null;
let _editorSelectedStyle = 'news';

function selectStyle(btn) {
  document.querySelectorAll('.style-option').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  _editorSelectedStyle = btn.dataset.style;
}

function switchEditorView(view) {
  const previewEl = document.getElementById('editor-view-preview');
  const resultEl = document.getElementById('editor-view-result');
  const previewBtn = document.getElementById('tab-preview-btn');
  const resultBtn = document.getElementById('tab-result-btn');
  const copyBtns = document.getElementById('rw-copy-buttons');
  if (view === 'result') {
    previewEl.style.display = 'none';
    resultEl.style.display = 'block';
    previewBtn.style.borderBottomColor = 'transparent';
    previewBtn.style.color = '#8899a6';
    resultBtn.style.borderBottomColor = '#1da1f2';
    resultBtn.style.color = '#1da1f2';
    copyBtns.style.display = _lastRewrite ? 'flex' : 'none';
  } else {
    previewEl.style.display = 'block';
    resultEl.style.display = 'none';
    previewBtn.style.borderBottomColor = '#1da1f2';
    previewBtn.style.color = '#1da1f2';
    resultBtn.style.borderBottomColor = 'transparent';
    resultBtn.style.color = '#8899a6';
    copyBtns.style.display = 'none';
  }
}

function updateMergeCounter() {
  const cnt = _editorMergeIds.size;
  const el = document.getElementById('merge-counter');
  const txt = document.getElementById('merge-count-text');
  if (cnt > 0) {
    el.style.display = 'inline-flex';
    txt.textContent = cnt + ' для слияния';
  } else {
    el.style.display = 'none';
  }
  document.getElementById('merge-btn').disabled = cnt < 2;
}

function clearMergeSelection() {
  _editorMergeIds.clear();
  updateMergeCounter();
  filterEditorNews();
}

function filterEditorNews() {
  const search = (document.getElementById('editor-search')?.value || '').toLowerCase();
  const source = document.getElementById('editor-source-filter')?.value || '';
  const status = document.getElementById('editor-status-filter')?.value || '';
  let filtered = _allNews;
  if (search) filtered = filtered.filter(n => (n.title||'').toLowerCase().includes(search));
  if (source) filtered = filtered.filter(n => n.source === source);
  if (status) filtered = filtered.filter(n => n.status === status);
  const shown = filtered.slice(0, 60);
  renderEditorList(shown);
  const countEl = document.getElementById('editor-list-count');
  if (countEl) countEl.textContent = shown.length + ' из ' + filtered.length + ' новостей';
}

function renderEditorList(news) {
  const el = document.getElementById('editor-news-list');
  if (!news.length) { el.innerHTML = '<div style="color:#8899a6;padding:40px 20px;text-align:center">Нет новостей</div>'; return; }
  el.innerHTML = news.map(n => {
    const isSelected = _editorNewsId === n.id;
    const isMerge = _editorMergeIds.has(n.id);
    const cls = 'editor-news-item' + (isSelected ? ' selected' : '') + (isMerge ? ' merge' : '');
    const dateStr = fmtDate(n.published_at || n.parsed_at);
    return `<div class="${cls}" onclick="selectEditorNews('${n.id}')">
      <input type="checkbox" ${isMerge?'checked':''} onclick="event.stopPropagation();toggleMerge('${n.id}',this.checked)" style="margin-top:2px;cursor:pointer;width:16px;height:16px;min-width:16px;flex-shrink:0">
      <div style="flex:1;min-width:0">
        <div style="font-size:0.88em;line-height:1.3;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden" title="${esc(n.title||'')}">${esc(n.title||'')}</div>
        <div style="font-size:0.72em;color:#8899a6;margin-top:3px;display:flex;align-items:center;gap:6px">
          <span style="font-weight:500;color:#657786">${n.source}</span>
          <span>${dateStr}</span>
          <span class="badge badge-${n.status}" style="font-size:0.9em">${STATUS_LABELS[n.status]||n.status}</span>
        </div>
      </div>
    </div>`;
  }).join('');
}

function toggleMerge(id, checked) {
  if (checked) _editorMergeIds.add(id); else _editorMergeIds.delete(id);
  updateMergeCounter();
  filterEditorNews();
}

async function selectEditorNews(id) {
  _editorNewsId = id;
  document.getElementById('rewrite-btn').disabled = false;
  document.getElementById('analyze-btn').disabled = false;
  filterEditorNews();
  switchEditorView('preview');
  const preview = document.getElementById('editor-view-preview');
  preview.innerHTML = '<div style="text-align:center;padding:30px;color:#8899a6">Загрузка...</div>';
  const r = await api('/api/news/detail', {news_id: id});
  if (r.status !== 'ok') { toast(r.message, true); return; }
  const n = r.news;
  const a = r.analysis;
  let html = '';
  html += `<div style="margin-bottom:10px">`;
  html += `<div style="color:#1da1f2;font-size:1.1em;font-weight:600;line-height:1.3;margin-bottom:6px">${esc(n.title||'')}</div>`;
  html += `<div style="display:flex;gap:10px;align-items:center;font-size:0.82em;color:#8899a6;flex-wrap:wrap">`;
  html += `<span style="font-weight:500;color:#657786">${n.source}</span>`;
  html += `<span>${fmtDate(n.published_at)}</span>`;
  html += `<a href="${n.url}" target="_blank" style="color:#1da1f2">Открыть оригинал &#8599;</a>`;
  html += `</div></div>`;

  if (n.h1 && n.h1 !== n.title) {
    html += `<div style="margin-bottom:8px;padding:6px 10px;background:#22303c;border-radius:6px;font-size:0.85em"><span style="color:#8899a6">H1:</span> ${esc(n.h1)}</div>`;
  }
  if (n.description) {
    html += `<div style="margin-bottom:8px;padding:6px 10px;background:#22303c;border-radius:6px;font-size:0.85em"><span style="color:#8899a6">Desc:</span> ${esc(n.description).slice(0,300)}</div>`;
  }

  // Analysis badges from stored data
  if (a) {
    html += `<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:10px;font-size:0.82em">`;
    if (a.llm_trend_forecast) html += `<span style="padding:3px 10px;background:#ffad1f20;border:1px solid #ffad1f40;border-radius:12px;color:#ffad1f">LLM Score: <b>${a.llm_trend_forecast}</b></span>`;
    if (a.llm_recommendation) html += `<span style="padding:3px 10px;background:#22303c;border-radius:12px">${esc(a.llm_recommendation)}</span>`;
    if (a.bigrams) { try { const bg = JSON.parse(a.bigrams); if (bg.length) html += `<span style="padding:3px 10px;background:#22303c;border-radius:12px;color:#8899a6">${bg.slice(0,5).map(b=>b[0]).join(', ')}</span>`; } catch(e){} }
    // Trends data
    if (a.trends_data) { try { const td = JSON.parse(a.trends_data); const tKeys = Object.entries(td).filter(([k,v])=>v); if (tKeys.length) html += `<span style="padding:3px 10px;background:#1da1f220;border:1px solid #1da1f240;border-radius:12px;color:#1da1f2">Trends: ${tKeys.map(([k,v])=>k+':'+v).join(' ')}</span>`; } catch(e){} }
    // Keyso data
    if (a.keyso_data) { try { const kd = JSON.parse(a.keyso_data); if (kd.freq) html += `<span style="padding:3px 10px;background:#17bf6320;border:1px solid #17bf6340;border-radius:12px;color:#17bf63">Keys.so: ${kd.freq}</span>`; } catch(e){} }
    html += `</div>`;
  }

  // Placeholder for analysis panel
  html += `<div id="editor-analysis-panel"></div>`;

  const textLen = (n.plain_text||'').length;
  html += `<div style="font-size:0.75em;color:#8899a6;margin-bottom:4px">Текст (${textLen} симв.)</div>`;
  html += `<div style="padding:12px;background:#22303c;border-radius:8px;font-size:0.85em;max-height:350px;overflow-y:auto;white-space:pre-wrap;line-height:1.55;color:#d9d9d9">${esc(n.plain_text||'Текст не загружен')}</div>`;

  preview.innerHTML = html;
  window._editorOriginalText = (n.title||'') + '\n\n' + (n.plain_text||'');
}

async function rewriteNews() {
  if (!_editorNewsId) { toast('Выберите новость', true); return; }
  const lang = document.getElementById('rewrite-lang').value;
  const loadEl = document.getElementById('rewrite-loading');
  loadEl.innerHTML = '<span style="display:inline-flex;align-items:center;gap:6px"><span class="spinner" style="width:14px;height:14px;border:2px solid #38444d;border-top-color:#1da1f2;border-radius:50%;animation:spin .8s linear infinite;display:inline-block"></span> Переписываем...</span>';
  document.getElementById('rewrite-btn').disabled = true;
  const r = await api('/api/rewrite', {news_id: _editorNewsId, style: _editorSelectedStyle, language: lang});
  loadEl.textContent = '';
  document.getElementById('rewrite-btn').disabled = false;
  if (r.status !== 'ok') { toast(r.message, true); return; }
  _lastRewrite = r.result;
  showRewriteResult(r.result, window._editorOriginalText || r.original_title);
  toast('Переписано!');
}

async function mergeSelected() {
  if (_editorMergeIds.size < 2) { toast('Выберите минимум 2 новости', true); return; }
  const loadEl = document.getElementById('rewrite-loading');
  loadEl.innerHTML = '<span style="display:inline-flex;align-items:center;gap:6px"><span class="spinner" style="width:14px;height:14px;border:2px solid #38444d;border-top-color:#ffad1f;border-radius:50%;animation:spin .8s linear infinite;display:inline-block"></span> Объединяем ' + _editorMergeIds.size + ' новостей...</span>';
  document.getElementById('merge-btn').disabled = true;
  const r = await api('/api/merge', {news_ids: [..._editorMergeIds]});
  loadEl.textContent = '';
  document.getElementById('merge-btn').disabled = _editorMergeIds.size < 2;
  if (r.status !== 'ok') { toast(r.message, true); return; }
  _lastRewrite = r.result;
  const mergeResult = {
    title: r.result.merged_title || '',
    text: r.result.merged_text || '',
    seo_title: 'Лучший источник: ' + (r.result.best_source||''),
    seo_description: 'Источники: ' + (r.sources||[]).join(', '),
    tags: r.result.unique_facts || [],
  };
  showRewriteResult(mergeResult, 'Источники:\n' + (r.sources||[]).map((s,i) => (i+1)+'. '+s).join('\n'), true);
  toast('Объединено!');
}

async function analyzeEditorNews() {
  if (!_editorNewsId) { toast('Выберите новость', true); return; }
  const loadEl = document.getElementById('rewrite-loading');
  loadEl.innerHTML = '<span style="display:inline-flex;align-items:center;gap:6px"><span class="spinner" style="width:14px;height:14px;border:2px solid #38444d;border-top-color:#17bf63;border-radius:50%;animation:spin .8s linear infinite;display:inline-block"></span> Анализируем...</span>';
  const r = await api('/api/analyze_news', {news_id: _editorNewsId});
  loadEl.textContent = '';
  if (r.status !== 'ok') { toast(r.message, true); return; }
  const a = r.analysis;
  renderAnalysisPanel(a);
  toast('Анализ завершён!');
}

function renderAnalysisPanel(a) {
  const panel = document.getElementById('editor-analysis-panel');
  if (!panel) return;

  const triggerColor = (level) => level==='high'?'#e0245e':level==='medium'?'#ffad1f':level==='low'?'#1da1f2':'#38444d';
  const scoreBar = (score, max, color) => `<div style="display:flex;align-items:center;gap:8px"><div style="flex:1;height:6px;background:#22303c;border-radius:3px;overflow:hidden"><div style="width:${Math.min(100,score)}%;height:100%;background:${color};border-radius:3px"></div></div><span style="font-size:0.82em;font-weight:600;color:${color}">${score}</span></div>`;

  let html = '<div style="margin:12px 0;padding:14px;background:#15202b;border:1px solid #22303c;border-radius:10px">';
  html += '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px"><span style="font-size:0.8em;color:#8899a6;text-transform:uppercase;letter-spacing:0.5px">Полный анализ</span>';
  html += `<span style="padding:4px 12px;border-radius:12px;font-weight:600;font-size:0.9em;background:${a.total_score>=60?'#17bf6320':a.total_score>=30?'#ffad1f20':'#e0245e20'};color:${a.total_score>=60?'#17bf63':a.total_score>=30?'#ffad1f':'#e0245e'}">Общий: ${a.total_score}/100</span></div>`;

  // Score bars
  html += '<div style="display:grid;grid-template-columns:80px 1fr;gap:6px 10px;margin-bottom:12px;font-size:0.82em">';
  html += `<span style="color:#8899a6">Виральность</span>${scoreBar(a.viral.score,100,triggerColor(a.viral.level))}`;
  html += `<span style="color:#8899a6">Качество</span>${scoreBar(a.quality.score,100,a.quality.pass?'#17bf63':'#e0245e')}`;
  html += `<span style="color:#8899a6">Релевантность</span>${scoreBar(a.relevance.score,100,a.relevance.pass?'#17bf63':'#e0245e')}`;
  html += `<span style="color:#8899a6">Свежесть</span>${scoreBar(a.freshness.score,100,a.freshness.score>=50?'#17bf63':'#ffad1f')}`;
  html += `<span style="color:#8899a6">Моментум</span>${scoreBar(a.momentum.score,100,'#1da1f2')}`;
  html += '</div>';

  // Viral triggers
  if (a.viral.triggers && a.viral.triggers.length) {
    html += '<div style="margin-bottom:10px"><span style="font-size:0.75em;color:#8899a6;text-transform:uppercase;letter-spacing:0.5px">Виральные триггеры</span>';
    html += '<div style="display:flex;flex-wrap:wrap;gap:5px;margin-top:5px">';
    a.viral.triggers.forEach(t => {
      const col = t.weight>=40?'#e0245e':t.weight>=20?'#ffad1f':'#1da1f2';
      html += `<span style="padding:3px 10px;background:${col}18;border:1px solid ${col}40;border-radius:12px;font-size:0.8em;color:${col}" title="Weight: ${t.weight}">${t.label} <b>+${t.weight}</b></span>`;
    });
    html += '</div></div>';
  } else {
    html += '<div style="margin-bottom:10px;font-size:0.82em;color:#657786">Виральные триггеры не обнаружены</div>';
  }

  // Sentiment
  if (a.sentiment) {
    const sc = a.sentiment.label==='positive'?'#17bf63':a.sentiment.label==='negative'?'#e0245e':'#8899a6';
    html += `<div style="margin-bottom:10px;display:flex;gap:8px;align-items:center;font-size:0.82em">`;
    html += `<span style="color:#8899a6">Тональность:</span>`;
    html += `<span style="padding:2px 8px;background:${sc}18;border:1px solid ${sc}40;border-radius:10px;color:${sc}">${a.sentiment.label} (${a.sentiment.score})</span>`;
    html += `</div>`;
  }

  // Tags
  if (a.tags && a.tags.length) {
    html += '<div style="margin-bottom:10px"><span style="font-size:0.75em;color:#8899a6;text-transform:uppercase;letter-spacing:0.5px">Авто-теги</span>';
    html += '<div style="display:flex;flex-wrap:wrap;gap:4px;margin-top:5px">';
    a.tags.forEach(t => { html += `<span class="tag tag-${t.id}" style="font-size:0.8em">${t.label}</span>`; });
    html += '</div></div>';
  }

  // Trends + Keyso
  const hasTrends = a.trends_data && Object.keys(a.trends_data).length;
  const hasKeyso = a.keyso_data && (a.keyso_data.freq || a.keyso_data.similar);
  if (hasTrends || hasKeyso || a.bigrams?.length) {
    html += '<div style="display:flex;gap:12px;flex-wrap:wrap;font-size:0.82em">';
    if (hasTrends) {
      html += '<div style="padding:8px 12px;background:#22303c;border-radius:8px;flex:1;min-width:120px"><span style="color:#8899a6;font-size:0.9em">Google Trends</span>';
      Object.entries(a.trends_data).forEach(([k,v]) => {
        if (v) html += `<div style="margin-top:3px">${k}: <b style="color:#1da1f2">${v}</b></div>`;
      });
      html += '</div>';
    }
    if (hasKeyso) {
      html += '<div style="padding:8px 12px;background:#22303c;border-radius:8px;flex:1;min-width:120px"><span style="color:#8899a6;font-size:0.9em">Keys.so</span>';
      if (a.keyso_data.freq) html += `<div style="margin-top:3px">Частота: <b style="color:#17bf63">${a.keyso_data.freq}</b></div>`;
      if (a.keyso_data.similar?.length) html += `<div style="margin-top:3px;color:#8899a6">Похожие: ${a.keyso_data.similar.slice(0,5).join(', ')}</div>`;
      html += '</div>';
    }
    if (a.bigrams?.length) {
      html += '<div style="padding:8px 12px;background:#22303c;border-radius:8px;flex:1;min-width:120px"><span style="color:#8899a6;font-size:0.9em">Биграммы</span>';
      html += `<div style="margin-top:3px;color:#e1e8ed">${a.bigrams.slice(0,8).map(b=>Array.isArray(b)?b[0]:b).join(', ')}</div>`;
      html += '</div>';
    }
    html += '</div>';
  }

  // LLM recommendation
  if (a.llm_recommendation || a.llm_trend_forecast) {
    html += `<div style="margin-top:10px;padding:8px 12px;background:#22303c;border-radius:8px;font-size:0.82em">`;
    html += `<span style="color:#8899a6">LLM:</span> `;
    if (a.llm_trend_forecast) html += `Score <b style="color:#ffad1f">${a.llm_trend_forecast}</b> `;
    if (a.llm_recommendation) html += `— ${esc(a.llm_recommendation)}`;
    html += '</div>';
  }

  html += '</div>';
  panel.innerHTML = html;
}

function showRewriteResult(result, originalText, isMerge) {
  document.getElementById('rw-title').textContent = result.title || '';
  const seoTitle = result.seo_title || '';
  const seoDesc = result.seo_description || '';
  document.getElementById('rw-seo-title').textContent = seoTitle;
  document.getElementById('rw-seo-desc').textContent = seoDesc;
  document.getElementById('rw-seo-title-len').textContent = seoTitle ? '(' + seoTitle.length + ')' : '';
  document.getElementById('rw-seo-desc-len').textContent = seoDesc ? '(' + seoDesc.length + ')' : '';
  const tagsLabel = isMerge ? 'Уникальные факты' : 'Теги';
  document.getElementById('rw-tags-wrap').querySelector('.rw-field-label').textContent = tagsLabel;
  document.getElementById('rw-tags').innerHTML = (result.tags||[]).map(t => `<span class="tag tag-release" style="cursor:pointer" onclick="copyField(null,'${esc(t)}')">${esc(t)}</span>`).join(' ');
  document.getElementById('rw-text').textContent = result.text || '';
  document.getElementById('rw-original').textContent = originalText || '';
  document.getElementById('rw-empty').style.display = 'none';
  document.getElementById('rewrite-result').style.display = 'block';
  switchEditorView('result');
}

function copyField(elId, directText) {
  const text = directText || document.getElementById(elId)?.textContent || '';
  if (!text) return;
  navigator.clipboard.writeText(text);
  toast('Скопировано!');
}

function copyRewrite() {
  if (!_lastRewrite) return;
  const text = (_lastRewrite.title || _lastRewrite.merged_title || '') + '\n\n' + (_lastRewrite.text || _lastRewrite.merged_text || '');
  navigator.clipboard.writeText(text);
  toast('Текст скопирован!');
}

function copyRewriteSeo() {
  if (!_lastRewrite) return;
  const parts = [];
  if (_lastRewrite.seo_title) parts.push('Title: ' + _lastRewrite.seo_title);
  if (_lastRewrite.seo_description) parts.push('Description: ' + _lastRewrite.seo_description);
  if (_lastRewrite.tags?.length) parts.push('Tags: ' + _lastRewrite.tags.join(', '));
  navigator.clipboard.writeText(parts.join('\n'));
  toast('SEO скопировано!');
}

function copyRewriteJson() {
  if (!_lastRewrite) return;
  navigator.clipboard.writeText(JSON.stringify(_lastRewrite, null, 2));
  toast('JSON скопирован!');
}

function copyRewriteHtml() {
  if (!_lastRewrite) return;
  const title = _lastRewrite.title || _lastRewrite.merged_title || '';
  const text = _lastRewrite.text || _lastRewrite.merged_text || '';
  const paragraphs = text.split('\n').filter(p => p.trim()).map(p => '<p>' + p.trim() + '</p>').join('\n');
  let html = '<h1>' + title + '</h1>\n' + paragraphs;
  if (_lastRewrite.tags?.length) {
    html += '\n<div class="tags">' + _lastRewrite.tags.map(t => '<span class="tag">' + t + '</span>').join(' ') + '</div>';
  }
  navigator.clipboard.writeText(html);
  toast('HTML скопирован!');
}

function initEditorSourceFilter() {
  const sources = [...new Set(_allNews.map(n => n.source))].sort();
  const sel = document.getElementById('editor-source-filter');
  if (sel && sel.options.length <= 1) {
    sources.forEach(s => { const o = document.createElement('option'); o.value = s; o.textContent = s; sel.appendChild(o); });
  }
  filterEditorNews();
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

function renderArticlesList(articles) {
  const el = document.getElementById('articles-list');
  if (!articles.length) {
    el.innerHTML = '<div style="text-align:center;padding:40px;color:#8899a6"><div style="font-size:2em;margin-bottom:10px;opacity:0.3">&#128221;</div>Нет статей<br><span style="font-size:0.85em">Создайте статью в Редакторе</span></div>';
    return;
  }
  el.innerHTML = articles.map(a => {
    const isSel = _currentArticleId === a.id;
    const statusCls = 'art-status art-status-' + (a.status || 'draft');
    const date = a.updated_at ? fmtDate(a.updated_at) : '';
    const textLen = (a.text||'').length;
    return `<div class="art-card${isSel?' selected':''}" onclick="selectArticle('${a.id}')">
      <div style="display:flex;justify-content:space-between;align-items:start;gap:8px">
        <div style="flex:1;min-width:0">
          <div style="font-size:0.92em;font-weight:500;line-height:1.3;margin-bottom:4px;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden">${esc(a.title||'Без заголовка')}</div>
          <div style="font-size:0.75em;color:#8899a6;display:flex;gap:8px;align-items:center;flex-wrap:wrap">
            <span class="${statusCls}">${{draft:'Черновик',ready:'Готово',published:'Опубликовано'}[a.status]||a.status}</span>
            <span>${a.style||''}</span>
            <span>${textLen} симв.</span>
            <span>${date}</span>
          </div>
        </div>
      </div>
    </div>`;
  }).join('');
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

  // Original
  const origBlock = document.getElementById('art-original-block');
  const origText = document.getElementById('art-original-text');
  if (a.original_text || a.original_title) {
    origBlock.style.display = 'block';
    origText.textContent = (a.original_title ? a.original_title + '\n\n' : '') + (a.original_text || '');
  } else {
    origBlock.style.display = 'none';
  }

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

// Save rewrite from Editor tab as article
async function saveRewriteAsArticle() {
  if (!_lastRewrite) { toast('Нет результата для сохранения', true); return; }
  const n = _allNews.find(x => x.id === _editorNewsId);
  const r = await api('/api/articles/save', {
    news_id: _editorNewsId || '',
    title: _lastRewrite.title || _lastRewrite.merged_title || '',
    text: _lastRewrite.text || _lastRewrite.merged_text || '',
    seo_title: _lastRewrite.seo_title || '',
    seo_description: _lastRewrite.seo_description || '',
    tags: _lastRewrite.tags || [],
    style: _editorSelectedStyle,
    language: document.getElementById('rewrite-lang')?.value || 'русский',
    original_title: n?.title || '',
    original_text: window._editorOriginalText || '',
    source_url: n?.url || '',
  });
  if (r.status === 'ok') {
    toast('Сохранено в Статьи!');
    loadArticles();
  } else toast(r.message, true);
}

// Init
function loadAll() { loadStats(); loadNews(); }
loadAll();
loadSources();
loadPrompts();
loadSettings();
loadUsers();
loadHealth();
loadDbInfo();
loadArticles();
setInterval(loadAll, 30000);
setInterval(loadHealth, 60000);
</script>
</body>
</html>"""


def start_web():
    server = HTTPServer(("0.0.0.0", PORT), AdminHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info("Admin panel running on port %d", PORT)
    return server
