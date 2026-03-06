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
            "/api/quick_tags": lambda: self._quick_tags(body),
            "/api/review": lambda: self._run_review(body),
            "/api/approve": lambda: self._approve_news(body),
            "/api/reject": lambda: self._reject_news(body),
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
      <input type="search" id="dash-search" placeholder="По заголовку..." oninput="applyDashFilters()">
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
    <div id="review-empty" style="padding:30px;text-align:center;color:#8899a6">
      Выберите новости на Дашборде и нажмите «Отправить на проверку»
    </div>
    <div id="review-content" style="display:none">
      <div class="btn-group">
        <button class="btn btn-success" onclick="approveSelected()">Одобрить выбранные</button>
        <button class="btn btn-danger" onclick="rejectSelected()">Отклонить выбранные</button>
        <button class="btn btn-secondary" onclick="toggleApproveAllPassed()">Выбрать прошедшие</button>
        <span id="review-count" style="color:#8899a6;font-size:0.9em;margin-left:10px"></span>
      </div>
      <div id="review-groups" style="margin-bottom:12px"></div>
      <table>
        <thead><tr>
          <th><input type="checkbox" id="approve-all" onchange="toggleApproveAll(this)"></th>
          <th>Заголовок</th>
          <th>Источник</th>
          <th>Дедуп</th>
          <th>Качество</th>
          <th>Релев.</th>
          <th>Свежесть</th>
          <th>Вирал.</th>
          <th>Тональн.</th>
          <th>Момент</th>
          <th>Теги</th>
          <th>Итог</th>
          <th>Ок</th>
          <th>Действия</th>
        </tr></thead>
        <tbody id="review-table"></tbody>
      </table>
    </div>
  </div>

  <!-- HEALTH -->
  <div class="panel" id="panel-health">
    <h2>Здоровье источников (24ч)</h2>
    <table>
      <thead><tr><th>Статус</th><th>Источник</th><th>Статей (24ч)</th><th>Последний парсинг</th><th>Минут назад</th></tr></thead>
      <tbody id="health-table"></tbody>
    </table>
  </div>

  <!-- NEWS -->
  <div class="panel" id="panel-news">
    <div class="filters">
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
      <select id="filter-source" onchange="loadNews()">
        <option value="">Все источники</option>
      </select>
      <input type="number" id="filter-limit" value="100" min="10" max="500" style="width:80px" onchange="loadNews()">
      <button class="btn btn-secondary btn-sm" onclick="loadNews()">Фильтр</button>
    </div>
    <table>
      <thead><tr><th>Источник</th><th>Заголовок</th><th>H1</th><th>Опубл.</th><th>Статус</th><th>Биграммы</th><th>LLM</th><th>Скор</th><th>Лист</th><th>Действия</th></tr></thead>
      <tbody id="news-table"></tbody>
    </table>
  </div>

  <!-- SOURCES -->
  <div class="panel" id="panel-sources">
    <div class="grid-2">
      <div class="card">
        <h2>Активные источники</h2>
        <table>
          <thead><tr><th>Имя</th><th>Тип</th><th>URL</th><th>Интервал</th><th>Селектор</th><th>Действия</th></tr></thead>
          <tbody id="sources-table"></tbody>
        </table>
      </div>
      <div class="card">
        <h2>Добавить источник</h2>
        <div class="form-group"><label>Имя</label><input id="src-name"></div>
        <div class="form-group"><label>Type</label>
          <select id="src-type" onchange="document.getElementById('src-selector-group').style.display=this.value==='html'?'block':'none'">
            <option value="rss">RSS</option><option value="html">HTML</option>
          </select>
        </div>
        <div class="form-group"><label>URL</label><input id="src-url"></div>
        <div class="form-group"><label>Интервал (мин)</label><input type="number" id="src-interval" value="15"></div>
        <div class="form-group" id="src-selector-group" style="display:none"><label>CSS Селектор</label><input id="src-selector" placeholder=".news-item"></div>
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
        <div class="form-group"><label>Логин</label><input id="new-username"></div>
        <div class="form-group"><label>Пароль</label><input id="new-password" type="password"></div>
        <button class="btn btn-primary" onclick="addUser()">Добавить</button>
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
  const status = document.getElementById('filter-status')?.value || '';
  const source = document.getElementById('filter-source')?.value || '';
  const limit = document.getElementById('filter-limit')?.value || 100;
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
  renderNewsTab(news, status, source, limit);
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

function renderNewsTab(news, status, source, limit) {
  let filtered = news;
  if (status) filtered = filtered.filter(n => n.status === status);
  if (source) filtered = filtered.filter(n => n.source === source);
  filtered = filtered.slice(0, parseInt(limit) || 100);

  const newsTb = document.getElementById('news-table');
  if (newsTb) {
    newsTb.innerHTML = filtered.map(n => {
      let bigrams = '';
      try { bigrams = JSON.parse(n.bigrams||'[]').map(b=>b[0]).join(', '); } catch(e){}
      const statusLabel = STATUS_LABELS[n.status] || n.status;
      return `<tr>
        <td>${n.source}</td>
        <td><a href="${n.url}" target="_blank">${esc(n.title||'')}</a></td>
        <td>${esc(n.h1||'')}</td>
        <td>${fmtDate(n.published_at)}</td>
        <td><span class="badge badge-${n.status}">${statusLabel}</span></td>
        <td title="${esc(bigrams)}">${bigrams.slice(0,40)}</td>
        <td>${esc(n.llm_recommendation||'-')}</td>
        <td>${n.llm_trend_forecast||'-'}</td>
        <td>${n.sheets_row||'-'}</td>
        <td>
          <button class="btn btn-sm btn-primary" onclick="processOne('${n.id}')">Анализ</button>
          <button class="btn btn-sm btn-success" onclick="exportOne('${n.id}')">Sheets</button>
        </td>
      </tr>`;
    }).join('');
  }
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

function switchToTab(tabName) {
  document.querySelectorAll('.tab').forEach(x => x.classList.remove('active'));
  document.querySelectorAll('.panel').forEach(x => x.classList.remove('active'));
  document.querySelector(`.tab[data-tab="${tabName}"]`).classList.add('active');
  document.getElementById('panel-' + tabName).classList.add('active');
}

async function sendToReview() {
  const ids = getSelectedIds();
  if (!ids.length) { toast('Сначала выберите новости', true); return; }
  toast('Запуск проверки...');
  const r = await api('/api/review', {news_ids: ids});
  if (r.status !== 'ok') { toast(r.message, true); return; }
  _reviewResults = r.results || [];

  // Show groups
  const groupsHtml = (r.groups||[]).map(g => {
    const icon = g.status === 'trending' ? '&#9889;' : g.status === 'popular' ? '&#128293;' : '&#128994;';
    const titles = g.members.map(m => esc(m.title)).join('<br>');
    return `<div class="card" style="margin-bottom:8px;padding:10px"><b>${icon} ${g.status.toUpperCase()}</b> (${g.members.length} шт):<div style="font-size:0.85em;color:#8899a6;margin-top:4px">${titles}</div></div>`;
  }).join('');
  document.getElementById('review-groups').innerHTML = groupsHtml;

  // Render results table with action buttons
  document.getElementById('review-table').innerHTML = _reviewResults.map(r => {
    const q = r.checks.quality, rel = r.checks.relevance, f = r.checks.freshness, v = r.checks.viral;
    const sent = r.sentiment || {};
    const mom = r.momentum || {};
    const tags = (r.tags||[]).map(t=>t.label).join(', ') || '-';
    const sentColor = sent.label==='positive'?'#17bf63':sent.label==='negative'?'#e0245e':'#8899a6';
    const momLevel = mom.level||'none';
    const passIcon = r.overall_pass ? '&#9989;' : '&#10060;';
    const dup = r.is_duplicate ? '&#128308; DUP' : (r.dedup_status||'unique');
    return `<tr id="review-row-${r.id}">
      <td><input type="checkbox" class="approve-check" data-id="${r.id}" ${r.overall_pass && !r.is_duplicate ? 'checked' : ''}></td>
      <td><a href="${r.url}" target="_blank" title="${esc(r.title||'')}">${esc((r.title||'').slice(0,55))}</a></td>
      <td>${r.source}</td>
      <td>${dup}</td>
      <td>${q.score} ${q.pass?'&#9989;':'&#10060;'}</td>
      <td>${rel.score} ${rel.pass?'&#9989;':'&#10060;'}</td>
      <td>${f.score} ${f.status||''}</td>
      <td>${v.score} ${v.level||''}</td>
      <td style="color:${sentColor}">${sent.score||0} ${sent.label||''}</td>
      <td>${mom.score||0} ${momLevel}</td>
      <td style="max-width:100px;overflow:hidden;text-overflow:ellipsis" title="${esc(tags)}">${tags}</td>
      <td><b>${r.total_score}</b></td>
      <td>${passIcon}</td>
      <td style="white-space:nowrap">
        <button class="btn btn-sm btn-success" onclick="approveOne('${r.id}')">&#10004;</button>
        <button class="btn btn-sm btn-danger" onclick="rejectOne('${r.id}')">&#10008;</button>
      </td>
    </tr>`;
  }).join('');

  // Update badge and switch to review tab
  const badge = document.getElementById('review-badge');
  badge.textContent = _reviewResults.length;
  badge.style.display = 'inline';
  document.getElementById('review-empty').style.display = 'none';
  document.getElementById('review-content').style.display = 'block';
  document.getElementById('review-count').textContent = _reviewResults.length + ' новостей';

  switchToTab('review');
  toast('Проверено: ' + _reviewResults.length + ' новостей');
  loadAll();
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
        <button class="btn btn-sm btn-secondary" onclick="openEditModal('${esc(s.name)}')">Ред.</button>
        <button class="btn btn-sm btn-primary" onclick="reparseSource('${esc(s.name)}')">Парсить</button>
        <button class="btn btn-sm btn-danger" onclick="deleteSource('${esc(s.name)}')">Удалить</button>
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
    `<tr><td>${u.username}</td><td>${u.username==='admin'?'':'<button class="btn btn-sm btn-danger" onclick="deleteUser(\''+u.username+'\')">Удалить</button>'}</td></tr>`
  ).join('');
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
  document.getElementById('test-sheets-result').textContent = 'Loading...';
  const r = await api('/api/test_sheets', {});
  document.getElementById('test-sheets-result').textContent = JSON.stringify(r, null, 2);
}

// Health
async function loadHealth() {
  const data = await api('/api/health');
  document.getElementById('health-table').innerHTML = data.map(h => {
    const icon = h.status==='healthy'?'✅':h.status==='low'?'🟡':h.status==='warning'?'⚠️':'❌';
    return `<tr>
      <td>${icon} ${h.status}</td>
      <td>${h.source}</td>
      <td>${h.count_24h}</td>
      <td>${fmtDate(h.last_parsed)}</td>
      <td>${h.minutes_ago >= 0 ? h.minutes_ago + ' min' : '?'}</td>
    </tr>`;
  }).join('');
}

// Init
function loadAll() { loadStats(); loadNews(); }
loadAll();
loadSources();
loadPrompts();
loadSettings();
loadUsers();
loadHealth();
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
