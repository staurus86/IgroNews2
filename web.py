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
    <div class="tab" data-tab="editor">Редактор</div>
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
    <div class="grid-2">
      <div class="card">
        <h2>Выбор новости</h2>
        <div class="dash-filters" style="background:transparent;padding:0;margin-bottom:10px">
          <input type="search" id="editor-search" placeholder="Поиск по заголовку..." oninput="filterEditorNews()" autocomplete="off" name="editor-search-nologin" style="flex:1">
          <select id="editor-source-filter" onchange="filterEditorNews()" style="width:auto">
            <option value="">Все источники</option>
          </select>
        </div>
        <div id="editor-news-list" style="max-height:500px;overflow-y:auto;font-size:0.85em"></div>
      </div>
      <div class="card">
        <h2>Предпросмотр</h2>
        <div id="editor-preview" style="color:#8899a6;font-size:0.9em">Выберите новость слева</div>
      </div>
    </div>
    <div class="card" style="margin-top:15px">
      <h2>Переписать через LLM</h2>
      <div style="display:flex;gap:10px;align-items:center;margin-bottom:12px;flex-wrap:wrap">
        <span class="filter-label">Стиль:</span>
        <select id="rewrite-style">
          <option value="news">Информационный</option>
          <option value="seo">SEO-оптимизированный</option>
          <option value="review">Обзорный</option>
          <option value="clickbait">Кликбейтный</option>
          <option value="short">Короткий</option>
          <option value="social">Для соцсетей</option>
        </select>
        <span class="filter-label">Язык:</span>
        <select id="rewrite-lang">
          <option value="русский">Русский</option>
          <option value="английский">English</option>
        </select>
        <button class="btn btn-primary" onclick="rewriteNews()" id="rewrite-btn" disabled>Переписать</button>
        <button class="btn btn-warning" onclick="mergeSelected()" id="merge-btn" disabled>Объединить выбранные</button>
        <span id="rewrite-loading" style="color:#8899a6;font-size:0.85em"></span>
      </div>
      <div id="rewrite-result" style="display:none">
        <div class="grid-2">
          <div>
            <h2 style="margin-bottom:8px">Результат</h2>
            <div style="margin-bottom:8px"><b>Заголовок:</b> <span id="rw-title" style="color:#1da1f2"></span></div>
            <div style="margin-bottom:8px"><b>SEO Title:</b> <span id="rw-seo-title" style="color:#17bf63"></span></div>
            <div style="margin-bottom:8px"><b>SEO Desc:</b> <span id="rw-seo-desc" style="color:#8899a6;font-size:0.9em"></span></div>
            <div style="margin-bottom:8px"><b>Теги:</b> <span id="rw-tags"></span></div>
            <div id="rw-text" style="white-space:pre-wrap;color:#e1e8ed;font-size:0.9em;line-height:1.6;margin-top:10px;padding:12px;background:#22303c;border-radius:8px;max-height:400px;overflow-y:auto"></div>
          </div>
          <div>
            <h2 style="margin-bottom:8px">Оригинал</h2>
            <div id="rw-original" style="white-space:pre-wrap;color:#8899a6;font-size:0.85em;line-height:1.5;padding:12px;background:#22303c;border-radius:8px;max-height:500px;overflow-y:auto"></div>
          </div>
        </div>
        <div style="margin-top:12px;display:flex;gap:8px">
          <button class="btn btn-success" onclick="copyRewrite()">Копировать текст</button>
          <button class="btn btn-secondary" onclick="copyRewriteJson()">Копировать JSON</button>
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
      <td>${v.score} <span style="color:#8899a6;font-size:0.8em">${v.level||''}</span></td>
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

function filterEditorNews() {
  const search = (document.getElementById('editor-search')?.value || '').toLowerCase();
  const source = document.getElementById('editor-source-filter')?.value || '';
  let filtered = _allNews;
  if (search) filtered = filtered.filter(n => (n.title||'').toLowerCase().includes(search));
  if (source) filtered = filtered.filter(n => n.source === source);
  renderEditorList(filtered.slice(0, 50));
}

function renderEditorList(news) {
  const el = document.getElementById('editor-news-list');
  if (!news.length) { el.innerHTML = '<div style="color:#8899a6;padding:20px;text-align:center">Нет новостей</div>'; return; }
  el.innerHTML = news.map(n => {
    const isSelected = _editorNewsId === n.id;
    const isMerge = _editorMergeIds.has(n.id);
    return `<div style="padding:8px;border-bottom:1px solid #22303c;cursor:pointer;display:flex;gap:8px;align-items:start;${isSelected?'background:#1da1f215;border-left:3px solid #1da1f2':''}${isMerge?'background:#ffad1f15;border-left:3px solid #ffad1f':''}" onclick="selectEditorNews('${n.id}')">
      <input type="checkbox" class="merge-check" data-id="${n.id}" ${isMerge?'checked':''} onclick="event.stopPropagation();toggleMerge('${n.id}',this.checked)" style="margin-top:3px">
      <div style="flex:1;min-width:0">
        <div style="font-size:0.9em;white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="${esc(n.title||'')}">${esc(n.title||'')}</div>
        <div style="font-size:0.75em;color:#8899a6">${n.source} | ${fmtDate(n.published_at||n.parsed_at)} | <span class="badge badge-${n.status}">${STATUS_LABELS[n.status]||n.status}</span></div>
      </div>
    </div>`;
  }).join('');
}

function toggleMerge(id, checked) {
  if (checked) _editorMergeIds.add(id); else _editorMergeIds.delete(id);
  document.getElementById('merge-btn').disabled = _editorMergeIds.size < 2;
  filterEditorNews();
}

async function selectEditorNews(id) {
  _editorNewsId = id;
  document.getElementById('rewrite-btn').disabled = false;
  filterEditorNews();
  // Load detail
  const r = await api('/api/news/detail', {news_id: id});
  if (r.status !== 'ok') { toast(r.message, true); return; }
  const n = r.news;
  const a = r.analysis;
  let html = `<div style="margin-bottom:8px"><b style="color:#1da1f2;font-size:1.1em">${esc(n.title||'')}</b></div>`;
  html += `<div style="margin-bottom:6px;font-size:0.85em;color:#8899a6">Источник: ${n.source} | <a href="${n.url}" target="_blank">Открыть</a> | ${fmtDate(n.published_at)}</div>`;
  if (n.h1 && n.h1 !== n.title) html += `<div style="margin-bottom:6px"><b>H1:</b> ${esc(n.h1)}</div>`;
  if (n.description) html += `<div style="margin-bottom:6px"><b>Description:</b> ${esc(n.description).slice(0,300)}</div>`;
  html += `<div style="margin-top:10px;padding:10px;background:#22303c;border-radius:8px;font-size:0.85em;max-height:300px;overflow-y:auto;white-space:pre-wrap;line-height:1.5">${esc(n.plain_text||'Текст не загружен')}</div>`;
  if (a) {
    html += `<div style="margin-top:10px;font-size:0.85em">`;
    if (a.llm_recommendation) html += `<div><b>LLM:</b> ${esc(a.llm_recommendation)} (скор: ${a.llm_trend_forecast||'-'})</div>`;
    if (a.bigrams) { try { const bg = JSON.parse(a.bigrams); html += `<div><b>Биграммы:</b> ${bg.map(b=>b[0]).join(', ')}</div>`; } catch(e){} }
    html += `</div>`;
  }
  document.getElementById('editor-preview').innerHTML = html;
}

async function rewriteNews() {
  if (!_editorNewsId) { toast('Выберите новость', true); return; }
  const style = document.getElementById('rewrite-style').value;
  const lang = document.getElementById('rewrite-lang').value;
  document.getElementById('rewrite-loading').textContent = 'Переписываем...';
  document.getElementById('rewrite-btn').disabled = true;
  const r = await api('/api/rewrite', {news_id: _editorNewsId, style, language: lang});
  document.getElementById('rewrite-loading').textContent = '';
  document.getElementById('rewrite-btn').disabled = false;
  if (r.status !== 'ok') { toast(r.message, true); return; }
  _lastRewrite = r.result;
  document.getElementById('rw-title').textContent = r.result.title || '';
  document.getElementById('rw-seo-title').textContent = r.result.seo_title || '';
  document.getElementById('rw-seo-desc').textContent = r.result.seo_description || '';
  document.getElementById('rw-tags').innerHTML = (r.result.tags||[]).map(t => `<span class="tag tag-release">${esc(t)}</span>`).join(' ');
  document.getElementById('rw-text').textContent = r.result.text || '';
  document.getElementById('rw-original').textContent = r.original_title + '\n\n' + (document.querySelector('#editor-preview pre, #editor-preview div[style*="pre-wrap"]')?.textContent || '');
  document.getElementById('rewrite-result').style.display = 'block';
  toast('Переписано!');
}

async function mergeSelected() {
  if (_editorMergeIds.size < 2) { toast('Выберите минимум 2 новости', true); return; }
  document.getElementById('rewrite-loading').textContent = 'Объединяем...';
  document.getElementById('merge-btn').disabled = true;
  const r = await api('/api/merge', {news_ids: [..._editorMergeIds]});
  document.getElementById('rewrite-loading').textContent = '';
  document.getElementById('merge-btn').disabled = false;
  if (r.status !== 'ok') { toast(r.message, true); return; }
  _lastRewrite = r.result;
  document.getElementById('rw-title').textContent = r.result.merged_title || '';
  document.getElementById('rw-seo-title').textContent = '';
  document.getElementById('rw-seo-desc').textContent = 'Источники: ' + (r.sources||[]).join(', ');
  document.getElementById('rw-tags').innerHTML = '';
  document.getElementById('rw-text').textContent = r.result.merged_text || '';
  const facts = r.result.unique_facts || [];
  document.getElementById('rw-original').textContent = 'Уникальные факты:\n' + facts.map((f,i) => (i+1)+'. '+f).join('\n') + '\n\nЛучший источник: ' + (r.result.best_source||'');
  document.getElementById('rewrite-result').style.display = 'block';
  toast('Объединено!');
}

function copyRewrite() {
  if (!_lastRewrite) return;
  const text = (_lastRewrite.title || _lastRewrite.merged_title || '') + '\n\n' + (_lastRewrite.text || _lastRewrite.merged_text || '');
  navigator.clipboard.writeText(text);
  toast('Скопировано!');
}

function copyRewriteJson() {
  if (!_lastRewrite) return;
  navigator.clipboard.writeText(JSON.stringify(_lastRewrite, null, 2));
  toast('JSON скопирован!');
}

function initEditorSourceFilter() {
  const sources = [...new Set(_allNews.map(n => n.source))].sort();
  const sel = document.getElementById('editor-source-filter');
  if (sel && sel.options.length <= 1) {
    sources.forEach(s => { const o = document.createElement('option'); o.value = s; o.textContent = s; sel.appendChild(o); });
  }
  filterEditorNews();
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
