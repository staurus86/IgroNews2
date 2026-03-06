import json
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

from storage.database import get_connection, _is_postgres

PORT = 8080


class DashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/api/stats":
            self._json_response(self._get_stats())
        elif self.path == "/api/news":
            self._json_response(self._get_recent_news())
        else:
            self._serve_dashboard()

    def _json_response(self, data):
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        self.wfile.write(body)

    def _get_stats(self):
        conn = get_connection()
        cur = conn.cursor()
        stats = {}
        for status in ["new", "processed", "approved", "rejected"]:
            cur.execute(
                "SELECT COUNT(*) FROM news WHERE status = %s" if _is_postgres()
                else "SELECT COUNT(*) FROM news WHERE status = ?",
                (status,)
            )
            row = cur.fetchone()
            stats[status] = row[0] if row else 0
        cur.execute("SELECT COUNT(*) FROM news")
        stats["total"] = cur.fetchone()[0]
        return stats

    def _get_recent_news(self):
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT source, title, url, status, parsed_at FROM news ORDER BY parsed_at DESC LIMIT 50"
        )
        if _is_postgres():
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
        else:
            return [dict(row) for row in cur.fetchall()]

    def _serve_dashboard(self):
        html = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>IgroNews Dashboard</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         background: #0f1923; color: #e1e8ed; padding: 20px; }
  h1 { color: #1da1f2; margin-bottom: 20px; }
  .stats { display: flex; gap: 15px; margin-bottom: 30px; flex-wrap: wrap; }
  .stat-card { background: #192734; border-radius: 12px; padding: 20px 25px; min-width: 140px; }
  .stat-card .number { font-size: 2.2em; font-weight: bold; color: #1da1f2; }
  .stat-card .label { color: #8899a6; font-size: 0.9em; margin-top: 4px; }
  .stat-card.new .number { color: #ffad1f; }
  .stat-card.processed .number { color: #17bf63; }
  table { width: 100%; border-collapse: collapse; background: #192734; border-radius: 12px; overflow: hidden; }
  th { background: #22303c; text-align: left; padding: 12px 15px; color: #8899a6; font-size: 0.85em; }
  td { padding: 10px 15px; border-bottom: 1px solid #22303c; font-size: 0.9em; }
  tr:hover { background: #22303c; }
  a { color: #1da1f2; text-decoration: none; }
  a:hover { text-decoration: underline; }
  .status { padding: 3px 10px; border-radius: 12px; font-size: 0.8em; }
  .status-new { background: #ffad1f22; color: #ffad1f; }
  .status-processed { background: #17bf6322; color: #17bf63; }
  .status-approved { background: #1da1f222; color: #1da1f2; }
  .refresh { color: #8899a6; font-size: 0.85em; margin-bottom: 15px; }
</style>
</head>
<body>
<h1>IgroNews Dashboard</h1>
<div class="refresh" id="refresh">Auto-refresh every 30s</div>
<div class="stats" id="stats"></div>
<table>
  <thead><tr><th>Source</th><th>Title</th><th>Status</th><th>Date</th></tr></thead>
  <tbody id="news"></tbody>
</table>
<script>
async function load() {
  try {
    const [statsRes, newsRes] = await Promise.all([
      fetch('/api/stats'), fetch('/api/news')
    ]);
    const stats = await statsRes.json();
    const news = await newsRes.json();

    document.getElementById('stats').innerHTML =
      `<div class="stat-card"><div class="number">${stats.total}</div><div class="label">Total</div></div>` +
      `<div class="stat-card new"><div class="number">${stats.new}</div><div class="label">New</div></div>` +
      `<div class="stat-card processed"><div class="number">${stats.processed}</div><div class="label">Processed</div></div>` +
      `<div class="stat-card"><div class="number">${stats.approved || 0}</div><div class="label">Approved</div></div>`;

    document.getElementById('news').innerHTML = news.map(n =>
      `<tr>
        <td>${n.source}</td>
        <td><a href="${n.url}" target="_blank">${n.title || ''}</a></td>
        <td><span class="status status-${n.status}">${n.status}</span></td>
        <td>${(n.parsed_at || '').slice(0,16)}</td>
      </tr>`
    ).join('');
  } catch(e) { console.error(e); }
}
load();
setInterval(load, 30000);
</script>
</body>
</html>"""
        body = html.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass  # suppress access logs


def start_web():
    """Запускает веб-сервер в фоновом потоке."""
    server = HTTPServer(("0.0.0.0", PORT), DashboardHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server
