# План доработок IgroNews

## Фаза 1 — UX Phase C: объединение endpoints + soft-delete + адаптив

### 1.1 Объединение 3 news-endpoints в один

**Файлы:** `api/news.py`, `web.py`, `static/dashboard.html`

**Текущее состояние:**
- `/api/news` (get_news) — возвращает approved/processed/ready, ~80 строк SQL
- `/api/editorial` (get_editorial) — ALL кроме rejected/duplicate, scoring breakdown, ~120 строк
- `/api/final` (get_final) — high-score кандидаты на публикацию, ~60 строк
- Все 3 делают JOIN `news` + `news_analysis` с пересекающимся кодом (~280 строк дублей)

**Что делаем:**
1. **api/news.py** — создаём единый `get_news_unified(params)`:
   - Query-параметр `view=editorial|final|all` (default: `editorial`)
   - `view=editorial` — текущая логика get_editorial (основной рабочий режим)
   - `view=final` — текущая логика get_final (фильтр по score + publication-ready)
   - `view=all` — без фильтра по статусу (для админов)
   - Общий SQL-билдер с WHERE-условиями по view
   - Единый набор фильтров: `status`, `source`, `date_from`, `date_to`, `search`, `viral_level`, `score_min`, `score_max`
   - Единый JSON-ответ со всеми полями (scoring breakdown всегда включён)
2. **web.py** — новый route `/api/news/list` → `get_news_unified()`
   - Старые 3 endpoint оставляем как прокси на 1 релиз (backward compat)
   - Помечаем deprecated-комментарием
3. **dashboard.html** — переключаем `loadEditorial()` и `loadFinal()` на `/api/news/list?view=...`
4. Удаляем старые функции через 1 релиз

**Результат:** −200 строк в api/news.py, единая точка входа

---

### 1.2 Soft-delete (мягкое удаление)

**Файлы:** `storage/database.py`, `api/news.py`, `api/articles.py`, `static/dashboard.html`

**Текущее состояние:**
- `DELETE FROM news` / `DELETE FROM articles` — безвозвратное удаление
- Нет поля is_deleted, нет корзины, нет аудита

**Что делаем:**
1. **storage/database.py** — миграция:
   - `ALTER TABLE news ADD COLUMN is_deleted INTEGER DEFAULT 0`
   - `ALTER TABLE news ADD COLUMN deleted_at TEXT DEFAULT NULL`
   - `ALTER TABLE articles ADD COLUMN is_deleted INTEGER DEFAULT 0`
   - `ALTER TABLE articles ADD COLUMN deleted_at TEXT DEFAULT NULL`
   - В `init_db()` добавить миграцию (проверка через PRAGMA table_info)
2. **api/news.py** — `delete_news()`:
   - Заменить `DELETE FROM` на `UPDATE news SET is_deleted=1, deleted_at=datetime('now')`
   - Не трогать news_analysis (остаётся привязана)
   - Все SELECT-запросы: добавить `WHERE is_deleted=0` (или `AND is_deleted=0`)
3. **api/articles.py** — `delete_article()`:
   - Аналогично: `UPDATE articles SET is_deleted=1, deleted_at=...`
   - Все SELECT: `WHERE is_deleted=0`
4. **api/news.py** — новый endpoint `restore_news(body)`:
   - `UPDATE news SET is_deleted=0, deleted_at=NULL WHERE id IN (...)`
   - Route: POST `/api/news/restore`
5. **api/articles.py** — новый endpoint `restore_article(body)`:
   - Route: POST `/api/articles/restore`
6. **api/news.py** — новый endpoint `get_trash(params)`:
   - SELECT с `WHERE is_deleted=1`, сортировка по deleted_at DESC
   - Route: GET `/api/news/trash`
7. **dashboard.html**:
   - Кнопка «Корзина» в навигации (badge с кол-вом)
   - Список удалённых с кнопками «Восстановить» / «Удалить навсегда»
   - Endpoint для hard-delete: POST `/api/news/purge` (только admin)
8. **Автоочистка**: в `pipeline/orchestrator.py` или отдельный cron — удалять записи с `is_deleted=1` старше 30 дней

**Результат:** безопасное удаление, возможность восстановления, корзина в UI

---

### 1.3 Адаптивная вёрстка 1024px

**Файл:** `static/dashboard.html` (CSS-секция)

**Текущее состояние:**
- Единственный breakpoint: `@media(max-width:768px)` (line 167)
- Container: max-width 1600px
- Таблицы: TD max-width 400px
- На планшетах (768-1024px) — горизонтальный скролл, мелкий текст

**Что делаем:**
1. Добавить `@media(max-width:1024px)`:
   - `.container` — padding: 10px (вместо 20px)
   - `.grid-2` — grid-template-columns: 1fr (одна колонка)
   - Таблицы новостей: скрыть колонки `source`, `date` (показывать в деталях)
   - `.triage-card` — max-width: 100%
   - Фильтры: flex-wrap, элементы по 48% ширины
   - Модалки: width: 95vw
   - Боковое меню: collapsible (hamburger)
2. Добавить `@media(max-width:480px)`:
   - Фильтры: 100% ширина, вертикальный stack
   - Кнопки действий: полная ширина
   - Font-size на таблицах: 13px
3. Мета-тег viewport (если отсутствует): `<meta name="viewport" content="width=device-width, initial-scale=1">`
4. Touch-friendly: min-height 44px на интерактивных элементах

**Результат:** нормальная работа на планшетах и крупных мобильных

---

## Фаза 2 — Безопасность

### 2.1 Credentials в env-переменные

**Файлы:** `web.py`, `config.py`

**Текущее состояние:**
- `web.py:24` — `_COOKIE_SECRET = os.getenv("COOKIE_SECRET", "igronews-default-secret-key-2024")`
- `web.py:27-28` — `USERS = {"admin": {"hash": hashlib.sha256("admin123".encode()).hexdigest(), ...}}`
- config.py — API-ключи уже в env ✓

**Что делаем:**
1. **web.py** — убрать fallback из COOKIE_SECRET:
   ```python
   _COOKIE_SECRET = os.getenv("COOKIE_SECRET")
   if not _COOKIE_SECRET:
       raise RuntimeError("COOKIE_SECRET env var is required")
   ```
2. **web.py** — USERS из env:
   - Env: `ADMIN_PASSWORD_HASH` (bcrypt hash)
   - Env: `ADMIN_USERNAME` (default: "admin")
   - Fallback для dev: если `ADMIN_PASSWORD_HASH` не задан и `ENV != production` — использовать дефолт с warning в логе
3. **Railway** — добавить переменные:
   - `COOKIE_SECRET` — рандомная строка 64 символа
   - `ADMIN_PASSWORD_HASH` — bcrypt hash нового пароля

**Результат:** нет секретов в коде

---

### 2.2 bcrypt для паролей

**Файлы:** `web.py`, `requirements.txt`

**Текущее состояние:**
- SHA256 напрямую: `hashlib.sha256(password.encode()).hexdigest()` — быстрый hash, уязвим к brute-force

**Что делаем:**
1. `pip install bcrypt` → добавить в requirements.txt
2. **web.py** — логин (line ~336):
   ```python
   import bcrypt
   # Проверка:
   bcrypt.checkpw(password.encode(), stored_hash.encode())
   ```
3. **web.py** — смена пароля (`change_password`):
   ```python
   new_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
   ```
4. **Миграция**: при первом логине с SHA256-хешем — автоконвертация в bcrypt
   - Проверить: если hash начинается с `$2b$` — bcrypt, иначе — SHA256
   - При успешном SHA256-логине — перехешировать в bcrypt и сохранить
5. Хранение пользователей: перенести из dict в SQLite таблицу `users`
   - `CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password_hash TEXT, role TEXT, created_at TEXT)`
   - `init_db()` — создать таблицу + миграция текущих USERS в неё

**Результат:** стойкое хеширование, пароли в БД, а не в коде

---

### 2.3 CSRF-защита

**Файлы:** `web.py`, `static/dashboard.html`

**Текущее состояние:**
- Ноль CSRF-защиты
- Все POST-запросы принимаются без токена

**Что делаем:**
1. **web.py** — генерация CSRF-токена:
   - При логине: генерировать `csrf_token = secrets.token_hex(32)`
   - Сохранять в сессии (отдельная cookie `csrf_token`, HttpOnly=False чтобы JS мог читать)
   - Или: возвращать в JSON-ответе логина
2. **web.py** — валидация на всех POST:
   - Новый метод `_check_csrf(self)`:
     - Читать header `X-CSRF-Token`
     - Сравнивать с токеном из сессии
     - Если не совпадает → 403
   - Вызывать в начале каждого POST-обработчика
   - Исключения: `/api/login`, `/api/diag`
3. **dashboard.html** — отправка токена:
   - В `api()` helper (line ~1569): добавить header `X-CSRF-Token`
   - Читать из cookie или из localStorage (заполняется при логине)
4. **Важно**: SameSite=Lax уже стоит на auth-cookie — это частичная защита, CSRF-токен добавляет полную

**Результат:** защита от CSRF-атак на все state-changing endpoints

---

### 2.4 _require_perm на все endpoints

**Файлы:** `web.py`, `api/news.py`, `api/articles.py`, `api/queue.py`, `api/viral.py`, `api/settings.py`, `api/dashboard.py`

**Текущее состояние:**
- 87 endpoints, только 7 защищены `_require_perm()`
- 77 endpoints без проверки прав
- Viewer может удалять, approve-ить, экспортировать

**Что делаем — маппинг прав:**

| Endpoint группа | Permission | Роли |
|----------------|------------|------|
| GET /api/news/*, editorial, final, stats, analytics | `read` | admin, editor, viewer |
| POST /api/approve, /api/reject | `approve` | admin, editor |
| POST /api/news/delete, /api/articles/delete | `delete` | admin |
| POST /api/news/restore, /api/news/purge | `delete` | admin |
| POST /api/rewrite, /api/batch_rewrite | `write` | admin, editor |
| POST /api/export_sheets | `write` | admin, editor |
| POST /api/articles/* (save, update) | `write` | admin, editor |
| POST /api/queue/* (cancel, retry) | `write` | admin, editor |
| POST /api/viral/* (save, delete) | `write` | admin, editor |
| POST /api/users/* (add, delete, change_password) | `users` | admin |
| POST /api/settings/*, feature_flags/* | `settings` | admin |
| POST /api/pipeline/* | `pipeline` | admin, editor |
| POST /api/process_one, run_auto_review, rescore | `write` | admin, editor |
| GET /api/diag | — (public) | без авторизации |

**Реализация:**
1. В каждом POST-обработчике в `web.py` добавить `if not self._require_perm("perm"): return`
2. В GET-обработчиках для read: проверять авторизацию (залогинен), но не perm
3. Исключения: `/api/diag`, `/api/login` — без проверки
4. Добавить новую permission `export` для экспорта в Sheets (опционально)

**Результат:** RBAC на всех endpoints, viewer не может менять данные

---

## Фаза 3 — Новые парсеры

### 3.1 VK-парсер для студийных страниц

**Файлы:** `parsers/vk_parser.py` (новый), `config.py`

**Зависимости:** VK API токен (сервисный ключ приложения)

**Что делаем:**
1. **config.py**:
   - `VK_API_TOKEN = os.getenv("VK_API_TOKEN", "")`
   - `VK_API_VERSION = "5.199"`
   - Список групп в SOURCES: `{"name": "VK:...", "type": "vk", "group_id": "...", ...}`
2. **parsers/vk_parser.py**:
   - Метод `parse_vk_source(source: dict) -> int`
   - VK API метод `wall.get`: https://api.vk.com/method/wall.get
   - Параметры: `owner_id=-{group_id}`, `count=20`, `filter=owner`
   - Из каждого поста извлекать:
     - `text` → title (первые 100 символов) + plain_text
     - `date` → published_at (unix timestamp → ISO)
     - `attachments[type=link]` → url (если есть внешняя ссылка)
     - Если нет внешней ссылки: url = `https://vk.com/wall{owner_id}_{id}`
   - Фильтрация: пропускать посты без текста, рекламные (#ad, #реклама)
   - Дедупликация: md5(url) как в остальных парсерах
   - Сохранение через `insert_news()`
3. **scheduler.py** — добавить VK-источники в расписание
4. **Целевые группы** (примеры):
   - PlayStation Russia, Xbox Russia, Nintendo Russia
   - Игровые студии: Mundfish, Owlcat Games и т.д.

**Результат:** парсинг игровых новостей из VK-сообществ

---

### 3.2 Twitter/X парсер (платный API)

**Файлы:** `parsers/twitter_parser.py` (новый), `config.py`

**Зависимости:** Twitter API v2 Bearer Token (Basic plan: $100/мес, 10K tweets/мес)

**Что делаем:**
1. **config.py**:
   - `TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN", "")`
   - Источники: `{"name": "X:...", "type": "twitter", "username": "...", ...}`
2. **parsers/twitter_parser.py**:
   - Метод `parse_twitter_source(source: dict) -> int`
   - Twitter API v2: `GET /2/users/:id/tweets`
   - Параметры: `max_results=20`, `tweet.fields=created_at,entities`, `exclude=retweets,replies`
   - Извлечение:
     - `text` → title (первые 100 символов) + plain_text
     - `created_at` → published_at
     - `entities.urls[0].expanded_url` → url (если есть ссылка на статью)
     - Если нет внешней ссылки: url = `https://x.com/{username}/status/{id}`
   - Rate limiting: 900 req/15min (user), 1500 req/15min (app)
   - Retry с backoff при 429
3. **scheduler.py** — добавить X-источники, интервал 30 мин (экономия лимитов)
4. **Целевые аккаунты** (примеры):
   - @IGN, @Wario64, @geoffkeighley, @jaboratory
   - Студии: @PlayStation, @Xbox, @NintendoAmerica

**Результат:** парсинг игровых новостей из Twitter/X

---

## Статус выполнения

| Задача | Статус |
|--------|--------|
| 1.1 Unified endpoint | ✅ Готово |
| 1.2 Soft-delete | ✅ Готово |
| 1.3 Responsive CSS | ✅ Готово |
| 2.x Безопасность | ⏭️ Отложено |
| 3.1 VK parser | ✅ Готово |
| 3.2 Telegram parser | ✅ Готово (Telethon + RSSHub fallback) |

## Порядок выполнения

```
Фаза 1 (UX) — 3 задачи, можно параллелить 1.2 и 1.3
  1.1 Unified news endpoint     ← первым (меняет API-контракт)
  1.2 Soft-delete                ← после 1.1 (зависит от нового endpoint)
  1.3 Responsive 1024px          ← параллельно с 1.2 (только CSS)

Фаза 2 (Security) — строго последовательно
  2.1 Credentials в env          ← первым (база для остального)
  2.2 bcrypt                     ← после 2.1 (зависит от таблицы users)
  2.3 CSRF                       ← после 2.2
  2.4 RBAC на все endpoints      ← последним (финальная обвязка)

Фаза 3 (Parsers) — независимо друг от друга
  3.1 VK-парсер                  ← при наличии VK API токена
  3.2 Twitter/X парсер           ← при наличии Twitter API ($100/мес)
```

## Оценка объёма

| Задача | Файлов | Строк кода (±) |
|--------|--------|-----------------|
| 1.1 Unified endpoint | 3 | −200 / +80 |
| 1.2 Soft-delete | 4 | +120 |
| 1.3 Responsive CSS | 1 | +60 |
| 2.1 Credentials | 2 | ±30 |
| 2.2 bcrypt + users table | 2 | +80 |
| 2.3 CSRF | 2 | +50 |
| 2.4 RBAC everywhere | 6 | +40 |
| 3.1 VK parser | 3 | +100 |
| 3.2 Twitter parser | 3 | +120 |
| **Итого** | — | **~+280 net** |
