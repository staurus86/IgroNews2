"""Generate IgroNews User Manual as DOCX."""
from docx import Document
from docx.shared import Inches, Pt, Cm, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.style import WD_STYLE_TYPE
from docx.oxml.ns import qn
import os

doc = Document()

# ── Page setup ──
for section in doc.sections:
    section.top_margin = Cm(2)
    section.bottom_margin = Cm(2)
    section.left_margin = Cm(2.5)
    section.right_margin = Cm(2.5)

# ── Styles ──
style = doc.styles['Normal']
style.font.name = 'Calibri'
style.font.size = Pt(11)
style.font.color.rgb = RGBColor(0x33, 0x33, 0x33)
style.paragraph_format.space_after = Pt(6)
style.paragraph_format.line_spacing = 1.15

for level in range(1, 4):
    h = doc.styles[f'Heading {level}']
    h.font.name = 'Calibri'
    h.font.color.rgb = RGBColor(0x1A, 0x73, 0xE8)
    if level == 1:
        h.font.size = Pt(22)
        h.paragraph_format.space_before = Pt(24)
        h.paragraph_format.space_after = Pt(12)
    elif level == 2:
        h.font.size = Pt(16)
        h.paragraph_format.space_before = Pt(18)
        h.paragraph_format.space_after = Pt(8)
    else:
        h.font.size = Pt(13)
        h.font.color.rgb = RGBColor(0x42, 0x85, 0xF4)
        h.paragraph_format.space_before = Pt(12)
        h.paragraph_format.space_after = Pt(6)

# Helper: styled table
def add_styled_table(headers, rows, col_widths=None):
    table = doc.add_table(rows=1 + len(rows), cols=len(headers))
    table.style = 'Table Grid'
    table.alignment = WD_TABLE_ALIGNMENT.CENTER

    # Header row
    hdr = table.rows[0]
    for i, text in enumerate(headers):
        cell = hdr.cells[i]
        cell.text = ''
        p = cell.paragraphs[0]
        run = p.add_run(text)
        run.bold = True
        run.font.size = Pt(10)
        run.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)
        run.font.name = 'Calibri'
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        # Blue background
        shading = cell._element.get_or_add_tcPr()
        shd = shading.makeelement(qn('w:shd'), {
            qn('w:fill'): '1A73E8',
            qn('w:val'): 'clear',
        })
        shading.append(shd)

    # Data rows
    for r_idx, row_data in enumerate(rows):
        row = table.rows[r_idx + 1]
        for c_idx, text in enumerate(row_data):
            cell = row.cells[c_idx]
            cell.text = ''
            p = cell.paragraphs[0]
            run = p.add_run(str(text))
            run.font.size = Pt(9.5)
            run.font.name = 'Calibri'
            # Zebra striping
            if r_idx % 2 == 1:
                shading = cell._element.get_or_add_tcPr()
                shd = shading.makeelement(qn('w:shd'), {
                    qn('w:fill'): 'F0F4FF',
                    qn('w:val'): 'clear',
                })
                shading.append(shd)

    if col_widths:
        for i, w in enumerate(col_widths):
            for row in table.rows:
                row.cells[i].width = Cm(w)

    doc.add_paragraph('')  # spacer
    return table

# Helper: bullet list
def add_bullets(items, bold_prefix=False):
    for item in items:
        p = doc.add_paragraph(style='List Bullet')
        if bold_prefix and ':' in item:
            parts = item.split(':', 1)
            run = p.add_run(parts[0] + ':')
            run.bold = True
            run.font.size = Pt(10.5)
            p.add_run(parts[1]).font.size = Pt(10.5)
        else:
            run = p.add_run(item)
            run.font.size = Pt(10.5)

# Helper: info box (colored paragraph)
def add_info_box(text, color='blue'):
    p = doc.add_paragraph()
    colors = {
        'blue': ('E8F0FE', '1A73E8'),
        'green': ('E6F4EA', '137333'),
        'orange': ('FEF7E0', 'E37400'),
        'red': ('FCE8E6', 'C5221F'),
    }
    bg, fg = colors.get(color, colors['blue'])
    run = p.add_run(f'  {text}')
    run.font.size = Pt(10)
    run.font.color.rgb = RGBColor(int(fg[:2], 16), int(fg[2:4], 16), int(fg[4:], 16))
    run.font.name = 'Calibri'
    # Background shading on paragraph
    pPr = p._element.get_or_add_pPr()
    shd = pPr.makeelement(qn('w:shd'), {
        qn('w:fill'): bg,
        qn('w:val'): 'clear',
    })
    pPr.append(shd)
    p.paragraph_format.left_indent = Cm(0.5)
    p.paragraph_format.space_before = Pt(4)
    p.paragraph_format.space_after = Pt(4)

def add_numbered(items):
    for item in items:
        p = doc.add_paragraph(style='List Number')
        if ':' in item:
            parts = item.split(':', 1)
            run = p.add_run(parts[0] + ':')
            run.bold = True
            run.font.size = Pt(10.5)
            p.add_run(parts[1]).font.size = Pt(10.5)
        else:
            p.add_run(item).font.size = Pt(10.5)

# ══════════════════════════════════════════════
# TITLE PAGE
# ══════════════════════════════════════════════

for _ in range(6):
    doc.add_paragraph('')

p = doc.add_paragraph()
p.alignment = WD_ALIGN_PARAGRAPH.CENTER
run = p.add_run('IgroNews')
run.font.size = Pt(42)
run.font.color.rgb = RGBColor(0x1A, 0x73, 0xE8)
run.bold = True
run.font.name = 'Calibri'

p = doc.add_paragraph()
p.alignment = WD_ALIGN_PARAGRAPH.CENTER
run = p.add_run('Агрегатор игровых новостей с NLP-аналитикой')
run.font.size = Pt(16)
run.font.color.rgb = RGBColor(0x66, 0x66, 0x66)
run.font.name = 'Calibri'

doc.add_paragraph('')

p = doc.add_paragraph()
p.alignment = WD_ALIGN_PARAGRAPH.CENTER
run = p.add_run('Руководство пользователя')
run.font.size = Pt(20)
run.font.color.rgb = RGBColor(0x42, 0x85, 0xF4)
run.font.name = 'Calibri'

for _ in range(8):
    doc.add_paragraph('')

p = doc.add_paragraph()
p.alignment = WD_ALIGN_PARAGRAPH.CENTER
run = p.add_run('Версия 2.0  •  Март 2026')
run.font.size = Pt(11)
run.font.color.rgb = RGBColor(0x99, 0x99, 0x99)

doc.add_page_break()

# ══════════════════════════════════════════════
# TABLE OF CONTENTS
# ══════════════════════════════════════════════

doc.add_heading('Содержание', level=1)

toc_items = [
    '1. Обзор системы',
    '2. Быстрый старт',
    '3. Рабочий процесс (пайплайн)',
    '4. Вкладка «Редакция» — основная работа',
    '5. Вкладка «Обогащённые» — данные API',
    '6. Вкладка «Финал» — лучшие новости для публикации',
    '7. Вкладка «Контент» — статьи и рерайт',
    '8. Вкладка «Виральность» — тренды и триггеры',
    '9. Вкладка «Аналитика» — статистика и дайджесты',
    '10. Вкладка «Здоровье» — мониторинг источников',
    '11. Вкладка «Настройки» — конфигурация',
    '12. Источники новостей',
    '13. Система проверок (Scoring)',
    '14. Финальный скор (формула)',
    '15. API-интеграции',
    '16. Часто задаваемые вопросы',
]
for item in toc_items:
    p = doc.add_paragraph()
    run = p.add_run(item)
    run.font.size = Pt(11.5)
    run.font.color.rgb = RGBColor(0x1A, 0x73, 0xE8)
    p.paragraph_format.space_after = Pt(3)

doc.add_page_break()

# ══════════════════════════════════════════════
# 1. OVERVIEW
# ══════════════════════════════════════════════

doc.add_heading('1. Обзор системы', level=1)

doc.add_paragraph(
    'IgroNews — это автоматизированный агрегатор игровых новостей с NLP-аналитикой. '
    'Система парсит 15 источников (IGN, PCGamer, DTF, StopGame и др.), '
    'автоматически оценивает каждую новость по 12 критериям, '
    'обогащает данными из внешних API и готовит контент к публикации.'
)

doc.add_heading('Ключевые возможности', level=3)
add_bullets([
    'Автоматический парсинг: 15 источников, RSS + HTML + JSON, каждые 5–30 минут',
    'Умная оценка: 12 проверок (качество, виральность, свежесть, релевантность, моментум и др.)',
    'Авто-дедупликация: MD5 по URL + TF-IDF cosine + entity overlap',
    'Обогащение API: Keys.so (поисковые частоты), Google Trends (4 региона), LLM (прогноз трендов)',
    'Финальный скор: композитный балл с учётом внутренних проверок + API-данных',
    'Рерайт через LLM: 6 стилей (новость, SEO, обзор, кликбейт, короткий, соцсети)',
    'Экспорт в Google Sheets: автоматический + ручной',
    'Обучаемость: система учится на решениях редактора (feedback loop)',
    'Веб-панель: 7 вкладок для полного управления рабочим процессом',
], bold_prefix=True)

doc.add_heading('Структура вкладок', level=3)

add_styled_table(
    ['Вкладка', 'Назначение'],
    [
        ['Редакция', 'Основная работа: ревью, одобрение, отклонение, все новости'],
        ['Обогащённые', 'Новости с API-данными (Keys.so, Trends, LLM)'],
        ['Финал', 'Лучшие новости (только publish_now) с финальным скором'],
        ['Контент', 'Статьи + новости для рерайта (Статьи/Новости)'],
        ['Виральность', 'Анализ виральности и управление триггерами'],
        ['Аналитика', 'Статистика, дайджесты, версии промптов'],
        ['Здоровье', 'Мониторинг источников и весов'],
        ['⚙ Настройки', '8 подвкладок: общие, источники, промпты и др.'],
    ],
    col_widths=[3.5, 11.5]
)

doc.add_heading('Технологии', level=3)
add_bullets([
    'Backend: Python, Flask, APScheduler',
    'NLP: scikit-learn (TF-IDF), NLTK, кастомный NER (80+ игр, 30+ студий)',
    'LLM: OpenRouter (GPT-4o-mini по умолчанию, настраивается)',
    'База данных: PostgreSQL (Railway) / SQLite (локально)',
    'Деплой: Railway + Docker',
])

doc.add_page_break()

# ══════════════════════════════════════════════
# 2. QUICK START
# ══════════════════════════════════════════════

doc.add_heading('2. Быстрый старт', level=1)

doc.add_heading('Первый вход', level=2)
doc.add_paragraph('Откройте панель управления в браузере:')
add_info_box('URL: https://igronews-production.up.railway.app/')
add_info_box('Логин: admin  |  Пароль: admin123', 'orange')

doc.add_heading('Что происходит автоматически', level=2)
add_numbered([
    'Парсинг: каждые 5–30 минут система парсит все 15 источников',
    'Авто-ревью: после каждого парсинга новости оцениваются по 12 критериям (бесплатно, без API)',
    'Дедупликация: дубликаты обнаруживаются и помечаются',
    'Авто-отклонение: новости с баллом < 15 отклоняются автоматически',
])

doc.add_heading('Ежедневный цикл работы', level=2)
add_numbered([
    'Редакция: откройте вкладку, посмотрите новости со статусом «на проверке»',
    'Проверить новые: если есть непроверенные (new), нажмите кнопку — система оценит батч из 20',
    'Одобрить лучшие: вручную или «Авто скор>70» для массового одобрения',
    'Обогащение: автоматически запускается после одобрения (Keys.so + Trends + LLM)',
    'Обогащённые: посмотрите результаты API-анализа',
    'Финал: откройте вкладку — здесь только лучшие (publish_now) с финальным скором',
    'В контент: отправьте лучшие на рерайт или экспортируйте в Sheets',
])

add_info_box('Парсинг и авто-ревью полностью бесплатны. API вызываются только при одобрении.', 'green')

doc.add_page_break()

# ══════════════════════════════════════════════
# 3. PIPELINE
# ══════════════════════════════════════════════

doc.add_heading('3. Рабочий процесс (пайплайн)', level=1)

doc.add_paragraph(
    'Каждая новость проходит чёткий жизненный цикл. '
    'Статус определяет, на каком этапе она находится и в какой вкладке отображается.'
)

doc.add_heading('Статусы новостей', level=2)

add_styled_table(
    ['Статус', 'Описание', 'Где видна', 'Следующий шаг'],
    [
        ['new', 'Только спарсена', 'Редакция', 'Авто-ревью → in_review'],
        ['in_review', 'Оценена, ждёт решения', 'Редакция', 'Одобрить / отклонить'],
        ['duplicate', 'Дубликат', 'Редакция', '— (терминальный)'],
        ['rejected', 'Отклонена', 'Редакция', '— (терминальный)'],
        ['approved', 'Идёт обогащение', 'Обогащённые', 'Авто → processed'],
        ['processed', 'Обогащена API', 'Обогащённые, Финал', 'В контент / Sheets'],
        ['ready', 'Готова к публикации', 'Обогащённые, Финал', 'Опубликовать'],
    ],
    col_widths=[2.5, 4, 3.5, 4]
)

doc.add_heading('Схема пайплайна', level=2)

pipeline_steps = [
    '① Парсинг (авто, каждые 5–30 мин) → статус: new',
    '② Авто-ревью (бесплатно, 12 локальных проверок) → in_review / duplicate / rejected',
    '③ Ручное одобрение (редактор в Редакции) → approved',
    '④ Фоновое обогащение (Keys.so + Trends + LLM) → processed',
    '⑤ Финал (publish_now + финальный скор) → отбор лучших',
    '⑥ Контент (рерайт) или Экспорт (Sheets) → ready',
    '⑦ Публикация',
]
for step in pipeline_steps:
    p = doc.add_paragraph()
    run = p.add_run(step)
    run.font.size = Pt(11)
    if '①' in step:
        run.font.color.rgb = RGBColor(0x42, 0x85, 0xF4)
    elif '⑤' in step:
        run.font.color.rgb = RGBColor(0x17, 0xBF, 0x63)
    elif '③' in step:
        run.font.color.rgb = RGBColor(0xE3, 0x74, 0x00)

add_info_box('API-вызовы (Keys.so, Trends, LLM) происходят ТОЛЬКО после одобрения. Парсинг и ревью бесплатны.', 'orange')

doc.add_page_break()

# ══════════════════════════════════════════════
# 4. EDITORIAL TAB
# ══════════════════════════════════════════════

doc.add_heading('4. Вкладка «Редакция»', level=1)
doc.add_paragraph('Главная рабочая вкладка. Здесь происходит основная работа с новостями.')

doc.add_heading('Карточки статистики', level=2)
doc.add_paragraph('В верхней части — карточки с количеством новостей по статусам. Клик по карточке фильтрует таблицу.')
add_bullets([
    'Всего — общее число новостей в базе',
    'Новые — непроверенные (new)',
    'На ревью — оценены, ждут решения (in_review)',
    'Дубли — обнаруженные дубликаты',
    'Одобрено — отправлены на обогащение',
    'Обработано — обогащены API-данными',
    'Отклонено — отклонённые',
    'Готовые — готовы к публикации',
])

doc.add_heading('Фильтры и поиск', level=2)
add_bullets([
    'Статус: фильтр по конкретному статусу',
    'Источник: выбор сайта-источника',
    'Виральность: LOW / MEDIUM / HIGH',
    'Тир сущности: S / A / B / C',
    'Мин. скор: минимальный total_score',
    'Поиск: текстовый поиск по заголовкам',
])

doc.add_heading('Действия', level=2)

add_styled_table(
    ['Кнопка', 'Что делает'],
    [
        ['Проверить новые', 'Авто-ревью для 20 новых новостей (бесплатно)'],
        ['✓ Одобрить', 'Одобряет → запускает фоновое обогащение API'],
        ['✗ Отклонить', 'Отклоняет новость (терминально)'],
        ['Авто скор>70', 'Массово одобряет все с total_score ≥ 70'],
        ['✎ В контент', 'Отправляет в очередь рерайта (LLM)'],
        ['Перевод', 'Переводит заголовок на русский (LLM)'],
        ['В Sheets', 'Экспорт в Google Sheets'],
        ['Удалить', 'Удаляет выбранные новости (необратимо)'],
    ],
    col_widths=[4, 11]
)

doc.add_heading('Раскрываемые детали', level=2)
doc.add_paragraph(
    'Нажмите на строку — раскроется детальная информация: '
    'полный текст, все баллы проверок, теги, тональность, '
    'обнаруженные сущности (игры, студии, платформы), кнопки действий.'
)

doc.add_page_break()

# ══════════════════════════════════════════════
# 5. ENRICHED TAB
# ══════════════════════════════════════════════

doc.add_heading('5. Вкладка «Обогащённые»', level=1)
doc.add_paragraph(
    'Показывает новости после обогащения API-данными (статусы: approved, processed, ready). '
    'Здесь видны результаты Keys.so, Google Trends и LLM-анализа.'
)

doc.add_heading('Колонки данных', level=2)
add_styled_table(
    ['Колонка', 'Описание'],
    [
        ['Биграммы', 'Ключевые словосочетания из TF-IDF анализа текста'],
        ['Keys.so (freq)', 'Частота поискового запроса (чем выше — тем популярнее тема)'],
        ['Похожие', 'Количество похожих поисковых запросов из Keys.so'],
        ['Trends', 'Индекс Google Trends по 4 регионам (RU, US, GB, DE)'],
        ['LLM рекомендация', 'publish_now / schedule / skip'],
        ['LLM Score', 'Оценка трендового потенциала от LLM (0–100)'],
        ['Скор', 'Внутренний total_score (12 проверок)'],
    ],
    col_widths=[4, 11]
)

doc.add_heading('Фильтры', level=2)
add_bullets([
    'Все обогащённые — approved + processed + ready (по умолчанию)',
    'Одобренные — ждут обогащения',
    'Обогащённые — есть API-данные (processed)',
    'Готовые — экспортированы (ready)',
    'LLM фильтр: publish_now / schedule / skip / есть/нет рекомендации',
])

doc.add_heading('Действия', level=2)
add_bullets([
    'Анализ: повторно запустить API-обогащение для выбранных',
    'В Sheets: экспортировать в Google Sheets',
    'В контент: отправить на рерайт',
    'Удалить: удалить выбранные новости',
])

add_info_box('Keys.so автоматически выбирает регион: US для английских источников, RU для русских.', 'blue')

doc.add_page_break()

# ══════════════════════════════════════════════
# 6. FINAL TAB
# ══════════════════════════════════════════════

doc.add_heading('6. Вкладка «Финал»', level=1)
doc.add_paragraph(
    'Финальная подборка лучших новостей. Попадают только новости с LLM-рекомендацией '
    'publish_now и статусом processed или ready. Здесь все данные на одном экране — '
    'внутренний скор, обогащение, и вычисленный финальный балл.'
)

add_info_box('Это главная вкладка для принятия решения «что публиковать».', 'green')

doc.add_heading('Колонки', level=2)
add_styled_table(
    ['Колонка', 'Описание'],
    [
        ['Источник', 'Сайт-источник (IGN, DTF, StopGame и др.)'],
        ['Заголовок', 'Кликабельная ссылка на оригинал'],
        ['Скор', 'Внутренний total_score (0–100) из 12 проверок'],
        ['Вирал', 'Виральный балл (0–100) + цветовой уровень'],
        ['Свеж.', 'Время с публикации (часы). Зелёный = свежая'],
        ['Тон', 'Тональность: 🟢 positive / ⚪ neutral / 🔴 negative'],
        ['Теги', 'Авто-теги (release, update, esports и др.)'],
        ['Биграммы', 'Ключевые словосочетания (TF-IDF)'],
        ['Keys.so', 'Частота поискового запроса'],
        ['Похож.', 'Количество похожих запросов'],
        ['Trends', 'Google Trends по регионам (RU:50 US:80 и т.д.)'],
        ['Финал', 'Финальный композитный скор (0–100)'],
    ],
    col_widths=[2.5, 12.5]
)

doc.add_heading('Финальный скор — формула', level=2)
doc.add_paragraph(
    'Финальный скор объединяет внутреннюю оценку и данные обогащения:'
)

add_styled_table(
    ['Компонент', 'Вес', 'Источник данных'],
    [
        ['Внутренний скор (total_score)', '40%', '12 локальных проверок'],
        ['Виральность (viral_score)', '20%', 'Триггеры + entity boost'],
        ['Keys.so бонус', '15%', 'Частота: ≥10K=100, ≥5K=80, ≥1K=60, ≥100=40, >0=20'],
        ['Google Trends бонус', '10%', 'Макс. по регионам: ≥80=100, ≥50=70, ≥20=40, >0=20'],
        ['Заголовок (headline_score)', '15%', 'Числа, вопросы, breaking, длина'],
    ],
    col_widths=[5.5, 1.5, 8]
)

p = doc.add_paragraph()
run = p.add_run('Формула: ')
run.bold = True
p.add_run('internal×0.4 + viral×0.2 + keyso_bonus×0.15 + trends_bonus×0.1 + headline×0.15')

doc.add_heading('Карточки статистики', level=2)
add_bullets([
    'Всего publish_now — количество новостей в финале',
    'Финал ≥ 60 — количество с высоким финальным скором',
    'Средний финал — средний финальный балл',
])

doc.add_heading('Действия', level=2)
add_bullets([
    '✎ В контент: отправить на рерайт через очередь задач',
    '☰ В Sheets: экспортировать в Google Sheets',
    'Массовый выбор: чекбоксы + кнопка «В контент»',
    'Сортировка: по финальному скору, внутреннему скору, виральности, свежести',
])

doc.add_page_break()

# ══════════════════════════════════════════════
# 7. CONTENT TAB
# ══════════════════════════════════════════════

doc.add_heading('7. Вкладка «Контент»', level=1)
doc.add_paragraph(
    'Единая вкладка для работы со статьями и рерайтом. '
    'В левой панели — переключатель «Статьи / Новости», в правой — редактор.'
)

doc.add_heading('Режим «Статьи» (левая панель)', level=2)
add_bullets([
    'Список всех сохранённых статей',
    'Поиск по заголовку',
    'Фильтр по статусу: Черновики / Готовые / Опубликованные',
    'Массовое скачивание в DOCX (ZIP-архив)',
    'Массовое удаление',
])

doc.add_heading('Режим «Новости» (левая панель)', level=2)
add_bullets([
    'Список одобренных и обработанных новостей',
    'Фильтр по источнику',
    'Выбор стиля рерайта (news, seo, review, clickbait, short, social)',
    'Массовый рерайт выбранных новостей',
    'Быстрый предпросмотр: клик → детали в правой панели',
])

doc.add_heading('Редактор статьи (правая панель)', level=2)
add_bullets([
    'Заголовок: редактирование с счётчиком символов',
    'SEO Title: оптимальная длина до 60 символов',
    'SEO Description: оптимальная длина до 155 символов',
    'Теги: через запятую',
    'Текст статьи: полное редактирование с счётчиком',
    'Статус: Черновик → Готово → Опубликовано',
    'AI-улучшение: переписать через LLM в выбранном стиле',
    'Скачать как DOCX',
])

doc.add_heading('Как новости попадают в контент', level=2)
add_numbered([
    'Из Редакции: кнопка «✎ В контент»',
    'Из Финала: кнопка «✎» у конкретной новости или массовая отправка',
    'Из Виральности: кнопка «В редактор»',
    'Из Обогащённых: кнопка «В контент»',
    'Завершённые задачи рерайта автоматически создают статьи',
])

doc.add_page_break()

# ══════════════════════════════════════════════
# 8. VIRAL TAB
# ══════════════════════════════════════════════

doc.add_heading('8. Вкладка «Виральность»', level=1)
doc.add_paragraph(
    'Анализ виральности новостей. '
    'Показывает новости с наибольшим виральным потенциалом.'
)

doc.add_heading('Таблица', level=2)
add_bullets([
    'Уровень виральности: HIGH (красный), MEDIUM (оранжевый), LOW (серый)',
    'Виральный балл (0–100)',
    'Активные триггеры для каждой новости',
    'Моментум: сколько источников пишут о том же',
])

doc.add_heading('Действия', level=2)
add_styled_table(
    ['Кнопка', 'Что делает'],
    [
        ['В редактор', 'Отправляет одну новость в Контент'],
        ['Отправить HIGH', 'Массово отправляет все high-виральные'],
        ['Отправить MEDIUM+', 'Отправляет medium и high'],
        ['Отправить все', 'Отправляет все виральные новости'],
    ],
    col_widths=[5, 10]
)

doc.add_heading('Настройка триггеров', level=2)
doc.add_paragraph(
    'Управление триггерами: ⚙ Настройки → Виральность. '
    '50+ встроенных триггеров с возможностью настройки.'
)

add_styled_table(
    ['Категория', 'Примеры триггеров'],
    [
        ['Скандалы', 'Увольнения студий, скандалы, контроверсии'],
        ['Утечки', 'Неанонсированные игры, инсайды'],
        ['Релизы', 'Shadow drop, неожиданные релизы'],
        ['Провалы', 'Баги, краши, негативные отзывы'],
        ['AI', 'Использование ИИ в играх'],
        ['События', 'E3, gamescom, TGA, State of Play'],
        ['M&A', 'Слияния: Microsoft-Activision и т.п.'],
        ['Киберспорт', 'Турниры, трансферы, результаты'],
        ['Железо', 'Консоли, видеокарты, VR'],
    ],
    col_widths=[3, 12]
)

doc.add_page_break()

# ══════════════════════════════════════════════
# 9. ANALYTICS TAB
# ══════════════════════════════════════════════

doc.add_heading('9. Вкладка «Аналитика»', level=1)

doc.add_heading('Статистика', level=2)
add_bullets([
    'Распределение по статусам: сколько новостей в каждом статусе',
    'Топ источников: по количеству одобренных/отклонённых',
    'Процент одобрений по источникам за 30 дней',
])

doc.add_heading('Дайджесты', level=2)
doc.add_paragraph(
    'Автоматическая генерация ежедневного дайджеста в 23:00 МСК. '
    'Топ-20 новостей за сутки по total_score, создаётся через LLM.'
)
add_bullets([
    'История дайджестов: просмотр всех сгенерированных',
    'Ручная генерация: кнопка «Сгенерировать дайджест»',
])

doc.add_heading('Версии промптов', level=2)
doc.add_paragraph(
    'Версионирование LLM-промптов для A/B-тестирования. '
    'Каждая версия отслеживает средний скор и количество использований.'
)

doc.add_page_break()

# ══════════════════════════════════════════════
# 10. HEALTH TAB
# ══════════════════════════════════════════════

doc.add_heading('10. Вкладка «Здоровье»', level=1)
doc.add_paragraph('Мониторинг работоспособности источников новостей.')

add_styled_table(
    ['Метрика', 'Описание'],
    [
        ['Всего статей', 'Количество спарсенных за 30 дней'],
        ['Одобрено', 'Одобренные редактором'],
        ['Отклонено', 'Отклонённые'],
        ['% одобрений', 'Процент одобрений от решений'],
        ['Вес', 'Текущий вес источника (0.5–2.0)'],
        ['Новые', 'Количество необработанных'],
    ],
    col_widths=[4, 11]
)

doc.add_paragraph(
    'Вес источника корректируется автоматически: '
    '>80% одобрений → +0.2, <30% → -0.2. Влияет на итоговый скор.'
)

doc.add_page_break()

# ══════════════════════════════════════════════
# 11. SETTINGS TAB
# ══════════════════════════════════════════════

doc.add_heading('11. Вкладка «Настройки» (⚙)', level=1)
doc.add_paragraph('Конфигурация системы. 8 подвкладок:')

add_styled_table(
    ['Подвкладка', 'Содержимое'],
    [
        ['Общие', 'Модель LLM, API-ключи, порог авто-одобрения, очистка БД/кэша'],
        ['Источники', 'Список 15 источников, добавление/удаление, тип/URL/интервал'],
        ['Промпты', 'Редактирование LLM-промптов с версионированием'],
        ['Виральность', '50+ триггеров: вес, ключевые слова, вкл/выкл'],
        ['Инструменты', 'Тест LLM, Keys.so, Sheets, перевода'],
        ['Очередь', 'Управление фоновыми задачами (рерайт, экспорт)'],
        ['Логи', 'Последние 50 записей лога (ERROR/WARNING/INFO)'],
        ['Пользователи', 'Управление аккаунтами'],
    ],
    col_widths=[3, 12]
)

doc.add_page_break()

# ══════════════════════════════════════════════
# 12. SOURCES
# ══════════════════════════════════════════════

doc.add_heading('12. Источники новостей', level=1)
doc.add_paragraph('Система парсит 15 игровых новостных сайтов:')

add_styled_table(
    ['#', 'Источник', 'Тип', 'Интервал', 'Язык', 'Keys.so регион'],
    [
        ['1', 'IGN', 'RSS', '5 мин', 'EN', 'US'],
        ['2', 'GameSpot', 'RSS', '10 мин', 'EN', 'US'],
        ['3', 'PCGamer', 'RSS', '10 мин', 'EN', 'US'],
        ['4', 'GameRant', 'RSS', '10 мин', 'EN', 'US'],
        ['5', 'DTF', 'JSON', '10 мин', 'RU', 'RU'],
        ['6', 'StopGame', 'HTML', '10 мин', 'RU', 'RU'],
        ['7', 'Eurogamer', 'RSS', '15 мин', 'EN', 'US'],
        ['8', 'Kotaku', 'RSS', '15 мин', 'EN', 'US'],
        ['9', 'GamesRadar', 'RSS', '15 мин', 'EN', 'US'],
        ['10', 'Polygon', 'RSS', '15 мин', 'EN', 'US'],
        ['11', 'Destructoid', 'RSS', '15 мин', 'EN', 'US'],
        ['12', 'Playground.ru', 'RSS', '15 мин', 'RU', 'RU'],
        ['13', 'RockPaperShotgun', 'RSS', '30 мин', 'EN', 'US'],
        ['14', 'iXBT.games', 'HTML', '30 мин', 'RU', 'RU'],
        ['15', 'VGTimes', 'HTML', '30 мин', 'RU', 'RU'],
    ],
    col_widths=[1, 4, 2, 2, 1.5, 3]
)

add_info_box('Keys.so регион выбирается автоматически: US для EN-источников, RU для русскоязычных.', 'blue')

doc.add_page_break()

# ══════════════════════════════════════════════
# 13. SCORING SYSTEM
# ══════════════════════════════════════════════

doc.add_heading('13. Система проверок (Scoring)', level=1)
doc.add_paragraph(
    'Каждая новость проходит 12 автоматических проверок при авто-ревью. '
    'Результаты формируют total_score (0–100).'
)

add_styled_table(
    ['Проверка', 'Диапазон', 'Что оценивает'],
    [
        ['Quality', '0–100', 'Длина текста (≥150), заголовка (≥20), кликбейт'],
        ['Relevance', '0–100', 'Наличие гейминг-ключевиков vs шум'],
        ['Freshness', '0–100', '<2ч=100, <6ч=80, <24ч=50, <72ч=25, старше=10'],
        ['Viral Score', '0–100', '50+ триггеров, entity boost (S/A/B/C), комбо'],
        ['Momentum', '0–100', 'Похожие новости из других источников за 1ч/6ч/24ч'],
        ['Headline', '0–100', 'Числа, вопросы, эксклюзив, breaking, длина'],
        ['Source Weight', '0.5–2.0', 'Базовый вес + история одобрений за 30 дней'],
        ['Sentiment', '-1..+1', 'Тональность: positive / neutral / negative'],
        ['Tags', 'список', '9 авто-тегов: industry, release, update, esports...'],
        ['Dedup', 'bool', 'TF-IDF cosine + entity overlap, порог 0.7'],
        ['NER', 'dict', 'Студии, игры, платформы, числа, события'],
        ['Feedback', '±10', 'Корректировка на основе решений редактора'],
    ],
    col_widths=[3, 2, 10]
)

doc.add_heading('Формула total_score (внутренний)', level=2)
p = doc.add_paragraph()
run = p.add_run('total_score = ')
run.bold = True
p.add_run('(quality + relevance + freshness + viral) / 4 + momentum/5 + source_weight + headline_bonus + feedback(±10)')

add_info_box('Авто-отклонение: score < 15 → rejected. Early exit: quality < 20 → тяжёлые проверки пропускаются.', 'red')

doc.add_page_break()

# ══════════════════════════════════════════════
# 14. FINAL SCORE
# ══════════════════════════════════════════════

doc.add_heading('14. Финальный скор (формула)', level=1)
doc.add_paragraph(
    'Финальный скор вычисляется во вкладке «Финал» и учитывает как внутренние проверки, '
    'так и данные обогащения из внешних API. Это итоговая метрика для принятия решения о публикации.'
)

doc.add_heading('Компоненты', level=2)

add_styled_table(
    ['Компонент', 'Вес', 'Диапазон', 'Откуда берётся'],
    [
        ['Внутренний скор', '40%', '0–100', '12 локальных проверок (quality, viral, freshness и др.)'],
        ['Виральность', '20%', '0–100', 'Триггеры + entity boost + комбо'],
        ['Keys.so бонус', '15%', '0–100', 'Частота: ≥10K→100, ≥5K→80, ≥1K→60, ≥100→40, >0→20'],
        ['Trends бонус', '10%', '0–100', 'Макс. по регионам: ≥80→100, ≥50→70, ≥20→40, >0→20'],
        ['Заголовок', '15%', '0–100', 'Числа, вопросы, breaking, эксклюзив, длина'],
    ],
    col_widths=[3.5, 1.5, 2, 8]
)

doc.add_heading('Цветовая индикация', level=2)

add_styled_table(
    ['Финальный скор', 'Цвет', 'Рекомендация'],
    [
        ['≥ 60', 'Зелёный', 'Отличный кандидат на публикацию'],
        ['35–59', 'Жёлтый', 'Рассмотреть, возможно стоит доработать'],
        ['< 35', 'Красный', 'Слабый, лучше пропустить'],
    ],
    col_widths=[3, 3, 9]
)

doc.add_heading('Тиры сущностей (бонус к виральности)', level=2)

add_styled_table(
    ['Тир', 'Бонус', 'Примеры'],
    [
        ['S (AAA)', '+30', 'GTA VI, Elder Scrolls VI, Nintendo, Rockstar'],
        ['A (Major)', '+25', 'Cyberpunk 2077, Elden Ring, Ubisoft, EA'],
        ['B (Notable)', '+15', "Baldur's Gate 3, Hollow Knight, Capcom"],
        ['C (Niche)', '+5', 'Indie-игры, малые студии'],
    ],
    col_widths=[3, 2, 10]
)

doc.add_page_break()

# ══════════════════════════════════════════════
# 15. API INTEGRATIONS
# ══════════════════════════════════════════════

doc.add_heading('15. API-интеграции', level=1)

doc.add_heading('Keys.so', level=2)
doc.add_paragraph('Сервис анализа поисковых запросов.')
add_bullets([
    'Данные: частота запроса (ws), конкуренция (wsk), похожие запросы',
    'Регион: автоматически US (англ. источники) или RU (рус. источники)',
    'Кэш: 24 часа',
    'Вызывается: при обогащении одобренных новостей',
])

doc.add_heading('Google Trends', level=2)
add_bullets([
    'Регионы: RU, US, GB, DE (все одновременно)',
    'Данные: индекс популярности 0–100 за последние 24 часа',
    'Кэш: 6 часов',
])

doc.add_heading('LLM (OpenRouter)', level=2)

add_styled_table(
    ['Промпт', 'Назначение', 'Когда'],
    [
        ['Trend Forecast', 'Прогноз тренда + рекомендация (publish/schedule/skip)', 'Обогащение'],
        ['Merge Analysis', 'Объединение дубликатов', 'Ручной запуск'],
        ['Rewrite (6 стилей)', 'Переписывание новости', 'Очередь задач'],
        ['Translate', 'Перевод заголовка на русский', 'Кнопка в Редакции'],
    ],
    col_widths=[3.5, 6, 4]
)

doc.add_heading('Стили рерайта', level=3)
add_styled_table(
    ['Стиль', 'Описание'],
    [
        ['news', 'Классическая новостная статья'],
        ['seo', 'SEO-оптимизированный текст с ключевиками'],
        ['review', 'Обзорный стиль с мнением автора'],
        ['clickbait', 'Цепляющий заголовок и интригующее начало'],
        ['short', 'Краткая заметка (2–3 абзаца)'],
        ['social', 'Пост для соцсетей (Twitter, Telegram)'],
    ],
    col_widths=[3, 12]
)

doc.add_heading('Google Sheets', level=2)
doc.add_paragraph('Экспорт новостей в таблицу. Лист1 — основной экспорт.')

doc.add_page_break()

# ══════════════════════════════════════════════
# 16. FAQ
# ══════════════════════════════════════════════

doc.add_heading('16. Часто задаваемые вопросы', level=1)

faqs = [
    ('Как часто парсятся новости?',
     'Каждые 5–30 минут в зависимости от источника. IGN — каждые 5 минут, '
     'HTML-парсеры (iXBT, VGTimes) — каждые 30 минут.'),

    ('Сколько стоят API-вызовы?',
     'Парсинг и авто-ревью бесплатны. API вызываются только при одобрении: '
     'Keys.so, Google Trends, LLM (OpenRouter). ~$0.01–0.03 за новость.'),

    ('Что такое авто-одобрение?',
     '«Авто скор>70» массово одобряет все in_review с total_score ≥ 70. '
     'Безопасно: такие новости прошли все проверки.'),

    ('Чем отличается total_score от финального скора?',
     'total_score (внутренний) — результат 12 локальных проверок (бесплатно). '
     'Финальный скор — композитный балл с учётом Keys.so, Trends и др. API-данных. '
     'Финальный видно только во вкладке «Финал» после обогащения.'),

    ('Почему Keys.so показывает 0?',
     'Keys.so ищет частоту в определённом регионе. Для EN-источников теперь '
     'используется US-регион. Если freq=0 — тема не имеет поискового трафика.'),

    ('Как работает дедупликация?',
     'Двойная: (1) MD5-хеш URL при парсинге. (2) TF-IDF cosine similarity '
     '+ пересечение сущностей при ревью. Порог: 0.7.'),

    ('Как добавить новый источник?',
     '⚙ Настройки → Источники → «Добавить». Имя, тип (rss/html), URL, интервал. '
     'Для HTML нужен CSS-селектор.'),

    ('Как изменить модель LLM?',
     '⚙ Настройки → Общие → Модель LLM. Любая модель через OpenRouter.'),

    ('Что делать если вкладка пустая?',
     'Обогащённые: одобрите новости в Редакции. Финал: нужны processed-новости '
     'с рекомендацией publish_now. Контент: отправьте новости на рерайт.'),

    ('Как перезапустить ошибочные задачи?',
     '⚙ Настройки → Очередь → отметить → «Повторить выбранные».'),
]

for q, a in faqs:
    p = doc.add_paragraph()
    run = p.add_run(f'Q: {q}')
    run.bold = True
    run.font.size = Pt(11)
    run.font.color.rgb = RGBColor(0x1A, 0x73, 0xE8)
    p = doc.add_paragraph()
    run = p.add_run(f'A: {a}')
    run.font.size = Pt(10.5)
    p.paragraph_format.space_after = Pt(10)

doc.add_page_break()

# ══════════════════════════════════════════════
# FOOTER
# ══════════════════════════════════════════════

for _ in range(4):
    doc.add_paragraph('')

p = doc.add_paragraph()
p.alignment = WD_ALIGN_PARAGRAPH.CENTER
run = p.add_run('IgroNews v2.0')
run.font.size = Pt(14)
run.font.color.rgb = RGBColor(0x1A, 0x73, 0xE8)
run.bold = True

p = doc.add_paragraph()
p.alignment = WD_ALIGN_PARAGRAPH.CENTER
run = p.add_run('Документация актуальна на март 2026 года')
run.font.size = Pt(10)
run.font.color.rgb = RGBColor(0x99, 0x99, 0x99)

p = doc.add_paragraph()
p.alignment = WD_ALIGN_PARAGRAPH.CENTER
run = p.add_run('Railway  •  github.com/staurus86/IgroNews')
run.font.size = Pt(10)
run.font.color.rgb = RGBColor(0x99, 0x99, 0x99)

# ── Save ──
out_path = os.path.join(os.path.dirname(__file__), 'IgroNews_Manual.docx')
doc.save(out_path)
print(f'Saved: {out_path}')
print(f'Size: {os.path.getsize(out_path) / 1024:.1f} KB')
