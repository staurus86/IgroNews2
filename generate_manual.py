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
run = p.add_run('Версия 1.1  •  Март 2026')
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
    '6. Вкладка «Контент» — статьи и редактор',
    '7. Вкладка «Виральность» — тренды и триггеры',
    '8. Вкладка «Аналитика» — статистика и дайджесты',
    '9. Вкладка «Здоровье» — мониторинг источников',
    '10. Вкладка «Настройки» — конфигурация',
    '11. Источники новостей',
    '12. Система проверок (Scoring)',
    '13. API-интеграции',
    '14. Часто задаваемые вопросы',
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
    'Рерайт через LLM: 6 стилей (новость, SEO, обзор, кликбейт, короткий, соцсети)',
    'Экспорт в Google Sheets: 3 вкладки (основная, Ready, NotReady)',
    'Обучаемость: система учится на решениях редактора (feedback loop)',
    'Веб-панель: 6 вкладок для полного управления рабочим процессом',
], bold_prefix=True)

doc.add_heading('Технологии', level=3)
add_bullets([
    'Backend: Python, Flask, APScheduler',
    'NLP: scikit-learn (TF-IDF), NLTK, кастомный NER',
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

doc.add_heading('Что произойдёт автоматически', level=2)
add_numbered([
    'Парсинг: каждые 5–30 минут система парсит все 15 источников',
    'Авто-ревью: после каждого парсинга новости автоматически оцениваются (бесплатно, без API)',
    'Дедупликация: дубликаты помечаются и исключаются',
    'Авто-отклонение: новости с баллом < 15 отклоняются автоматически',
])

doc.add_heading('Ваши действия (ежедневный цикл)', level=2)
add_numbered([
    'Откройте вкладку «Редакция»: посмотрите новые проверенные новости',
    'Нажмите «Проверить новые»: если есть непроверенные (статус new), система оценит батч из 20',
    'Одобрите лучшие: нажмите галочку у новостей с высоким скором, или «Авто скор>70» для массового одобрения',
    'Дождитесь обогащения: после одобрения система автоматически запускает Keys.so + Trends + LLM',
    'Проверьте «Обогащённые»: посмотрите результаты API-анализа',
    'Экспорт/Рерайт: отправьте в Google Sheets или в очередь рерайта',
])

add_info_box('Совет: парсинг и авто-ревью полностью бесплатны. API вызываются только при одобрении.', 'green')

doc.add_page_break()

# ══════════════════════════════════════════════
# 3. PIPELINE
# ══════════════════════════════════════════════

doc.add_heading('3. Рабочий процесс (пайплайн)', level=1)

doc.add_paragraph(
    'Каждая новость проходит чёткий жизненный цикл от парсинга до публикации. '
    'Статус новости определяет, на каком этапе она находится.'
)

doc.add_heading('Статусы новостей', level=2)

add_styled_table(
    ['Статус', 'Описание', 'Следующий шаг'],
    [
        ['new', 'Только что спарсена', 'Авто-ревью → in_review или rejected'],
        ['in_review', 'Оценена, ждёт решения редактора', 'Одобрить или отклонить'],
        ['duplicate', 'Дубликат (терминальный)', '—'],
        ['rejected', 'Отклонена (терминальный)', '—'],
        ['approved', 'Одобрена, идёт обогащение', 'Автоматически → processed'],
        ['processed', 'Обогащена API-данными', 'Экспорт в Sheets / рерайт'],
        ['moderation', 'Экспорт без LLM (в Sheets NotReady)', 'Одобрить или отклонить'],
        ['ready', 'Готова к публикации', 'Опубликовать'],
    ],
    col_widths=[3, 7, 5]
)

doc.add_heading('Схема пайплайна', level=2)

pipeline_steps = [
    '① Парсинг (авто, каждые 5–30 мин) → статус: new',
    '② Авто-ревью (бесплатно, локальные проверки) → in_review / duplicate / rejected',
    '③ Ручное одобрение (редактор) → approved',
    '④ Фоновое обогащение (Keys.so + Trends + LLM) → processed',
    '⑤ Экспорт в Sheets или рерайт → ready',
    '⑥ Публикация',
]
for step in pipeline_steps:
    p = doc.add_paragraph()
    run = p.add_run(step)
    run.font.size = Pt(11)
    if '①' in step:
        run.font.color.rgb = RGBColor(0x42, 0x85, 0xF4)
    elif '③' in step:
        run.font.color.rgb = RGBColor(0xE3, 0x74, 0x00)

add_info_box('Важно: API-вызовы (Keys.so, Trends, LLM) происходят ТОЛЬКО после одобрения. Парсинг и ревью бесплатны.', 'orange')

doc.add_page_break()

# ══════════════════════════════════════════════
# 4. EDITORIAL TAB
# ══════════════════════════════════════════════

doc.add_heading('4. Вкладка «Редакция»', level=1)
doc.add_paragraph('Главная рабочая вкладка. Здесь происходит основная работа с новостями.')

doc.add_heading('Карточки статистики (верх)', level=2)
doc.add_paragraph('В верхней части отображаются карточки с количеством новостей по статусам:')
add_bullets([
    'Всего — общее число новостей в базе',
    'Новые — непроверенные (new)',
    'На ревью — оценены, ждут решения (in_review)',
    'Дубли — обнаруженные дубликаты',
    'Одобрено — одобрены для обогащения',
    'Обработано — обогащены API-данными',
    'Отклонено — отклонённые',
    'Готовые — экспортированы, готовы к публикации',
])

doc.add_heading('Фильтры и поиск', level=2)
add_bullets([
    'Статус: Активные, Новые, На проверке, Модерация, Одобренные, Обработанные, Готовые, Дубли, Отклонённые',
    'Источник: выбор конкретного сайта-источника',
    'Дата: диапазон дат (от—до) + быстрые кнопки (Сегодня, Вчера, Неделя, Месяц)',
    'LLM рекомендация: publish_now / schedule / skip (после обогащения)',
    'Поиск: текстовый поиск по заголовкам',
])

doc.add_heading('Действия', level=2)

add_styled_table(
    ['Кнопка / Действие', 'Что делает'],
    [
        ['Проверить новые', 'Запускает авто-ревью для 20 новых новостей (бесплатно)'],
        ['✓ Одобрить', 'Одобряет новость → запускает фоновое обогащение API'],
        ['✗ Отклонить', 'Отклоняет новость (терминально)'],
        ['Авто скор>70', 'Массово одобряет все новости с total_score ≥ 70'],
        ['✎ В контент', 'Отправляет выбранные новости в очередь рерайта (LLM)'],
        ['Перевод заголовка', 'Переводит английский заголовок на русский (LLM)'],
        ['Экспорт в Sheets', 'Добавляет в очередь экспорта в Google Sheets'],
        ['Удалить', 'Удаляет выбранные новости (необратимо)'],
    ],
    col_widths=[4.5, 10.5]
)

doc.add_heading('Раскрываемые детали', level=2)
doc.add_paragraph(
    'Нажмите на строку новости, чтобы раскрыть детальную информацию: '
    'полный текст, все баллы проверок (качество, виральность, свежесть, моментум, заголовок), '
    'теги, тональность, обнаруженные сущности (игры, студии, платформы), '
    'а также кнопки быстрых действий.'
)

add_info_box('Сортировка по умолчанию: новейшие сверху (по дате парсинга).', 'blue')

doc.add_page_break()

# ══════════════════════════════════════════════
# 5. ENRICHED TAB
# ══════════════════════════════════════════════

doc.add_heading('5. Вкладка «Обогащённые»', level=1)
doc.add_paragraph(
    'Показывает одобренные, обогащённые и готовые новости (статусы approved, processed, ready). '
    'Здесь видны данные из Keys.so, Google Trends и LLM-анализа. '
    'Одобренные новости появляются сразу — даже пока обогащение ещё идёт в фоне.'
)

doc.add_heading('Что отображается', level=2)
add_styled_table(
    ['Колонка', 'Описание'],
    [
        ['Keys.so (ws)', 'Частота поискового запроса (чем выше, тем популярнее тема)'],
        ['Trends RU/US/GB/DE', 'Индекс Google Trends по 4 регионам (0–100)'],
        ['LLM Score', 'Оценка трендового потенциала от LLM (0–100)'],
        ['LLM Рекомендация', 'publish_now (публиковать сейчас), schedule (запланировать), skip (пропустить)'],
        ['Биграммы', 'Ключевые словосочетания из текста (TF-IDF)'],
    ],
    col_widths=[4, 11]
)

doc.add_heading('Фильтры', level=2)
add_styled_table(
    ['Фильтр', 'Описание'],
    [
        ['Все обогащённые', 'Показывает approved + processed + ready (по умолчанию)'],
        ['Одобренные', 'Ждут обогащения (Keys.so + Trends + LLM)'],
        ['Обогащённые', 'Обогащение завершено, есть API-данные'],
        ['Готовые', 'Экспортированы в Sheets, готовы к публикации'],
        ['LLM: publish_now', 'LLM рекомендует публиковать немедленно'],
        ['LLM: schedule', 'LLM рекомендует запланировать'],
        ['LLM: skip', 'LLM рекомендует пропустить'],
    ],
    col_widths=[4, 11]
)

doc.add_heading('Действия', level=2)
add_bullets([
    'Экспорт в Sheets: отправить обогащённую новость в Google Sheets',
    'Повторный анализ: пересчитать API-данные для выбранной новости',
    'В контент: отправить на рерайт через очередь задач',
    'Одобрить/Отклонить: массовое управление статусами',
])

doc.add_page_break()

# ══════════════════════════════════════════════
# 6. CONTENT TAB
# ══════════════════════════════════════════════

doc.add_heading('6. Вкладка «Контент»', level=1)
doc.add_paragraph(
    'Единая вкладка для работы со статьями. Слева — список всех сохранённых статей, '
    'справа — редактор выбранной статьи.'
)

doc.add_heading('Список статей (левая панель)', level=2)
add_bullets([
    'Поиск по заголовку',
    'Фильтр по статусу: Черновики / Готовые / Опубликованные',
    'Чекбоксы для массовых действий',
    'Массовое скачивание в DOCX (ZIP-архив)',
    'Массовое удаление',
])

doc.add_heading('Редактор статьи (правая панель)', level=2)
add_bullets([
    'Заголовок: редактирование с счётчиком символов',
    'SEO Title: оптимальная длина до 60 символов',
    'SEO Description: оптимальная длина до 155 символов',
    'Теги: через запятую',
    'Текст статьи: полное редактирование с счётчиком',
    'Статус: Черновик → Готово → Опубликовано',
])

doc.add_heading('AI-улучшение', level=2)
doc.add_paragraph(
    'Кнопка «Улучшить» переписывает текст статьи через LLM в выбранном стиле. '
    'Результат заполняет поля редактора — не забудьте сохранить!'
)

doc.add_heading('Как статьи попадают в список', level=2)
add_numbered([
    'Из Редакции: кнопка «✎ В контент» отправляет новости в очередь рерайта',
    'Из Виральности: кнопка «В редактор» добавляет виральные новости',
    'Из Очереди: завершённые задачи рерайта автоматически создают статьи',
])

doc.add_page_break()

# ══════════════════════════════════════════════
# 7. VIRAL TAB
# ══════════════════════════════════════════════

doc.add_heading('7. Вкладка «Виральность»', level=1)
doc.add_paragraph(
    'Анализ виральности новостей и управление триггерами. '
    'Показывает новости с наибольшим виральным потенциалом.'
)

doc.add_heading('Таблица виральных новостей', level=2)
add_bullets([
    'Уровень виральности: HIGH (красный), MEDIUM (оранжевый), LOW (синий)',
    'Виральный балл (0–100)',
    'Активные триггеры для каждой новости',
    'Моментум: сколько источников пишут о том же',
])

doc.add_heading('Действия', level=2)
add_styled_table(
    ['Кнопка', 'Что делает'],
    [
        ['В редактор (одна)', 'Отправляет одну новость в Контент'],
        ['Отправить HIGH', 'Массово отправляет все high-виральные новости'],
        ['Отправить MEDIUM+', 'Отправляет medium и high'],
        ['Отправить все', 'Отправляет все виральные новости'],
    ],
    col_widths=[5, 10]
)

doc.add_heading('Настройка триггеров', level=2)
doc.add_paragraph(
    'Управление триггерами виральности находится в ⚙ Настройки → Виральность. '
    'Система содержит 50+ встроенных триггеров, которые можно настраивать.'
)

add_styled_table(
    ['Поле', 'Описание'],
    [
        ['Label', 'Название триггера (напр. "Утечка данных")'],
        ['Weight', 'Вес в баллах (0–100), чем выше — тем сильнее влияет на скор'],
        ['Keywords', 'Ключевые слова для срабатывания (JSON-массив)'],
        ['Active', 'Включён/выключен'],
    ],
    col_widths=[3, 12]
)

doc.add_heading('Категории триггеров', level=3)
add_bullets([
    'Скандалы и увольнения: крупные игровые скандалы, увольнения студий',
    'Утечки и слухи: неанонсированные игры, инсайдерская информация',
    'Shadow drop: неожиданные релизы без предупреждения',
    'Провальные запуски: баги, краши, негативные отзывы',
    'AI-контроверсии: использование ИИ в играх',
    'События: E3, gamescom, TGA, State of Play',
    'M&A: слияния и поглощения (Microsoft-Activision и т.п.)',
    'Культура: мемы, коммьюнити, моды',
    'Киберспорт: турниры, трансферы, результаты',
    'Железо: консоли, видеокарты, VR',
])

doc.add_page_break()

# ══════════════════════════════════════════════
# 8. ANALYTICS TAB
# ══════════════════════════════════════════════

doc.add_heading('8. Вкладка «Аналитика»', level=1)

doc.add_heading('Статистика', level=2)
add_bullets([
    'Распределение по статусам: сколько новостей в каждом статусе',
    'Топ источников: по количеству одобренных/отклонённых',
    'Процент одобрений по источникам за 30 дней',
])

doc.add_heading('Дайджесты', level=2)
doc.add_paragraph(
    'Система автоматически генерирует ежедневный дайджест в 23:00 МСК. '
    'Используются топ-20 новостей за сутки по total_score. Дайджест создаётся через LLM.'
)
add_bullets([
    'История дайджестов: просмотр всех сгенерированных',
    'Ручная генерация: кнопка «Сгенерировать дайджест»',
])

doc.add_heading('Версии промптов', level=2)
doc.add_paragraph(
    'Система поддерживает версионирование LLM-промптов для A/B-тестирования. '
    'Каждая версия отслеживает средний скор и количество использований.'
)

doc.add_page_break()

# ══════════════════════════════════════════════
# 9. HEALTH TAB
# ══════════════════════════════════════════════

doc.add_heading('9. Вкладка «Здоровье»', level=1)
doc.add_paragraph('Мониторинг работоспособности источников новостей.')

doc.add_heading('Таблица здоровья источников', level=2)
add_styled_table(
    ['Метрика', 'Описание'],
    [
        ['Всего статей', 'Количество спарсенных за 30 дней'],
        ['Одобрено', 'Количество одобренных редактором'],
        ['Отклонено', 'Количество отклонённых'],
        ['% одобрений', 'Процент одобрений от общего числа решений'],
        ['Вес', 'Текущий вес источника (0.5–2.0)'],
        ['Новые', 'Количество необработанных (new)'],
    ],
    col_widths=[4, 11]
)

doc.add_paragraph(
    'Вес источника автоматически корректируется на основе истории одобрений: '
    'если >80% новостей одобряются — вес +0.2, если <30% — вес -0.2. '
    'Это влияет на итоговый скор новостей из этого источника.'
)

doc.add_page_break()

# ══════════════════════════════════════════════
# 10. SETTINGS TAB
# ══════════════════════════════════════════════

doc.add_heading('10. Вкладка «Настройки» (⚙)', level=1)
doc.add_paragraph('Конфигурация системы. Содержит 8 подвкладок.')

doc.add_heading('Общие', level=2)
add_bullets([
    'Модель LLM: выбор модели через выпадающий список',
    'API-ключи: отображение текущих ключей (OPENAI, KEYSO, SHEETS)',
    'Порог авто-одобрения: минимальный скор для «Авто скор>70»',
    'Очистка БД: удаление old plaintext для экономии памяти',
    'Очистка кэша: сброс кэша API-запросов',
])

doc.add_heading('Источники', level=2)
doc.add_paragraph(
    'Список всех 15 источников с возможностью добавления, редактирования и удаления. '
    'Для каждого источника указывается: имя, тип (rss/html/dtf), URL, интервал парсинга.'
)

doc.add_heading('Промпты', level=2)
doc.add_paragraph(
    'Редактирование LLM-промптов: прогноз трендов, merge-анализ, SEO-запросы, рерайт. '
    'Поддерживается версионирование и откат к предыдущей версии.'
)

doc.add_heading('Виральность', level=2)
doc.add_paragraph(
    'Управление триггерами виральности: добавление, редактирование, включение/выключение. '
    'Поиск по названию, сортировка по весу. 50+ встроенных триггеров + возможность создать свои.'
)

doc.add_heading('Инструменты', level=2)
add_bullets([
    'Тест LLM: отправить произвольный запрос и получить ответ',
    'Тест Keys.so: проверить частоту ключевого слова',
    'Тест Sheets: проверить подключение к Google Sheets',
    'Тест перевода: перевести заголовок через LLM',
])

doc.add_heading('Очередь', level=2)
doc.add_paragraph(
    'Управление очередью фоновых задач (рерайт, экспорт). '
    'Для каждой задачи видны: тип, статус, результат, время.'
)
add_styled_table(
    ['Кнопка', 'Действие'],
    [
        ['Повторить', 'Перезапустить ошибочную задачу'],
        ['Повторить выбранные', 'Массовый перезапуск (чекбоксы)'],
        ['Отменить', 'Отменить задачу'],
        ['Очистить завершённые', 'Удалить выполненные задачи из списка'],
    ],
    col_widths=[5, 10]
)

doc.add_heading('Логи', level=2)
doc.add_paragraph('Последние 50 записей лога с фильтром по уровню (ERROR, WARNING, INFO).')

doc.add_heading('Пользователи', level=2)
doc.add_paragraph('Управление аккаунтами: добавить пользователя, удалить, сменить пароль.')

doc.add_page_break()

# ══════════════════════════════════════════════
# 11. SOURCES
# ══════════════════════════════════════════════

doc.add_heading('11. Источники новостей', level=1)
doc.add_paragraph('Система парсит 15 игровых новостных сайтов с разными интервалами.')

add_styled_table(
    ['#', 'Источник', 'Тип', 'Интервал', 'Язык'],
    [
        ['1', 'IGN', 'RSS', '5 мин', 'EN'],
        ['2', 'GameSpot', 'RSS', '10 мин', 'EN'],
        ['3', 'PCGamer', 'RSS', '10 мин', 'EN'],
        ['4', 'GameRant', 'RSS', '10 мин', 'EN'],
        ['5', 'DTF', 'JSON (DTF API)', '10 мин', 'RU'],
        ['6', 'StopGame', 'HTML', '10 мин', 'RU'],
        ['7', 'Eurogamer', 'RSS', '15 мин', 'EN'],
        ['8', 'Kotaku', 'RSS', '15 мин', 'EN'],
        ['9', 'GamesRadar', 'RSS', '15 мин', 'EN'],
        ['10', 'Polygon', 'RSS', '15 мин', 'EN'],
        ['11', 'Destructoid', 'RSS', '15 мин', 'EN'],
        ['12', 'Playground.ru', 'RSS', '15 мин', 'RU'],
        ['13', 'RockPaperShotgun', 'RSS', '30 мин', 'EN'],
        ['14', 'iXBT.games', 'HTML', '30 мин', 'RU'],
        ['15', 'VGTimes', 'HTML', '30 мин', 'RU'],
    ],
    col_widths=[1, 4.5, 3.5, 2.5, 2]
)

add_info_box('RSS — стандартный RSS-фид. HTML — парсинг страницы через BeautifulSoup. DTF — JSON API.', 'blue')

doc.add_page_break()

# ══════════════════════════════════════════════
# 12. SCORING SYSTEM
# ══════════════════════════════════════════════

doc.add_heading('12. Система проверок (Scoring)', level=1)
doc.add_paragraph(
    'Каждая новость проходит 12 автоматических проверок. '
    'Результаты складываются в итоговый total_score (0–100).'
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

doc.add_heading('Формула итогового скора', level=2)

p = doc.add_paragraph()
run = p.add_run('total_score = ')
run.bold = True
run.font.size = Pt(11)
p.add_run(
    '(quality + relevance + freshness + viral) / 4 '
    '+ momentum/5 '
    '+ source_weight × множитель '
    '+ headline_bonus '
    '+ feedback_adjustment (±10)'
).font.size = Pt(11)

doc.add_paragraph('')

add_info_box('Авто-отклонение: score < 15 → rejected. Early exit: quality < 20 → тяжёлые проверки пропускаются.', 'red')

doc.add_heading('Тиры сущностей (Entity Tiers)', level=2)
doc.add_paragraph('80+ игр, 30+ студий, 10+ платформ распределены по тирам:')

add_styled_table(
    ['Тир', 'Бонус к viral', 'Примеры'],
    [
        ['S (AAA)', '+30', 'GTA VI, Elder Scrolls VI, Nintendo, Rockstar'],
        ['A (Major)', '+25', 'Cyberpunk 2077, Elden Ring, Ubisoft, EA'],
        ['B (Notable)', '+15', 'Baldur\'s Gate 3, Hollow Knight, Capcom'],
        ['C (Niche)', '+5', 'Indie-игры, малые студии'],
    ],
    col_widths=[3, 3, 9]
)

doc.add_page_break()

# ══════════════════════════════════════════════
# 13. API INTEGRATIONS
# ══════════════════════════════════════════════

doc.add_heading('13. API-интеграции', level=1)

doc.add_heading('Keys.so', level=2)
doc.add_paragraph('Сервис анализа поисковых запросов. Показывает частоту запросов и похожие слова.')
add_bullets([
    'Данные: ws (частота запроса), wsk (конкуренция), похожие запросы',
    'Кэш: 24 часа',
    'Используется: при обогащении одобренных новостей',
])

doc.add_heading('Google Trends', level=2)
doc.add_paragraph('Индекс популярности темы по регионам.')
add_bullets([
    'Регионы: RU, US, GB, DE',
    'Данные: индекс 0–100 по каждому региону',
    'Кэш: 6 часов',
])

doc.add_heading('LLM (OpenRouter)', level=2)
doc.add_paragraph('Основной AI-движок системы. По умолчанию: GPT-4o-mini через OpenRouter.')

add_styled_table(
    ['Промпт', 'Назначение', 'Когда вызывается'],
    [
        ['Trend Forecast', 'Прогноз тренда (0–100) + рекомендация', 'При обогащении'],
        ['Merge Analysis', 'Объединение дубликатов в одну статью', 'Ручной запуск'],
        ['SEO Queries', 'Генерация SEO-запросов (10 шт)', 'При обогащении'],
        ['Rewrite (6 стилей)', 'Переписывание новости', 'Через очередь задач'],
        ['Translate', 'Перевод заголовка на русский', 'Кнопка в Редакции'],
        ['AI Recommendation', 'Рекомендация: publish/rewrite/skip', 'При обогащении'],
    ],
    col_widths=[3.5, 5.5, 4.5]
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
doc.add_paragraph('Экспорт новостей в таблицу для финальной публикации.')
add_bullets([
    'Лист1: основной экспорт всех данных',
    'Ready: готовые статьи для публикации',
    'NotReady: на модерации',
])

doc.add_page_break()

# ══════════════════════════════════════════════
# 14. FAQ
# ══════════════════════════════════════════════

doc.add_heading('14. Часто задаваемые вопросы', level=1)

faqs = [
    ('Как часто парсятся новости?',
     'Каждые 5–30 минут в зависимости от источника. IGN — каждые 5 минут, '
     'HTML-парсеры (iXBT, VGTimes) — каждые 30 минут.'),

    ('Сколько стоят API-вызовы?',
     'Парсинг и авто-ревью полностью бесплатны (локальные NLP-проверки). '
     'API вызываются только при одобрении: Keys.so, Google Trends, и LLM (OpenRouter). '
     'Средняя стоимость обогащения одной новости: ~$0.01–0.03.'),

    ('Что такое авто-одобрение?',
     'Кнопка «Авто скор>70» массово одобряет все новости со скором ≥ 70. '
     'Это безопасно: такие новости прошли все проверки с высокими баллами.'),

    ('Как работает дедупликация?',
     'Двойная: (1) MD5-хеш URL — при парсинге. (2) TF-IDF cosine similarity заголовков (вес 0.6) + '
     'пересечение сущностей из текста (вес 0.4) — при ревью. Порог: 0.7.'),

    ('Почему вкладка «Обогащённые» пустая?',
     'Вкладка показывает одобренные, обогащённые и готовые новости. '
     'Одобрите новости в Редакции — они сразу появятся. '
     'Обогащение API запускается автоматически в фоне (1–2 минуты).'),

    ('Как добавить новый источник?',
     '⚙ Настройки → Источники → кнопка «Добавить». Укажите имя, тип (rss/html), URL фида, '
     'интервал парсинга. Для HTML-парсеров нужен CSS-селектор ссылок.'),

    ('Как изменить модель LLM?',
     '⚙ Настройки → Общие → Модель LLM. По умолчанию GPT-4o-mini. '
     'Можно выбрать любую модель, доступную через OpenRouter.'),

    ('Можно ли использовать локально?',
     'Да! Установите зависимости (pip install -r requirements.txt), '
     'настройте .env-файл и запустите python main.py. Будет использоваться SQLite.'),

    ('Как перезапустить ошибочные задачи?',
     '⚙ Настройки → Очередь → отметьте задачи с ошибкой → «Повторить выбранные». '
     'Или кнопка «Повторить» у каждой ошибочной задачи.'),

    ('Что делать если Sheets-экспорт ошибается?',
     'Проверьте подключение: ⚙ → Инструменты → Тест Sheets. '
     'Если ок — перезапустите задачи экспорта через Очередь → Повторить.'),
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
run = p.add_run('IgroNews v1.1')
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
run = p.add_run('Задеплоено на Railway  •  github.com/staurus86/IgroNews')
run.font.size = Pt(10)
run.font.color.rgb = RGBColor(0x99, 0x99, 0x99)

# ── Save ──
out_path = os.path.join(os.path.dirname(__file__), 'IgroNews_Manual.docx')
doc.save(out_path)
print(f'Saved: {out_path}')
print(f'Size: {os.path.getsize(out_path) / 1024:.1f} KB')
