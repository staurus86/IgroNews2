# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Gaming news aggregator and analyzer (IgroNews). Parses RSS feeds from 15+ gaming news sites (IGN, PCGamer, DTF, StopGame, Eurogamer, etc.), analyzes them with NLP, checks trends, and outputs results to Google Sheets for editorial review.

## Architecture

Pipeline flow: RSS/HTML sources -> Parser -> TF-IDF NLP -> Google Trends + Keys.so APIs -> OpenAI GPT analysis -> Google Sheets -> Editorial approval

Key modules:
- `parsers/` — RSS (`feedparser`) and HTML (`beautifulsoup4`) parsing
- `nlp/tfidf.py` — TF-IDF bigrams/trigrams extraction (scikit-learn, nltk)
- `apis/google_trends.py` — Google Trends by region (RU, US, GB, DE)
- `apis/keyso.py` — Keys.so API for search frequency data
- `apis/llm.py` — OpenAI GPT API with 3 prompt scenarios: trend forecast, news merging, search query generation
- `storage/database.py` — SQLite/PostgreSQL persistence
- `storage/sheets.py` — Google Sheets output via `gspread`
- `scheduler.py` — APScheduler with 5/15/30 min intervals per source group
- `main.py` — entry point, starts the scheduler
- `config.py` — sources, API keys, regions, model settings

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run the application
python main.py
```

## Deployment

Deployed on Railway via Docker (`Dockerfile` + `railway.toml`). Required env vars:
- `OPENAI_API_KEY`
- `KEYSO_API_KEY`
- `GOOGLE_SHEETS_ID`
- `GOOGLE_SERVICE_ACCOUNT_JSON` (base64-encoded service account JSON)
- `DATABASE_URL` (PostgreSQL, provided by Railway)

## Database

Two tables: `news` (raw parsed articles with status workflow: new -> processed -> approved/rejected) and `news_analysis` (NLP results, trends data, LLM recommendations, linked via `news_id`).

## Key Conventions

- LLM model: OpenAI GPT (configured in `config.py`)
- All LLM prompts expect and return JSON responses
- News deduplication uses `md5(url)` as primary key
- Google Sheets column N controls editorial workflow (status field)
- Language: project targets Russian-speaking editors; prompts and UI are in Russian
