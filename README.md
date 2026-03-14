# DBDocs

**Automated database documentation — schema discovery, data profiling, and AI-powered data dictionary.**

Connect DBDocs to any SQL database and it generates a set of interactive HTML reports you can open in a browser, share with your team, or export to JSON/CSV.

---

## What you get

| Report | What's inside |
|---|---|
| 📖 **Data Dictionary** | Every table and column with types, constraints, PII flags, lineage tags, FK integrity, sample data, and optional AI descriptions |
| 📊 **Data Profiling** | Row counts, null rates, min/max/avg, top values, outlier detection, quality scores per column |
| 🗺 **ERD** | Interactive entity-relationship diagram built from your actual FK constraints |
| 📄 **Index page** | Summary dashboard linking all three reports |

> **AI is optional.** All schema discovery, profiling, and the ERD work with no LLM configured. AI only adds natural-language descriptions to tables and columns.

---

## Quick start (SQLite — no database setup needed)

```bash
# 1. Clone
git clone https://github.com/your-username/dbdocs.git
cd dbdocs

# 2. Install
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 3. Run against the bundled sample database
python main.py --db-url sqlite:///sample.db --output-dir ./output/sample

# 4. Open the result
open output/sample/index.html    # macOS
# or just double-click the file in your file manager
```

That's it. No `.env` file, no API keys needed for this first run.

---

## Installation

**Requirements:** Python 3.9+

```bash
git clone https://github.com/your-username/dbdocs.git
cd dbdocs
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

---

## Step-by-step setup for your own database

### Step 1 — Create your config file

```bash
cp .env.example .env
```

Open `.env` in any text editor and set at minimum:

```env
DB_URL=postgresql://user:password@localhost:5432/mydb
OUTPUT_DIR=./output/mydb
```

### Step 2 — Run

```bash
python main.py
```

### Step 3 — Open the output

```bash
open output/mydb/index.html
```

---

## Database connection strings

Set `DB_URL` in your `.env` file:

| Database | Connection string format |
|---|---|
| **SQLite** | `sqlite:///relative/path.db` |
| **PostgreSQL** | `postgresql://user:password@host:5432/dbname` |
| **MySQL / MariaDB** | `mysql+pymysql://user:password@host:3306/dbname` |
| **MS SQL Server** | `mssql+pyodbc://user:pass@server/dbname?driver=ODBC+Driver+17+for+SQL+Server` |
| **Oracle** | `oracle+cx_oracle://user:pass@host:1521/service` |

> **Driver note:** PostgreSQL needs `psycopg2`, MySQL needs `pymysql`, MSSQL needs `pyodbc`. All are included in `requirements.txt`.

---

## Adding AI descriptions (optional)

AI enrichment adds natural-language descriptions to every table and column. It is entirely optional — skip this section if you don't need it.

### Option A — OpenRouter (recommended for getting started)

OpenRouter gives access to 200+ models including **free-tier models** that cost nothing.

1. Sign up at [openrouter.ai](https://openrouter.ai) and get an API key
2. Add to your `.env`:

```env
LLM_PROVIDER=openrouter
LLM_MODEL=meta-llama/llama-3.1-8b-instruct:free
OPENROUTER_API_KEY=sk-or-...
```

### Option B — OpenAI

```env
LLM_PROVIDER=openai
LLM_MODEL=gpt-4o-mini
OPENAI_API_KEY=sk-...
```

### Option C — Anthropic

```env
LLM_PROVIDER=anthropic
LLM_MODEL=claude-3-5-haiku-20241022
ANTHROPIC_API_KEY=sk-ant-...
```

### Option D — Ollama (local, free, no internet required)

1. Install [Ollama](https://ollama.ai) and pull a model:
   ```bash
   ollama pull llama3.2
   ```
2. Add to your `.env`:
   ```env
   LLM_PROVIDER=ollama
   LLM_MODEL=llama3.2
   ```

### Option E — Any OpenAI-compatible API (LM Studio, vLLM, Together AI …)

```env
LLM_PROVIDER=openai_compatible
OPENAI_COMPATIBLE_BASE_URL=http://localhost:1234/v1
OPENAI_COMPATIBLE_API_KEY=not-needed
LLM_MODEL=your-model-name
```

### All LLM providers at a glance

| Provider | Cost | Internet required | Key required |
|---|---|---|---|
| OpenRouter (free models) | Free | Yes | Yes (free) |
| OpenRouter (paid models) | Pay-per-token | Yes | Yes |
| OpenAI | Pay-per-token | Yes | Yes |
| Anthropic | Pay-per-token | Yes | Yes |
| Ollama | Free | No | No |
| OpenAI-compatible | Varies | No (local) | Optional |

> **Rate limits:** Free models typically allow ~16 calls/minute. DBDocs handles this automatically and will wait between calls. You can tune the delay: `LLM_CALL_DELAY=4` in `.env`.

---

## All configuration options

All settings can be placed in `.env`, set as environment variables, or passed as CLI flags.

| `.env` key | CLI flag | Default | Description |
|---|---|---|---|
| `DB_URL` | `--db-url` | *(required)* | Database connection string |
| `DB_SCHEMA` | `--schema` | all schemas | Limit to one schema (e.g. `public`) |
| `TABLE_FILTER` | `--tables` | all tables | Comma-separated names or regex patterns |
| `OUTPUT_DIR` | `--output-dir` | `./output` | Where to write HTML files |
| `EXPORT` | `--export` | `html` | Output formats: `html`, `json`, `csv`, `all` |
| `LLM_PROVIDER` | — | `none` | LLM provider (see above) |
| `LLM_MODEL` | — | — | Model name for your provider |
| `LLM_CALL_DELAY` | — | `1.0` | Seconds between LLM calls (increase for free tiers) |
| `MAX_SAMPLE_ROWS` | `--sample-rows` | `100000` | Rows to sample per table for profiling |
| `SAMPLE_DATA_ROWS` | — | `5` | Sample rows shown in Data Dictionary |

---

## CLI reference

```
python main.py [OPTIONS]

Options:
  --db-url TEXT          Database URL (overrides DB_URL in .env)
  --tables, -t TEXT      Table filter: names or regex (e.g. "dim_.*,fact_.*")
  --schema TEXT          Limit to a specific schema
  --output-dir, -o PATH  Output directory
  --export TEXT          Formats: html, json, csv, all
  --skip-llm             Skip AI description generation
  --skip-profiling       Skip data profiling (faster run)
  --skip-erd             Skip ERD generation
  --skip-integrity       Skip FK referential integrity checks
  --no-cache             Force re-generate LLM descriptions (ignore cache)
  --sample-rows INT      Max rows to sample per table (default: 100000)
  --config, -c PATH      YAML config file (default: ./db-data-dict.yaml)
```

### Examples

```bash
# Run with no AI (fast — schema + profiling + ERD only)
python main.py --skip-llm

# Document only specific tables
python main.py --tables "orders,customers,products"

# Document all tables matching a pattern
python main.py --tables "dim_.*,fact_.*"

# Export everything (HTML + JSON + CSV)
python main.py --export all

# Override the database without editing .env
python main.py --db-url postgresql://user:pass@host/mydb

# Faster run — skip profiling and ERD
python main.py --skip-profiling --skip-erd

# Re-generate AI descriptions from scratch (clear cache)
python main.py --no-cache
```

---

## YAML config file (alternative to `.env`)

For complex setups or multiple databases you can use a YAML file instead of `.env`:

```yaml
# db-data-dict.yaml
db_url: postgresql://user:password@localhost:5432/mydb
db_schema: public
table_filter: "orders,customers,dim_.*"
llm_provider: openrouter
llm_model: meta-llama/llama-3.1-8b-instruct:free
max_sample_rows: 50000
```

```bash
python main.py --config db-data-dict.yaml
```

**Priority order:** CLI flags > environment variables > `.env` file > YAML file > defaults.

---

## Project structure

```
dbdocs/
├── main.py                   # Entry point & CLI
├── .env.example              # Config template — copy to .env
├── requirements.txt
├── src/
│   ├── config.py             # Configuration loader
│   ├── db/
│   │   ├── connector.py      # Database connection
│   │   ├── inspector.py      # Schema introspection
│   │   └── profiler.py       # Column statistics & profiling
│   ├── analyzers/
│   │   ├── table_classifier.py   # Fact/Dimension/Lookup/Junction detection
│   │   ├── pii_detector.py       # PII column detection
│   │   ├── lineage_tagger.py     # Data lineage tags
│   │   ├── naming_convention.py  # Column naming analysis
│   │   └── referential_integrity.py  # FK integrity checks
│   ├── llm/
│   │   └── client.py         # LLM provider abstraction + caching
│   ├── generators/
│   │   ├── index_page.py     # Summary dashboard
│   │   ├── data_dictionary.py
│   │   ├── data_profiling.py
│   │   └── erd.py
│   ├── exporters/
│   │   ├── json_exporter.py
│   │   └── csv_exporter.py
│   └── templates/            # HTML/JS templates (self-contained, no server needed)
│       ├── index.html
│       ├── data_dictionary.html
│       ├── data_profiling.html
│       └── erd.html
└── output/                   # Generated reports (git-ignored)
```

---

## Troubleshooting

**`DB_URL not set` error**
→ Make sure you created `.env` from `.env.example` and set `DB_URL`.

**`ModuleNotFoundError: No module named 'psycopg2'`**
→ Install the driver for your database: `pip install psycopg2-binary` (PostgreSQL), `pip install pymysql` (MySQL).

**LLM descriptions not appearing / all columns blank**
→ Check `output/llm_calls.log` for error details. Common causes: wrong API key, rate limit hit. Try increasing `LLM_CALL_DELAY=3` in `.env`.

**Rate limit errors with free LLM models**
→ Free models (e.g. OpenRouter free tier) allow ~16 calls/minute. Set `LLM_CALL_DELAY=4` in `.env`. DBDocs will auto-detect the rate limit and wait, but a higher base delay avoids hitting it in the first place.

**ERD is empty / has no edges**
→ The ERD is built from actual `FOREIGN KEY` constraints in your database. If your schema doesn't define FK constraints (common with legacy databases), the ERD will show nodes but no edges.

**Output HTML files look broken when opened directly in some browsers**
→ All assets are CDN-loaded (Tailwind CSS, Chart.js, Cytoscape). You need an internet connection the first time you open the files, or use a local HTTP server:
```bash
cd output/mydb && python -m http.server 8080
# then open http://localhost:8080
```

---

## License

MIT — free to use, modify, and distribute.
