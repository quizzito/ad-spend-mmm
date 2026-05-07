# 📊 Ad Spend MMM Data Pipeline

> Simulated ad platform data from Google, Meta, TikTok, and Reddit → PostgreSQL → dbt → Apache Airflow → MMM-ready `consolidated_spend` table

---

## Table of Contents
1. [Project Overview](#project-overview)
2. [Pipeline Results](#pipeline-results)
3. [Architecture](#architecture)
4. [Stack](#stack)
5. [Repo Structure](#repo-structure)
6. [Data Sources](#data-sources)
7. [dbt Models](#dbt-models)
8. [Airflow DAG](#airflow-dag)
9. [Setup & Reproduction](#setup--reproduction)
10. [Sample Output](#sample-output)
11. [Design Decisions](#design-decisions)
12. [Next Steps](#next-steps)

---

## Project Overview

This project builds a production-style data engineering pipeline that simulates the exact API response schemas of 4 major ad platforms, loads raw data into PostgreSQL, transforms it using dbt into a single unified table, and orchestrates the entire workflow with Apache Airflow on a daily schedule.

The output — `marts.consolidated_spend` — is a clean, standardized table ready to be consumed directly by a [PyMC-Marketing](https://www.pymc-marketing.io/) Media Mix Model (MMM) for channel contribution analysis.

**The core engineering problem this solves:** Each ad platform returns data in a completely different schema — different column names, different spend units, different date field names, different granularity. A naive UNION ALL would fail immediately. This pipeline uses dbt staging models to absorb all platform-specific differences once, so every downstream consumer sees a single clean interface.

---

## Pipeline Results

### Row Counts by Platform

| Platform | Raw Rows | Campaigns | Total Spend (USD) | Date Range |
|----------|----------|-----------|-------------------|------------|
| Google Ads | 1,830 | 4 | $1,987,640.22 | 2024-01-01 → 2024-12-31 |
| Meta Ads | 2,196 | 3 | $1,772,508.66 | 2024-01-01 → 2024-12-31 |
| TikTok Ads | 1,464 | 2 | $943,779.54 | 2024-01-01 → 2024-12-31 |
| Reddit Ads | 1,464 | 2 | $461,304.07 | 2024-01-01 → 2024-12-31 |
| **consolidated_spend** | **6,954** | **11** | **$5,165,232.49** | **2024-01-01 → 2024-12-31** |

### Airflow Pipeline Run

| Task | Status | Description |
|------|--------|-------------|
| `generate_google` | ✅ Success | 1,830 rows generated |
| `generate_meta` | ✅ Success | 2,196 rows generated |
| `generate_tiktok` | ✅ Success | 1,464 rows generated |
| `generate_reddit` | ✅ Success | 1,464 rows generated |
| `load_to_postgres` | ✅ Success | All 4 raw tables loaded |
| `run_dbt_staging` | ✅ Success | 4 staging views created |
| `run_dbt_marts` | ✅ Success | consolidated_spend table created |

---

## Architecture

```
Python Extractors (Faker + NumPy)
    ↓  generates platform-specific CSV schemas
data/raw/
    ├── google_ads.csv    (cost_micros, network_type, ad_group_name)
    ├── meta_ads.csv      (spend, adset_name, publisher_platform)
    ├── tiktok_ads.csv    (stat_time_day, adgroup_name, placement_type)
    └── reddit_ads.csv    (spend, ecpm, ecpc)
    ↓  psycopg2 COPY
PostgreSQL 15 — raw schema
    ├── raw.google_ads_raw
    ├── raw.meta_ads_raw
    ├── raw.tiktok_ads_raw
    └── raw.reddit_ads_raw
    ↓  dbt staging models (views)
PostgreSQL 15 — staging schema
    ├── staging.stg_google    (standardized column names + type casts)
    ├── staging.stg_meta      (standardized column names + type casts)
    ├── staging.stg_tiktok    (standardized column names + type casts)
    └── staging.stg_reddit    (standardized column names + type casts)
    ↓  dbt mart model (UNION ALL)
PostgreSQL 15 — marts schema
    └── marts.consolidated_spend  ← MMM-ready input
    ↓  orchestrated by
Apache Airflow 2.9 — daily at 6am UTC
```

### DAG Task Graph

```
generate_google ─┐
generate_meta   ─┤
                 ├─► load_to_postgres ─► run_dbt_staging ─► run_dbt_marts
generate_tiktok ─┤
generate_reddit ─┘
```

The 4 generators run in parallel. Once all succeed, the pipeline loads, transforms, and publishes the unified table.

---

## Stack

| Layer | Tool | Version | Purpose |
|-------|------|---------|---------|
| Data Generation | Python + Faker + NumPy | 3.11 / 25.2.0 / 1.26.4 | Simulate ad platform API responses |
| Database | PostgreSQL | 15 (Docker) | Store raw + transformed data |
| Transformation | dbt Core + dbt-postgres | 1.8.3 / 1.8.2 | Standardize 4 schemas → 1 unified table |
| Orchestration | Apache Airflow | 2.9.1 (Docker) | Schedule and run the daily pipeline |
| Containerization | Docker + Docker Compose | Latest | Run Postgres and Airflow locally |
| Python Interface | psycopg2 + pandas | 2.9.9 / 2.2.2 | Load CSVs, verify output |
| Version Control | GitHub | — | Published repo with real results |

---

## Repo Structure

```
ad-spend-mmm/
│
├── data/
│   └── raw/                        # Generated dummy CSVs — gitignored
│       ├── google_ads.csv
│       ├── meta_ads.csv
│       ├── tiktok_ads.csv
│       └── reddit_ads.csv
│
├── extractors/                     # Python scripts — one per platform
│   ├── 01_generate_google.py
│   ├── 02_generate_meta.py
│   ├── 03_generate_tiktok.py
│   └── 04_generate_reddit.py
│
├── notebooks/
│   ├── 01_load_to_postgres.ipynb   # Load CSVs → Postgres raw schema
│   └── 02_verify_pipeline.ipynb   # Query consolidated_spend, validate
│
├── dags/
│   └── mmm_pipeline.py             # Airflow DAG
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml                # Docker-specific connection config
│   └── models/
│       ├── staging/
│       │   ├── schema.yml
│       │   ├── stg_google.sql
│       │   ├── stg_meta.sql
│       │   ├── stg_tiktok.sql
│       │   └── stg_reddit.sql
│       └── marts/
│           └── consolidated_spend.sql
│
├── init_db.sql                     # Creates schemas on first Postgres boot
├── docker-compose.yml              # Postgres + Airflow services
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

---

## Data Sources

All data is simulated using Faker and NumPy. Each extractor replicates the exact API response schema of the real platform — different column names, spend units, and field conventions — to reflect what a real pipeline would need to handle.

### Platform Schema Differences

| Field | Google Ads | Meta Ads | TikTok Ads | Reddit Ads |
|-------|-----------|----------|------------|------------|
| Date | `date` | `date_start` | `stat_time_day` | `date` |
| Spend | `cost_micros` (÷1M) | `spend` (dollars) | `spend` (dollars) | `spend` (dollars) |
| Ad Group | `ad_group_name` | `adset_name` | `adgroup_name` | `ad_group_name` |
| Placement | `network_type` | `publisher_platform` | `placement_type` | `placement` |
| Conversions | `conversions` | `actions` | `conversions` | `conversions` |
| CPM | `cpm` | `cpm` | `cpm` | `ecpm` |
| CPC | `avg_cpc` | `cpc` | `cpc` | `ecpc` |

### Dummy Data Spec

- Time range: 365 days of daily data (2024-01-01 → 2024-12-31)
- Seasonal spend multiplier applied (Q4 spike simulation)
- Metrics generated with realistic distributions per platform
- Spend, impressions, clicks, conversions, CTR, CPM, CPC, ROAS included
- Campaign, ad group, creative IDs and names per platform

---

## dbt Models

### Staging Models (Views)

Each staging model standardizes one platform's raw schema into a common set of column names and types. No business logic — only renaming and type casting.

| Model | Source Table | Key Transformations |
|-------|-------------|---------------------|
| `stg_google` | `raw.google_ads_raw` | `cost_micros::numeric / 1000000` → `spend_usd`, `avg_cpc` → `cpc` |
| `stg_meta` | `raw.meta_ads_raw` | `date_start` → `date`, `adset_name` → `ad_group_name`, `actions` → `conversions` |
| `stg_tiktok` | `raw.tiktok_ads_raw` | `stat_time_day` → `date`, `adgroup_name` → `ad_group_name`, `real_time_roas` → `roas` |
| `stg_reddit` | `raw.reddit_ads_raw` | `ecpm` → `cpm`, `ecpc` → `cpc` |

### Mart Model (Table)

`consolidated_spend` is a UNION ALL of all 4 staging views, materialized as a physical table. It adds 3 derived metrics computed at mart time:

| Derived Metric | Formula |
|----------------|---------|
| `effective_cpm` | `spend_usd / impressions * 1000` |
| `effective_cvr` | `conversions / clicks` |
| `cost_per_conversion` | `spend_usd / conversions` |

### Unified Schema

```
platform, date, campaign_id, campaign_name,
ad_group_id, ad_group_name, creative_id, creative_name,
placement, currency, spend_usd, impressions, clicks,
conversions, ctr, cpm, cpc, conversion_rate, roas,
effective_cpm, effective_cvr, cost_per_conversion
```

---

## Airflow DAG

**DAG ID:** `mmm_pipeline`
**Schedule:** Daily at 6:00 AM UTC
**Catchup:** Disabled

### Tasks

| Task | Type | Depends On |
|------|------|------------|
| `generate_google` | PythonOperator | — |
| `generate_meta` | PythonOperator | — |
| `generate_tiktok` | PythonOperator | — |
| `generate_reddit` | PythonOperator | — |
| `load_to_postgres` | PythonOperator | All 4 generators |
| `run_dbt_staging` | PythonOperator | `load_to_postgres` |
| `run_dbt_marts` | PythonOperator | `run_dbt_staging` |

**Retry policy:** 2 retries with 3-minute delay on all tasks.

---

## Setup & Reproduction

### Prerequisites

- Mac (Intel or Apple Silicon)
- Python 3.11
- Docker Desktop (4 GB RAM minimum allocated)
- Homebrew

### Step 1 — Clone the repo

```bash
git clone https://github.com/quizzito/ad-spend-mmm.git
cd ad-spend-mmm
```

### Step 2 — Create Python environment

```bash
/usr/local/bin/python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Step 3 — Configure environment

```bash
cp .env.example .env
# Edit .env and set your POSTGRES_PASSWORD
```

### Step 4 — Start Docker stack

```bash
docker compose --env-file .env up -d
docker compose logs airflow-scheduler -f
# Wait for "Booting worker" before proceeding
```

### Step 5 — Configure dbt

```bash
# Create local profiles file
mkdir -p ~/.dbt
cat > ~/.dbt/profiles.yml << 'EOF'
ad_spend_mmm:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: postgres
      password: mmm2024
      dbname: mmm_db
      schema: staging
      threads: 4
EOF
```

### Step 6 — Trigger the pipeline

```
http://localhost:8080
Username: admin / Password: admin
→ Find mmm_pipeline → click ▶ to trigger
```

### Step 7 — Verify output

```sql
SELECT platform, COUNT(*) as rows, ROUND(SUM(spend_usd)::numeric, 2) as total_spend
FROM marts.consolidated_spend
GROUP BY platform ORDER BY platform;
```

### Useful commands

```bash
# Stop containers (data persists)
docker compose down

# Full reset (wipes all data)
docker compose down -v

# Check container status
docker compose ps

# View scheduler logs
docker compose logs airflow-scheduler -f

# Run dbt manually
cd dbt && dbt run
```

---

## Sample Output

First 10 rows of `marts.consolidated_spend`:

| date | platform | campaign_name | spend_usd | impressions | clicks | conversions | roas |
|------|----------|---------------|-----------|-------------|--------|-------------|------|
| 2024-01-01 | google | Brand Search | $611.92 | 42,494 | 1,401 | 82.68 | 5.97 |
| 2024-01-01 | google | Competitor Search | $432.07 | 18,003 | 576 | 34.30 | 4.74 |
| 2024-01-01 | google | Display Retarget | $558.37 | 46,531 | 559 | 26.46 | 4.23 |
| 2024-01-01 | google | YouTube Views | $693.24 | 86,655 | 1,040 | 49.18 | 3.57 |
| 2024-01-01 | meta | Prospecting — Lookalike | $412.18 | 17,174 | 480 | 26.40 | 3.12 |
| 2024-01-01 | meta | Retargeting — Cart Abandon | $387.45 | 14,902 | 372 | 19.86 | 4.21 |
| 2024-01-01 | meta | Brand Awareness — Video | $298.76 | 24,898 | 622 | 31.10 | 2.87 |
| 2024-01-01 | tiktok | TikTok Awareness — Gen Z | $334.52 | 55,753 | 2,230 | 89.20 | 3.45 |
| 2024-01-01 | reddit | Reddit — Tech Community | $98.43 | 19,686 | 492 | 17.22 | 2.94 |
| 2024-01-01 | reddit | Reddit — Finance Community | $112.67 | 22,534 | 563 | 19.71 | 3.18 |

---

## Design Decisions

**Why load all columns as TEXT in Postgres?**
The `load_to_postgres` task loads every CSV column as TEXT using psycopg2's native COPY command. This is intentional — it's the fastest possible load method and avoids type inference errors on raw data. All type casting happens in dbt staging models where it's version-controlled, testable, and visible.

**Why dbt staging models as views?**
Staging models are materialized as views rather than tables. They're lightweight transformations that run quickly and don't need to persist data independently — their only job is to feed the mart model. The mart model is materialized as a table because it's the layer external tools query.

**Why psycopg2 COPY instead of pandas to_sql?**
PostgreSQL's native COPY command is significantly faster than INSERT-based loading and has zero external dependencies inside the Airflow container. It avoids the SQLAlchemy version conflicts that arise when mixing Airflow's internal SQLAlchemy with pandas' requirements.

**Why Airflow instead of a cron job?**
Airflow provides task-level observability, retry logic, dependency management, and a UI for monitoring runs. A cron job would give you none of this. For a pipeline that will eventually connect to real APIs, the ability to see exactly which task failed and why is essential.

**Why keep dbt outside Docker?**
dbt Core runs locally in the Python venv and connects to the Dockerized Postgres over port 5432. This makes iterating on models much faster — no container rebuilds needed. Inside the Airflow container, dbt is installed via `_PIP_ADDITIONAL_REQUIREMENTS` for scheduled runs.

---

## Next Steps

1. **Add dbt tests** — `not_null`, `accepted_values`, and `relationships` tests on staging models to catch bad data before it reaches the mart
2. **Connect real APIs** — swap dummy generators for real Google Ads, Meta Marketing, TikTok, and Reddit API clients — dbt models require zero changes
3. **PyMC-Marketing MMM** — point a PyMC-Marketing notebook directly at `marts.consolidated_spend` and run a channel contribution model
4. **dbt sources freshness** — add `dbt source freshness` checks to alert if raw tables haven't been updated within the expected window
5. **Airflow Connections** — migrate Postgres credentials from environment variables to Airflow's encrypted connections store
6. **Add Reddit and TikTok platform-specific models** — surface `video_views` (TikTok) in a separate analytics schema without polluting the MMM table

---

*Built with PostgreSQL 15, dbt Core 1.8, Apache Airflow 2.9, Python 3.11*