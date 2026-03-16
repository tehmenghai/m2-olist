# Payment Analytics — Tasks Summary

**Owner:** Meng Hai
**Domain:** Payment Analytics (Port 7863)
**Last Updated:** 2026-03-15

---

## Project Overview

End-to-end Payment Analytics module for the Olist e-commerce data product on GCP. Covers dashboard UI (Gradio + Plotly), BigQuery queries, GCP configuration, and a full batch ELT pipeline (Meltano → GCS → BigQuery → dbt).

---

## Completed Tasks

### Task 1: Dashboard — `queries.py`

| | |
|---|---|
| **File** | `dashboards/meng_hai/queries.py` (82 lines) |
| **Status** | DONE |
| **Date** | 2026-03-15 |

**What was built:**
- 4 BigQuery query functions, each returning a pandas DataFrame
- All SQL lives in `queries.py` (no SQL in `app.py` per coding conventions)
- Uses `shared.utils.run_query` and `qualified_table` helpers

**Functions:**

| Function | Purpose | Tables Used |
|----------|---------|-------------|
| `get_payment_summary()` | Revenue, order count, AOV, avg instalments by payment type | `Dim_Payments`, `Fact_Orders` |
| `get_monthly_revenue_by_type()` | Monthly revenue split by payment type | `Dim_Payments`, `Fact_Orders` |
| `get_instalment_distribution()` | Credit card orders grouped by instalment count | `Dim_Payments` |
| `get_cancellation_rate()` | Monthly cancellation rate percentage | `Fact_Orders` |

---

### Task 2: Dashboard — `app.py`

| | |
|---|---|
| **File** | `dashboards/meng_hai/app.py` (197 lines) |
| **Status** | DONE |
| **Date** | 2026-03-15 |

**What was built:**
- Gradio Blocks dashboard with `olist_theme` and `CUSTOM_CSS`
- Exports `dashboard` object (merge contract compliant)
- Standalone launch under `if __name__ == "__main__"` on port 7863

**KPIs (4):**

| KPI | Color | Source |
|-----|-------|--------|
| Total Revenue | Orange | `get_payment_summary()` sum |
| Total Orders | Gold | `get_payment_summary()` sum |
| Avg Order Value | Green | Calculated (revenue / orders) |
| Top Payment Type | Orange | First row from summary |

**Charts (4):**

| Chart | Type | Library |
|-------|------|---------|
| Revenue by Payment Type | Donut (pie with hole) | `px.pie` |
| Monthly Revenue Trend | Stacked area | `px.area` |
| Credit Card Instalment Distribution | Bar | `px.bar` |
| Monthly Cancellation Rate | Line with fill | `go.Scatter` |

**Design decisions:**
- All charts use `PLOTLY_LAYOUT` base with `deepcopy` + overrides
- Color palette: `COLORS["orange"]`, `COLORS["gold"]`, `COLORS["green"]`, `COLORS["red"]`
- Graceful error handling: each loader catches exceptions and returns `error_figure()` or error HTML
- GCP client loaded lazily via `make_bq_client_getter()`

---

### Task 3: GCP Config — `gcp_config.yaml`

| | |
|---|---|
| **File** | `dashboards/meng_hai/config/gcp_config.yaml` (gitignored) |
| **Status** | DONE |
| **Date** | 2026-03-15 |

**Configuration:**

```yaml
auth_method: adc
project_id: olist-dsai
dataset: olist_gold
location: US
```

---

### Task 4: Batch Pipeline — Full ELT Setup

| | |
|---|---|
| **Directory** | `pipelines/meng_hai/batch/` |
| **Status** | DONE |
| **Date** | 2026-03-15 |
| **Reference** | Adapted from `pipelines/lik_hong/batch/` |

#### 4a. Pipeline Runner — `run_batch.py`

| | |
|---|---|
| **File** | `pipelines/meng_hai/batch/run_batch.py` (149 lines) |

**3-step medallion architecture:**

| Step | Tool | Source → Target |
|------|------|-----------------|
| 1 | Meltano | CSV files → GCS Bronze (`gs://olist-dsai-bronze`) |
| 2 | GCS→BQ | GCS JSONL → `olist_raw` dataset (8 tables) |
| 3 | dbt | `olist_raw` → `olist_silver` → `olist_gold` |

**Usage:**
```bash
python pipelines/meng_hai/batch/run_batch.py               # all steps, full refresh
python pipelines/meng_hai/batch/run_batch.py --mode cdc    # incremental
python pipelines/meng_hai/batch/run_batch.py --step meltano
python pipelines/meng_hai/batch/run_batch.py --step gcs-to-bq
python pipelines/meng_hai/batch/run_batch.py --step dbt
```

**Changes from Lik Hong's version:**
- `_CONFIG_PATH` → `dashboards/meng_hai/config/gcp_config.yaml`
- `meltano_dir` → `pipelines/meng_hai/batch/meltano`
- `dbt_dir` → `pipelines/meng_hai/batch/dbt`
- Default GCS bucket → `olist-dsai-bronze`

#### 4b. Meltano Config — `meltano.yml`

| | |
|---|---|
| **File** | `pipelines/meng_hai/batch/meltano/meltano.yml` |

**Config details:**
- `project_id`: `olist-meng-hai`
- CSV paths: relative (`../../../../data/olist_*.csv`)
- GCS bucket: `olist-dsai-bronze`
- GCS prefix: `olist/raw/`
- Format: JSONL

**8 entities extracted:**
`orders`, `order_items`, `order_payments`, `order_reviews`, `customers`, `products`, `sellers`, `geolocation`

#### 4c. dbt Configuration

| File | Key Settings |
|------|-------------|
| `dbt/dbt_project.yml` | name: `olist_meng_hai`, profile: `olist_meng_hai` |
| `dbt/profiles.yml` | project: `olist-dsai`, method: `oauth`, dataset: `olist_gold` |
| `dbt/packages.yml` | `dbt-labs/dbt_utils` >=1.0.0 |
| `dbt/macros/generate_schema_name.sql` | Schema override (use `+schema` as-is) |

#### 4d. dbt Models — Staging (Silver Layer)

| Model | Source Table | Key Transformations |
|-------|-------------|---------------------|
| `stg_orders.sql` | `orders` | CAST timestamps, filter nulls |
| `stg_order_items.sql` | `order_items` | Composite key, CAST numerics |
| `stg_order_payments.sql` | `order_payments` | Payment key (`order_id_sequential`), CAST types |
| `stg_order_reviews.sql` | `order_reviews` | CAST score, timestamps |
| `stg_products.sql` | `products` | CAST dimensions, category name |
| `stg_sellers.sql` | `sellers` | MD5 hash zip code (PII masking) |
| `stg_customers.sql` | `customers` | MD5 hash zip code (PII masking) |
| `stg_geolocation.sql` | `geolocation` | Deduplicate by zip prefix, avg lat/lng |

All staging models: `+materialized: table`, `+schema: olist_silver`

#### 4e. dbt Models — Marts (Gold Layer)

| Model | Columns | Description |
|-------|---------|-------------|
| `Fact_Orders.sql` | 23 | Order-level fact table joining items, payments, reviews, products, sellers, customers. Includes calculated fields: `delivery_delay_days`, `on_time_delivery`, `product_category_name_english`, `customer_city`, `customer_state` |
| `Dim_Payments.sql` | 8 | Payment dimension: `payment_key`, type, instalments, value, joined with order timestamp and customer_id |
| `Mart_Payment_Analytics.sql` | 17 | Payment-level grain mart: payment details, order context, customer geography, product category, delivery metrics |

All mart models: `+materialized: table`, `+schema: olist_gold` (default)

#### 4f. dbt Sources — `sources.yml`

- Database: `olist-dsai`
- Schema: `olist_raw`
- 8 source tables defined with descriptions

---

### Task 5: Action Tracker — `tracker.html`

| | |
|---|---|
| **File** | `dashboards/meng_hai/tracker.html` |
| **Status** | DONE |
| **Date** | 2026-03-15 |

Dark-themed HTML tracker with progress table and manual task completion template.

---

### Task 6: Lint Check

| | |
|---|---|
| **Status** | PASS |
| **Date** | 2026-03-15 |
| **Command** | `make lint` |
| **Result** | All files compile cleanly |

---

### Task 7: Pipeline Revert — Meltano → Direct CSV→BQ Loading

| | |
|---|---|
| **File** | `pipelines/meng_hai/batch/run_batch.py` (153 lines) |
| **Status** | DONE |
| **Date** | 2026-03-15 |

**What changed:**
The batch pipeline runner was reverted from a 3-step Meltano-based ingestion (CSV → GCS → BigQuery) to a simpler **2-step direct CSV→BigQuery loading** using the `google-cloud-bigquery` Python client.

**Why:**
- Meltano added unnecessary complexity for loading 8 static CSV files
- Eliminated dependency on GCS bucket (`gs://olist-dsai-bronze`)
- Direct approach is simpler, faster, and easier to debug
- Aligns with keeping individual developer pipelines lightweight

**Pipeline architecture (updated):**

| Step | Tool | Source → Target |
|------|------|-----------------|
| 1 | Python (`google-cloud-bigquery`) | `data/*.csv` → `olist_raw` (8 tables) |
| 2 | dbt | `olist_raw` → `olist_silver` → `olist_gold` |

**Key implementation details:**
- Uses `bigquery.Client.load_table_from_file()` with `WRITE_TRUNCATE`
- Auto-creates `olist_raw` dataset if it doesn't exist
- Auto-detects CSV schema (`autodetect=True`)
- Loads all 8 CSV files with progress logging

**Updated usage:**
```bash
python pipelines/meng_hai/batch/run_batch.py               # all steps (csv-to-bq + dbt)
python pipelines/meng_hai/batch/run_batch.py --step csv-to-bq  # CSV load only
python pipelines/meng_hai/batch/run_batch.py --step dbt         # dbt only
python pipelines/meng_hai/batch/run_batch.py --step dbt --mode cdc  # incremental
```

---

### Task 8: Production Deployment Options Overview

| | |
|---|---|
| **Type** | Informational / Reference |
| **Status** | DOCUMENTED |
| **Date** | 2026-03-15 |

**Four deployment options for the Olist data product:**

#### Option A — Gradio Share Link (Simplest)
- **How:** `./launch.sh --port 7860 --share`
- **Pros:** Zero infrastructure, instant, free
- **Cons:** Link expires after 72 hours, no custom domain
- **Best for:** Demos, testing, course presentations

#### Option B — Google Cloud Run (Recommended for Production)
- **How:** Containerize the Gradio app with Docker, deploy to Cloud Run
- **Pros:** Auto-scaling (scale-to-zero), HTTPS, custom domain, pay-per-use
- **Cons:** Requires Docker setup, cold start latency (~2-5s), needs GCP billing
- **Best for:** Production dashboards with variable traffic

#### Option C — Cloud Scheduler + Cloud Run Jobs (Batch Pipeline)
- **How:** Schedule `run_batch.py` as a Cloud Run Job triggered by Cloud Scheduler
- **Pros:** Fully managed scheduling, no always-on compute, integrated with GCP IAM
- **Cons:** Additional GCP configuration, job timeout limits
- **Best for:** Automated daily/weekly data refreshes

#### Option D — Full GCP Production Stack
- **How:** Cloud Run (dashboard) + Cloud Scheduler (batch) + Pub/Sub (real-time) + Cloud SQL/Redis
- **Pros:** Enterprise-ready, scalable, all components managed
- **Cons:** Most complex, highest cost, requires DevOps knowledge
- **Best for:** Long-term production deployment beyond course scope

**Recommendation:** For this course project, **Option A** (Gradio Share) is sufficient for demos. If a persistent deployment is needed, **Option B** (Cloud Run) offers the best balance of simplicity and reliability.

---

### Task 9: dbt Transform — Payment Analytics Mart

| | |
|---|---|
| **Files** | `Fact_Orders.sql` (updated), `Mart_Payment_Analytics.sql` (new), `queries.py` (updated), `charts.py` (new), `app.py` (refactored) |
| **Status** | DONE |
| **Date** | 2026-03-15 |

**What was built:**
1. **Modified `Fact_Orders.sql`** — Added `customer_city` and `customer_state` columns (21→23 columns) by joining `stg_customers`
2. **Created `Mart_Payment_Analytics.sql`** — Payment-level grain mart with 17 columns: payment details, order context, customer geography, product category, and calculated fields (delivery delay, on-time flag)
3. **Added 4 new query functions to `queries.py`** — All query `Mart_Payment_Analytics` for analytics insights (82→180 lines)
4. **Created `charts.py`** — Extracted existing chart builders + 4 new chart functions (244 lines)
5. **Refactored `app.py`** — Layout-only module importing from `charts.py`, now renders 8 charts total (197→87 lines)

**New dbt models:**

| Model | Type | Columns | Description |
|-------|------|---------|-------------|
| `Fact_Orders.sql` | Updated | 23 | Added `customer_city`, `customer_state` via `stg_customers` join |
| `Mart_Payment_Analytics.sql` | New | 17 | Payment-level grain: payment details + order context + customer geo + product category + delivery metrics |

**New query functions (querying `Mart_Payment_Analytics`):**

| Function | Purpose |
|----------|---------|
| `get_payment_overview()` | Payment count, avg value, avg instalments by payment type |
| `get_geo_payment_analysis()` | Top 15 states by payment volume with avg value and credit card share |
| `get_product_payment_analysis()` | Top 15 product categories by revenue with avg instalments and credit card share |
| `get_price_band_analysis()` | Payment distribution across price bands (0-50, 50-150, 150-500, 500+) |

**New charts (in `charts.py`):**

| Chart | Type | Library |
|-------|------|---------|
| Payment Overview (treemap) | Treemap | `px.treemap` |
| Geographic Payment Analysis | Grouped bar | `go.Bar` |
| Product Category Payment Analysis | Horizontal bar | `go.Bar` |
| Price Band Distribution | Stacked bar | `go.Bar` |

**Architecture note:** `app.py` now contains only layout and imports all chart-building functions from `charts.py`. Total charts displayed: 8 (4 original + 4 new).

---

## Pending Tasks (Manual — Require User Action)

| # | Task | Prerequisites | Command |
|---|------|--------------|---------|
| 1 | Create GCP project `olist-dsai` | GCP account | Console |
| 2 | Enable BigQuery API + Cloud Storage API | GCP project | Console |
| 3 | Create datasets | APIs enabled | `bq mk --dataset --location=US olist-dsai:olist_gold` and `olist_raw` |
| 4 | Authenticate | GCP project | `gcloud auth application-default login` |
| 5 | Run batch pipeline | Steps 1-4 | `python pipelines/meng_hai/batch/run_batch.py` |
| 6 | Verify BigQuery tables | Step 5 | Check `olist_gold.Fact_Orders`, `olist_gold.Dim_Payments`, and `olist_gold.Mart_Payment_Analytics` in console |
| 7 | Test standalone dashboard (8 charts) | Step 6 | `make dev-menghai` → http://localhost:7863 |
| 8 | Test in main app | Step 7 | `make run` → http://localhost:7860 |
| 9 | Graceful error test | None | Run `make dev-menghai` without GCP config — verify no crashes |

---

## File Inventory

```
dashboards/meng_hai/
├── app.py                          ← Dashboard UI (87 lines, layout-only)
├── queries.py                      ← BigQuery queries (180 lines)
├── charts.py                       ← Chart builders (244 lines)
├── tracker.html                    ← Action tracker
├── TASKS_SUMMARY.md                ← This file
├── TASKS_SUMMARY.html              ← HTML version of this file
└── config/
    └── gcp_config.yaml             ← GCP config (gitignored)

pipelines/meng_hai/batch/
├── README.md                       ← Scaffold readme
├── run_batch.py                    ← Pipeline runner (149 lines)
├── meltano/
│   └── meltano.yml                 ← EL config (CSV → GCS)
└── dbt/
    ├── dbt_project.yml             ← Project config
    ├── profiles.yml                ← BigQuery connection
    ├── packages.yml                ← dbt_utils dependency
    ├── macros/
    │   └── generate_schema_name.sql
    └── models/
        ├── staging/
        │   ├── sources.yml         ← Bronze source definitions
        │   ├── stg_orders.sql
        │   ├── stg_order_items.sql
        │   ├── stg_order_payments.sql
        │   ├── stg_order_reviews.sql
        │   ├── stg_customers.sql
        │   ├── stg_products.sql
        │   ├── stg_sellers.sql
        │   └── stg_geolocation.sql
        └── marts/
            ├── Fact_Orders.sql     ← 23-column order fact table
            ├── Dim_Payments.sql    ← 8-column payment dimension
            └── Mart_Payment_Analytics.sql ← 17-column payment analytics mart
```

**Total files created/modified: 22**
