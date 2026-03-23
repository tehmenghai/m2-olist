# Project Plan — Olist E-Commerce Data Product (GCP)
> Status: APPROVED — Phase 0 complete, Phase 1+2 in progress (parallel)
> Last updated: 2026-03-14


---

## 1. Objective

Build an end-to-end data product from the Kaggle Brazil Olist e-commerce dataset, hosted on GCP, surfaced via a unified Gradio web application. Six team members each own a domain dashboard; all dashboards merge into a single professional launchpad.

---

## 2. Team & Domain Ownership

| # | Developer | Domain | Pipeline Ownership |
|---|-----------|--------|-------------------|
| 1 | Lik Hong | Customer 360 + Next Best Action | Batch (CDC) + Real-time + Dagster + Launchpad + Admin Panel |
| 2 | Meng Hai | Payment Analytics | Own batch pipeline (CDC) — real-time optional |
| 3 | Lanson | Customer Reviews & Satisfaction | Own batch pipeline (CDC) + future sentiment Cloud Function |
| 4 | Ben | Product Analytics | Own batch pipeline (CDC) — real-time optional |
| 5 | Huey Ling | Seller Performance | Own batch pipeline (CDC) — real-time optional |
| 6 | Kendra | Geography Analytics | Own batch pipeline (CDC) — real-time optional |

> Each developer owns their full slice: pipeline → Gold tables → dashboard. Developed in parallel.

---

## 3. Architecture Overview

```
DATA SOURCES
  ├── Batch: Olist CSVs → Meltano (EL) → GCS Bronze Bucket
  └── Real-time: Orders Simulator → Google Pub/Sub → GCS Streaming Bucket
                                  → Google Memorystore (Redis) [Product/Seller Cache]

STORAGE & ELT (GCS + dbt)
  Bronze (raw)  →  dbt  →  Silver (cleansed, PII masked)  →  dbt  →  Gold (Star Schema)
    └─ Dim_Customers, Dim_Payments, Dim_Reviews, Dim_Products, Dim_Sellers, Fact_Orders

ORCHESTRATION: Dagster (batch schedules + real-time sensors)

SERVING
  Google BigQuery ← GCS Gold ← each developer reads their own GCP Gold dataset

CONSUMPTION LAYER
  Gradio App (app.py)
    ├── Home / Launchpad
    ├── Admin Panel        ← cache flush, CDC pipeline reset, real-time agent control
    ├── Customer 360 + NBA (Lik Hong)
    ├── Payment (Meng Hai)
    ├── Reviews & Satisfaction (Lanson)
    ├── Products (Ben)
    ├── Seller Performance (Huey Ling)
    └── Geography (Kendra)

UNSTRUCTURED (Optional)
  Customer comments → Cloud Function (Sentiment Analysis) → Gold
```

---

## 4. Proposed Folder Structure

```
m2-olist/
├── CLAUDE.md                         # Master dev guide & conventions
├── project-plan.md                   # This file
├── quick-setup.md                    # GCP auth + first-run instructions for all devs
├── .gitignore
├── requirements.txt                  # Shared Python deps (gradio, google-cloud-bigquery, etc.)
├── Makefile                          # make run | make dev-<name> | make pipeline-batch | make pipeline-rt
├── launch.sh                         # One-command launcher
│
├── docs/
│   ├── FUNCTIONAL_TESTING.md
│   ├── UX_TESTING.md
│   ├── PERFORMANCE_TESTING.md
│   └── INDEPENDENT_TESTING.md
│
├── data/                             # Kaggle CSVs (source of truth for batch ingest)
│
├── shared/
│   ├── theme.py                      # Gradio dark theme: red→orange→yellow→green palette
│   ├── components.py                 # Reusable UI: header, footer, metric cards, nav tiles
│   └── utils.py                      # GCP client factory (supports ADC + service account key)
│
├── config/
│   └── gcp_config_template.yaml     # Template — each dev copies to their own dashboards/<name>/config/
│
├── pipelines/
│   ├── README.md                     # Pipeline overview + per-developer guide
│   ├── lik_hong/                     # Full batch + real-time pipeline (Lik Hong)
│   │   ├── batch/
│   │   │   ├── meltano/              # Extract & Load: CSVs → GCS Bronze
│   │   │   ├── dbt/                  # Transform: Bronze → Silver → Gold (star schema)
│   │   │   └── run_batch.py          # Top-level batch runner (supports --mode cdc)
│   │   ├── realtime/
│   │   │   ├── simulator/run_simulator.py  # Fabricated order event generator (Pub/Sub)
│   │   │   ├── consumer/             # Pub/Sub subscriber → GCS Streaming Bucket
│   │   │   └── redis_cache/          # Memorystore config
│   │   └── dagster/                  # Orchestration: jobs, schedules, sensors
│   ├── meng_hai/batch/               # Batch only — reference lik_hong/batch/
│   ├── lanson/batch/                 # Batch only (+ future sentiment pipeline)
│   ├── ben/batch/                    # Batch only
│   ├── huey_ling/batch/              # Batch only
│   └── kendra/batch/                 # Batch only
│   # Note: real-time pipeline is optional for developers 2–6.
│   #       Coordinate with Lik Hong to subscribe to Pub/Sub topic olist-orders-live.
│
├── dashboards/
│   ├── home/                         # Launchpad + Project Overview (Lik Hong)
│   │   └── app.py                    # exports: dashboard (gr.Blocks)
│   ├── admin/                        # Admin Panel (Lik Hong)
│   │   └── app.py                    # exports: dashboard (gr.Blocks)
│   ├── lik_hong/                     # Customer 360 + Next Best Action
│   │   ├── config/
│   │   │   └── gcp_config.yaml      # GITIGNORED — personal GCP credentials
│   │   ├── app.py                    # exports: dashboard; also runnable standalone
│   │   ├── queries.py
│   │   └── README.md
│   ├── meng_hai/
│   │   ├── config/gcp_config.yaml
│   │   ├── app.py
│   │   ├── queries.py
│   │   └── README.md
│   ├── lanson/
│   │   ├── config/gcp_config.yaml
│   │   ├── app.py
│   │   ├── queries.py
│   │   └── README.md
│   ├── ben/
│   │   ├── config/gcp_config.yaml
│   │   ├── app.py
│   │   ├── queries.py
│   │   └── README.md
│   ├── huey_ling/
│   │   ├── config/gcp_config.yaml
│   │   ├── app.py
│   │   ├── queries.py
│   │   └── README.md
│   └── kendra/
│       ├── config/gcp_config.yaml
│       ├── app.py
│       ├── queries.py
│       └── README.md
│
└── app.py                            # Main app — mounts all dashboards as gr.Tabs
```

---

## 5. Technology Stack

| Layer | Technology |
|-------|-----------|
| Data Storage | Google Cloud Storage (GCS) — Bronze / Silver / Gold buckets |
| Data Warehouse | Google BigQuery |
| EL (Extract-Load) | Meltano |
| Transform | dbt (runs on GCS/BigQuery) |
| Orchestration | Dagster |
| Real-time Messaging | Google Pub/Sub |
| Real-time Cache | Google Memorystore (Redis) |
| Sentiment Analysis | Cloud Functions (optional, unstructured comments) |
| Dashboard Framework | Gradio |
| Auth (GCP) | Service Account JSON key OR Application Default Credentials (ADC) — configurable |
| Language | Python 3.11+ |

---

## 6. Data Pipeline Design

**Pipeline ownership:** Each developer has their own `pipelines/<name>/` folder.
Lik Hong owns the **full pipeline** (batch + real-time + Dagster orchestration).
Developers 2–6 own their **batch pipeline only**; real-time is opt-in via coordination with Lik Hong.
Reference implementation for batch: `pipelines/lik_hong/batch/`

### 6.1 Batch Pipeline — CDC-first approach

**Design principle:** The batch pipeline runs in **incremental / CDC mode by default** at every layer.
This prevents duplicate rows and avoids re-writing the entire dataset to GCS on each run,
keeping storage costs and processing time proportional to what actually changed.

| Layer | Strategy | How deduplication is enforced |
|-------|----------|-------------------------------|
| **GCS Bronze** (Meltano EL) | Write new files only — use Meltano state bookmarks to track last-loaded offset. Existing files are never overwritten. | Meltano state file (`.meltano/state/`) tracks the high-water mark. |
| **Silver** (dbt) | dbt `incremental` materialisation with `unique_key` on natural keys (e.g. `order_id`, `review_id`). New rows are inserted; changed rows are updated (MERGE). | `unique_key` clause prevents duplicate inserts. |
| **Gold** (dbt) | dbt `incremental` materialisation on all Dim and Fact tables. `Fact_Orders` uses `order_id` as unique key. | BigQuery MERGE statement — no duplicate facts, no storage explosion. |
| **BigQuery** | Tables are never dropped and recreated unless explicitly triggered via Admin Panel → Recreate Pipeline. | Normal runs are always incremental. Full refresh only on explicit admin action. |

**When to use full refresh:**
- First-time setup (initial load)
- Schema changes in dbt models
- Corrupt/inconsistent state detected
- Admin Panel → "Recreate Pipeline (CDC Mode)" button

**dbt model config (applied to all Gold models):**
```sql
{{ config(
    materialized='incremental',
    unique_key='order_id',          -- or relevant PK per table
    on_schema_change='sync_all_columns'
) }}
```

**Steps:**
1. **Extract & Load:** Meltano reads only new/changed CSVs from `/data/`, writes Parquet to GCS Bronze bucket using state bookmarks
2. **Transform (Bronze → Silver):** dbt incremental run — cleans nulls, masks PII, standardises types; only new rows processed
3. **Transform (Silver → Gold):** dbt incremental run — builds/updates star schema via MERGE; no duplicate facts
4. **Load to BigQuery:** dbt materialises Gold tables incrementally into each dev's BigQuery dataset
5. **Schedule:** Dagster job — configurable (default: on-demand / daily); always runs incremental unless `--full-refresh` is explicitly passed

### 6.2 Real-time Pipeline (CDC Mode)
1. **Simulator:** Python script fabricates order events (new orders, status changes, payments) and publishes to Google Pub/Sub topic `olist-orders-live`
2. **Consumer:** Pub/Sub subscriber writes events to GCS Streaming Bucket (raw landing)
3. **Redis Cache:** Product and seller dimension data cached in Memorystore for low-latency lookup
4. **CDC Mode:** When activated, pipeline switches to Change Data Capture mode — incremental updates applied on top of Gold tables rather than full refresh
5. **Dagster Sensor:** Monitors streaming bucket, triggers incremental dbt runs

### 6.3 Admin Panel Controls
The Admin Panel (accessible from main app) provides:

| Control | Function |
|---------|----------|
| **Clear App Cache** | Flushes Redis Memorystore cache + any in-memory Gradio state |
| **Recreate Pipeline (CDC Mode)** | Tears down and rebuilds Gold tables in CDC/incremental mode, wipes streaming state |
| **Start Real-time Agent** | Launches the order event simulator; shows live event log |
| **Stop Real-time Agent** | Gracefully stops the simulator |
| **Pipeline Status** | Shows current batch/real-time pipeline health from Dagster |
| **Last Refresh Timestamps** | Per-table freshness indicators |

---

## 7. Individual Dashboard Specs

> **Note for developers 2–6 (Meng Hai, Lanson, Ben, Huey Ling, Kendra):**
> The feature lists below are **suggestions only**. Each developer is free to design
> their own dashboard — choose the metrics, charts, and interactions that best tell the
> story of your domain. The only hard requirements are the merge contract rules (Section 11)
> and the shared theme (Section 8). Use the scaffold in `dashboards/<name>/app.py` and
> `queries.py` as a starting point, not a constraint.

### 7.1 Home / Launchpad (Lik Hong)
- Project title, team, and architecture overview
- 6 navigation cards (one per dashboard) with domain icon, owner name, and live data freshness badge
- System health indicators (pipeline status, BigQuery connection, cache status)
- Dark mode hero banner with red→green accent palette

### 7.2 Customer 360 + Next Best Action (Lik Hong)
- Customer profile lookup (by ID or segment)
- 360° view: order history, payment behaviour, review sentiment, location
- RFM segmentation (Recency, Frequency, Monetary) heatmap
- Churn probability score (ML model output)
- Next Best Action recommendations (product category, discount trigger, re-engagement timing)
- Real-time order feed for selected customer (when real-time agent is active)

### 7.3 Payment Analytics (Meng Hai)
- Payment method breakdown (credit card, boleto, voucher, debit)
- Instalment analysiswhat
- Revenue by payment type over time
- Average Order Value (AOV) by payment method
- Cancellation rate tracking

### 7.4 Reviews & Satisfaction (Lanson)
- Review score distribution
- Sentiment analysis on review text (positive / neutral / negative) — **Lanson also owns Cloud Function for sentiment pipeline (later phase)**
- Review score vs delivery time correlation
- CSAT / NPS-equivalent trend
- Low-score alert list (orders needing intervention)

### 7.5 Product Analytics (Ben)
- Top/bottom products by revenue and volume
- Category performance matrix
- Product return/refund rate
- Inventory proxy (order frequency as demand signal)
- Category drill-down with translation (Portuguese → English)

### 7.6 Seller Performance (Huey Ling)
- Seller leaderboard (revenue, fulfilment speed, review score)
- Delivery latency: actual vs expected
- Star rating distribution per seller
- Seller geographic coverage
- Risk flag: sellers with high late delivery or low rating

### 7.7 Geography Analytics (Kendra)
- Customer density heatmap by state/city
- Seller distribution map
- Delivery time by region
- Revenue concentration map
- Underserved region opportunity analysis

---

## 8. UI/UX Design System

### Theme
- **Mode:** Dark
- **Background:** `#0D0D0D` (base) / `#1A1A1A` (card surface) / `#262626` (elevated)
- **Accent Palette:**
  - Critical / High: `#FF4444` (red)
  - Warning / Medium: `#FF8C00` (orange)
  - Neutral / Info: `#FFD700` (gold/yellow)
  - Positive / Good: `#00C851` (green)
- **Text:** `#F0F0F0` primary / `#A0A0A0` secondary
- **Border:** `#2A2A2A` subtle / `#FF8C00` focused
- **Font:** Inter or system sans-serif — clean, no decorative elements
- **Charts:** Dark canvas, accent color series, minimal gridlines

### Layout Principles
- Card-based, generous padding
- Single sidebar or top-tab navigation only
- KPI metrics always visible above the fold
- No unnecessary decorations — data first

---

## 9. GCP Configuration

Each developer has their own GCP project/dataset. Auth method is configurable per developer:

**Option A: Service Account JSON Key**
```yaml
auth_method: service_account
key_path: dashboards/<name>/config/service_account.json
project_id: my-gcp-project-id
dataset: olist_gold
```

**Option B: Application Default Credentials (ADC)**
```yaml
auth_method: adc
project_id: my-gcp-project-id
dataset: olist_gold
```

The `shared/utils.py` GCP client factory reads this config and selects the auth method automatically. Full instructions in `quick-setup.md`.

**Security:** All `config/gcp_config.yaml` and `*.json` credential files are gitignored globally.

---

## 10. Development Phases & Milestones

**Core principle:** This is a **learning project**. Each developer is fully responsible for
their own end-to-end slice: their own data pipeline (`pipelines/<name>/`) AND their own
dashboard (`dashboards/<name>/`). Work is done **in parallel** — no developer should be
blocked waiting for another's pipeline. Each developer integrates their dashboard with their
own pipeline and GCP project before the final merge step.

```
Developer workflow (per person, done in parallel):
  Own pipeline ──► Own GCP Gold tables ──► Own dashboard ──► Standalone test ──► Merge to main app
```

---

### Phase 0 — Project Setup ✅ COMPLETE (Lik Hong scaffolded)
- [x] Scaffold full folder structure
- [x] Create all shared files (theme, utils, components, config template)
- [x] Create CLAUDE.md, quick-setup.md, docs/testing files
- [ ] **Each developer:** clone repo, copy config template, set up own GCP project & credentials

---

### Phase 1 — Each Developer: Build Own Batch Pipeline (All, in parallel)

Each developer builds and owns their `pipelines/<name>/batch/` pipeline independently.
Reference implementation: `pipelines/lik_hong/batch/` (Lik Hong builds this first as lead).

**All developers:**
- [ ] Set up GCP project, enable BigQuery + GCS APIs
- [ ] Configure `dashboards/<name>/config/gcp_config.yaml`
- [ ] Configure Meltano to ingest relevant CSVs → GCS Bronze bucket
- [ ] Write dbt models: Bronze → Silver (cleanse, PII mask) → Gold (star schema, incremental)
- [ ] Validate: required Gold tables are queryable in own BigQuery dataset
- [ ] Confirm CDC/incremental mode works — re-run pipeline, verify no duplicates

**Lik Hong additionally (pipeline lead):**
- [ ] Dagster: batch job definition + manual trigger
- [ ] `run_batch.py` batch runner (supports `--mode cdc`)

> **Note for developers 2–6:** See `pipelines/lik_hong/batch/` as a reference.
> Use Option A in your `pipelines/<name>/batch/README.md` (point to Lik Hong's shared GCS/BigQuery)
> if you prefer to focus time on the dashboard rather than re-building the full ELT stack.

---

### Phase 2 — Each Developer: Build Own Dashboard (All, in parallel with Phase 1)

Each developer builds their `dashboards/<name>/app.py` connected to their own GCP Gold dataset.
Pipeline and dashboard are developed together — you build both, you own both.

**All developers:**
- [ ] Build `dashboards/<name>/app.py` — query your own Gold tables, design your own charts
- [ ] Build `dashboards/<name>/queries.py` — all BigQuery SQL for your domain
- [ ] Standalone test: `python dashboards/<name>/app.py` (or `make dev-<name>`)
- [ ] Confirm dashboard exports a `dashboard` (gr.Blocks) object per merge contract
- [ ] Apply shared theme: `from shared.theme import olist_theme, CUSTOM_CSS`
- [ ] Run `make lint` — no syntax errors

---

### Phase 3 — Real-time Pipeline (Lik Hong only)

- [ ] Pub/Sub topic + subscription setup (`olist-orders-live`)
- [ ] Order event simulator (`pipelines/lik_hong/realtime/simulator/run_simulator.py`)
- [ ] Streaming consumer → GCS Streaming Bucket
- [ ] Redis / Memorystore cache for dimension lookups
- [ ] Dagster sensor triggering incremental dbt runs on streaming data
- [ ] Customer 360 dashboard updated with real-time order feed

**Developers 2–6:** Optional. Coordinate with Lik Hong if you want to subscribe to the live feed.

---

### Phase 4 — Admin Panel (Lik Hong)

- [ ] Cache clear (Redis + Gradio state)
- [ ] CDC pipeline recreate (full-refresh trigger via Admin Panel)
- [ ] Real-time simulator start / stop controls
- [ ] Pipeline health status display
- [ ] Table freshness timestamps

---

### Phase 5 — Integration & Final Merge (All → Lik Hong coordinates)

Each developer submits their completed `dashboards/<name>/app.py` for integration.
Lik Hong merges all dashboards into `app.py` and finalises the home launchpad.

- [ ] Each developer: confirm `dashboard` export works when imported in `app.py`
- [ ] Lik Hong: merge all 6 dashboards into main `app.py` tabs
- [ ] Home / Launchpad page reflecting live pipeline status
- [ ] Cross-dashboard tab navigation verified
- [ ] Unified theme consistent across all tabs
- [ ] `make run` launches full app without errors

---

### Phase 6 — Testing (All)
- [ ] Functional testing per `docs/FUNCTIONAL_TESTING.md`
- [ ] UX testing per `docs/UX_TESTING.md`
- [ ] Performance testing per `docs/PERFORMANCE_TESTING.md`
- [ ] Independent testing per `docs/INDEPENDENT_TESTING.md`

---

## 11. Merge Contract (Rules for All Developers)

1. Your dashboard `app.py` **must export** a variable named `dashboard` of type `gr.Blocks`
2. Do **not** call `dashboard.launch()` at module level — only guard with `if __name__ == "__main__": dashboard.launch()`
3. Import theme from `shared.theme`, not inline CSS
4. All GCP config loaded via `shared.utils.get_gcp_client()` — do not hardcode credentials
5. No global mutable state outside your own module
6. Tab title must be: `"<Icon> <Domain> — <Your Name>"` e.g. `"🧭 Customer 360 — Lik Hong"`
7. Run `make lint` before submitting your dashboard for merge

---

## 12. Testing Strategy

| Type | Owner | File |
|------|-------|------|
| Functional | Domain dev + 1 peer | `docs/FUNCTIONAL_TESTING.md` |
| UX / Design | UX tester | `docs/UX_TESTING.md` |
| Performance | Performance tester | `docs/PERFORMANCE_TESTING.md` |
| Independent | External reviewer | `docs/INDEPENDENT_TESTING.md` |

---

## 13. Architectural Decisions (Resolved)

| Decision | Resolution |
|----------|-----------|
| GCP project structure | **6 separate GCP projects** — one per developer. Each dev has full billing/IAM control. `shared/utils.py` reads project_id from each dev's own `gcp_config.yaml`. |
| ML model for churn + NBA | **In-pipeline** — trained on Olist batch data (RFM + order history features). Model artefacts stored in GCS, loaded at dashboard runtime. |
| Messaging layer | **Google Pub/Sub** — Redpanda excluded from scope. |
| Sentiment analysis (Cloud Functions) | **Later phase** — owned by **Lanson** alongside Reviews & Satisfaction dashboard. Not in Phase 1-4 scope. |
| Deployment target | **Local only** (Phase 1-6). Cloud Run left as future option; no infra provisioning required now. |

---

*Phase 0 scaffolding is complete. All developers can now begin Phase 1 (pipeline) and Phase 2 (dashboard) in parallel — each in their own GCP project and pipeline folder. Merge to main app happens in Phase 5.*
