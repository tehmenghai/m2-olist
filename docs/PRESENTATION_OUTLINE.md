# Project Presentation Outline

**Duration:** 10 minutes + 5 min Q&A
**Audience:** Mixed business/technical executives
**Focus:** Data engineering decisions (pipeline design, tool choices, schema, scalability)

---

## Slide 1: Title

- **Project:** Olist E-Commerce Data Platform
- **Team:** Lik Hong, Meng Hai, Lanson, Ben, Huey Ling, Kendra
- **Date:** March 2026
- **Tagline:** "End-to-end data pipeline & analytics platform for Brazil's largest e-commerce marketplace"

---

## Slide 2: Problem Statement & Dataset Choice (~1.5 min)

### Why Olist?
- **Real-world data** — 100K+ orders across 8 interrelated tables (not synthetic)
- **Rich relational complexity** — customers, orders, payments, reviews, products, sellers, geolocation
- **Natural star schema fit** — clear fact/dimension separation in the source data
- **Supports dual pipeline patterns** — both batch and real-time ingestion
- **Business-relevant** — enables actionable insights across 6 distinct domains

### Problem Statement
> "How can Olist leverage its marketplace data to improve customer satisfaction, reduce operational inefficiencies, and increase revenue?"

**Speaker notes:**
- Emphasise that this is production-quality data with real business complexity (nullable fields, duplicates, messy timestamps)
- Mention the dataset has enough volume to demonstrate CDC and incremental processing benefits

---

## Slide 3: Architecture Overview (~2 min)

### End-to-End Flow
```
Raw CSVs → Meltano (EL) → GCS Bronze → dbt (Silver → Gold) → BigQuery → Gradio Dashboards
                                                                              ↑
Pub/Sub → Streaming Consumer → GCS Streaming → dbt Incremental ──────────────┘
```

### Key Points
- **Medallion architecture:** Bronze (raw) → Silver (cleansed) → Gold (star schema)
- **Dual pipeline:** Batch ELT for historical data + real-time streaming for live events
- **6 independent GCP projects** — each developer owns their own pipeline end-to-end
- **Unified frontend:** All 6 dashboards merge into one Gradio app via merge contract

### Visual
- Architecture diagram showing all components and data flow
- Colour-code batch vs. real-time paths

**Speaker notes:**
- Walk through the diagram left-to-right
- Highlight that each developer independently built and tested their own pipeline
- The merge contract pattern is what allows 6 developers to work in parallel

---

## Slide 4: Data Warehouse Design — Star Schema (~2 min)

### Schema Diagram
```
                    ┌──────────────┐
                    │  Dim_Date    │
                    └──────┬───────┘
                           │
┌──────────────┐    ┌──────┴───────┐    ┌──────────────┐
│ Dim_Customers├────┤  Fact_Orders ├────┤ Dim_Products  │
└──────────────┘    └──┬────┬──────┘    └──────────────┘
                       │    │
              ┌────────┘    └────────┐
              │                      │
       ┌──────┴───────┐      ┌──────┴───────┐
       │ Dim_Sellers   │      │ Dim_Payments  │
       └───────────────┘      └───────────────┘
```

- **Fact table:** `Fact_Orders` — order_id, customer_id, product_id, seller_id, payment details, timestamps, delivery metrics
- **Dimensions:** Customers, Products, Sellers, Payments, Reviews, Date

### Why Star Schema?
- **Optimised for analytics** — aggregations and joins are straightforward
- **Clear separation** of measures (facts) vs. attributes (dimensions)
- **BigQuery-friendly** — performs best with denormalised/star patterns
- **Analyst-accessible** — BI users can query without complex multi-table joins

**Speaker notes:**
- Show how a typical analytical query (e.g., "revenue by product category by month") only needs one fact + two dimension joins
- Mention that BigQuery's columnar storage makes star schema particularly efficient

---

## Slide 5: ELT Pipeline Design & CDC (~2 min)

### Why ELT over ETL?
- **Push-down processing** — leverage BigQuery's compute for transformations
- **Raw data preserved** — Bronze layer maintains full audit trail
- **Flexibility** — transform logic can evolve without re-ingesting data

### CDC (Change Data Capture) Approach
- dbt **incremental materialisation** with `unique_key` — uses MERGE, not full refresh
- **Prevents duplicates** at every layer
- **Cost-efficient** — processing time proportional to changes, not total data volume
- Three-layer validation: Bronze (raw) → Silver (cleansed, PII masked) → Gold (star schema)

### Tool Choices & Justification

| Tool | Role | Why This Tool? |
|------|------|---------------|
| **Meltano** | Extract & Load | Open-source, plugin-based, built-in state management for incremental loads |
| **dbt** | Transform | SQL-first, version-controlled, built-in testing, industry standard |
| **Dagster** | Orchestration | Asset-based (not task-based), native sensor support for real-time triggers |
| **BigQuery** | Warehouse | Serverless, scales to petabytes, native GCS integration |
| **GCS** | Storage | Medallion layer storage, native BigQuery integration, cost-effective |

**Speaker notes:**
- Emphasise the MERGE strategy: `INSERT` new rows, `UPDATE` changed rows — no duplicates
- Compare briefly to alternatives: "We chose Meltano over Airbyte for its lightweight footprint and state management; dbt over stored procedures for version control and testability"

---

## Slide 6: Real-time Pipeline — Bonus (~1 min)

### Architecture
```
Pub/Sub → Streaming Consumer → GCS (streaming bucket) → dbt Incremental → BigQuery
                                        ↑
                                  Redis Cache
                              (dimension lookups)
```

### Key Design Decisions
- **Pub/Sub** for message queuing — decouples producers from consumers
- **Redis cache** for low-latency dimension lookups (avoid hitting BigQuery on every event)
- **Dagster sensor** monitors streaming bucket, triggers incremental dbt runs
- **Admin Panel** provides operational controls: start/stop simulator, pipeline health, cache management

**Speaker notes:**
- Clarify that the real-time pipeline uses simulated events (not live production data)
- The architecture is production-ready — only the data source would change in a real deployment

---

## Slide 7: Data Quality & Testing (~1 min)

### dbt Testing
- **Schema tests:** `not_null`, `unique`, `accepted_values` on every model
- **Referential integrity:** `relationships` tests between fact and dimension tables
- **Custom SQL tests** for business logic validation (e.g., payment amounts > 0)

### Three-Layer Validation
| Layer | Checks |
|-------|--------|
| Bronze | Schema conformance, data type validation |
| Silver | Deduplication, PII masking, null handling |
| Gold | Business rule validation, referential integrity |

### Application Testing
- **Functional testing** — each dashboard tested against live BigQuery data
- **UX testing** — responsive layout, error handling, loading states
- **Performance testing** — query optimisation, caching strategies
- **Independent testing** — cross-developer peer review

**Speaker notes:**
- Mention that dbt tests run as part of every pipeline execution (not separate)
- Highlight that testing documentation is maintained in `docs/` for audit trail

---

## Slide 8: Key Findings Summary (~1 min)

One insight per domain — keep this high-level:

| Domain | Key Finding |
|--------|-------------|
| **Customer 360** (Lik Hong) | RFM segmentation reveals at-risk customers for churn prediction |
| **Payment** (Meng Hai) | Instalment plans drive higher AOV; boleto has highest abandonment |
| **Reviews** (Lanson) | Late deliveries are the #1 predictor of low review scores |
| **Products** (Ben) | Top 20% categories account for 80% of revenue (Pareto principle) |
| **Sellers** (Huey Ling) | Late-shipping sellers have significantly lower ratings |
| **Geography** (Kendra) | North/Northeast Brazil underserved — high demand, few sellers |

**Speaker notes:**
- This slide is a summary only — direct audience to the live demo or dashboards for deep-dives
- Each finding is backed by SQL queries in the Gold layer

---

## Slide 9: Live Demo — Optional (~30 sec)

### What to Show
1. Open the Gradio app (all 6 dashboards + admin panel)
2. Quick walkthrough of **one** dashboard (pick whichever is most polished)
3. Show the admin panel — pipeline controls, health monitoring

### Backup Plan
- If live demo fails, have screenshots prepared
- Have the app running locally before the presentation starts (`make run`)

---

## Slide 10: Risks, Limitations & Future Work

### Risks & Limitations
- **Historical dataset** (2016–2018) — no live production data feed
- **PII masking is simulated** — source data already uses hashed customer/seller IDs
- **Real-time pipeline uses simulated events**, not actual live orders
- **Single-developer GCP projects** — no shared production environment

### Future Work
- **Cloud Run deployment** — containerise the Gradio app for production hosting
- **Sentiment analysis Cloud Function** — NLP on review text (Lanson)
- **ML-based churn prediction & NBA** — Next Best Action engine (Lik Hong)
- **Cross-domain analytics** — combine insights across all 6 domains in a unified Gold layer

---

## Slide 11: Q&A Preparation

### Anticipated Questions & Talking Points

**Q: "Why GCS + BigQuery vs. other cloud providers?"**
> Native integration between GCS and BigQuery (external tables, easy loads). Serverless scaling — no cluster management. Cost-effective for analytics workloads with columnar storage and on-demand pricing.

**Q: "Why Meltano over Airbyte or Fivetran?"**
> Open-source and lightweight. Built-in state management enables incremental loads out of the box. Plugin architecture makes it easy to add new sources. Fivetran is SaaS (cost); Airbyte is heavier to self-host.

**Q: "Why dbt for transformations?"**
> SQL-first approach — accessible to analysts, not just engineers. Version-controlled transformations (Git). Built-in testing framework. Industry standard for the T in ELT. Incremental materialisation with MERGE for CDC.

**Q: "Why Dagster over Airflow?"**
> Asset-based paradigm (data-aware) vs. Airflow's task-based approach. Native sensor support for real-time pipeline triggers. Better developer experience with type checking and testability. Purpose-built for data pipelines.

**Q: "Why Gradio over Streamlit or Dash?"**
> Quick prototyping with minimal boilerplate. Native Python — no frontend framework needed. Easy multi-tab merge pattern for 6 developers working in parallel. Built-in sharing capability.

**Q: "How do you handle duplicates?"**
> CDC at every layer via dbt incremental materialisation with `unique_key`. The MERGE strategy inserts new rows and updates existing ones — no duplicates possible. Applied consistently across Bronze, Silver, and Gold layers.

**Q: "How does this scale?"**
> BigQuery is serverless — scales to petabytes without cluster management. GCS provides virtually unlimited storage. Pub/Sub handles millions of messages per second. dbt incremental models process only changed data, keeping costs proportional to change volume.

**Q: "What would you do differently?"**
> Consider a shared GCP project with proper IAM for production. Use Terraform for infrastructure-as-code. Add data contracts between producers and consumers. Implement data lineage tracking.

---

## Presentation Checklist

### Assignment Coverage Verification
- [x] **Ingestion** — Meltano EL, GCS Bronze layer (Slides 3, 5)
- [x] **Warehouse design** — Star schema, BigQuery (Slide 4)
- [x] **ELT pipeline** — dbt transformations, medallion architecture (Slide 5)
- [x] **Data quality** — dbt tests, three-layer validation (Slide 7)
- [x] **Analysis** — Key findings across 6 domains (Slide 8)
- [x] **Orchestration** — Dagster batch + sensors (Slides 5, 6)
- [x] **Documentation** — CLAUDE.md, project-plan.md, testing docs (implied)
- [x] **Presentation** — This document

### Timing Guide
| Slide | Topic | Time |
|-------|-------|------|
| 1 | Title | 15 sec |
| 2 | Problem & Dataset | 1.5 min |
| 3 | Architecture | 2 min |
| 4 | Star Schema | 2 min |
| 5 | ELT & CDC | 2 min |
| 6 | Real-time Pipeline | 1 min |
| 7 | Data Quality | 1 min |
| 8 | Key Findings | 1 min |
| 9 | Live Demo | 30 sec |
| 10 | Risks & Future | 30 sec |
| **Total** | | **~10 min** |
