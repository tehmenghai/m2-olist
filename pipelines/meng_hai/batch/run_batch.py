"""
pipelines/meng_hai/batch/run_batch.py
──────────────────────────────────────
Batch pipeline runner for Meng Hai (Payment Analytics).

Usage:
    python pipelines/meng_hai/batch/run_batch.py               # all steps (csv-to-bq + dbt)
    python pipelines/meng_hai/batch/run_batch.py --step csv-to-bq
    python pipelines/meng_hai/batch/run_batch.py --step dbt [--mode full|cdc]

Pipeline
────────
  Step 1  Python (google-cloud-bigquery) : CSV files → olist_raw (Bronze)
  Step 2  dbt                            : olist_raw → olist_silver → olist_gold
"""

import argparse
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml
from google.cloud import bigquery

# ── Paths ────────────────────────────────────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_DATA_DIR = _PROJECT_ROOT / "data"
_DBT_DIR = Path("pipelines/meng_hai/batch/dbt")
_GCP_CONFIG = _PROJECT_ROOT / "dashboards" / "meng_hai" / "config" / "gcp_config.yaml"

# ── CSV → BQ table mapping ──────────────────────────────────
_CSV_TABLE_MAP = {
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_customers_dataset.csv": "customers",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "olist_geolocation_dataset.csv": "geolocation",
}


def log(msg: str):
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}")


def _load_config() -> dict:
    """Load GCP config from dashboards/meng_hai/config/gcp_config.yaml."""
    if not _GCP_CONFIG.exists():
        log(f"✗ GCP config not found: {_GCP_CONFIG}")
        log("  Copy the template: cp config/gcp_config_template.yaml dashboards/meng_hai/config/gcp_config.yaml")
        sys.exit(1)
    with open(_GCP_CONFIG) as f:
        return yaml.safe_load(f)


# ── Step 1: CSV → BigQuery (olist_raw) ──────────────────────

def load_csv_to_bq():
    """Load all 8 CSV files directly into BigQuery olist_raw dataset."""
    log("Step 1/2 — CSV → BigQuery (olist_raw)...")
    cfg = _load_config()
    project_id = cfg["project_id"]
    location = cfg.get("location", "US")

    client = bigquery.Client(project=project_id, location=location)
    dataset_ref = f"{project_id}.olist_raw"

    # Ensure olist_raw dataset exists
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = location
    client.create_dataset(dataset, exists_ok=True)

    total_rows = 0
    for csv_file, table_name in _CSV_TABLE_MAP.items():
        csv_path = _DATA_DIR / csv_file
        if not csv_path.exists():
            log(f"  ⚠ Skipping {csv_file} — file not found")
            continue

        table_id = f"{dataset_ref}.{table_name}"
        # Drop existing table first to avoid partition/schema conflicts
        client.delete_table(table_id, not_found_ok=True)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            allow_quoted_newlines=True,
        )

        with open(csv_path, "rb") as f:
            load_job = client.load_table_from_file(f, table_id, job_config=job_config)
        load_job.result()  # Wait for completion

        table = client.get_table(table_id)
        total_rows += table.num_rows
        log(f"  ✓ {table_name}: {table.num_rows:,} rows")

    log(f"✓ CSV → BQ complete — {total_rows:,} total rows across {len(_CSV_TABLE_MAP)} tables.")


# ── Step 2: dbt (Silver + Gold) ──────────────────────────────

def run_dbt(mode: str = "full"):
    log(f"Step 2/2 — dbt: olist_raw → olist_silver → olist_gold (mode={mode})...")
    # Install dbt packages (dbt_utils) if not already present
    try:
        deps_result = subprocess.run(
            ["dbt", "deps", "--profiles-dir", "."],
            cwd=_DBT_DIR, capture_output=True, text=True, timeout=120,
        )
        if deps_result.returncode != 0:
            log(f"⚠ dbt deps warning: {deps_result.stderr[-500:]}")
    except FileNotFoundError:
        pass  # Will be caught below

    extra = ["--full-refresh"] if mode != "cdc" else []
    try:
        result = subprocess.run(
            ["dbt", "run", "--profiles-dir", "."] + extra,
            cwd=_DBT_DIR, capture_output=True, text=True, timeout=600,
        )
        if result.returncode == 0:
            log("✓ dbt run complete — olist_silver and olist_gold populated.")
        else:
            log(f"✗ dbt run failed (exit {result.returncode}):")
            log(result.stderr[-2000:])
            sys.exit(1)
    except FileNotFoundError:
        log("✗ dbt not installed.")
        log("  Run: pip install dbt-bigquery")
        sys.exit(1)


# ── Entry point ───────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Olist batch pipeline runner (Meng Hai)")
    parser.add_argument(
        "--step",
        choices=["csv-to-bq", "dbt", "all"],
        default="all",
        help="Which step to run (default: all)",
    )
    parser.add_argument(
        "--mode",
        choices=["full", "cdc"],
        default="full",
        help="dbt run mode: 'full' = full refresh; 'cdc' = incremental",
    )
    args = parser.parse_args()

    log(f"=== Batch Pipeline Start (step={args.step}, mode={args.mode}) ===")
    start = time.time()

    if args.step in ("csv-to-bq", "all"):
        load_csv_to_bq()
    if args.step in ("dbt", "all"):
        run_dbt(mode=args.mode)

    elapsed = round(time.time() - start, 1)
    log(f"=== Batch Pipeline Complete ({elapsed}s) ===")
