"""
pipelines/lik_hong/batch/run_batch.py
──────────────────────────────────────
Batch pipeline runner for Lik Hong.  Also called by Admin Panel CDC control
and by Dagster ops.

Usage:
    python pipelines/lik_hong/batch/run_batch.py               # all steps, full refresh
    python pipelines/lik_hong/batch/run_batch.py --mode cdc    # all steps, incremental
    python pipelines/lik_hong/batch/run_batch.py --step meltano
    python pipelines/lik_hong/batch/run_batch.py --step gcs-to-bq
    python pipelines/lik_hong/batch/run_batch.py --step dbt [--mode full|cdc]

Medallion architecture
──────────────────────
  Step 1  Meltano   : tap-csv → target-gcs → GCS Bronze bucket
  Step 2  GCS→BQ    : load JSONL files from Bronze bucket → olist_raw (Bronze BQ)
  Step 3  dbt       : olist_raw → olist_silver (staging) → olist_gold (marts)
"""

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# ── GCP config path (relative to project root) ────────────────
_CONFIG_PATH = Path("dashboards/lik_hong/config/gcp_config.yaml")


import re as _re
_ANSI = _re.compile(r"\x1b\[[0-9;]*m")

def _strip(s: str) -> str:
    return _ANSI.sub("", s)

def log(msg: str):
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {_strip(msg)}")


# ── Config reader ─────────────────────────────────────────────

def _read_gcp_config() -> dict:
    try:
        import yaml  # PyYAML
    except ImportError:
        import json as yaml  # fallback (limited)
    if not _CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"GCP config not found: {_CONFIG_PATH}\n"
            "  Run: cp config/gcp_config_template.yaml dashboards/lik_hong/config/gcp_config.yaml"
        )
    with open(_CONFIG_PATH) as f:
        return yaml.safe_load(f)


# ── Step 1: Meltano EL (CSV → GCS Bronze) ────────────────────

def run_meltano():
    log("Step 1/3 — Meltano: tap-csv → target-gcs (GCS Bronze)...")
    meltano_dir = "pipelines/lik_hong/batch/meltano"
    try:
        result = subprocess.run(
            ["meltano", "run", "--force", "tap-csv", "target-gcs"],
            cwd=meltano_dir, capture_output=True, text=True, timeout=600,
        )
        if result.returncode == 0:
            log("✓ Meltano EL complete — CSV data written to GCS Bronze.")
        else:
            log(f"✗ Meltano failed (exit {result.returncode}):")
            log(result.stderr[-2000:])
            sys.exit(1)
    except FileNotFoundError:
        log("✗ meltano not installed.")
        log("  Run: pip install meltano  &&  cd pipelines/lik_hong/batch/meltano && meltano install")
        sys.exit(1)


# ── Step 2: GCS Bronze → BQ olist_raw ────────────────────────

def load_gcs_to_bq():
    """Load JSONL files from GCS Bronze bucket into BQ olist_raw (Bronze tables)."""
    log("Step 2/3 — GCS Bronze → BQ olist_raw...")
    try:
        from google.cloud import bigquery, storage as gcs
    except ImportError:
        log("✗ google-cloud-bigquery / google-cloud-storage not installed.")
        log("  Run: pip install google-cloud-bigquery google-cloud-storage")
        sys.exit(1)

    cfg = _read_gcp_config()
    project_id  = cfg["project_id"]
    bucket      = cfg.get("gcs_bronze_bucket", f"{project_id}-bronze")
    prefix      = "olist/raw/"
    auth_method = cfg.get("auth_method", "adc").lower().strip()

    if auth_method == "service_account":
        from google.oauth2 import service_account as sa_module
        from pathlib import Path as _Path
        key_path = cfg.get("key_path", "")
        key_file = _Path(key_path)
        if not key_file.exists():
            raise FileNotFoundError(f"Service account key not found: {key_path}")
        _creds = sa_module.Credentials.from_service_account_file(
            str(key_file),
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        bq_client  = bigquery.Client(project=project_id, credentials=_creds)
        gcs_client = gcs.Client(project=project_id, credentials=_creds)
    else:
        bq_client  = bigquery.Client(project=project_id)
        gcs_client = gcs.Client(project=project_id)

    # Ensure Bronze BQ dataset exists
    bq_client.create_dataset(f"{project_id}.olist_raw", exists_ok=True)

    entities = [
        "orders", "order_items", "order_payments", "order_reviews",
        "customers", "products", "sellers", "geolocation",
    ]

    blobs = list(gcs_client.list_blobs(bucket, prefix=prefix))

    for entity in entities:
        uris = [
            f"gs://{bucket}/{b.name}"
            for b in blobs
            if b.name.endswith(".jsonl") and b.name.split("/")[-1].startswith(entity)
        ]
        if not uris:
            log(f"  ⚠ No JSONL files for '{entity}' in gs://{bucket}/{prefix} — skipping.")
            continue

        table_id   = f"{project_id}.olist_raw.{entity}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        load_job = bq_client.load_table_from_uri(uris, table_id, job_config=job_config)
        load_job.result()
        tbl  = bq_client.get_table(table_id)
        log(f"  ✓ {entity} → {table_id}  ({tbl.num_rows:,} rows)")

    log("✓ GCS Bronze → BQ olist_raw complete.")


# ── Step 3: dbt (Silver + Gold) ──────────────────────────────

def run_dbt(mode: str = "full"):
    log(f"Step 3/3 — dbt: olist_raw → olist_silver → olist_gold (mode={mode})...")
    dbt_dir = str(Path(__file__).resolve().parent / "dbt")
    extra   = ["--full-refresh"] if mode != "cdc" else []
    # Resolve keyfile to absolute path so dbt can find it regardless of cwd
    project_root = Path(__file__).resolve().parent.parent.parent.parent
    key_abs = str(project_root / "dashboards" / "lik_hong" / "config" / "service_account.json")
    env = {**os.environ, "DBT_KEYFILE": key_abs}
    try:
        result = subprocess.run(
            ["dbt", "run", "--profiles-dir", dbt_dir, "--project-dir", dbt_dir] + extra,
            cwd=dbt_dir, capture_output=True, text=True, timeout=600, env=env,
        )
        if result.returncode == 0:
            log("✓ dbt run complete — olist_silver and olist_gold populated.")
        else:
            log(f"✗ dbt run failed (exit {result.returncode}):")
            output = (result.stdout or "") + (result.stderr or "")
            log(_strip(output[-3000:]))
            sys.exit(1)
    except FileNotFoundError:
        log("✗ dbt not installed.")
        log("  Run: pip install dbt-bigquery")
        sys.exit(1)


# ── Entry point ───────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Olist batch pipeline runner")
    parser.add_argument(
        "--step",
        choices=["meltano", "gcs-to-bq", "dbt", "all"],
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

    if args.step in ("meltano", "all"):
        run_meltano()
    if args.step in ("gcs-to-bq", "all"):
        load_gcs_to_bq()
    if args.step in ("dbt", "all"):
        run_dbt(mode=args.mode)

    elapsed = round(time.time() - start, 1)
    log(f"=== Batch Pipeline Complete ({elapsed}s) ===")
