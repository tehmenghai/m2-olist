"""
pipelines/lik_hong/dagster/definitions.py
──────────────────────────────────────────
Dagster orchestration for Olist Lik Hong pipeline.

Jobs:
  - batch_pipeline_job    : Meltano EL → dbt full-refresh → Redis cache refresh
  - rt_cache_refresh_job  : Refresh Redis cache from Gold tables

Schedules:
  - daily_batch_schedule  : batch_pipeline_job at 02:00 UTC daily

Sensors:
  - streaming_file_sensor : polls GCS for new streaming JSONL; triggers incremental dbt
"""

import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from dagster import (
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    RunRequest,
    SkipReason,
    get_dagster_logger,
    job,
    op,
    schedule,
    sensor,
)

# ── Path constants ────────────────────────────────────────────

_HERE         = Path(__file__).resolve().parent          # .../pipelines/lik_hong/dagster
_PIPELINE_DIR = _HERE.parent                             # .../pipelines/lik_hong
_PROJECT_ROOT = _PIPELINE_DIR.parents[1]                  # .../m2-olist

_MELTANO_DIR  = str(_PIPELINE_DIR / "batch" / "meltano")
_DBT_DIR      = str(_PIPELINE_DIR / "batch" / "dbt")
_RUN_BATCH    = str(_PIPELINE_DIR / "batch" / "run_batch.py")
_LOAD_CACHE   = str(_PIPELINE_DIR / "realtime" / "redis_cache" / "load_cache.py")

GCS_BUCKET    = "project-12fdd3b7-c899-4bef-931-streaming"
GCS_PREFIX    = "olist/streaming/"


# ── Helper: run subprocess with logging ──────────────────────

def _run(cmd: list[str], cwd: str | None = None, logger=None) -> int:
    """Run a subprocess, streaming output to Dagster logger. Returns returncode."""
    log = logger or get_dagster_logger()
    log.info("Running: %s  (cwd=%s)", " ".join(cmd), cwd or os.getcwd())
    result = subprocess.run(
        cmd,
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=900,
    )
    if result.stdout:
        log.info(result.stdout[-4000:])
    if result.stderr:
        (log.error if result.returncode != 0 else log.info)(result.stderr[-4000:])
    return result.returncode


# ── Ops ───────────────────────────────────────────────────────

@op
def meltano_el_op(context):
    """Step 1: Extract CSVs and load to GCS Bronze via Meltano."""
    log = context.log
    log.info("Starting Meltano EL (tap-csv → target-gcs)...")
    rc = _run(["meltano", "run", "tap-csv", "target-gcs"], cwd=_MELTANO_DIR, logger=log)
    if rc != 0:
        raise Exception(f"meltano run failed with exit code {rc}")
    log.info("Meltano EL complete.")


@op
def gcs_to_bq_op(context):
    """Step 2: Load JSONL files from GCS Bronze bucket → BQ olist_raw (Bronze tables)."""
    log = context.log
    log.info("Loading GCS Bronze → BQ olist_raw...")
    rc = _run(
        ["python", _RUN_BATCH, "--step", "gcs-to-bq"],
        cwd=str(_PROJECT_ROOT),
        logger=log,
    )
    if rc != 0:
        raise Exception(f"GCS to BQ load failed with exit code {rc}")
    log.info("GCS to BQ load complete.")


@op
def dbt_full_refresh_op(context):
    """Step 3: Full-refresh dbt (olist_raw → olist_silver → olist_gold)."""
    log = context.log
    log.info("Starting dbt full refresh (Silver + Gold)...")
    rc = _run(
        ["python", _RUN_BATCH, "--step", "dbt", "--mode", "full"],
        cwd=str(_PROJECT_ROOT),
        logger=log,
    )
    if rc != 0:
        raise Exception(f"dbt full refresh failed with exit code {rc}")
    log.info("dbt full refresh complete.")


@op
def dbt_incremental_op(context):
    """Incremental dbt run — mart layer only. Used after streaming file arrival."""
    log = context.log
    log.info("Starting dbt incremental run (marts)...")
    rc = _run(
        ["dbt", "run", "--select", "marts", "--profiles-dir", "."],
        cwd=_DBT_DIR,
        logger=log,
    )
    if rc != 0:
        raise Exception(f"dbt incremental run failed with exit code {rc}")
    log.info("dbt incremental run complete.")


@op
def redis_cache_op(context):
    """Refresh Redis/Memorystore dimension cache from BigQuery Gold tables."""
    log = context.log
    log.info("Starting Redis cache refresh...")
    rc = _run(
        ["python", _LOAD_CACHE],
        cwd=str(_PROJECT_ROOT),
        logger=log,
    )
    if rc != 0:
        raise Exception(f"Redis cache refresh failed with exit code {rc}")
    log.info("Redis cache refresh complete.")


# ── Jobs ──────────────────────────────────────────────────────

@job(description="Full batch pipeline: Meltano EL → GCS→BQ Bronze → dbt Silver+Gold → Redis cache")
def batch_pipeline_job():
    redis_cache_op(dbt_full_refresh_op(gcs_to_bq_op(meltano_el_op())))


@job(description="Refresh Redis dimension cache from BigQuery Gold tables")
def rt_cache_refresh_job():
    redis_cache_op()


@job(description="Incremental dbt run triggered by new streaming JSONL files in GCS")
def streaming_incremental_job():
    dbt_incremental_op()


# ── Schedule ──────────────────────────────────────────────────

@schedule(
    cron_schedule="0 2 * * *",
    job=batch_pipeline_job,
    default_status=DefaultScheduleStatus.RUNNING,
    description="Run full batch pipeline daily at 02:00 UTC",
    execution_timezone="UTC",
)
def daily_batch_schedule(context):
    """Trigger the full batch pipeline every day at 02:00 UTC."""
    return {}


# ── Streaming file sensor ─────────────────────────────────────

@sensor(
    job=streaming_incremental_job,
    default_status=DefaultSensorStatus.RUNNING,
    description=(
        "Polls GCS streaming bucket for new JSONL files. "
        "Triggers an incremental dbt run when new files are found."
    ),
    minimum_interval_seconds=60,
)
def streaming_file_sensor(context):
    """
    Checks gs://project-12fdd3b7-c899-4bef-931-streaming/olist/streaming/ for
    new JSONL blobs written since the last cursor timestamp.

    Cursor format: ISO-8601 UTC timestamp stored as sensor cursor string.
    On first run, defaults to epoch (processes all existing files).
    """
    log = context.log

    # ── Parse cursor ──────────────────────────────────────────
    raw_cursor = context.cursor
    try:
        last_ts = datetime.fromisoformat(raw_cursor) if raw_cursor else datetime.fromtimestamp(0, tz=timezone.utc)
    except ValueError:
        last_ts = datetime.fromtimestamp(0, tz=timezone.utc)

    # ── GCS scan ──────────────────────────────────────────────
    try:
        from google.cloud import storage as gcs
    except ImportError:
        yield SkipReason("google-cloud-storage not installed; cannot poll GCS.")
        return

    try:
        gcs_client = gcs.Client()
        # Restrict the GCS list to the cursor date and today to avoid unbounded enumeration.
        # Blobs are stored as olist/streaming/YYYY-MM-DD/HH/<uuid>.jsonl, so date prefixes
        # bound the scan to at most 2 date-directories regardless of bucket age.
        cursor_date = last_ts.strftime("%Y-%m-%d")
        today_date  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        date_prefixes = {f"{GCS_PREFIX}{cursor_date}/", f"{GCS_PREFIX}{today_date}/"}
        blobs: list = []
        for prefix in date_prefixes:
            blobs.extend(gcs_client.list_blobs(GCS_BUCKET, prefix=prefix))
    except Exception as exc:
        log.warning("GCS credential or connection error: %s", exc)
        yield SkipReason(f"GCS unavailable: {exc}")
        return

    # ── Find new blobs ────────────────────────────────────────
    new_blobs = [
        b for b in blobs
        if b.name.endswith(".jsonl")
        and b.updated is not None
        and b.updated.replace(tzinfo=timezone.utc) > last_ts
    ]

    if not new_blobs:
        yield SkipReason(
            f"No new streaming files since {last_ts.isoformat()} "
            f"(scanned date-prefixed objects in gs://{GCS_BUCKET}/{GCS_PREFIX})"
        )
        return

    # ── Advance cursor ────────────────────────────────────────
    latest_ts = max(b.updated.replace(tzinfo=timezone.utc) for b in new_blobs)
    context.update_cursor(latest_ts.isoformat())

    log.info(
        "Found %d new streaming file(s) since %s — triggering incremental dbt run.",
        len(new_blobs),
        last_ts.isoformat(),
    )

    yield RunRequest(
        run_key=f"streaming-{latest_ts.isoformat()}",
        run_config={},
        tags={
            "trigger":    "streaming_file_sensor",
            "new_files":  str(len(new_blobs)),
            "latest_file": new_blobs[-1].name,
        },
    )


# ── Definitions ───────────────────────────────────────────────

defs = Definitions(
    jobs=[
        batch_pipeline_job,
        rt_cache_refresh_job,
        streaming_incremental_job,
    ],
    schedules=[
        daily_batch_schedule,
    ],
    sensors=[
        streaming_file_sensor,
    ],
)
