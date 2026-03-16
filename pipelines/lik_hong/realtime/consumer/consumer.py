#!/usr/bin/env python3
"""
pipelines/lik_hong/realtime/consumer/consumer.py
─────────────────────────────────────────────────
Subscribes to Pub/Sub olist-orders-sub and writes batches of
events to GCS Streaming Bucket as JSONL files.

File naming: gs://<bucket>/olist/streaming/YYYY-MM-DD/HH/<uuid>.jsonl
Each file written when batch size OR flush interval is reached.

Usage:
    python pipelines/lik_hong/realtime/consumer/consumer.py [--project PROJECT] [--batch-size 50] [--flush-secs 30]
"""

import argparse
import json
import signal
import sys
import threading
import uuid
from datetime import datetime, timezone
from typing import Optional

# ── Optional GCP imports (graceful degradation) ───────────────

try:
    from google.cloud import pubsub_v1
    _PUBSUB_AVAILABLE = True
except ImportError:
    pubsub_v1 = None
    _PUBSUB_AVAILABLE = False

try:
    from google.cloud import storage as gcs
    _GCS_AVAILABLE = True
except ImportError:
    gcs = None
    _GCS_AVAILABLE = False

# ── Constants ─────────────────────────────────────────────────

SUBSCRIPTION_ID  = "olist-orders-sub"
GCS_BUCKET       = "dsai-m2-gcp-streaming"
GCS_PREFIX       = "olist/streaming"
GCS_CAP_MB       = 50          # stop consumer when streaming bucket exceeds this
DRY_RUN          = not (_PUBSUB_AVAILABLE and _GCS_AVAILABLE)


# ── Logging helper ────────────────────────────────────────────

def log(msg: str):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] [consumer] {msg}", flush=True)


# ── GCS flush ─────────────────────────────────────────────────

def flush_to_gcs(
    messages: list[dict],
    bucket_name: str,
    gcs_client,
    dry_run: bool = False,
) -> Optional[str]:
    """
    Write a batch of message dicts to GCS as a JSONL file.
    Returns the GCS object path on success, None on failure.
    Caller must NOT clear the buffer until this returns a non-None path.
    """
    if not messages:
        return None

    now      = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    hour_str = now.strftime("%H")
    blob_name = f"{GCS_PREFIX}/{date_str}/{hour_str}/{uuid.uuid4()}.jsonl"
    payload   = "\n".join(json.dumps(m) for m in messages) + "\n"

    if dry_run:
        log(f"[dry-run] Would write {len(messages)} events → gs://{GCS_BUCKET}/{blob_name}")
        return f"gs://{GCS_BUCKET}/{blob_name}"

    try:
        bucket = gcs_client.bucket(bucket_name)
        blob   = bucket.blob(blob_name)
        blob.upload_from_string(payload, content_type="application/jsonl")
        log(f"Flushed {len(messages)} events → gs://{bucket_name}/{blob_name}")
        return f"gs://{bucket_name}/{blob_name}"
    except Exception as exc:
        log(f"ERROR writing to GCS: {exc}")
        return None


# ── Consumer state ────────────────────────────────────────────

class _State:
    def __init__(self):
        self.buffer: list[dict]  = []
        self.ack_ids: list[str]  = []
        self.lock                = threading.Lock()
        self.last_flush          = datetime.now(timezone.utc).timestamp()
        self.total_written       = 0
        self.shutdown            = False
        self.subscriber          = None   # pubsub StreamingPullFuture
        self.subscription_client = None
        self.gcs_client          = None   # created once at startup


_state = _State()


def _do_flush(project_id: str, dry_run: bool):
    """Flush buffer → GCS and acknowledge Pub/Sub messages. Must hold lock.

    Buffer and ack_ids are only cleared AFTER a confirmed GCS write.
    On GCS failure the buffer is retained for retry on the next flush cycle.
    """
    messages = list(_state.buffer)
    ack_ids  = list(_state.ack_ids)
    _state.last_flush = datetime.now(timezone.utc).timestamp()

    if not messages:
        return

    result = flush_to_gcs(messages, GCS_BUCKET, _state.gcs_client, dry_run=dry_run)

    if result is None and not dry_run:
        # GCS write failed — retain buffer so next flush can retry
        log(f"GCS write failed — retaining {len(messages)} events in buffer for retry")
        if len(_state.buffer) > 5_000:
            log("WARNING: buffer limit exceeded (>5000 events) — dropping oldest events to prevent OOM")
            _state.buffer = _state.buffer[-5_000:]
            _state.ack_ids = _state.ack_ids[-5_000:]
        return

    # GCS write confirmed — safe to clear buffer and acknowledge Pub/Sub
    _state.buffer.clear()
    _state.ack_ids.clear()
    _state.total_written += len(messages)

    if not dry_run and ack_ids and _state.subscription_client:
        subscription_path = _state.subscription_client.subscription_path(
            project_id, SUBSCRIPTION_ID
        )
        try:
            _state.subscription_client.acknowledge(
                request={"subscription": subscription_path, "ack_ids": ack_ids}
            )
        except Exception as exc:
            log(f"WARNING: Pub/Sub acknowledge failed: {exc}")


# ── Pub/Sub callback ──────────────────────────────────────────

def _make_callback(project_id: str, batch_size: int, flush_secs: float, dry_run: bool):
    def callback(message):
        try:
            data = json.loads(message.data.decode("utf-8"))
        except Exception:
            data = {"_raw": message.data.decode("utf-8", errors="replace")}

        with _state.lock:
            _state.buffer.append(data)
            _state.ack_ids.append(message.ack_id)

            elapsed = datetime.now(timezone.utc).timestamp() - _state.last_flush
            if len(_state.buffer) >= batch_size or elapsed >= flush_secs:
                _do_flush(project_id, dry_run)

    return callback


# ── Timer-based flush (catches time-based trigger) ────────────

def _bucket_size_mb(gcs_client) -> float:
    """Return total size of GCS_BUCKET/GCS_PREFIX in MB."""
    try:
        blobs = gcs_client.list_blobs(GCS_BUCKET, prefix=GCS_PREFIX)
        return sum(b.size for b in blobs) / (1024 * 1024)
    except Exception:
        return 0.0


def _flush_timer(project_id: str, flush_secs: float, dry_run: bool):
    """Background thread that flushes on interval and enforces the storage cap."""
    import time
    check_interval = 0
    while not _state.shutdown:
        time.sleep(min(flush_secs, 5.0))   # wake frequently to check shutdown
        with _state.lock:
            elapsed = datetime.now(timezone.utc).timestamp() - _state.last_flush
            if elapsed >= flush_secs and _state.buffer:
                log(f"Timer flush: {len(_state.buffer)} buffered events")
                _do_flush(project_id, dry_run)

        # Check cap every ~60s (not every flush to avoid excessive GCS list calls)
        check_interval += 1
        if not dry_run and check_interval >= max(1, int(60 / max(flush_secs, 1))):
            check_interval = 0
            size_mb = _bucket_size_mb(_state.gcs_client)
            if size_mb >= GCS_CAP_MB:
                log(f"STORAGE CAP REACHED: {size_mb:.1f} MB >= {GCS_CAP_MB} MB — shutting down consumer.")
                _state.shutdown = True
                if _state.subscriber:
                    _state.subscriber.cancel()
                break


# ── Signal handlers ───────────────────────────────────────────

def _handle_signal(signum, frame):
    sig_name = "SIGTERM" if signum == signal.SIGTERM else "SIGINT"
    log(f"{sig_name} received — flushing remaining buffer and shutting down...")
    _state.shutdown = True

    # Final flush
    with _state.lock:
        if _state.buffer:
            log(f"Final flush: {len(_state.buffer)} events")
            _do_flush(_state._project_id, _state._dry_run)

    if _state.subscriber:
        _state.subscriber.cancel()

    log(f"Shutdown complete. Total events written: {_state.total_written}")
    sys.exit(0)


# ── Dry-run stdin reader ──────────────────────────────────────

def run_dry_run(project_id: str, batch_size: int, flush_secs: float):
    """
    Dry-run mode: reads JSON lines from stdin and simulates batch flushing.
    Useful for local testing without GCP credentials.
    """
    log("=== DRY RUN MODE (no Pub/Sub / GCS available) ===")
    log("  Paste JSON lines (one per line), or pipe a file. Ctrl+C to stop.")
    log(f"  Batch size: {batch_size}  |  Flush interval: {flush_secs}s")

    import time
    last_flush = time.time()

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            data = {"_raw": line}

        with _state.lock:
            _state.buffer.append(data)
            elapsed = time.time() - last_flush
            if len(_state.buffer) >= batch_size or elapsed >= flush_secs:
                _do_flush(project_id, dry_run=True)
                last_flush = time.time()

    # Flush remaining
    with _state.lock:
        if _state.buffer:
            _do_flush(project_id, dry_run=True)

    log(f"stdin EOF. Total events processed: {_state.total_written}")


# ── Main ──────────────────────────────────────────────────────

def run(project_id: str, batch_size: int, flush_secs: float):
    """Subscribe to Pub/Sub and stream events into GCS."""
    if DRY_RUN:
        run_dry_run(project_id, batch_size, flush_secs)
        return

    log(f"Starting consumer | project={project_id} | batch_size={batch_size} | flush_secs={flush_secs}s")
    log(f"Subscription: {SUBSCRIPTION_ID}  →  GCS bucket: {GCS_BUCKET}")

    # Create GCS client once at startup (not per-flush)
    _state.gcs_client = gcs.Client(project=project_id)

    subscriber        = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, SUBSCRIPTION_ID)

    _state.subscription_client = subscriber

    # Store for signal handlers
    _state._project_id = project_id
    _state._dry_run    = False

    callback = _make_callback(project_id, batch_size, flush_secs, dry_run=False)
    future   = subscriber.subscribe(subscription_path, callback=callback)
    _state.subscriber = future

    # Start timer-based flush thread
    timer_thread = threading.Thread(
        target=_flush_timer,
        args=(project_id, flush_secs, False),
        daemon=True,
    )
    timer_thread.start()

    log(f"Listening on {subscription_path} ... (Ctrl+C to stop)")

    try:
        future.result()   # blocks until cancelled or error
    except Exception as exc:
        if not _state.shutdown:
            log(f"Streaming pull error: {exc}")
    finally:
        # Final flush on any exit
        with _state.lock:
            if _state.buffer:
                log(f"Final flush on exit: {len(_state.buffer)} events")
                _do_flush(project_id, dry_run=False)
        log(f"Consumer stopped. Total events written: {_state.total_written}")


# ── Entry point ───────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Olist Pub/Sub → GCS JSONL consumer")
    parser.add_argument("--project",     type=str, default=None, help="GCP project ID")
    parser.add_argument("--batch-size",  type=int, default=50,   help="Flush batch size (default: 50)")
    parser.add_argument("--flush-secs",  type=float, default=30.0, help="Flush interval in seconds (default: 30)")
    args = parser.parse_args()

    # Resolve project ID
    project_id = args.project
    if not project_id:
        try:
            import sys as _sys
            import os as _os
            _sys.path.insert(0, _os.path.join(_os.path.dirname(__file__), "..", "..", "..", "..", ".."))
            from shared.utils import load_config, dev_config_path
            cfg        = load_config(dev_config_path("lik_hong"))
            project_id = cfg["project_id"]
        except Exception as exc:
            log(f"Could not load GCP config: {exc}")
            log("Falling back to hardcoded project ID.")
            project_id = "project-12fdd3b7-c899-4bef-931"

    # Register signal handlers
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT,  _handle_signal)

    # Store project/dry_run on state for signal handler access
    _state._project_id = project_id
    _state._dry_run    = DRY_RUN

    if not _PUBSUB_AVAILABLE:
        log("google-cloud-pubsub not installed — entering DRY RUN mode.")
    if not _GCS_AVAILABLE:
        log("google-cloud-storage not installed — entering DRY RUN mode.")

    run(project_id=project_id, batch_size=args.batch_size, flush_secs=args.flush_secs)
