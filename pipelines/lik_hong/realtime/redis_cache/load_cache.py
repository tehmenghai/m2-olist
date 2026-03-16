#!/usr/bin/env python3
"""
pipelines/lik_hong/realtime/redis_cache/load_cache.py
───────────────────────────────────────────────────────
Pre-loads Gold dimension data (products, sellers) into Redis/Memorystore.
Run this after each batch pipeline run to refresh the cache.

Usage:
    python pipelines/lik_hong/realtime/redis_cache/load_cache.py

Keys:
    product:{product_id}  → JSON of product category + weight
    seller:{seller_id}    → JSON of seller city + state + avg_rating
    meta:last_loaded      → ISO timestamp of last cache load
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# ── Logging setup ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [load_cache] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Resolve project root & shared imports ─────────────────────

_HERE        = Path(__file__).resolve()
_PROJECT_ROOT = _HERE.parents[4]   # .../m2-olist
sys.path.insert(0, str(_PROJECT_ROOT))

try:
    from shared.utils import load_config, dev_config_path, qualified_table
    _CONFIG_PATH = dev_config_path("lik_hong")
except ImportError as exc:
    log.error("Cannot import shared.utils: %s", exc)
    sys.exit(1)

# ── Optional GCP/Redis imports ────────────────────────────────

try:
    from google.cloud import bigquery
    _BQ_AVAILABLE = True
except ImportError:
    bigquery      = None
    _BQ_AVAILABLE = False

try:
    import redis as redis_lib
    _REDIS_AVAILABLE = True
except ImportError:
    redis_lib        = None
    _REDIS_AVAILABLE = False

# ── Cache TTL ─────────────────────────────────────────────────

CACHE_TTL_SECS = 86_400   # 24 hours


# ── BigQuery queries ──────────────────────────────────────────

_PRODUCTS_SQL = """
SELECT
    product_id,
    product_category_name_english,
    product_weight_g
FROM {table}
WHERE product_id IS NOT NULL
"""

_SELLERS_SQL = """
SELECT
    seller_id,
    seller_city,
    seller_state,
    avg_review_score
FROM {table}
WHERE seller_id IS NOT NULL
"""


def _get_bq_client(cfg: dict):
    """Build a BigQuery client from config (mirrors shared.utils logic)."""
    from google.oauth2 import service_account
    import google.auth

    auth_method = cfg.get("auth_method", "adc").lower().strip()
    project_id  = cfg["project_id"]

    if auth_method == "service_account":
        key_path = cfg.get("key_path")
        if not key_path or not Path(key_path).exists():
            raise FileNotFoundError(f"Service account key not found: {key_path}")
        creds = service_account.Credentials.from_service_account_file(
            str(key_path),
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return bigquery.Client(project=project_id, credentials=creds)
    else:
        creds, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        return bigquery.Client(project=project_id, credentials=creds)


# ── Redis connection ──────────────────────────────────────────

def _get_redis():
    """Return a Redis client; raises ConnectionError if unavailable."""
    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", 6379))
    r    = redis_lib.Redis(host=host, port=port, db=0, decode_responses=True)
    r.ping()   # raise immediately if unreachable
    return r


# ── Loaders ───────────────────────────────────────────────────

def load_products(client, cfg: dict, r) -> int:
    """Query Dim_Products and write product:{id} keys to Redis."""
    table_ref = qualified_table(cfg, "Dim_Products")
    sql       = _PRODUCTS_SQL.format(table=table_ref)

    log.info("Querying Dim_Products from BigQuery...")
    try:
        rows = client.query(sql).result()
    except Exception as exc:
        log.error("BigQuery query failed (Dim_Products): %s", exc)
        raise

    count = 0
    pipe  = r.pipeline(transaction=False)

    for row in rows:
        key   = f"product:{row.product_id}"
        value = json.dumps({
            "product_category_name_english": row.product_category_name_english,
            "product_weight_g":              row.product_weight_g,
        })
        pipe.set(key, value, ex=CACHE_TTL_SECS)
        count += 1

        if count % 1000 == 0:
            pipe.execute()
            pipe = r.pipeline(transaction=False)
            log.info("  products: %d keys loaded...", count)

    # Flush remaining pipeline batch
    pipe.execute()
    log.info("Dim_Products done: %d keys written.", count)
    return count


def load_sellers(client, cfg: dict, r) -> int:
    """Query Dim_Sellers and write seller:{id} keys to Redis."""
    table_ref = qualified_table(cfg, "Dim_Sellers")
    sql       = _SELLERS_SQL.format(table=table_ref)

    log.info("Querying Dim_Sellers from BigQuery...")
    try:
        rows = client.query(sql).result()
    except Exception as exc:
        log.error("BigQuery query failed (Dim_Sellers): %s", exc)
        raise

    count = 0
    pipe  = r.pipeline(transaction=False)

    for row in rows:
        key   = f"seller:{row.seller_id}"
        value = json.dumps({
            "seller_city":       row.seller_city,
            "seller_state":      row.seller_state,
            "avg_review_score":  float(row.avg_review_score) if row.avg_review_score is not None else None,
        })
        pipe.set(key, value, ex=CACHE_TTL_SECS)
        count += 1

        if count % 1000 == 0:
            pipe.execute()
            pipe = r.pipeline(transaction=False)
            log.info("  sellers: %d keys loaded...", count)

    pipe.execute()
    log.info("Dim_Sellers done: %d keys written.", count)
    return count


# ── Main ──────────────────────────────────────────────────────

def main():
    # ── Guard: BigQuery ───────────────────────────────────────
    if not _BQ_AVAILABLE:
        log.error("google-cloud-bigquery not installed. Run: pip install google-cloud-bigquery")
        sys.exit(1)

    # ── Load config ───────────────────────────────────────────
    try:
        cfg = load_config(_CONFIG_PATH)
    except Exception as exc:
        log.error("Failed to load GCP config: %s", exc)
        sys.exit(1)

    # ── BigQuery client ───────────────────────────────────────
    try:
        client = _get_bq_client(cfg)
        log.info("BigQuery client ready (project=%s, dataset=%s)", cfg["project_id"], cfg["dataset"])
    except Exception as exc:
        log.error("Cannot create BigQuery client: %s", exc)
        sys.exit(1)

    # ── Redis client ──────────────────────────────────────────
    if not _REDIS_AVAILABLE:
        log.warning("redis-py not installed — cache will not be loaded. Run: pip install redis")
        r = None
    else:
        try:
            r = _get_redis()
            log.info("Redis connected at %s:%s", os.getenv("REDIS_HOST", "localhost"), os.getenv("REDIS_PORT", 6379))
        except Exception as exc:
            log.warning("Redis unavailable: %s — proceeding without cache load.", exc)
            r = None

    if r is None:
        log.warning("Skipping cache load (no Redis connection).")
        sys.exit(0)

    # ── Load products ─────────────────────────────────────────
    try:
        n_products = load_products(client, cfg, r)
    except Exception as exc:
        log.error("Product cache load failed: %s", exc)
        sys.exit(1)

    # ── Load sellers ──────────────────────────────────────────
    try:
        n_sellers = load_sellers(client, cfg, r)
    except Exception as exc:
        log.error("Seller cache load failed: %s", exc)
        sys.exit(1)

    # ── Metadata key ─────────────────────────────────────────
    last_loaded = datetime.now(timezone.utc).isoformat()
    r.set("meta:last_loaded", last_loaded, ex=CACHE_TTL_SECS)

    log.info(
        "Cache load complete — products: %d, sellers: %d | meta:last_loaded = %s",
        n_products, n_sellers, last_loaded,
    )


if __name__ == "__main__":
    main()
