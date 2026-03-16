"""
shared/utils.py
───────────────
GCP client factory — supports both Application Default Credentials (ADC)
and Service Account key file, driven by each developer's gcp_config.yaml.

Usage:
    from shared.utils import get_bq_client, load_config

    client = get_bq_client("dashboards/lik_hong/config/gcp_config.yaml")
    df = client.query("SELECT * FROM `project.dataset.table`").to_dataframe()
"""

import os
import threading
import yaml
from pathlib import Path
from typing import Optional

from google.cloud import bigquery
from google.oauth2 import service_account
import google.auth


# ── Config loader ─────────────────────────────────────────────

def load_config(config_path: str) -> dict:
    """Load a gcp_config.yaml file and return as dict."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(
            f"GCP config not found at: {config_path}\n"
            "Copy config/gcp_config_template.yaml to your dashboard config folder "
            "and fill in your values. See quick-setup.md for instructions."
        )
    with open(path) as f:
        cfg = yaml.safe_load(f)

    required = ["auth_method", "project_id", "dataset"]
    for key in required:
        if key not in cfg:
            raise ValueError(f"Missing required key '{key}' in {config_path}")
    return cfg


# ── BigQuery client factory ───────────────────────────────────

def get_bq_client(config_path: str) -> bigquery.Client:
    """
    Return a BigQuery client configured from a developer's gcp_config.yaml.

    Supported auth_method values:
        "adc"              — Application Default Credentials
        "service_account"  — Service account JSON key file (key_path required)
    """
    cfg = load_config(config_path)
    auth_method = cfg.get("auth_method", "adc").lower().strip()
    project_id  = cfg["project_id"]

    if auth_method == "service_account":
        key_path = cfg.get("key_path")
        if not key_path:
            raise ValueError(
                "'key_path' is required when auth_method = service_account. "
                f"Check {config_path}"
            )
        key_path = Path(key_path)
        if not key_path.exists():
            raise FileNotFoundError(
                f"Service account key not found: {key_path}\n"
                "Download from GCP Console → IAM → Service Accounts → Keys."
            )
        credentials = service_account.Credentials.from_service_account_file(
            str(key_path),
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return bigquery.Client(project=project_id, credentials=credentials)

    elif auth_method == "adc":
        credentials, detected_project = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        return bigquery.Client(project=project_id, credentials=credentials)

    else:
        raise ValueError(
            f"Unknown auth_method '{auth_method}' in {config_path}. "
            "Use 'adc' or 'service_account'."
        )


# ── Query helper ─────────────────────────────────────────────

def run_query(client: bigquery.Client, sql: str, params: Optional[list] = None):
    """Run a BigQuery SQL query and return a pandas DataFrame."""
    job_config = bigquery.QueryJobConfig(
        query_parameters=params or [],
        job_timeout_ms=30_000,
    )
    return client.query(sql, job_config=job_config).to_dataframe(timeout=35, max_results=10_000)


def qualified_table(cfg: dict, table_name: str) -> str:
    """
    Return a fully qualified BigQuery table reference.
    Example: qualified_table(cfg, "Fact_Orders") → "project.dataset.Fact_Orders"
    """
    return f"`{cfg['project_id']}.{cfg['dataset']}.{table_name}`"


# ── Redis / Memorystore client ────────────────────────────────

def get_redis_client(host: str = "localhost", port: int = 6379, db: int = 0):
    """
    Return a Redis client for Google Memorystore.
    In local dev, this connects to a local Redis instance.
    Set REDIS_HOST env var to override host (e.g. Memorystore IP).
    """
    import redis
    host = os.getenv("REDIS_HOST", host)
    port = int(os.getenv("REDIS_PORT", port))
    return redis.Redis(host=host, port=port, db=db, decode_responses=True)


# ── Config path resolver (convenience) ───────────────────────

def dev_config_path(dev_folder: str) -> str:
    """
    Resolve gcp_config.yaml path for a given developer folder name.
    Uses an absolute path derived from this module's location so it works
    regardless of the working directory the script is launched from.

    Example: dev_config_path("lik_hong")
             → "/abs/path/to/m2-olist/dashboards/lik_hong/config/gcp_config.yaml"
    """
    project_root = Path(__file__).resolve().parent.parent
    return str(project_root / "dashboards" / dev_folder / "config" / "gcp_config.yaml")


# ── Lazy BigQuery client factory ──────────────────────────────

def make_bq_client_getter(config_path: str, timeout_secs: int = 8):
    """
    Return a lazily-initialised _get_client() function bound to a specific config path.
    The returned function caches the client after first successful connection.

    Usage in a dashboard:
        from shared.utils import make_bq_client_getter, dev_config_path
        _get_client = make_bq_client_getter(dev_config_path("lik_hong"))

    Returns (client, cfg, None) on success, or (None, None, error_str) on failure.
    Client initialisation is bounded by timeout_secs to avoid hanging on stale ADC credentials.
    """
    _state: dict = {"client": None, "cfg": None, "err": None}
    _lock = threading.Lock()

    def _get_client():
        if _state["client"] is None and _state["err"] is None:
            with _lock:
                if _state["client"] is None and _state["err"] is None:
                    result: list = []

                    def _init():
                        try:
                            cfg    = load_config(config_path)
                            client = get_bq_client(config_path)
                            result.append((client, cfg, None))
                        except Exception as e:
                            result.append((None, None, str(e)))

                    t = threading.Thread(target=_init, daemon=True)
                    t.start()
                    t.join(timeout=timeout_secs)

                    if not result:
                        _state["err"] = (
                            f"GCP connection timed out (>{timeout_secs}s) — "
                            "credentials may be expired. Run: gcloud auth application-default login"
                        )
                    else:
                        _state["client"], _state["cfg"], _state["err"] = result[0]

        if _state["err"]:
            return None, None, _state["err"]
        return _state["client"], _state["cfg"], None

    return _get_client
