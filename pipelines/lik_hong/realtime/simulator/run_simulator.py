"""
pipelines/lik_hong/realtime/simulator/run_simulator.py
────────────────────────────────────────────────────────
Fabricated real-time order event generator.
Publishes synthetic events to Google Pub/Sub topic: olist-orders-live

Usage:
    python pipelines/lik_hong/realtime/simulator/run_simulator.py [--rate 5] [--project PROJECT_ID]

This script is also triggered via:
    make pipeline-rt-start
    Admin Panel → Start Real-time Agent
"""

import argparse
import json
import random
import time
import uuid
import signal
import sys
from datetime import datetime, timezone

# ── Constants ─────────────────────────────────────────────────
TOPIC_ID = "olist-orders-live"
EVENT_TYPES = ["new_order", "status_update", "payment_confirmed", "delivered"]
PAYMENT_TYPES = ["credit_card", "boleto", "voucher", "debit_card"]
ORDER_STATUSES = ["created", "approved", "processing", "shipped", "delivered", "canceled"]
STATES = ["SP","RJ","MG","RS","PR","SC","BA","GO","ES","PE","CE","MA","MT","MS","PA","RN","AM","PI","AL","SE","RO","TO","PB","AC","AP","RR","DF"]

# ── Fake data generators ──────────────────────────────────────

def fake_order_event(event_type: str = None) -> dict:
    event_type = event_type or random.choice(EVENT_TYPES)
    return {
        "order_id":       str(uuid.uuid4()),
        "customer_id":    str(uuid.uuid4()),
        "seller_id":      str(uuid.uuid4()),
        "product_id":     str(uuid.uuid4()),
        "event_type":     event_type,
        "order_status":   random.choice(ORDER_STATUSES),
        "payment_type":   random.choices(
            PAYMENT_TYPES, weights=[0.74, 0.19, 0.05, 0.02]
        )[0],
        "payment_value":  round(random.lognormvariate(4.5, 0.8), 2),
        "payment_installments": random.choices(
            range(1, 13), weights=[0.4,0.15,0.1,0.08,0.07,0.05,0.04,0.03,0.03,0.02,0.02,0.01]
        )[0],
        "customer_state": random.choice(STATES),
        "seller_state":   random.choice(STATES),
        "timestamp":      datetime.now(timezone.utc).isoformat(),
        "review_score":   random.choices([1,2,3,4,5], weights=[0.05,0.05,0.1,0.2,0.6])[0]
                          if event_type == "delivered" else None,
    }


# ── Publisher ─────────────────────────────────────────────────

def run(project_id: str, rate: float):
    """Publish events at the given rate (events/second)."""
    try:
        from google.cloud import pubsub_v1
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, TOPIC_ID)
        print(f"[simulator] Publishing to {topic_path} at {rate} event/s — Ctrl+C to stop")
    except ImportError:
        print("[simulator] google-cloud-pubsub not installed. Running in DRY RUN mode.")
        publisher = None
        topic_path = None

    def _on_publish_error(future):
        try:
            future.result()
        except Exception as exc:
            print(f"[simulator] Pub/Sub publish error: {exc}", flush=True)

    count = 0
    try:
        while True:
            event = fake_order_event()
            data  = json.dumps(event).encode("utf-8")

            if publisher:
                # Fire-and-forget: attach error callback; do not block the publish loop
                future = publisher.publish(topic_path, data)
                future.add_done_callback(_on_publish_error)
            else:
                # Dry-run: print to stdout
                print(f"[dry-run] {event['event_type']:20s} order={event['order_id'][:8]}... "
                      f"R${event['payment_value']:8.2f} via {event['payment_type']}")

            count += 1
            if count % 10 == 0:
                print(f"[simulator] {count} events published | {datetime.now().strftime('%H:%M:%S')}")
            time.sleep(1.0 / rate)

    except KeyboardInterrupt:
        print(f"\n[simulator] Stopped. Total events published: {count}")
        sys.exit(0)


# ── Entry point ───────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Olist real-time order event simulator")
    parser.add_argument("--rate",    type=float, default=2.0,   help="Events per second (default: 2)")
    parser.add_argument("--project", type=str,   default=None,  help="GCP project ID (reads gcp_config.yaml if not set)")
    args = parser.parse_args()

    project_id = args.project
    if not project_id:
        try:
            from shared.utils import load_config, dev_config_path
            cfg = load_config(dev_config_path("lik_hong"))
            project_id = cfg["project_id"]
        except Exception as e:
            print(f"[simulator] Could not load GCP config: {e}")
            print("[simulator] Starting in DRY RUN mode (no Pub/Sub).")
            project_id = "dry-run"

    def _handle_sigterm(sig, frame):
        print("\n[simulator] SIGTERM received — shutting down.")
        sys.exit(0)
    signal.signal(signal.SIGTERM, _handle_sigterm)

    run(project_id=project_id, rate=args.rate)
