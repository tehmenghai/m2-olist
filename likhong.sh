#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────
# likhong.sh — Starts Lik Hong's dashboards (Home · Admin · Customer 360)
#
# Ports:
#   7860  Admin Panel     (dashboards/admin/app.py)
#   7861  Home Launchpad  (dashboards/home/app.py)
#   7862  Customer 360    (dashboards/lik_hong/app.py)
# ──────────────────────────────────────────────────────────────
set -e

cd "$(dirname "$0")"
export PYTHONPATH="$(pwd):${PYTHONPATH}"

echo ""
echo "  ╔══════════════════════════════════════════════════╗"
echo "  ║   Lik Hong — Customer 360 + Admin + Home         ║"
echo "  ║   Admin       → http://localhost:7860            ║"
echo "  ║   Home        → http://localhost:7861            ║"
echo "  ║   Customer360 → http://localhost:7862            ║"
echo "  ╚══════════════════════════════════════════════════╝"
echo ""

cleanup() {
    echo ""
    echo "  Stopping all dashboards…"
    kill "$PID_ADMIN" "$PID_HOME" "$PID_C360" 2>/dev/null
    wait "$PID_ADMIN" "$PID_HOME" "$PID_C360" 2>/dev/null
    echo "  Done."
}
trap cleanup INT TERM

python dashboards/admin/app.py   &  PID_ADMIN=$!
python dashboards/home/app.py    &  PID_HOME=$!
python dashboards/lik_hong/app.py &  PID_C360=$!

echo "  PIDs — Admin: $PID_ADMIN | Home: $PID_HOME | Customer360: $PID_C360"
echo "  Press Ctrl+C to stop all."
echo ""

wait
