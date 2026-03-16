#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────
# launch.sh — One-command launcher for the Olist Data Product
# Usage: ./launch.sh [--port 7860] [--share]
# ──────────────────────────────────────────────────────────────
set -e

PORT=7860
SHARE=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --port) PORT="$2"; shift 2 ;;
    --share) SHARE=true; shift ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

echo ""
echo "  ╔══════════════════════════════════════════════════╗"
echo "  ║   Olist E-Commerce Data Product                  ║"
echo "  ║   Team 3: Lik Hong · Meng Hai · Lanson ·         ║"
echo "  ║           Ben · Huey Ling · Kendra               ║"
echo "  ╚══════════════════════════════════════════════════╝"
echo ""
echo "  Starting on http://localhost:$PORT"
echo ""

export GRADIO_SERVER_PORT=$PORT

if [ "$SHARE" = true ]; then
  export GRADIO_SHARE=true
fi

python app.py
