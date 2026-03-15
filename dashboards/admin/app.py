"""
dashboards/admin/app.py — Admin Control Panel
──────────────────────────────────────────────
Owner: Lik Hong

Sections:
  1. Batch Ingestion Pipeline  — 6-stage gauge progress tracker
  2. Cache Management          — Redis flush
  3. Real-time Simulator       — Start / Stop agent
  4. Pipeline Status           — Quick health check

Exports: dashboard (gr.Blocks)
"""

import atexit
import os
import sys
import gradio as gr
import plotly.graph_objects as go
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from shared.theme import olist_theme, CUSTOM_CSS, COLORS, PLOTLY_LAYOUT, FONT_HEAD
from shared.components import page_header, section_title, alert_box

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# ── Simulator state ────────────────────────────────────────────
_simulator_proc: subprocess.Popen | None = None
_simulator_lock = threading.Lock()

# ── Live chart series (cumulative event count per tick) ────────
_sim_series: list[tuple] = []   # [(datetime, cumulative_count), ...]
_reader_thread: threading.Thread | None = None


def _read_simulator_output():
    """Background thread: parse simulator stdout into _sim_series."""
    import re as _re
    _pat = _re.compile(r'\[simulator\]\s+(\d+)\s+events published')
    while True:
        proc = _simulator_proc
        if proc is None or proc.poll() is not None:
            break
        try:
            line = proc.stdout.readline()
        except Exception:
            break
        if not line:
            break
        m = _pat.search(line.strip())
        if m:
            _sim_series.append((datetime.now(timezone.utc), int(m.group(1))))


def _build_live_chart() -> go.Figure:
    """Build live order-count chart from accumulated series."""
    is_running = _simulator_proc is not None and _simulator_proc.poll() is None
    status_dot = '<span style="color:#FF4444">◎ Stopped</span>' if not is_running else \
                 '<span style="color:#00C851">● LIVE</span>'
    fig = go.Figure()
    if _sim_series:
        xs = [t.strftime("%H:%M:%S") for t, _ in _sim_series]
        ys = [c for _, c in _sim_series]
        fig.add_trace(go.Scatter(
            x=xs, y=ys,
            mode="lines+markers",
            line=dict(color=COLORS["orange"], width=2.5),
            fill="tozeroy",
            fillcolor="rgba(255,140,0,0.10)",
            marker=dict(size=5, color=COLORS["gold"]),
            hovertemplate="%{x}: %{y:,} events<extra></extra>",
        ))
    fig.update_layout(**{
        **PLOTLY_LAYOUT,
        "title": dict(
            text=f"Live Orders Published · {status_dot}",
            font=dict(size=12, color=COLORS["gold"]), x=0.0, xanchor="left",
        ),
        "xaxis": dict(title="Time (UTC)", tickangle=-30,
                      gridcolor="rgba(255,140,0,0.08)", tickfont=dict(size=9)),
        "yaxis": dict(title="Cumulative Events",
                      gridcolor="rgba(255,140,0,0.08)"),
        "height": 210,
        "margin": dict(l=50, r=16, t=44, b=56),
        "showlegend": False,
    })
    return fig


def _cleanup_simulator():
    global _simulator_proc
    if _simulator_proc and _simulator_proc.poll() is None:
        _simulator_proc.terminate()
        try:
            _simulator_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            _simulator_proc.kill()

atexit.register(_cleanup_simulator)


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("[%H:%M:%S]")


# ══════════════════════════════════════════════════════════════
# Section 1 — Batch Ingestion Pipeline gauges
# ══════════════════════════════════════════════════════════════

_STAGES = [
    ("Extract→Bronze",  "Meltano/GCS tap",
     str(_PROJECT_ROOT / "pipelines/lik_hong/batch/meltano"),
     ["meltano", "run", "tap-csv", "target-gcs"]),
    ("Load→BigQuery",   "Bronze→olist_raw",
     str(_PROJECT_ROOT),
     [sys.executable, "pipelines/lik_hong/batch/run_batch.py", "--step", "gcs-to-bq"]),
    ("dbt Silver",      "PII mask · CDC",
     str(_PROJECT_ROOT / "pipelines/lik_hong/batch/dbt"),
     ["dbt", "run", "--select", "staging", "--profiles-dir", ".", "--full-refresh"]),
    ("dbt Gold",        "Star Schema",
     str(_PROJECT_ROOT / "pipelines/lik_hong/batch/dbt"),
     ["dbt", "run", "--select", "marts",   "--profiles-dir", ".", "--full-refresh"]),
    ("Feature Store",   "Redis cache",
     str(_PROJECT_ROOT),
     [sys.executable, "pipelines/lik_hong/realtime/redis_cache/load_cache.py"]),
    ("Dagster Sensor",  "Cursor advance",     None, None),
]


def _gauge(pct: float, title: str, state: str = "idle") -> go.Figure:
    if state == "done":
        bar_col = "#00C851";  num_col = "#00C851"
    elif state == "active":
        if pct >= 67:
            bar_col = "#00C851"; num_col = "#00C851"
        elif pct >= 34:
            bar_col = "#FFD700"; num_col = "#FFD700"
        else:
            bar_col = "#FF4444"; num_col = "#FF4444"
    elif state == "error":
        bar_col = "#FF4444";  num_col = "#FF4444"
    else:
        bar_col = "rgba(80,50,0,0.35)"; num_col = "rgba(180,140,60,0.35)"

    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=pct,
        number={"suffix": " %", "font": {"size": 18, "color": num_col, "family": "Space Mono"}},
        title={"text": title, "font": {"size": 11, "color": "rgba(200,170,80,0.85)", "family": "Space Mono"}},
        gauge={
            "axis": {
                "range": [0, 100], "tickwidth": 1,
                "tickcolor": "rgba(255,255,255,0.08)",
                "tickfont": {"size": 7, "color": "rgba(255,200,100,0.2)"},
            },
            "bar": {"color": bar_col, "thickness": 0.22},
            "bgcolor": "rgba(0,0,0,0)",
            "borderwidth": 0,
            "steps": [{"range": [0, 100], "color": "rgba(40,20,0,0.65)"}],
        },
    ))
    fig.update_layout(**{
        **PLOTLY_LAYOUT,
        "paper_bgcolor": "rgba(6,3,0,0.97)",
        "margin": dict(l=8, r=8, t=48, b=4),
        "height": 168,
        "font": dict(family="Space Mono"),
    })
    return fig


def _pipe_status_html(state: str, stage_label: str = "") -> str:
    if state == "running":
        bg, col, label = "rgba(255,140,0,0.1)", "#FF8C00", f"▶  Running…  {stage_label}"
    elif state == "done":
        bg, col, label = "rgba(0,200,81,0.1)", "#00C851", "✓  Pipeline Complete"
    elif state == "error":
        bg, col, label = "rgba(255,68,68,0.1)", "#FF4444", "✗  Stage Failed — see log"
    else:
        bg, col, label = "rgba(60,40,0,0.1)", "rgba(180,140,60,0.4)", "◈  Idle — click Run to start"
    return (
        f'<div style="padding:9px 18px;background:{bg};border:1px solid {col};border-radius:4px;'
        f'font-family:"Space Mono",monospace;font-size:11px;color:{col};letter-spacing:2px;">'
        f'{label}</div>'
    )


def _idle_gauges():
    return tuple(_gauge(0, f"Stage {i+1}<br>{_STAGES[i][0]}", "idle") for i in range(6))


def _run_pipeline_gen(mode: str):
    """Generator: runs real pipeline stages, yields (g1…g6, status_html, log)."""
    progress = [0.0] * 6
    states   = ["idle"] * 6
    log_lines: list[str] = []

    def emit(state_key: str = "running", stage_lbl: str = ""):
        figs = [_gauge(progress[i], f"Stage {i+1}<br>{_STAGES[i][0]}", states[i]) for i in range(6)]
        return (*figs, _pipe_status_html(state_key, stage_lbl), "\n".join(log_lines[-40:]))

    log_lines.append(f"{_ts()} === Batch Pipeline Start (mode={mode}) ===")
    yield emit("running")

    fr_flag = [] if mode == "cdc" else ["--full-refresh"]

    _STAGE_TIMEOUTS = [600, 300, 300, 300, 300, 300]

    for idx, (label, desc, cwd, cmd) in enumerate(_STAGES):
        states[idx] = "active"
        log_lines.append(f"{_ts()} ▶ Stage {idx+1}: {label}  ({desc})")
        yield emit("running", f"Stage {idx+1} · {label}")

        if cmd is None:
            # Informational/no-op stage
            for p in range(0, 101, 8):
                progress[idx] = float(p)
                yield emit("running", f"Stage {idx+1} · {label}")
                time.sleep(0.06)
            progress[idx] = 100.0
            states[idx] = "done"
            log_lines.append(f"{_ts()} ✓ Stage {idx+1} complete")
            yield emit("running", f"Stage {idx+1} · {label}")
            continue

        # Patch dbt commands with --full-refresh / CDC flag
        effective_cmd = list(cmd)
        if "dbt" in effective_cmd and "--full-refresh" in effective_cmd and mode == "cdc":
            effective_cmd.remove("--full-refresh")

        result: list = []
        _timeout = _STAGE_TIMEOUTS[idx]

        def _run(c=effective_cmd, d=cwd, t=_timeout):
            try:
                r = subprocess.run(c, cwd=d, capture_output=True, text=True, timeout=t)
                result.append(r)
            except subprocess.TimeoutExpired as e:
                class _FakeResult:
                    returncode = 1
                    stdout = ""
                    stderr = f"Stage timed out after {t}s — command: {' '.join(c)}"
                result.append(_FakeResult())

        t = threading.Thread(target=_run, daemon=True)
        t.start()

        p = 0.0
        while t.is_alive():
            p = min(90.0, p + 1.2)
            progress[idx] = p
            yield emit("running", f"Stage {idx+1} · {label}")
            time.sleep(0.35)

        t.join()

        if result and result[0].returncode == 0:
            progress[idx] = 100.0
            states[idx] = "done"
            for line in (result[0].stdout or "").splitlines()[-6:]:
                log_lines.append(f"  {line}")
            log_lines.append(f"{_ts()} ✓ Stage {idx+1} complete ({int((idx+1)/6*100)} %)")
        else:
            states[idx] = "error"
            if result:
                for line in (result[0].stderr or "").splitlines()[-8:]:
                    log_lines.append(f"  ERR: {line}")
            log_lines.append(f"{_ts()} ✗ Stage {idx+1} failed — stopping.")
            yield emit("error")
            return

        yield emit("running", f"Stage {idx+1} · done")

    log_lines.append(f"{_ts()} === Pipeline Complete ===")
    yield emit("done")


# ══════════════════════════════════════════════════════════════
# Section 2 — Cache / CDC / Simulator
# ══════════════════════════════════════════════════════════════

GCS_STREAMING_BUCKET = "dsai-m2-gcp-streaming"
GCS_STREAMING_PREFIX = "olist/streaming"
GCS_STREAMING_CAP_MB = 50   # auto-stop simulator above this threshold


def _gcs_streaming_size_mb() -> float:
    """Return total size of streaming bucket prefix in MB. Returns -1 on error."""
    try:
        from google.cloud import storage as gcs
        from google.oauth2 import service_account
        from pathlib import Path as _Path
        key_path = _Path(_PROJECT_ROOT / "dashboards/lik_hong/config/service_account.json")
        creds = service_account.Credentials.from_service_account_file(
            str(key_path), scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        client = gcs.Client(credentials=creds)
        blobs  = client.list_blobs(GCS_STREAMING_BUCKET, prefix=GCS_STREAMING_PREFIX)
        total  = sum(b.size for b in blobs)
        return round(total / (1024 * 1024), 2)
    except Exception:
        return -1.0


def _flush_gcs_streaming() -> str:
    """Delete all objects under the streaming prefix. Returns status line."""
    try:
        from google.cloud import storage as gcs
        from google.oauth2 import service_account
        from pathlib import Path as _Path
        key_path = _Path(_PROJECT_ROOT / "dashboards/lik_hong/config/service_account.json")
        creds = service_account.Credentials.from_service_account_file(
            str(key_path), scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        client = gcs.Client(credentials=creds)
        blobs  = list(client.list_blobs(GCS_STREAMING_BUCKET, prefix=GCS_STREAMING_PREFIX))
        if not blobs:
            return f"{_ts()} ℹ GCS streaming bucket already empty."
        client.bucket(GCS_STREAMING_BUCKET).delete_blobs(blobs)
        return f"{_ts()} ✓ GCS streaming bucket flushed — {len(blobs)} files deleted."
    except Exception as e:
        return f"{_ts()} ⚠ GCS streaming flush failed: {e}"


def action_clear_cache() -> str:
    lines = [f"{_ts()} Starting cache clear..."]
    try:
        from shared.utils import get_redis_client
        r = get_redis_client()
        n = r.dbsize(); r.flushdb()
        lines.append(f"{_ts()} ✓ Redis flushed — {n} keys cleared.")
    except Exception as e:
        lines.append(f"{_ts()} ⚠ Redis unavailable: {e}  (skipping)")
    lines.append(_flush_gcs_streaming())
    lines.append(f"{_ts()} ✓ Cache clear complete.")
    return "\n".join(lines)


def action_check_streaming_cap() -> str:
    """Check streaming bucket size and stop simulator if over cap."""
    global _simulator_proc
    size_mb = _gcs_streaming_size_mb()
    if size_mb < 0:
        return f"{_ts()} ⚠ Could not read streaming bucket size."
    lines = [f"{_ts()} Streaming bucket: {size_mb} MB / {GCS_STREAMING_CAP_MB} MB cap"]
    if size_mb >= GCS_STREAMING_CAP_MB:
        lines.append(f"{_ts()} ⚠ Cap reached — stopping simulator...")
        lines.append(action_stop_simulator())
    return "\n".join(lines)


def action_start_simulator() -> str:
    global _simulator_proc, _sim_series, _reader_thread
    with _simulator_lock:
        if _simulator_proc and _simulator_proc.poll() is None:
            return f"{_ts()} ⚠ Simulator already running (PID {_simulator_proc.pid})."
        try:
            _sim_series = []          # reset chart series
            _simulator_proc = subprocess.Popen(
                [sys.executable, str(_PROJECT_ROOT / "pipelines/lik_hong/realtime/simulator/run_simulator.py")],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            )
            time.sleep(0.2)
            if _simulator_proc.poll() is not None:
                return f"{_ts()} ✗ Simulator exited immediately."
            # Kick off stdout reader thread for live chart
            _reader_thread = threading.Thread(target=_read_simulator_output, daemon=True)
            _reader_thread.start()
            return (
                f"{_ts()} ✓ Simulator started (PID {_simulator_proc.pid}).\n"
                f"{_ts()}   Publishing events to olist-orders-live…"
            )
        except FileNotFoundError:
            return f"{_ts()} ⚠ Simulator script not found. Build real-time pipeline first."


def action_stop_simulator() -> str:
    global _simulator_proc
    with _simulator_lock:
        if _simulator_proc is None or _simulator_proc.poll() is not None:
            return f"{_ts()} ℹ Simulator is not running."
        pid = _simulator_proc.pid
        _simulator_proc.terminate()
        try:
            _simulator_proc.wait(timeout=10)
            return f"{_ts()} ✓ Simulator stopped (PID {pid})."
        except subprocess.TimeoutExpired:
            _simulator_proc.kill()
            return f"{_ts()} ✓ Simulator force-killed (PID {pid})."


def get_pipeline_status() -> str:
    sim_running = _simulator_proc is not None and _simulator_proc.poll() is None
    return "\n".join([
        f"{_ts()} Pipeline Status Report",
        "─" * 48,
        f"  Real-time Simulator : {'🟢 RUNNING' if sim_running else '⚫ STOPPED'}",
        f"  Dagster UI          : http://localhost:3000",
        "─" * 48,
        "  Gold Tables (BigQuery) — run pipeline to populate.",
    ])


# ══════════════════════════════════════════════════════════════
# UI
# ══════════════════════════════════════════════════════════════

_ADMIN_CSS = """
/* Gauge tiles */
.gauge-row .block { background: rgba(6,3,0,0.97) !important;
                    border: 1px solid rgba(255,140,0,0.22) !important;
                    border-radius: 6px !important; padding: 0 !important; }
/* Section headers */
.pipe-section { background: rgba(5,2,0,0.98); border: 1px solid rgba(255,140,0,0.25);
                border-radius: 8px; padding: 14px 18px; margin-bottom: 10px; }
.pipe-hdr { font-size: 12px; font-weight: bold; color: #FF8C00; letter-spacing: 2px;
            text-transform: uppercase; margin-bottom: 4px; }
.pipe-sub { font-size: 11px; color: rgba(255,200,100,0.5); margin-bottom: 0; line-height:1.5; }
/* Separate rounded buttons — override Gradio's connected-row style */
.admin-btn-row { display: flex !important; gap: 8px !important; }
.admin-btn-row button,
.admin-btn-row .gr-button { border-radius: 8px !important;
                             font-size: 11px !important;
                             padding: 8px 14px !important;
                             min-height: 36px !important; }
/* Log output smaller label */
.log-output label { font-size: 10px !important; }
.log-output textarea, .log-output input { font-size: 0.5rem !important; }
"""
_GAUGE_CSS = CUSTOM_CSS + _ADMIN_CSS

with gr.Blocks(analytics_enabled=False, title="Data Pipeline Management") as dashboard:

    page_header(
        "Admin Control Panel",
        subtitle="Pipeline management · Cache control · Real-time agent",
        icon="⚙️",
    )

    alert_box(
        "Admin actions affect the shared pipeline and data state. "
        "Coordinate with the team before running destructive operations.",
        level="warn",
    )

    # ── 1. Batch Ingestion Pipeline ───────────────────────────────
    gr.HTML("""<div class="pipe-section">
        <div class="pipe-hdr">1 · Batch Ingestion Pipeline</div>
        <div class="pipe-sub">Meltano → Bronze (GCS/BQ) → dbt Silver (PII masking, CDC)
        → dbt Gold (Star Schema) → Redis Feature Store → Dagster sensor.</div>
    </div>""")

    with gr.Row(elem_classes=["admin-btn-row"]):
        run_full_btn = gr.Button("▶  Run Full Refresh", variant="primary",  scale=3)
        run_cdc_btn  = gr.Button("⟳  Run CDC Mode",     variant="secondary", scale=3)
        reset_btn    = gr.Button("↺  Reset",             variant="secondary", scale=1)

    pipe_status = gr.HTML(_pipe_status_html("idle"))

    with gr.Row(elem_classes=["gauge-row"]):
        g1 = gr.Plot(value=_gauge(0, "Stage 1<br>Extract→Bronze",  "idle"), show_label=False)
        g2 = gr.Plot(value=_gauge(0, "Stage 2<br>Load→BigQuery",   "idle"), show_label=False)
        g3 = gr.Plot(value=_gauge(0, "Stage 3<br>dbt Silver",      "idle"), show_label=False)
        g4 = gr.Plot(value=_gauge(0, "Stage 4<br>dbt Gold",        "idle"), show_label=False)
        g5 = gr.Plot(value=_gauge(0, "Stage 5<br>Feature Store",   "idle"), show_label=False)
        g6 = gr.Plot(value=_gauge(0, "Stage 6<br>Dagster Sensor",  "idle"), show_label=False)

    pipe_log = gr.Textbox(
        label="Pipeline Log", lines=5, interactive=False,
        placeholder="Click Run to start the batch pipeline…",
        elem_classes=["log-output"],
    )

    _full_mode = gr.State("full")
    _cdc_mode  = gr.State("cdc")

    _pipe_outs = [g1, g2, g3, g4, g5, g6, pipe_status, pipe_log]
    run_full_btn.click(fn=_run_pipeline_gen, inputs=[_full_mode], outputs=_pipe_outs)
    run_cdc_btn.click( fn=_run_pipeline_gen, inputs=[_cdc_mode],  outputs=_pipe_outs)
    reset_btn.click(
        fn=lambda: (*_idle_gauges(), _pipe_status_html("idle"), ""),
        outputs=_pipe_outs,
    )

    # ── 2. Real-time  ·  3. Cache  (side by side) ─────────────────
    with gr.Row():

        # ── 2. Real-time Order Simulator ─────────────────────────
        with gr.Column(scale=1):
            gr.HTML(f"""<div class="pipe-section">
                <div class="pipe-hdr">2 · Real-time Order Simulator</div>
                <div class="pipe-sub">Publishes synthetic order events to Pub/Sub
                <code>olist-orders-live</code>.
                Auto-stops at <strong>{GCS_STREAMING_CAP_MB} MB</strong> GCS cap.</div>
            </div>""")
            with gr.Row(elem_classes=["admin-btn-row"]):
                start_btn  = gr.Button("▶ Start Agent", variant="primary",   scale=2)
                stop_btn   = gr.Button("■ Stop Agent",  variant="stop",       scale=2)
                cap_btn    = gr.Button("⚖ Check Cap",   variant="secondary",  scale=1)
            sim_status = gr.Textbox(
                label="Status", lines=1, max_lines=2, interactive=False,
                elem_classes=["log-output"],
            )
            sim_chart  = gr.Plot(value=_build_live_chart(), show_label=False)
            sim_timer  = gr.Timer(value=2.0)
            sim_timer.tick(fn=_build_live_chart, outputs=sim_chart)
            start_btn.click(fn=action_start_simulator,     outputs=sim_status)
            stop_btn.click( fn=action_stop_simulator,      outputs=sim_status)
            cap_btn.click(  fn=action_check_streaming_cap, outputs=sim_status)

        # ── 3. Cache Management ───────────────────────────────────
        with gr.Column(scale=1):
            gr.HTML("""<div class="pipe-section">
                <div class="pipe-hdr">3 · Cache Management</div>
                <div class="pipe-sub">Flushes Redis Memorystore and GCS streaming bucket.
                Safe to run at any time.</div>
            </div>""")
            with gr.Row(elem_classes=["admin-btn-row"]):
                clear_cache_btn = gr.Button("🗑  Clear App Cache", variant="secondary")
            cache_log = gr.Textbox(
                label="Output", lines=5, interactive=False,
                placeholder="Cache clear log will appear here…",
                elem_classes=["log-output"],
            )
            clear_cache_btn.click(fn=action_clear_cache, outputs=cache_log)



if __name__ == "__main__":
    dashboard.launch(server_port=7860, show_error=True, theme=olist_theme, css=_GAUGE_CSS, head=FONT_HEAD)
