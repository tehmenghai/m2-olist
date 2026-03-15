"""
shared/components.py
────────────────────
Reusable Gradio HTML/Markdown components for all dashboards.
All components return HTML strings or gr.HTML objects.

Import:
    from shared.components import page_header, kpi_card, freshness_badge, status_row,
                                   error_figure
"""

import gradio as gr
import plotly.graph_objects as go
from shared.theme import COLORS, PLOTLY_LAYOUT
from datetime import datetime


# ── Page header ───────────────────────────────────────────────

def page_header(title: str, subtitle: str = "", icon: str = "") -> gr.HTML:
    """Render a styled page header block with a Home link on the far right."""
    sub_html = f'<p class="page-subtitle">{subtitle}</p>' if subtitle else ""
    icon_html = f'<span style="margin-right:10px;font-size:1.5rem">{icon}</span>' if icon else ""
    home_link = (
        '<a href="http://localhost:7861"'
        ' style="display:inline-flex;align-items:center;gap:6px;'
        'color:#FF8C00;text-decoration:none;font-size:0.8rem;'
        'border:1px solid rgba(255,140,0,0.35);border-radius:5px;'
        'padding:5px 12px;letter-spacing:1px;white-space:nowrap;'
        'transition:color 0.15s,border-color 0.15s;"'
        ' onmouseover="this.style.color=\'#FFD700\';this.style.borderColor=\'rgba(255,215,0,0.6)\'"'
        ' onmouseout="this.style.color=\'#FF8C00\';this.style.borderColor=\'rgba(255,140,0,0.35)\'">'
        '🏠 Home</a>'
    )
    return gr.HTML(f"""
    <div class="page-header" style="display:flex;align-items:center;justify-content:space-between;gap:16px;">
        <div>
            <h1 class="page-title">{icon_html}{title}</h1>
            {sub_html}
        </div>
        <div style="flex-shrink:0;">{home_link}</div>
    </div>
    """)


# ── KPI card ──────────────────────────────────────────────────

def kpi_card(label: str, value: str, color: str = "orange", delta: str = "") -> str:
    """
    Return HTML for a single KPI metric card.
    color: 'red' | 'orange' | 'gold' | 'green'
    delta: optional change string e.g. '+12%' or '-3.2'
    """
    delta_html = ""
    if delta:
        delta_color = COLORS["green"] if delta.startswith("+") else COLORS["red"]
        delta_html = f'<span style="font-size:0.8rem;color:{delta_color};margin-left:8px">{delta}</span>'
    return f"""
    <div class="olist-card" style="text-align:center;min-width:140px;flex:1">
        <div class="kpi-value kpi-{color}">{value}{delta_html}</div>
        <div class="kpi-label">{label}</div>
    </div>
    """


def kpi_row(metrics: list[dict]) -> gr.HTML:
    """
    Render a horizontal row of KPI cards.
    metrics = [{"label": "...", "value": "...", "color": "orange", "delta": "+5%"}, ...]
    """
    cards = "".join(kpi_card(**m) for m in metrics)
    return gr.HTML(f"""
    <div style="display:flex;gap:12px;flex-wrap:nowrap;margin:8px 0;width:100%">
        {cards}
    </div>
    """)


# ── Freshness badge ───────────────────────────────────────────

def freshness_badge(last_updated: datetime | str | None, label: str = "Data as of") -> str:
    """Return an inline HTML freshness indicator."""
    if last_updated is None:
        return f'<span style="color:{COLORS["text_muted"]};font-size:0.75rem">⏱ {label}: unknown</span>'
    if isinstance(last_updated, datetime):
        ts = last_updated.strftime("%Y-%m-%d %H:%M UTC")
    else:
        ts = str(last_updated)
    return f'<span style="color:{COLORS["text_secondary"]};font-size:0.75rem">⏱ {label}: {ts}</span>'


# ── Status dot ────────────────────────────────────────────────

def status_dot(state: str) -> str:
    """
    Inline HTML status indicator.
    state: 'ok' | 'warn' | 'error' | 'inactive'
    """
    labels = {"ok": "Online", "warn": "Degraded", "error": "Error", "inactive": "Offline"}
    return (
        f'<span class="status-dot status-{state}"></span>'
        f'<span style="font-size:0.8rem;color:{COLORS["text_secondary"]}">'
        f'{labels.get(state, state)}</span>'
    )


def status_row(items: list[dict]) -> gr.HTML:
    """
    Render a row of named status indicators.
    items = [{"name": "BigQuery", "state": "ok"}, {"name": "Redis", "state": "warn"}]
    """
    parts = []
    for item in items:
        dot = status_dot(item["state"])
        parts.append(
            f'<div style="display:flex;align-items:center;gap:6px;margin-right:20px">'
            f'<span style="color:{COLORS["text_muted"]};font-size:0.8rem">{item["name"]}</span>'
            f'{dot}</div>'
        )
    return gr.HTML(
        f'<div style="display:flex;align-items:center;flex-wrap:wrap;'
        f'background:{COLORS["bg_elevated"]};padding:10px 16px;'
        f'border-radius:6px;border:1px solid {COLORS["border"]}">'
        + "".join(parts)
        + "</div>"
    )


# ── Section divider ───────────────────────────────────────────

def section_title(text: str, accent: str = "orange") -> gr.HTML:
    """Render a section heading with a left accent bar."""
    color = COLORS.get(accent, COLORS["orange"])
    return gr.HTML(f"""
    <div style="border-left:3px solid {color};padding-left:12px;margin:20px 0 8px 0">
        <span style="font-size:1rem;font-weight:600;color:{COLORS['text_primary']}">{text}</span>
    </div>
    """)


# ── Alert box ─────────────────────────────────────────────────

def alert_box(message: str, level: str = "warn") -> gr.HTML:
    """
    Render a coloured alert/notice box.
    level: 'info' | 'warn' | 'error' | 'success'
    """
    cfg = {
        "info":    (COLORS["gold"],   "ℹ️"),
        "warn":    (COLORS["orange"], "⚠️"),
        "error":   (COLORS["red"],    "✕"),
        "success": (COLORS["green"],  "✓"),
    }
    color, icon = cfg.get(level, cfg["info"])
    return gr.HTML(f"""
    <div style="border:1px solid {color}33;background:{color}0D;border-radius:6px;
                padding:12px 16px;color:{COLORS['text_primary']};font-size:0.875rem">
        <span style="color:{color};margin-right:8px">{icon}</span>{message}
    </div>
    """)


# ── Nav tile (launchpad) ──────────────────────────────────────

def nav_tile(icon: str, title: str, owner: str, badge: str = "batch") -> str:
    """
    Return HTML for a launchpad navigation tile.
    badge: 'live' | 'batch' | 'offline'
    """
    badge_labels = {"live": "● LIVE", "batch": "BATCH", "offline": "OFFLINE"}
    return f"""
    <div class="nav-tile">
        <div class="nav-tile-icon">{icon}</div>
        <div class="nav-tile-title">{title}</div>
        <div class="nav-tile-owner">{owner}</div>
        <span class="nav-tile-badge badge-{badge}">{badge_labels.get(badge, badge)}</span>
    </div>
    """


# ── Error figure (Plotly) ─────────────────────────────────────

def error_figure(title: str = "Error loading chart") -> go.Figure:
    """
    Return a themed empty Plotly figure with an error title.
    Use in data loaders when GCP is not configured or a query fails:

        def load_my_chart():
            client, cfg, err = _get_client()
            if err:
                return error_figure("GCP not configured")
            try:
                ...
            except Exception as e:
                return error_figure(f"Error: {e}")
    """
    from copy import deepcopy
    fig = go.Figure()
    layout = deepcopy(PLOTLY_LAYOUT)
    layout["title"] = title
    fig.update_layout(**layout)
    return fig


# ── Empty state ───────────────────────────────────────────────

def empty_state(message: str = "No data available", icon: str = "📭") -> gr.HTML:
    """Placeholder shown when a chart/table has no data."""
    return gr.HTML(f"""
    <div style="text-align:center;padding:48px;color:{COLORS['text_muted']}">
        <div style="font-size:2.5rem;margin-bottom:12px">{icon}</div>
        <div style="font-size:0.9rem">{message}</div>
    </div>
    """)
