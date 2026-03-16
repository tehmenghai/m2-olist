"""
dashboards/home/app.py — Mission-Control Launchpad
────────────────────────────────────────────────────
Owner: Lik Hong
Layout: 3-column × 2-row mockup3 design.
        Each panel uses gr.Group to ensure seamless header+chart border.
        ADMIN ↗ injected via gr.Blocks(js=) — survives Gradio re-renders.
Exports: dashboard (gr.Blocks)
"""

import gradio as gr
import plotly.graph_objects as go
import numpy as np

from shared.theme import olist_theme, CUSTOM_CSS, COLORS, PLOTLY_LAYOUT, FONT_HEAD

AMBER = COLORS["orange"]
GOLD  = COLORS["gold"]
GREEN = COLORS["green"]
RED   = COLORS["red"]

_M = {
    **PLOTLY_LAYOUT,
    "paper_bgcolor": "rgba(0,0,0,0)",
    "plot_bgcolor":  "rgba(0,0,0,0)",
    "margin":        dict(l=10, r=10, t=22, b=8),
    "font":          dict(color=AMBER, family="Space Mono, monospace", size=9),
    "showlegend":    False,
    "title":         dict(font=dict(size=8, color=GOLD), x=0.5, xanchor="center", pad=dict(t=0, b=0)),
}


# ── Mini charts ───────────────────────────────────────────────

def _fig_radar_c360():
    cats = ["Recency", "Frequency", "Monetary", "Satisfaction", "Loyalty", "Diversity"]
    champs  = [85, 92, 88, 95, 78, 72]
    at_risk = [28, 65, 72, 60, 45, 38]
    r_champs  = champs  + [champs[0]]
    r_atrisk  = at_risk + [at_risk[0]]
    theta = cats + [cats[0]]
    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=r_champs, theta=theta, fill="toself", name="Champions",
        fillcolor="rgba(0,200,81,0.14)",
        line=dict(color=GREEN, width=1.5),
        marker=dict(size=4, color=GREEN),
    ))
    fig.add_trace(go.Scatterpolar(
        r=r_atrisk, theta=theta, fill="toself", name="At Risk",
        fillcolor="rgba(255,68,68,0.1)",
        line=dict(color=RED, width=1.2, dash="dot"),
        marker=dict(size=3, color=RED),
    ))
    fig.update_layout(**{**_M, "height": 269,
        "title": dict(text="RFM Segments", font=dict(size=8, color=GOLD), x=0.5, xanchor="center", pad=dict(t=0, b=0)),
        "showlegend": True,
        "legend": dict(font=dict(size=7, color=AMBER), bgcolor="rgba(0,0,0,0)",
                       x=0.72, y=0.05, orientation="v"),
        "polar": dict(
            bgcolor="rgba(0,0,0,0)",
            radialaxis=dict(visible=True, range=[0, 100], showticklabels=False,
                            gridcolor="rgba(255,140,0,0.12)", linecolor="rgba(255,140,0,0.15)"),
            angularaxis=dict(tickfont=dict(size=8, color=GOLD),
                             gridcolor="rgba(255,140,0,0.1)", linecolor="rgba(255,140,0,0.2)"),
        ),
    })
    return fig


def _fig_donut():
    fig = go.Figure(go.Pie(
        labels=["Credit Card", "Boleto", "Voucher", "Debit"],
        values=[76795, 19784, 5775, 1529], hole=0.55,
        marker=dict(colors=[AMBER, "#CC7000", GOLD, "#996000"],
                    line=dict(color="rgba(0,0,0,0.5)", width=1)),
        textinfo="label+percent", textfont=dict(size=8, color=GOLD),
        insidetextorientation="horizontal",
    ))
    fig.update_layout(**{**_M, "height": 269,
        "title": dict(text="Payment Methods", font=dict(size=8, color=GOLD), x=0.5, xanchor="center", pad=dict(t=0, b=0)),
    })
    return fig


def _fig_reviews():
    fig = go.Figure(go.Bar(
        x=[1, 2, 3, 4, 5], y=[11424, 3244, 8179, 19142, 57328],
        marker_color=[RED, "#FF6600", AMBER, "#CCAA00", GREEN],
        marker_line=dict(color="rgba(0,0,0,0.4)", width=1),
        text=["11.5k", "3.2k", "8.2k", "19.1k", "57.3k"],
        textfont=dict(size=7, color=GOLD), textposition="outside",
    ))
    fig.update_layout(**{**_M, "height": 269,
        "title": dict(text="Review Score Distribution", font=dict(size=8, color=GOLD), x=0.5, xanchor="center", pad=dict(t=0, b=0)),
        "xaxis": dict(showgrid=False, tickvals=[1, 2, 3, 4, 5],
                      ticktext=["★", "★★", "★★★", "★★★★", "★★★★★"],
                      tickfont=dict(size=9, color=AMBER)),
        "yaxis": dict(showgrid=False, showticklabels=False, range=[0, 68000]),
    })
    return fig


def _fig_products():
    cats = ["Bed Bath", "Health Beauty", "Sports", "Furniture", "Computers", "Electronics", "Housewares"]
    vals = [11115, 9670, 8641, 8334, 6526, 5964, 5750]
    colors = [f"rgba(255,{int(80+i*18)},0,{0.95-i*0.07})" for i in range(len(cats))]
    fig = go.Figure(go.Bar(
        x=vals, y=cats, orientation="h",
        marker_color=colors, marker_line=dict(color="rgba(0,0,0,0.3)", width=1),
        text=[f"{v:,}" for v in vals], textfont=dict(size=8, color=GOLD), textposition="inside",
    ))
    fig.update_layout(**{**_M, "height": 269,
        "title": dict(text="Top Categories by Orders", font=dict(size=8, color=GOLD), x=0.5, xanchor="center", pad=dict(t=0, b=0)),
        "xaxis": dict(showgrid=False, showticklabels=False),
        "yaxis": dict(showgrid=False, tickfont=dict(size=8, color=GOLD)),
    })
    return fig


def _fig_sellers():
    return _FIG_SELLERS


# Precompute at module load (fixed seed — deterministic, no benefit in re-generating)
def _build_fig_sellers():
    np.random.seed(42)
    n = 90
    orders  = np.random.exponential(45, n)
    ratings = np.clip(3.5 + np.random.normal(0, 0.7, n), 1, 5)
    revenue = orders * np.random.uniform(80, 600, n)
    fig = go.Figure(go.Scatter(
        x=orders, y=ratings, mode="markers",
        marker=dict(size=np.sqrt(revenue / 400), color=revenue,
                    colorscale=[[0, "#3D1A00"], [0.5, AMBER], [1, GOLD]],
                    opacity=0.75, line=dict(color="rgba(255,140,0,0.3)", width=0.5)),
        hovertemplate="Orders: %{x:.0f}<br>Rating: %{y:.2f}<extra></extra>",
    ))
    fig.update_layout(**{**_M, "height": 269,
        "title": dict(text="Orders vs Rating", font=dict(size=8, color=GOLD), x=0.5, xanchor="center", pad=dict(t=0, b=0)),
        "xaxis": dict(showgrid=False, tickfont=dict(size=8),
                      title=dict(text="Orders →", font=dict(size=8))),
        "yaxis": dict(showgrid=False, tickfont=dict(size=8), range=[1, 5],
                      title=dict(text="Rating →", font=dict(size=8))),
    })
    return fig


_FIG_SELLERS = _build_fig_sellers()


def _fig_geo():
    states = [
        ("SP", -23.5, -46.6, 41746), ("RJ", -22.9, -43.2, 12852),
        ("MG", -19.9, -43.9, 11635), ("RS", -30.0, -51.2, 8523),
        ("PR", -25.4, -49.3, 7011),  ("SC", -27.6, -48.5, 6360),
        ("BA", -12.9, -38.5, 3380),  ("GO", -16.7, -49.3, 2684),
        ("ES", -20.3, -40.3, 2222),  ("PE",  -8.1, -34.9, 1650),
        ("CE",  -3.7, -38.5, 1336),  ("AM",  -3.1, -60.0, 480),
        ("MT", -15.6, -56.1, 908),   ("MS", -20.4, -54.6, 714),
        ("PA",  -1.5, -48.5, 975),
    ]
    fig = go.Figure(go.Scattergeo(
        lat=[s[1] for s in states], lon=[s[2] for s in states], mode="markers",
        marker=dict(
            size=[max(6, min(26, s[3] // 600)) for s in states],
            color=[GREEN if s[3] > 5000 else (AMBER if s[3] > 2000 else RED) for s in states],
            opacity=0.85, line=dict(color="rgba(255,255,255,0.3)", width=0.5),
        ),
        customdata=[f"{s[0]}: {s[3]:,}" for s in states],
        hovertemplate="%{customdata}<extra></extra>",
    ))
    fig.update_layout(**{**_M, "height": 269,
        "title": dict(text="Orders by State", font=dict(size=8, color=GOLD), x=0.5, xanchor="center", pad=dict(t=0, b=0)),
        "geo": dict(
            bgcolor="rgba(0,0,0,0)",
            showland=True,  landcolor="rgba(25,12,0,0.9)",
            showocean=True, oceancolor="rgba(0,4,15,0.6)",
            showcoastlines=True, coastlinecolor="rgba(255,140,0,0.5)",
            showcountries=True, countrycolor="rgba(255,140,0,0.15)",
            showframe=False, scope="south america",
            lataxis_range=[-34, 6], lonaxis_range=[-75, -28],
        ),
    })
    return fig



# ── CSS ────────────────────────────────────────────────────────
_CSS = """
.gradio-container {
    background: #060400 !important;
    font-family: 'Space Grotesk', system-ui, sans-serif !important;
    max-width: 100% !important; min-height: 100vh;
}
footer, header { display: none !important; }
.gradio-container::before {
    content: ''; position: fixed; inset: 0;
    background:
        radial-gradient(ellipse 75% 55% at 50% 24%, rgba(190,78,0,0.24) 0%, transparent 65%),
        radial-gradient(ellipse 45% 35% at 18% 68%, rgba(110,44,0,0.11) 0%, transparent 55%),
        radial-gradient(ellipse 45% 35% at 82% 68%, rgba(110,44,0,0.11) 0%, transparent 55%);
    pointer-events: none; z-index: 0;
}
.gradio-container::after {
    content: ''; position: fixed; inset: 0;
    background-image: radial-gradient(circle, rgba(255,140,0,0.06) 1px, transparent 1px);
    background-size: 32px 32px; pointer-events: none; z-index: 0;
}

/* ── KPI strip ── */
.kpi-strip {
    display: flex; justify-content: space-around; align-items: center;
    padding: 10px 28px; flex-wrap: wrap; gap: 6px;
    background: linear-gradient(90deg, rgba(255,140,0,0.02), rgba(255,140,0,0.12), rgba(255,140,0,0.02));
    border-bottom: 1px solid rgba(255,140,0,0.22);
}
.kpi-item { text-align: center; min-width: 78px; }
.kpi-val  { display: block; font-size: 22px; font-weight: 900; color: #FFD700;
            text-shadow: 0 0 12px rgba(255,215,0,0.55); line-height: 1.1; }
.kpi-lbl  { display: block; font-size: 8px; color: rgba(255,190,70,0.82);
            letter-spacing: 1.2px; text-transform: uppercase; margin-top: 1px; }
.kpi-chg-p { color: #00C851; font-size: 9px; }
.kpi-chg-n { color: #FF4444; font-size: 9px; }
.kpi-sep   { width: 1px; height: 30px; background: rgba(255,140,0,0.14); }

/* ── Panel: use gr.Group as the border container ── */
.panel-group {
    border: 1px solid rgba(255,215,0,0.55) !important;
    border-radius: 8px !important;
    overflow: hidden !important;
    padding: 0 !important;
    margin-bottom: 8px !important;
    background: rgba(6,3,0,0.98) !important;
    box-shadow: 0 4px 22px rgba(255,140,0,0.08), 0 0 0 1px rgba(255,215,0,0.08) !important;
    position: relative;
}
.panel-group::before {
    content: '';
    position: absolute; top: 0; left: 0; right: 0; height: 2px;
    background: linear-gradient(90deg, transparent, rgba(255,215,0,0.9), transparent);
    z-index: 1; pointer-events: none;
}
/* Strip inner Gradio block borders/padding/margins inside panel-group */
.panel-group .block,
.panel-group .form,
.panel-group > div > div {
    border: none !important;
    padding: 0 !important;
    margin: 0 !important;
    background: transparent !important;
    gap: 0 !important;
}
/* Panel header inside the group */
.dh {
    padding: 10px 12px 9px;
    border-bottom: 1px solid rgba(255,140,0,0.15);
    position: relative;
}
.dh-title { font-size: 10px; font-weight: bold; color: #FF8C00; letter-spacing: 2px; }
.dh-mrow  { display: flex; gap: 10px; margin-top: 6px; }
.dh-m     { text-align: center; flex: 1; }
.dh-mv    { display: block; font-size: 14px; font-weight: bold; color: #FFD700; line-height: 1.1; }
.dh-ml    { display: block; font-size: 8px; color: rgba(255,190,70,0.80); text-transform: uppercase; }
.live-dot {
    width: 7px; height: 7px; border-radius: 50%;
    background: #00C851; box-shadow: 0 0 6px #00C851;
    display: inline-block; animation: livePulse 2s ease-in-out infinite;
}
.launch-link {
    font-size: 9px; color: #FF8C00; text-decoration: none;
    letter-spacing: 1px; border: 1px solid rgba(255,140,0,0.4);
    padding: 2px 8px; border-radius: 3px;
}
.launch-link:hover { background: rgba(255,140,0,0.12); color: #FFD700; }

/* ── Center hero ── */
.hero {
    text-align: center; padding: 18px 14px 10px;
    background: rgba(8,4,0,0.75);
    border: 1px solid rgba(255,140,0,0.22); border-radius: 8px 8px 0 0;
    position: relative; overflow: hidden;
}
.hero::before {
    content: ''; position: absolute; top: 0; left: 0; right: 0; height: 2px;
    background: linear-gradient(90deg, transparent, #FF8C00, #FFD700, #FF8C00, transparent);
}
.hero-t1 { font-size: 34px; font-weight: 900; color: #FFD700; letter-spacing: 7px;
           text-shadow: 0 0 22px rgba(255,215,0,0.7), 0 0 55px rgba(255,140,0,0.4); line-height: 1; }
.hero-t2 { font-size: 13px; font-weight: bold; color: rgba(255,140,0,0.85); letter-spacing: 5px; margin: 5px 0; }
.hero-t3 { font-size: 10px; color: rgba(255,140,0,0.4); letter-spacing: 3px; margin: 6px 0 10px; }
.hero-eyebrow { font-size: 9px; color: rgba(255,140,0,0.35); letter-spacing: 4px; margin-bottom: 10px; }
.hero-arrow {
    font-size: 56px; color: #FF8C00; display: block; line-height: 1;
    text-shadow: 0 0 28px rgba(255,140,0,0.95), 0 0 65px rgba(255,140,0,0.45);
    animation: arrowFloat 2.6s ease-in-out infinite;
}
.hero-stats { display: flex; justify-content: center; gap: 0; margin: 10px 0 6px; }
.hero-stat  { text-align: center; padding: 0 18px; }
.hero-stat + .hero-stat { border-left: 1px solid rgba(255,140,0,0.18); }
.hero-sv { font-size: 20px; font-weight: bold; color: #FFD700; display: block; }
.hero-sl { font-size: 8px; color: rgba(255,140,0,0.45); letter-spacing: 1px; text-transform: uppercase; }
.hero-live { font-size: 9px; color: rgba(0,200,81,0.72); letter-spacing: 2px; margin-top: 6px; }

/* Sync bar */
.sync-wrap {
    background: rgba(8,4,0,0.97); border: 1px solid rgba(255,140,0,0.22);
    border-top: none; border-radius: 0 0 8px 8px; padding: 9px 14px 11px;
}
.sync-lbl   { font-size: 9px; color: rgba(255,140,0,0.48); letter-spacing: 2px; margin-bottom: 5px; }
.sync-track { height: 7px; background: rgba(255,140,0,0.08); border-radius: 4px; overflow: hidden; }
.sync-fill  { height: 100%; width: 94%; background: linear-gradient(90deg, #3D1A00, #FF8C00, #FFD700);
              border-radius: 4px; position: relative; overflow: hidden; }
.sync-shimmer { position: absolute; top: 0; bottom: 0; width: 55px;
                background: linear-gradient(90deg, transparent, rgba(255,255,255,0.38), transparent);
                animation: shimmer 2s linear infinite; }
.sync-legs { display: flex; justify-content: space-between;
             margin-top: 5px; font-size: 8px; color: rgba(255,140,0,0.32); }
/* Team panel */
.team-panel { background: rgba(8,4,0,0.92); border: 1px solid rgba(255,140,0,0.28);
              border-radius: 8px; padding: 12px 14px; }
.team-title { font-size: 9px; color: rgba(255,190,70,0.78); letter-spacing: 3px; text-transform: uppercase;
              border-bottom: 1px solid rgba(255,140,0,0.1); padding-bottom: 6px; margin-bottom: 8px; }
.team-row   { display: flex; align-items: center; gap: 8px; padding: 5px 0;
              border-bottom: 1px solid rgba(255,140,0,0.06); }
.team-icon  { width: 28px; height: 28px; border-radius: 50%;
              display: flex; align-items: center; justify-content: center;
              font-size: 10px; flex-shrink: 0; }
.team-name  { font-size: 11px; font-weight: bold; flex: 1; }
.team-role  { font-size: 8px; color: rgba(255,190,70,0.68); display: block; }
.team-port  { font-size: 9px; color: rgba(255,190,70,0.58); }
/* Pipeline section */
.pipe-panel { background: rgba(5,2,0,0.98); border: 1px solid rgba(255,140,0,0.18);
              border-radius: 8px; padding: 14px 20px; margin-top: 6px; }
.pipe-title { font-size: 9px; color: rgba(255,190,70,0.75); letter-spacing: 3px; text-transform: uppercase;
              border-bottom: 1px solid rgba(255,140,0,0.1); padding-bottom: 6px; margin-bottom: 10px; }
.pipe-grid  { display: grid; grid-template-columns: repeat(4, 1fr); gap: 4px 20px; }
.pipe-row   { display: flex; justify-content: space-between; align-items: center; padding: 4px 0; }
.pipe-name  { font-size: 10px; color: rgba(255,210,120,0.90); }
.pipe-badge { padding: 1px 8px; border-radius: 3px; font-size: 8px; letter-spacing: 1px; }
.ok   { background: rgba(0,200,81,0.09);  color: #00C851; border: 1px solid rgba(0,200,81,0.22); }
.warn { background: rgba(255,140,0,0.09); color: #FF8C00; border: 1px solid rgba(255,140,0,0.22); }

/* Gradio global resets */
:root { --block-gap: 0px !important; --layout-gap: 0px !important; --spacing-lg: 0px !important; }
.form { background: transparent !important; border: none !important; padding: 0 !important; }
.block { background: transparent !important; border: none !important;
         padding: 0 !important; margin: 0 !important; }
div[data-testid="column"] > div,
div[data-testid="column"] > div > div { gap: 0 !important; }

/* Animations */
@keyframes arrowFloat {
    0%,100% { transform: translateY(0); text-shadow: 0 0 28px rgba(255,140,0,0.95), 0 0 65px rgba(255,140,0,0.45); }
    50%      { transform: translateY(-7px); text-shadow: 0 0 45px rgba(255,215,0,1), 0 0 90px rgba(255,140,0,0.7); }
}
@keyframes shimmer  { 0% { left: -55px; } 100% { left: 110%; } }
@keyframes livePulse {
    0%,100% { opacity: 1; box-shadow: 0 0 6px #00C851; }
    50%      { opacity: 0.28; box-shadow: none; }
}
"""

# ── Admin button JS — injected via gr.Blocks(js=) ─────────────
_ADMIN_JS = """() => {
    var _st = 'position:fixed;bottom:18px;right:18px;z-index:99999;'
        + 'display:inline-flex;align-items:center;gap:5px;'
        + 'background:rgba(5,2,0,0.97);border:1px solid rgba(255,140,0,0.5);'
        + 'border-radius:6px;padding:8px 14px;'
        + 'font-family:"Space Mono",monospace;font-size:10px;'
        + 'color:#FF8C00;text-decoration:none;letter-spacing:1.5px;'
        + 'box-shadow:0 0 22px rgba(255,140,0,0.18);cursor:pointer;';
    function inject() {
        if (document.getElementById('olist-admin-btn')) return;
        var a = document.createElement('a');
        a.id = 'olist-admin-btn'; a.href = 'http://localhost:7860';
        a.innerHTML = '&#9881; ADMIN &#8599;';
        a.style.cssText = _st;
        a.onmouseenter = function(){ this.style.color='#FFD700'; };
        a.onmouseleave = function(){ this.style.color='#FF8C00'; };
        document.body.appendChild(a);
    }
    inject(); setInterval(inject, 500);
}"""


# ── HTML helpers ───────────────────────────────────────────────

def _hex2rgb(h):
    h = h.lstrip("#")
    return ",".join(str(int(h[i:i+2], 16)) for i in (0, 2, 4))


def _traffic_light_pill(state: str = "not_run") -> str:
    """Returns just the clickable pill HTML (no wrapper)."""
    if state == "complete":
        dot_color = "#00C851"; glow = "#00C851"; label = "PIPELINE COMPLETE"
    elif state == "running":
        dot_color = "#FFD700"; glow = "#FFD700"; label = "PIPELINE RUNNING"
    else:
        dot_color = "#FF4444"; glow = "#FF4444"; label = "PIPELINE NOT RUN"
    js = "window.location.href='http://localhost:7860'"
    return (
        f'<div onclick="{js}" title="Open Admin Panel in new window" '
        'style="display:inline-flex;align-items:center;gap:7px;cursor:pointer;'
        'background:rgba(6,3,0,0.92);border:1px solid rgba(255,140,0,0.22);'
        'border-radius:5px;padding:5px 12px;white-space:nowrap;'
        'transition:border-color 0.2s,box-shadow 0.2s;" '
        'onmouseover="this.style.borderColor=\'rgba(255,140,0,0.6)\';this.style.boxShadow=\'0 0 10px rgba(255,140,0,0.15)\'" '
        'onmouseout="this.style.borderColor=\'rgba(255,140,0,0.22)\';this.style.boxShadow=\'none\'">'
        f'<span style="width:9px;height:9px;border-radius:50%;background:{dot_color};'
        f'box-shadow:0 0 7px {glow};display:inline-block;flex-shrink:0;'
        'animation:livePulse 2s ease-in-out infinite;"></span>'
        f'<span style="font-size:10px;color:rgba(255,210,130,0.85);letter-spacing:1.5px;">{label}</span>'
        '</div>'
    )


def _home_header(state: str = "not_run") -> str:
    """Page header with pipeline status pill aligned to the right."""
    pill = _traffic_light_pill(state)
    return f"""
    <div class="page-header" style="display:flex;align-items:center;justify-content:space-between;gap:16px;">
        <div>
            <h1 class="page-title"><span style="margin-right:10px;font-size:1.5rem">🏠</span>Brazil E-commerce Intelligence</h1>
            <p class="page-subtitle">Six Domains · One Data Architecture</p>
        </div>
        <div style="flex-shrink:0;">{pill}</div>
    </div>
    """


def _panel_header(title: str, port: str, metrics: list, owner: str = "", badge: str = "batch") -> str:
    """Panel header — title, owner, launch link, metrics, badge."""
    mh = "".join(
        f'<div class="dh-m"><span class="dh-mv">{v}</span><span class="dh-ml">{l}</span></div>'
        for l, v in metrics
    )
    badge_col  = "#00C851" if badge == "live" else "#FF8C00"
    badge_bg   = "rgba(0,200,81,0.09)" if badge == "live" else "rgba(255,140,0,0.09)"
    badge_html = (
        f'<span style="font-size:7px;padding:1px 5px;border-radius:2px;'
        f'background:{badge_bg};color:{badge_col};'
        f'border:1px solid {badge_col}33;letter-spacing:1px;">'
        f'{"● LIVE" if badge == "live" else "BATCH"}</span>'
    )
    owner_html = (
        f'<span style="font-size:8px;color:rgba(255,140,0,0.65);margin-left:4px;">{owner}</span>'
        if owner else ""
    )
    return f"""<div class="dh">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:2px;">
            <div style="display:flex;align-items:center;gap:6px;">
                <span class="dh-title">{title}</span>
                {badge_html}
                {owner_html}
            </div>
            <div style="display:flex;align-items:center;gap:8px;">
                <a href="http://localhost:{port}" class="launch-link">LAUNCH ↗</a>
                <span class="live-dot"></span>
            </div>
        </div>
        <div class="dh-mrow">{mh}</div>
    </div>"""


def _team():
    """Flattened 2-row grid of team members."""
    team = [
        ("Lik Hong",  "Customer 360 + Pipeline", "#FF8C00", "7862"),
        ("Meng Hai",  "Payment Analytics",        "#CC8800", "7863"),
        ("Lanson",    "Reviews & Satisfaction",    "#CCAA00", "7864"),
        ("Ben",       "Product Analytics",         "#FF8C00", "7865"),
        ("Huey Ling", "Seller Performance",        "#CC8800", "7866"),
        ("Kendra",    "Geography Analytics",       "#CCAA00", "7867"),
    ]
    cards = "".join(
        f'<div style="display:flex;align-items:center;gap:7px;padding:5px 10px;'
        f'background:rgba({_hex2rgb(c)},0.05);border:1px solid rgba({_hex2rgb(c)},0.22);'
        f'border-radius:5px;min-width:160px;flex:1;">'
        f'<span style="font-size:16px;color:{c};">◈</span>'
        f'<div><span style="font-size:13px;font-weight:bold;color:{c};">{name}</span>'
        f'<span style="display:block;font-size:11px;color:rgba(255,190,70,0.68);">{role}</span></div>'
        f'</div>'
        for name, role, c, _ in team
    )
    return (
        '<div style="background:rgba(6,3,0,0.9);border:1px solid rgba(255,140,0,0.2);'
        'border-radius:8px;padding:10px 14px;margin-top:6px;">'
        '<div style="font-size:11px;color:rgba(255,190,70,0.75);letter-spacing:3px;'
        'text-transform:uppercase;margin-bottom:8px;">◈ TEAM 3</div>'
        f'<div style="display:flex;flex-wrap:wrap;gap:6px;">{cards}</div>'
        '</div>'
    )


# ── App ────────────────────────────────────────────────────────
with gr.Blocks(analytics_enabled=False, title="Olist Data Product") as dashboard:

    gr.HTML(_home_header("not_run"))

    # ── Row 1: 3 panels ───────────────────────────────────────────
    with gr.Row(equal_height=False):

        with gr.Column(scale=1):
            with gr.Group(elem_classes=["panel-group"]):
                gr.HTML(_panel_header(
                    "CUSTOMER 360", "7862",
                    [("Orders", "99,441"), ("Repeat Rate", "3.0%"), ("Avg CLV", "R$406")],
                    badge="batch",
                ))
                gr.Plot(_fig_radar_c360(), show_label=False)

        with gr.Column(scale=1):
            with gr.Group(elem_classes=["panel-group"]):
                gr.HTML(_panel_header(
                    "REVIEWS & SATISFACTION", "7864",
                    [("Avg Score", "4.09 ★"), ("5-Stars", "57.8%"), ("NPS", "67")],
                    badge="batch",
                ))
                gr.Plot(_fig_reviews(), show_label=False)

        with gr.Column(scale=1):
            with gr.Group(elem_classes=["panel-group"]):
                gr.HTML(_panel_header(
                    "PAYMENT ANALYTICS", "7863",
                    [("Revenue", "R$13.6M"), ("Avg Instal.", "3.7×"), ("CC Share", "73.9%")],
                    badge="batch",
                ))
                gr.Plot(_fig_donut(), show_label=False)

    # ── Row 2: 3 panels ───────────────────────────────────────────
    with gr.Row(equal_height=False):

        with gr.Column(scale=1):
            with gr.Group(elem_classes=["panel-group"]):
                gr.HTML(_panel_header(
                    "PRODUCT ANALYTICS", "7865",
                    [("Categories", "71"), ("Top Cat", "Bed Bath"), ("Avg Weight", "2.3 kg")],
                    badge="batch",
                ))
                gr.Plot(_fig_products(), show_label=False)

        with gr.Column(scale=1):
            with gr.Group(elem_classes=["panel-group"]):
                gr.HTML(_panel_header(
                    "SELLER PERFORMANCE", "7866",
                    [("Sellers", "3,095"), ("Avg Rating", "4.1 ★"), ("Top State", "SP")],
                    badge="batch",
                ))
                gr.Plot(_fig_sellers(), show_label=False)

        with gr.Column(scale=1):
            with gr.Group(elem_classes=["panel-group"]):
                gr.HTML(_panel_header(
                    "GEOGRAPHY ANALYTICS", "7867",
                    [("States", "27"), ("Top State", "SP 42%"), ("Cities", "4,119")],
                    badge="batch",
                ))
                gr.Plot(_fig_geo(), show_label=False)

    # ── Team — flat 2-row grid at bottom ─────────────────────────
    gr.HTML(_team())


if __name__ == "__main__":
    dashboard.launch(server_port=7861, show_error=True, js=_ADMIN_JS, theme=olist_theme, css=CUSTOM_CSS + _CSS, head=FONT_HEAD)
