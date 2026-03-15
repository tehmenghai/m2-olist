"""
shared/theme.py
───────────────
Olist Data Product — unified dark theme for all Gradio dashboards.
Import this in every dashboard:
    from shared.theme import olist_theme, CUSTOM_CSS, COLORS
"""

import gradio as gr

# ── Colour palette ────────────────────────────────────────────
COLORS = {
    "red":          "#FF4444",
    "orange":       "#FF8C00",
    "gold":         "#FFD700",
    "green":        "#00C851",
    "bg_base":      "#0D0D0D",
    "bg_card":      "#1A1A1A",
    "bg_elevated":  "#262626",
    "bg_input":     "#1E1E1E",
    "text_primary": "#F0F0F0",
    "text_secondary":"#A0A0A0",
    "text_muted":   "#606060",
    "border":       "#2A2A2A",
    "border_focus": "#FF8C00",
}

# ── Gradio theme ──────────────────────────────────────────────
olist_theme = gr.themes.Base(
    primary_hue=gr.themes.colors.orange,
    secondary_hue=gr.themes.colors.red,
    neutral_hue=gr.themes.colors.gray,
    font=[gr.themes.GoogleFont("Space Grotesk"), "system-ui", "sans-serif"],
    font_mono=[gr.themes.GoogleFont("Space Mono"), "monospace"],
    radius_size=gr.themes.sizes.radius_sm,
    spacing_size=gr.themes.sizes.spacing_md,
    text_size=gr.themes.sizes.text_md,
).set(
    # Body
    body_background_fill=COLORS["bg_base"],
    body_background_fill_dark=COLORS["bg_base"],
    body_text_color=COLORS["text_primary"],
    body_text_color_dark=COLORS["text_primary"],
    body_text_size="15px",

    # Blocks / containers
    block_background_fill=COLORS["bg_card"],
    block_background_fill_dark=COLORS["bg_card"],
    block_border_color=COLORS["border"],
    block_border_color_dark=COLORS["border"],
    block_border_width="1px",
    block_label_text_color=COLORS["text_secondary"],
    block_label_text_color_dark=COLORS["text_secondary"],
    block_label_background_fill=COLORS["bg_card"],
    block_label_background_fill_dark=COLORS["bg_card"],
    block_title_text_color=COLORS["text_primary"],
    block_title_text_color_dark=COLORS["text_primary"],

    # Inputs
    input_background_fill=COLORS["bg_input"],
    input_background_fill_dark=COLORS["bg_input"],
    input_border_color=COLORS["border"],
    input_border_color_dark=COLORS["border"],
    input_border_color_focus=COLORS["border_focus"],
    input_border_color_focus_dark=COLORS["border_focus"],
    input_placeholder_color=COLORS["text_muted"],
    input_placeholder_color_dark=COLORS["text_muted"],
    input_text_size="15px",

    # Buttons
    button_primary_background_fill=COLORS["orange"],
    button_primary_background_fill_dark=COLORS["orange"],
    button_primary_background_fill_hover=COLORS["gold"],
    button_primary_background_fill_hover_dark=COLORS["gold"],
    button_primary_text_color="#000000",
    button_primary_text_color_dark="#000000",
    button_secondary_background_fill=COLORS["bg_elevated"],
    button_secondary_background_fill_dark=COLORS["bg_elevated"],
    button_secondary_background_fill_hover=COLORS["border_focus"],
    button_secondary_text_color=COLORS["text_primary"],
    button_secondary_text_color_dark=COLORS["text_primary"],
    button_cancel_background_fill=COLORS["red"],
    button_cancel_background_fill_dark=COLORS["red"],
    button_cancel_text_color="#FFFFFF",

    # Tabs
    background_fill_primary=COLORS["bg_base"],
    background_fill_secondary=COLORS["bg_card"],

    # Shadows
    shadow_drop="0 2px 8px rgba(0,0,0,0.6)",
    shadow_drop_lg="0 4px 20px rgba(0,0,0,0.8)",

    # Sliders / toggles
    slider_color=COLORS["orange"],
    checkbox_background_color_selected=COLORS["orange"],
    checkbox_border_color_focus=COLORS["orange"],

    # Table
    table_border_color=COLORS["border"],
    table_even_background_fill=COLORS["bg_card"],
    table_odd_background_fill=COLORS["bg_elevated"],
    table_row_focus=COLORS["bg_elevated"],
)

# ── Font preload (inject via head= in gr.Blocks) ─────────────
FONT_HEAD = (
    '<link rel="preconnect" href="https://fonts.googleapis.com">'
    '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>'
    '<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700'
    '&family=Space+Grotesk:wght@400;500;600;700&display=swap" rel="stylesheet">'
)

# ── Custom CSS (applied to all dashboards via css= param) ─────
CUSTOM_CSS = """
/* ── Reset & base ───────────────────────────────────────── */
* { box-sizing: border-box; }

body, .gradio-container {
    background-color: #060400 !important;
    color: #F0F0F0 !important;
    font-family: 'Space Grotesk', system-ui, sans-serif !important;
}

/* Ambient amber radial glow */
.gradio-container::before {
    content: '';
    position: fixed;
    inset: 0;
    background:
        radial-gradient(ellipse 70% 50% at 50% 20%, rgba(180,72,0,0.18) 0%, transparent 65%),
        radial-gradient(ellipse 40% 30% at 15% 75%, rgba(100,40,0,0.09) 0%, transparent 55%),
        radial-gradient(ellipse 40% 30% at 85% 75%, rgba(100,40,0,0.09) 0%, transparent 55%);
    pointer-events: none;
    z-index: 0;
}

/* Subtle dot-grid overlay */
.gradio-container::after {
    content: '';
    position: fixed;
    inset: 0;
    background-image: radial-gradient(circle, rgba(255,140,0,0.05) 1px, transparent 1px);
    background-size: 32px 32px;
    pointer-events: none;
    z-index: 0;
}

footer { display: none !important; }

/* ── Tabs ───────────────────────────────────────────────── */
.tab-nav button {
    background: rgba(10,5,0,0.9) !important;
    color: rgba(255,140,0,0.5) !important;
    border: 1px solid rgba(255,140,0,0.15) !important;
    border-bottom: none !important;
    transition: all 0.2s ease;
    font-weight: 600;
    letter-spacing: 1px;
    font-family: 'Space Mono', monospace !important;
    font-size: 13px !important;
    text-transform: uppercase;
}
.tab-nav button.selected {
    background: rgba(20,10,0,0.95) !important;
    color: #FF8C00 !important;
    border-bottom: 2px solid #FF8C00 !important;
    text-shadow: 0 0 8px rgba(255,140,0,0.5);
}
.tab-nav button:hover:not(.selected) {
    color: #FFD700 !important;
    border-color: rgba(255,215,0,0.3) !important;
}

/* ── Cards ──────────────────────────────────────────────── */
.olist-card {
    background: rgba(10,5,0,0.92);
    border: 1px solid rgba(255,140,0,0.3);
    border-radius: 8px;
    padding: 20px;
    margin: 8px 0;
    transition: border-color 0.2s ease, box-shadow 0.2s ease;
    position: relative;
    overflow: hidden;
}
.olist-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0; height: 2px;
    background: linear-gradient(90deg, transparent, rgba(255,140,0,0.5), transparent);
}
.olist-card:hover {
    border-color: rgba(255,140,0,0.6);
    box-shadow: 0 0 20px rgba(255,140,0,0.1);
}


.kpi-red    { color: #FF4444; }
.kpi-orange { color: #FF8C00; }
.kpi-gold   { color: #FFD700; }
.kpi-green  { color: #00C851; }

/* ── Page header ────────────────────────────────────────── */
.page-header {
    background: linear-gradient(135deg, rgba(15,8,0,0.95) 0%, rgba(25,12,0,0.9) 100%);
    border-bottom: 1px solid rgba(255,140,0,0.25);
    border-left: 3px solid #FF8C00;
    padding: 16px 24px;
    margin-bottom: 16px;
    border-radius: 0 8px 8px 0;
    box-shadow: 0 0 30px rgba(255,140,0,0.06);
}
.page-title {
    font-size: 1.4rem;
    font-weight: 700;
    color: #FFD700;
    margin: 0;
    letter-spacing: 2px;
    text-shadow: 0 0 12px rgba(255,215,0,0.4);
    text-transform: uppercase;
}
.page-subtitle {
    font-size: 0.875rem;
    color: rgba(255,140,0,0.75);
    margin: 4px 0 0 0;
    letter-spacing: 1px;
}

/* ── Nav tiles (launchpad) ──────────────────────────────── */
.nav-tile {
    background: rgba(10,5,0,0.92);
    border: 1px solid rgba(255,140,0,0.28);
    border-radius: 8px;
    padding: 20px;
    text-align: center;
    cursor: pointer;
    transition: all 0.25s ease;
    text-decoration: none;
    display: block;
    position: relative;
    overflow: hidden;
}
.nav-tile::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0; height: 2px;
    background: linear-gradient(90deg, transparent, rgba(255,140,0,0.6), transparent);
}
.nav-tile:hover {
    border-color: rgba(255,140,0,0.65);
    background: rgba(18,9,0,0.95);
    transform: translateY(-2px);
    box-shadow: 0 8px 28px rgba(255,140,0,0.18);
}
.nav-tile-icon { font-size: 2.2rem; margin-bottom: 10px; }
.nav-tile-title {
    font-size: 1rem; font-weight: 700; color: #FFD700;
    letter-spacing: 1px; text-transform: uppercase;
}
.nav-tile-owner { font-size: 0.8rem; color: rgba(255,140,0,0.65); margin-top: 4px; }
.nav-tile-badge {
    display: inline-block;
    font-size: 0.7rem;
    padding: 2px 8px;
    border-radius: 3px;
    margin-top: 8px;
    font-weight: 700;
    letter-spacing: 1px;
    text-transform: uppercase;
}
.badge-live    { background: rgba(0,200,81,0.1);   color: #00C851; border: 1px solid rgba(0,200,81,0.25); }
.badge-batch   { background: rgba(255,140,0,0.1);  color: #FF8C00; border: 1px solid rgba(255,140,0,0.25); }
.badge-offline { background: rgba(80,80,80,0.1);   color: rgba(255,255,255,0.25); border: 1px solid rgba(80,80,80,0.2); }

/* ── Status indicators ──────────────────────────────────── */
.status-dot {
    display: inline-block;
    width: 8px; height: 8px;
    border-radius: 50%;
    margin-right: 6px;
    vertical-align: middle;
}
.status-ok      { background: #00C851; box-shadow: 0 0 6px #00C851; }
.status-warn     { background: #FFD700; box-shadow: 0 0 6px #FFD700; }
.status-error    { background: #FF4444; box-shadow: 0 0 6px #FF4444; }
.status-inactive { background: #606060; }

/* ── Tables ─────────────────────────────────────────────── */
table { border-collapse: collapse; width: 100%; }
th {
    background: #262626 !important;
    color: #A0A0A0 !important;
    font-size: 0.82rem;
    text-transform: uppercase;
    letter-spacing: 1px;
    padding: 10px 12px !important;
    border-bottom: 1px solid #2A2A2A !important;
}
td {
    padding: 10px 12px !important;
    border-bottom: 1px solid #1E1E1E !important;
    color: #F0F0F0 !important;
}

/* ── Admin panel ────────────────────────────────────────── */
.admin-danger {
    border: 1px solid rgba(255,68,68,0.35) !important;
    background: rgba(255,68,68,0.04) !important;
    border-radius: 8px;
    padding: 16px;
}
.admin-safe {
    border: 1px solid rgba(0,200,81,0.25) !important;
    background: rgba(0,200,81,0.03) !important;
    border-radius: 8px;
    padding: 16px;
}
.log-output {
    background: rgba(2,1,0,0.95) !important;
    border: 1px solid rgba(255,140,0,0.2) !important;
    border-radius: 6px;
    font-family: 'Space Mono', monospace !important;
    font-size: 0.55rem !important;
    color: #00C851 !important;
    padding: 12px !important;
}

/* ── KPI metrics (amber) — overrides base with bolder sizing ── */
.kpi-value {
    font-size: 2rem;
    font-weight: 900;
    letter-spacing: -0.5px;
    line-height: 1.1;
    text-shadow: 0 0 12px currentColor;
}
.kpi-label {
    font-size: 0.8rem;
    color: #A0A0A0;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    margin-top: 4px;
}

/* ── Section title ──────────────────────────────────────── */
.section-title {
    font-size: 1rem;
    font-weight: 600;
    color: rgba(255,140,0,0.75);
    letter-spacing: 2px;
    text-transform: uppercase;
    border-bottom: 1px solid rgba(255,140,0,0.12);
    padding-bottom: 5px;
    margin: 14px 0 8px;
}

/* ── Pulse animation ─────────────────────────────────────── */
@keyframes amberPulse {
    0%,100% { opacity: 1; box-shadow: 0 0 6px currentColor; }
    50%      { opacity: 0.3; box-shadow: none; }
}
.status-dot { animation: amberPulse 2s ease-in-out infinite; }

/* ── Scrollbars ─────────────────────────────────────────── */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: #0D0D0D; }
::-webkit-scrollbar-thumb { background: #2A2A2A; border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: #FF8C00; }

/* ── Plotly chart backgrounds ───────────────────────────── */
.js-plotly-plot .plotly { background: transparent !important; }

/* ── Compact table (Order History) ──────────────────────── */
.compact-table table { font-size: 11px !important; }
.compact-table th, .compact-table td { padding: 4px 10px !important; line-height: 1.3 !important; }
.compact-table .label { display: none !important; }
"""

# ── Plotly layout defaults (use in every chart) ───────────────
PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(8,4,0,0.85)",
    font=dict(family="Space Mono, monospace", color="#FF8C00", size=12),
    margin=dict(l=40, r=20, t=40, b=40),
    xaxis=dict(gridcolor="rgba(255,140,0,0.08)", linecolor="rgba(255,140,0,0.2)",
               zerolinecolor="rgba(255,140,0,0.15)", tickfont=dict(color="rgba(255,140,0,0.6)")),
    yaxis=dict(gridcolor="rgba(255,140,0,0.08)", linecolor="rgba(255,140,0,0.2)",
               zerolinecolor="rgba(255,140,0,0.15)", tickfont=dict(color="rgba(255,140,0,0.6)")),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color="rgba(255,140,0,0.7)")),
    title=dict(font=dict(color="#FFD700", size=14)),
    colorway=[
        COLORS["orange"], COLORS["gold"], COLORS["green"],
        COLORS["red"], "#00BFFF", "#BF00FF", "#FF69B4"
    ],
)

# ── Shared colour scale (use in px.choropleth, px.bar with color=, etc.) ─────
OLIST_COLORSCALE = [COLORS["red"], COLORS["orange"], COLORS["gold"], COLORS["green"]]
