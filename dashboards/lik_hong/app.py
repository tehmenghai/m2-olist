"""
dashboards/lik_hong/app.py — Customer 360 + Next Best Action
──────────────────────────────────────────────────────────────
Owner: Lik Hong
Standalone: python dashboards/lik_hong/app.py
Merged via: app.py → gr.Tab("👤 Customer 360")
Exports: dashboard (gr.Blocks)
"""

import gradio as gr
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd

from shared.theme import olist_theme, CUSTOM_CSS, COLORS, PLOTLY_LAYOUT, FONT_HEAD
from shared.components import (
    page_header, kpi_row, section_title, alert_box,
    status_row, freshness_badge, empty_state, error_figure
)
from shared.utils import dev_config_path, make_bq_client_getter

_get_client = make_bq_client_getter(dev_config_path("lik_hong"))

# ── Customer 360 Radar helpers ─────────────────────────────────

_RADAR_CATS = ["Recency", "Frequency", "Monetary", "Satisfaction", "Loyalty", "Diversity"]
# Portfolio benchmarks (Champion / Average / At-Risk)
_BENCH_CHAMP = [90, 88, 85, 95, 82, 75]
_BENCH_AVG   = [52, 45, 48, 72, 38, 50]


def _score_customer(profile: dict) -> list[float]:
    """Normalise a customer profile dict into 0-100 radar scores."""
    days   = float(profile.get("days_since_last_order") or 365)
    orders = float(profile.get("total_orders") or 1)
    spend  = float(profile.get("total_spend") or 0)
    score  = float(profile.get("avg_review_score") or 3.0)

    recency     = max(0.0, 100.0 - days / 3.0)          # 0d→100, 300d→0
    frequency   = min(100.0, orders * 20.0)              # 5+ orders→100
    monetary    = min(100.0, spend / 10.0)               # R$1000+→100
    satisfaction = (score / 5.0) * 100.0
    loyalty     = min(100.0, 60.0 + (20.0 if orders >= 5 else 0)
                      + (20.0 if spend > 500 else 0))
    diversity   = min(100.0, orders * 15.0)              # proxy via orders

    return [round(x, 1) for x in [recency, frequency, monetary, satisfaction, loyalty, diversity]]


def build_radar_portfolio():
    """Default radar: Champions vs Average portfolio (shown before any lookup)."""
    cats  = _RADAR_CATS + [_RADAR_CATS[0]]
    champ = _BENCH_CHAMP + [_BENCH_CHAMP[0]]
    avg   = _BENCH_AVG   + [_BENCH_AVG[0]]

    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=champ, theta=cats, fill="toself", name="Champions",
        fillcolor="rgba(0,200,81,0.15)",
        line=dict(color="#00C851", width=2),
        marker=dict(size=5, color="#00C851"),
    ))
    fig.add_trace(go.Scatterpolar(
        r=avg, theta=cats, fill="toself", name="Avg Customer",
        fillcolor="rgba(255,140,0,0.10)",
        line=dict(color=COLORS["orange"], width=1.5, dash="dot"),
        marker=dict(size=4, color=COLORS["orange"]),
    ))
    _layout = {
        **PLOTLY_LAYOUT,
        "title": "Customer 360° — Portfolio Benchmark",
        "showlegend": True,
        "legend": dict(font=dict(size=10), bgcolor="rgba(0,0,0,0)",
                       x=0.78, y=0.08, orientation="v"),
        "polar": dict(
            bgcolor="rgba(8,4,0,0.6)",
            radialaxis=dict(visible=True, range=[0, 100], showticklabels=False,
                            gridcolor="rgba(255,140,0,0.12)",
                            linecolor="rgba(255,140,0,0.15)"),
            angularaxis=dict(tickfont=dict(size=11, color=COLORS["gold"]),
                             gridcolor="rgba(255,140,0,0.1)",
                             linecolor="rgba(255,140,0,0.2)"),
        ),
        "height": 608,
    }
    fig.update_layout(**_layout)
    return fig


def build_radar_customer(profile: dict, customer_id: str):
    """Radar for a single looked-up customer vs Champion benchmark, with key info panel."""
    scores  = _score_customer(profile)
    cats    = _RADAR_CATS + [_RADAR_CATS[0]]
    cust    = scores + [scores[0]]
    champ   = _BENCH_CHAMP + [_BENCH_CHAMP[0]]
    segment = _infer_segment(profile)

    # Derive display values
    city       = profile.get("customer_city", "—").title()
    state      = profile.get("customer_state", "—")
    orders     = profile.get("total_orders", "—")
    spend      = profile.get("total_spend", 0)
    spend_fmt  = f"R$ {float(spend):,.0f}" if spend else "—"
    aov        = profile.get("avg_order_value", 0)
    aov_fmt    = f"R$ {float(aov):,.0f}" if aov else "—"
    score      = profile.get("avg_review_score", "—")
    days       = profile.get("days_since_last_order", "—")
    last_order = profile.get("last_order_date", "—")
    cid_short  = customer_id[:20] + "…" if len(customer_id) > 20 else customer_id

    # Segment colour
    seg_color = {
        "Champions": "#00C851", "Loyal Customers": "#FF8C00",
        "Recent Customers": "#FFD700", "At Risk": "#FF8C00",
        "Lost": "#FF4444", "Potential Loyalists": "#FFD700",
    }.get(segment, "#A0A0A0")

    info_text = (
        f"<b style='color:#FFD700'>{cid_short}</b><br>"
        f"<span style='color:{seg_color}'>{segment}</span><br><br>"
        f"📍 {city}, {state}<br>"
        f"📦 Orders: <b>{orders}</b><br>"
        f"💰 Spend: <b>{spend_fmt}</b><br>"
        f"🛒 AOV: <b>{aov_fmt}</b><br>"
        f"⭐ Avg Score: <b>{score}</b><br>"
        f"🕒 Days inactive: <b>{days}</b><br>"
        f"📅 Last order: <b>{last_order}</b>"
    )

    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=champ, theta=cats, fill="toself", name="Champion Benchmark",
        fillcolor="rgba(0,200,81,0.08)",
        line=dict(color="#00C851", width=1, dash="dot"),
        marker=dict(size=3, color="#00C851"),
    ))
    fig.add_trace(go.Scatterpolar(
        r=cust, theta=cats, fill="toself", name="Customer",
        fillcolor="rgba(255,140,0,0.18)",
        line=dict(color=COLORS["gold"], width=2),
        marker=dict(size=6, color=COLORS["gold"]),
        text=[f"{s}%" for s in scores],
        hovertemplate="%{theta}: %{r:.0f}%<extra></extra>",
    ))
    _layout = {
        **PLOTLY_LAYOUT,
        "title": dict(text="Customer 360° Profile", font=dict(color="#FFD700", size=14), x=0.5),
        "showlegend": True,
        "legend": dict(font=dict(size=10), bgcolor="rgba(0,0,0,0)",
                       x=0.72, y=0.08, orientation="v"),
        "polar": dict(
            domain=dict(x=[0, 0.65]),          # radar occupies left 65%
            bgcolor="rgba(8,4,0,0.6)",
            radialaxis=dict(visible=True, range=[0, 100], showticklabels=False,
                            gridcolor="rgba(255,140,0,0.12)",
                            linecolor="rgba(255,140,0,0.15)"),
            angularaxis=dict(tickfont=dict(size=12, color=COLORS["gold"]),
                             gridcolor="rgba(255,140,0,0.1)",
                             linecolor="rgba(255,140,0,0.2)"),
        ),
        "annotations": [dict(
            x=1.0, y=1.0,             # top-right corner
            xanchor="right", yanchor="top",
            xref="paper", yref="paper",
            text=info_text,
            showarrow=False,
            align="left",
            font=dict(size=11, color="rgba(255,220,150,0.9)", family="Space Mono"),
            bgcolor="rgba(8,4,0,0.90)",
            bordercolor=seg_color,
            borderwidth=1,
            borderpad=10,
        )],
        "height": 608,
    }
    fig.update_layout(**_layout)
    return fig


# ── Sankey helpers ─────────────────────────────────────────────

import re as _re

_SANKEY_COLORS = {
    "Champions":           "rgba(0,200,81,0.9)",
    "Loyal Customers":     "rgba(255,140,0,0.9)",
    "Recent Customers":    "rgba(255,215,0,0.85)",
    "Potential Loyalists": "rgba(255,200,80,0.75)",
    "At Risk":             "rgba(255,100,30,0.85)",
    "Lost":                "rgba(140,100,60,0.7)",
    "credit_card":         "rgba(255,160,50,0.85)",
    "boleto":              "rgba(255,215,100,0.8)",
    "voucher":             "rgba(100,200,255,0.75)",
    "debit_card":          "rgba(150,180,255,0.75)",
    "unknown":             "rgba(150,150,150,0.6)",
    "Delivered":           "rgba(0,200,81,0.85)",
    "Canceled":            "rgba(255,68,68,0.85)",
    "In Progress":         "rgba(255,215,0,0.75)",
    "Satisfied (4-5★)":    "rgba(0,200,81,0.85)",
    "Neutral (3★)":        "rgba(255,215,0,0.8)",
    "Unhappy (1-2★)":      "rgba(255,68,68,0.85)",
    "No Review":           "rgba(140,140,140,0.6)",
}
_CAT_PALETTE = [
    "rgba(255,140,0,0.75)", "rgba(255,170,50,0.75)", "rgba(255,200,80,0.7)",
    "rgba(200,120,0,0.75)", "rgba(180,100,20,0.7)",  "rgba(220,150,30,0.75)",
    "rgba(240,180,60,0.7)", "rgba(160,90,10,0.75)",  "rgba(210,130,20,0.7)",
    "rgba(230,160,40,0.7)",
]


def _fade_rgba(rgba: str, alpha: float) -> str:
    m = _re.match(r'rgba\((\d+),(\d+),(\d+),', rgba)
    if m:
        return f"rgba({m.group(1)},{m.group(2)},{m.group(3)},{alpha})"
    return f"rgba(255,140,0,{alpha})"


def _build_sankey_fig(df: pd.DataFrame, col_order: list, title: str) -> go.Figure:
    """Build a Plotly Sankey from a dataframe with flow columns + 'order_count' column."""
    if df is None or df.empty:
        return error_figure(f"No journey data")
    node_labels, node_colors, seen, cat_idx = [], [], set(), 0
    for col in col_order:
        for val in df[col].dropna().unique():
            key = str(val)
            if key not in seen:
                node_labels.append(key)
                seen.add(key)
                c = _SANKEY_COLORS.get(key)
                if c is None:
                    c = _CAT_PALETTE[cat_idx % len(_CAT_PALETTE)]
                    cat_idx += 1
                node_colors.append(c)
    node_idx = {n: i for i, n in enumerate(node_labels)}
    sources, targets, values, link_colors = [], [], [], []
    for i in range(len(col_order) - 1):
        edges = df.groupby([col_order[i], col_order[i + 1]])["order_count"].sum()
        for (src, tgt), val in edges.items():
            si, ti = node_idx[str(src)], node_idx[str(tgt)]
            sources.append(si)
            targets.append(ti)
            values.append(int(val))
            link_colors.append(_fade_rgba(node_colors[si], 0.18))
    fig = go.Figure(go.Sankey(
        arrangement="snap",
        node=dict(
            label=node_labels,
            color=node_colors,
            pad=22, thickness=20,
            line=dict(color="rgba(0,0,0,0.25)", width=0.5),
            hovertemplate="%{label}: %{value:,} orders<extra></extra>",
        ),
        link=dict(
            source=sources, target=targets, value=values,
            color=link_colors,
            hovertemplate="%{source.label} → %{target.label}: %{value:,}<extra></extra>",
        ),
    ))
    fig.update_layout(**{
        **PLOTLY_LAYOUT,
        "title": title,
        "height": 420,
        "font": dict(size=11, color=COLORS["text_secondary"]),
    })
    return fig


def _build_customer_journey_from_history(df) -> go.Figure:
    """Derive customer journey Sankey from the order-history dataframe (no extra BQ call)."""
    if df is None or (hasattr(df, "empty") and df.empty):
        return error_figure("No order history to build journey")
    flow = df.copy()
    flow["delivery_outcome"] = flow["order_status"].map(
        lambda s: "Delivered" if s == "delivered" else
                  "Canceled" if s == "canceled" else "In Progress"
    )
    flow["review_bucket"] = flow["review_score"].apply(
        lambda r: "Satisfied (4-5★)" if pd.notna(r) and float(r) >= 4
        else "Neutral (3★)"    if pd.notna(r) and float(r) >= 3
        else "Unhappy (1-2★)"  if pd.notna(r)
        else "No Review"
    )
    flow["category"]     = flow["category"].fillna("Other")
    flow["payment_type"] = flow["payment_type"].fillna("unknown")
    flow["order_count"]  = 1
    cols = ["category", "payment_type", "delivery_outcome", "review_bucket"]
    flow = flow.groupby(cols, dropna=False)["order_count"].sum().reset_index()
    return _build_sankey_fig(flow, cols, "Customer Journey Flow")


# ── Data loaders ──────────────────────────────────────────────

def load_rfm_chart():
    client, cfg, err = _get_client()
    if err:
        return error_figure("GCP not configured — see quick-setup.md")
    from dashboards.lik_hong.queries import get_rfm_segments
    try:
        df = get_rfm_segments(client, cfg)
        if df.empty:
            return error_figure("No RFM segments found — ensure Gold tables are populated")
        _SEGMENT_RANK = [
            "Champions", "Loyal Customers", "Potential Loyalists",
            "Recent Customers", "At Risk", "Lost",
        ]
        ordered = [s for s in _SEGMENT_RANK if s in df["segment"].values]
        fig = px.bar(
            df, x="segment", y="customer_count",
            color="avg_monetary",
            color_continuous_scale=["#FF4444", "#FF8C00", "#FFD700", "#00C851"],
            labels={"customer_count": "Customers", "segment": "Segment",
                    "avg_monetary": "Avg Spend (R$)"},
            title="",
            category_orders={"segment": ordered},
        )
        fig.update_layout(**PLOTLY_LAYOUT)
        return fig
    except Exception as e:
        return error_figure(f"Error: {e}")


def load_revenue_trend():
    client, cfg, err = _get_client()
    if err:
        return error_figure("GCP not configured — see quick-setup.md")
    from dashboards.lik_hong.queries import get_revenue_trend
    try:
        df = get_revenue_trend(client, cfg)
        if df.empty:
            return error_figure("No revenue data available")
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df["month"], y=df["revenue"],
            mode="lines+markers",
            line=dict(color=COLORS["orange"], width=2),
            fill="tozeroy",
            fillcolor="rgba(255,140,0,0.1)",
            name="Revenue (R$)",
        ))
        fig.add_trace(go.Bar(
            x=df["month"], y=df["unique_customers"],
            name="Unique Customers",
            marker_color=COLORS["gold"],
            opacity=0.5,
            yaxis="y2",
        ))
        _layout = {
            **PLOTLY_LAYOUT,
            "title": "",
            "yaxis2": dict(overlaying="y", side="right", gridcolor="#2A2A2A"),
        }
        fig.update_layout(**_layout)
        return fig
    except Exception as e:
        return error_figure(f"Error: {e}")


def load_portfolio_bubble():
    """Bubble chart: segments plotted by Recency × Monetary, sized by customer count."""
    client, cfg, err = _get_client()
    if err:
        return error_figure("GCP not configured — see quick-setup.md")
    from dashboards.lik_hong.queries import get_portfolio_radar
    try:
        df = get_portfolio_radar(client, cfg)
        if df.empty:
            return error_figure("No segment data available")
        seg_colors = {
            "Champions":           "#00C851",
            "Loyal Customers":     "#FF8C00",
            "Recent Customers":    "#FFD700",
            "Potential Loyalists": "#FFD700",
            "At Risk":             "#FF4444",
            "Lost":                "#888888",
        }
        max_count = df["customer_count"].max()
        fig = go.Figure()
        for _, row in df.iterrows():
            seg = row["segment"]
            color = seg_colors.get(seg, "#A0A0A0")
            count = int(row["customer_count"])
            size = max(18, min(75, float(count) / max_count * 75))
            fig.add_trace(go.Scatter(
                x=[float(row["recency"])],
                y=[float(row["monetary"])],
                mode="markers+text",
                name=f"{seg} ({count:,})",
                text=[seg],
                textposition="top center",
                textfont=dict(size=10, color=color),
                marker=dict(
                    size=size,
                    color=color,
                    opacity=0.82,
                    line=dict(color="rgba(255,255,255,0.25)", width=1.5),
                ),
                hovertemplate=(
                    f"<b>{seg}</b><br>"
                    f"Customers: {count:,}<br>"
                    "Recency: %{x:.0f}<br>"
                    f"Frequency: {row['frequency']:.0f}<br>"
                    "Monetary: %{y:.0f}<br>"
                    f"Satisfaction: {row['satisfaction']:.0f}<br>"
                    f"Loyalty: {row['loyalty']:.0f}<br>"
                    f"Diversity: {row['diversity']:.0f}"
                    "<extra></extra>"
                ),
                showlegend=True,
            ))
        fig.update_layout(**{
            **PLOTLY_LAYOUT,
            "title": "",
            "xaxis": dict(
                title="Recency Score (0–100)", range=[-5, 110],
                gridcolor="rgba(255,140,0,0.10)", zeroline=False,
                tickfont=dict(size=10, color=COLORS["text_muted"]),
            ),
            "yaxis": dict(
                title="Monetary Score (0–100)", range=[-5, 110],
                gridcolor="rgba(255,140,0,0.10)", zeroline=False,
                tickfont=dict(size=10, color=COLORS["text_muted"]),
            ),
            "showlegend": True,
            "legend": dict(
                font=dict(size=10), bgcolor="rgba(0,0,0,0.5)",
                bordercolor="rgba(255,140,0,0.15)", borderwidth=1,
                x=0.99, y=0.02, xanchor="right", yanchor="bottom",
                orientation="v",
            ),
            "height": 420,
        })
        return fig
    except Exception as e:
        return error_figure(f"Error: {e}")


def load_category_heatmap():
    """Segment × category purchase frequency heatmap."""
    client, cfg, err = _get_client()
    if err:
        return error_figure("GCP not configured")
    from dashboards.lik_hong.queries import get_category_affinity
    try:
        df = get_category_affinity(client, cfg)
        if df.empty:
            return error_figure("No data")
        pivot = df.pivot_table(
            index="category", columns="segment",
            values="purchase_count", fill_value=0,
        )
        seg_order = ["Champions", "Loyal Customers", "Recent Customers",
                     "Potential Loyalists", "At Risk", "Lost"]
        cols = [c for c in seg_order if c in pivot.columns] + \
               [c for c in pivot.columns if c not in seg_order]
        pivot = pivot[cols]
        fig = go.Figure(go.Heatmap(
            z=pivot.values,
            x=pivot.columns.tolist(),
            y=pivot.index.tolist(),
            colorscale=[[0,"rgba(20,10,0,0.9)"], [0.5,"rgba(255,140,0,0.6)"],
                        [1,"rgba(255,215,0,0.95)"]],
            showscale=True,
            colorbar=dict(tickfont=dict(size=9, color=COLORS["text_muted"]),
                          thickness=12),
            hovertemplate="<b>%{y}</b><br>%{x}: %{z:,} purchases<extra></extra>",
        ))
        fig.update_layout(**{
            **PLOTLY_LAYOUT,
            "title": "Category Affinity by Segment",
            "xaxis": dict(tickfont=dict(size=10), tickangle=-20),
            "yaxis": dict(tickfont=dict(size=10)),
            "height": 480,
            "margin": dict(l=160, r=40, t=48, b=60),
        })
        return fig
    except Exception as e:
        return error_figure(f"Error: {e}")


def load_funnel_chart():
    """Repeat-purchase funnel: 1 → 2 → 3-4 → 5+ orders."""
    client, cfg, err = _get_client()
    if err:
        return error_figure("GCP not configured")
    from dashboards.lik_hong.queries import get_purchase_funnel
    try:
        df = get_purchase_funnel(client, cfg)
        if df.empty:
            return error_figure("No data")
        first = df["customers"].iloc[0]
        df["conversion_pct"] = (df["customers"] / first * 100).round(1)
        fig = go.Figure(go.Funnel(
            y=df["bucket"].tolist(),
            x=df["customers"].tolist(),
            textinfo="value+percent initial",
            textfont=dict(size=13, color="#fff"),
            marker=dict(
                color=["#FF8C00", "#FFD700", "#00C851", "#00C851"],
                line=dict(color="rgba(0,0,0,0.4)", width=1),
            ),
            connector=dict(line=dict(color="rgba(255,140,0,0.3)", width=1)),
        ))
        fig.update_layout(**{
            **PLOTLY_LAYOUT,
            "title": "Repeat Purchase Funnel",
            "funnelmode": "stack",
        })
        return fig
    except Exception as e:
        return error_figure(f"Error: {e}")


def load_portfolio_journey() -> go.Figure:
    """Sankey: segment → payment_type → delivery_outcome → review_bucket (all customers)."""
    client, cfg, err = _get_client()
    if err:
        return error_figure("GCP not configured")
    from dashboards.lik_hong.queries import get_portfolio_journey
    try:
        df = get_portfolio_journey(client, cfg)
        return _build_sankey_fig(
            df,
            ["segment", "payment_type", "delivery_outcome", "review_bucket"],
            "",
        )
    except Exception as e:
        return error_figure(f"Error: {e}")


def load_customer_profile(customer_id: str):
    """Returns (summary_md, order_table, radar_fig, profile_dict, journey_fig)."""
    _empty = (build_radar_portfolio(), {}, error_figure("No journey data"))
    if not customer_id.strip():
        return "Enter a customer ID to look up their 360° profile.", None, *_empty
    client, cfg, err = _get_client()
    if err:
        return f"GCP not configured: {err}", None, *_empty
    from dashboards.lik_hong.queries import get_customer_profile, get_order_history
    try:
        profile = get_customer_profile(client, cfg, customer_id.strip())
        if not profile:
            return "Customer not found.", None, *_empty
        cid = profile.get('customer_unique_id', '—')
        summary = (
            f"**Customer:** `{cid}`  \n"
            f"**Location:** {profile.get('customer_city','—')}, {profile.get('customer_state','—')}  \n"
            f"**Total Orders:** {profile.get('total_orders','—')}  \n"
            f"**Total Spend:** R$ {profile.get('total_spend','—')}  \n"
            f"**Avg Order Value:** R$ {profile.get('avg_order_value','—')}  \n"
            f"**Last Order:** {profile.get('last_order_date','—')}  \n"
            f"**Days Since Last Order:** {profile.get('days_since_last_order','—')}  \n"
            f"**Avg Review Score:** {profile.get('avg_review_score','—')} / 5.0  \n"
        )
        history = get_order_history(client, cfg, customer_id.strip())
        radar   = build_radar_customer(profile, cid)
        journey = _build_customer_journey_from_history(history)
        return summary, history, radar, profile, journey
    except Exception as e:
        return f"Error: {e}", None, *_empty


_SEGMENTS = ["All", "Champions", "Loyal Customers", "Recent Customers", "At Risk", "Lost", "Potential Loyalists"]

# Customer-facing NBA (personalised action to offer/show the customer)
_CUSTOMER_NBA = {
    "Champions":           ("🏆 VIP Exclusive", "#FFD700",
                            "You're one of our top customers! Enjoy early access to new arrivals "
                            "and an invitation to our exclusive VIP loyalty tier."),
    "Loyal Customers":     ("🎁 Loyalty Reward", "#FF8C00",
                            "Thank you for your continued loyalty. Here's a personalised discount "
                            "on your favourite categories — valid for 14 days."),
    "Recent Customers":    ("🌟 Welcome Bonus", "#00C851",
                            "Great to have you! Complete your second purchase within 30 days "
                            "to unlock loyalty points and free shipping on your next order."),
    "At Risk":             ("💌 We Miss You", "#FF8C00",
                            "It's been a while! We have a special comeback offer — 15% off "
                            "your next order on us. Expires in 7 days."),
    "Lost":                ("🔁 Come Back Offer", "#FF4444",
                            "We'd love to reconnect. Exclusive win-back offer: free shipping "
                            "+ R$50 voucher on orders above R$200. Limited time."),
    "Potential Loyalists": ("⭐ Almost VIP", "#FFD700",
                            "You're close to VIP status! One more purchase unlocks exclusive "
                            "benefits including priority support and member-only deals."),
}
_CUSTOMER_NBA_DEFAULT = ("📋 Review Profile", "#A0A0A0",
                         "Look up a customer to generate a personalised next best action.")

# Management NBA (exec-level strategic actions per segment)
_EXEC_ADVICE = {
    "Champions":           ("Protect revenue base — invest in exclusivity and loyalty tiers to retain top cohort.",       "Refresh Feature Store cadence. Champion cohort is highest-signal ML training data.",      "Activate as brand advocates. Referral programmes yield 3–5× ROAS on this cohort.",     "Zero tolerance on fulfillment SLA for Champions. Prioritise same-day dispatch."),
    "Loyal Customers":     ("Upsell to premium tier — Loyal Customers are graduation candidates for Champions.",          "Monitor repeat-purchase latency. Trigger loyalty nudge at day 60 of inactivity.",       "Launch cross-sell campaigns using purchase-category affinity data.",                     "Ensure stock availability for top categories — Loyal segment drives steady repeat."),
    "Recent Customers":    ("Convert first-timers before the 90-day drop-off window. Second purchase is critical.",       "Onboarding event stream: send day-7, day-30, day-60 nudges via CDC pipeline.",           "Welcome series + second-purchase incentive. Category affinity not yet established.",     "First-order experience is paramount — escalate any delivery failure immediately."),
    "At Risk":             ("Investigate product gaps or pricing issues driving defection from previously loyal buyers.",  "Deploy real-time win-back trigger via CDC. Alert within 24 h of inactivity threshold.",  "Personalised re-engagement offer on last purchased category. 30-day urgency window.",    "Audit delivery and return SLAs for At-Risk cohort. Operational friction drives churn."),
    "Lost":                ("Root-cause analysis on Lost segment — identify product gaps or pricing driving defection.",  "Feature Store (Redis) latency within SLA. Reduce Silver-layer lag to under 5 min.",      "Pause brand spend on Lost. Reallocate budget to lookalike audiences modelled on Champions.", "Streamline returns and fulfilment for mid-tier to reduce cost-to-serve and improve NPS."),
    "Potential Loyalists": ("Accelerate conversion with time-limited loyalty programme invitation.",                      "A/B test personalisation model on Potential Loyalists — ideal undecided-cohort test.",   "Targeted mid-funnel content. Category affinity is starting to form — act now.",          "Fulfillment reliability is the key conversion lever for this undecided segment."),
}
_EXEC_DEFAULT = (
    "Review customer portfolio health and define strategic priorities.",
    "Ensure data pipeline freshness for accurate segment classification.",
    "Tailor messaging to the customer's current lifecycle stage.",
    "Monitor fulfilment SLA across all active segments.",
)

def _infer_segment(p: dict) -> str:
    days   = float(p.get("days_since_last_order") or 999)
    orders = int(p.get("total_orders") or 0)
    if days < 90  and orders >= 5: return "Champions"
    if days < 180 and orders >= 3: return "Loyal Customers"
    if days < 90:                  return "Recent Customers"
    if days > 180 and orders >= 3: return "At Risk"
    if days > 365:                 return "Lost"
    return "Potential Loyalists"


def _customer_nba_html(p: dict) -> str:
    """Personalised offer card for the looked-up customer."""
    segment = _infer_segment(p)
    cust_title, cust_color, cust_text = _CUSTOMER_NBA.get(segment, _CUSTOMER_NBA_DEFAULT)
    seg_lbl = (
        f'<div style="margin-bottom:12px;font-size:13px;color:#FFD700;letter-spacing:1px">'
        f'Segment: <strong>{segment}</strong></div>'
    )
    card = (
        f'<div style="margin-bottom:8px;font-size:11px;color:rgba(255,200,100,0.6);'
        f'letter-spacing:2px;text-transform:uppercase">▸ For the Customer</div>'
        f'<div style="background:rgba(10,5,0,0.9);border:1px solid {cust_color}55;'
        f'border-left:3px solid {cust_color};border-radius:6px;padding:14px 18px">'
        f'<div style="font-size:16px;font-weight:700;color:{cust_color};margin-bottom:8px">{cust_title}</div>'
        f'<div style="font-size:14px;color:rgba(255,240,200,0.9);line-height:1.7">{cust_text}</div>'
        f'</div>'
    )
    return f'{seg_lbl}{card}'


def _mgmt_nba_html(segment: str) -> str:
    """C-suite action cards for a given segment (used in All Customers tab)."""
    adv = _EXEC_ADVICE.get(segment, _EXEC_DEFAULT)
    roles = [
        ("CEO", "👔", "#1e3a5f", adv[0]),
        ("CIO", "💻", "#2d1a64", adv[1]),
        ("CMO", "📣", "#6b2d14", adv[2]),
        ("COO", "⚙️", "#0a3a2a", adv[3]),
    ]
    mgmt_cards = "".join(
        f'<div style="background:{bg};border:1px solid rgba(255,255,255,0.13);border-radius:6px;overflow:hidden">'
        f'<div style="padding:9px 12px;border-bottom:1px solid rgba(255,255,255,0.1);color:#fff;'
        f'font-weight:bold;font-size:13px;letter-spacing:1.5px">{icon} {role}</div>'
        f'<div style="padding:11px 12px;color:rgba(255,255,255,0.88);font-size:13px;line-height:1.65">{text}</div>'
        f'</div>'
        for role, icon, bg, text in roles
    )
    seg_color = {
        "Champions": "#00C851", "Loyal Customers": "#FF8C00", "Recent Customers": "#FFD700",
        "Potential Loyalists": "#FFD700", "At Risk": "#FF4444", "Lost": "#888888",
    }.get(segment, "#FF8C00")
    return (
        f'<div style="margin-bottom:12px;font-size:13px;color:{seg_color};letter-spacing:1px">'
        f'Segment: <strong>{segment}</strong></div>'
        f'<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px">{mgmt_cards}</div>'
    )


def get_nba(customer_id: str, profile: dict):
    """Next Best Action — uses cached profile from gr.State; avoids a second BQ query."""
    # customer_id_input may be empty if customer was chosen from the dropdown
    cid = customer_id.strip() or str((profile or {}).get("customer_unique_id", "")).strip()
    if not cid:
        return "Look up a customer first."
    if profile:
        return _customer_nba_html(profile)
    # Fallback: profile not yet cached (NBA clicked before Look Up)
    client, cfg, err = _get_client()
    if err:
        return f"GCP not configured: {err}"
    from dashboards.lik_hong.queries import get_customer_profile
    try:
        p = get_customer_profile(client, cfg, customer_id.strip())
        if not p:
            return "Customer not found."
        return _customer_nba_html(p)
    except Exception as e:
        return f"Error: {e}"


def get_portfolio_nba(segment: str) -> str:
    """Management NBA for a chosen segment (All Customers tab)."""
    if not segment or segment == "— Select Segment —":
        return "<p style='color:rgba(255,200,100,0.4);font-size:13px;padding:12px 0'>Select a segment to see management actions.</p>"
    return _mgmt_nba_html(segment)


def load_kpi_summary():
    """Populate the header KPI strip from BigQuery on dashboard load."""
    client, cfg, err = _get_client()
    if err:
        return ""   # silently skip — KPI strip stays at '—' until GCP is configured
    from dashboards.lik_hong.queries import get_kpi_summary
    from shared.components import kpi_card
    try:
        k = get_kpi_summary(client, cfg)
        if not k:
            return ""
        cards = "".join(kpi_card(**m) for m in [
            {"label": "Total Customers",  "value": f"{int(k.get('total_customers', 0)):,}",  "color": "orange"},
            {"label": "Active (90d)",     "value": f"{int(k.get('active_90d', 0)):,}",        "color": "green"},
            {"label": "At-Risk (180d+)",  "value": f"{int(k.get('at_risk_180d', 0)):,}",      "color": "red"},
            {"label": "Avg Review Score", "value": f"{k.get('avg_review_score', 0):.2f}",     "color": "gold"},
            {"label": "Total Revenue",    "value": f"R$ {k.get('total_revenue', 0):,.0f}",    "color": "orange"},
        ])
        return f'<div style="display:flex;gap:12px;flex-wrap:nowrap;margin:8px 0;width:100%">{cards}</div>'
    except Exception:
        return ""


def do_search(customer_id: str, segment: str):
    """Search customers by partial ID and/or segment, return dropdown choices."""
    client, cfg, err = _get_client()
    if err:
        return gr.update(choices=[], value=None)
    from dashboards.lik_hong.queries import search_customers
    try:
        seg    = "" if (not segment or segment == "All") else segment
        id_pat = customer_id.strip() or "*"
        if "*" not in id_pat and len(id_pat) < 36:
            id_pat += "*"
        df = search_customers(client, cfg, id_pat, seg, limit=100)
        if df.empty:
            return gr.update(choices=["(no results)"], value=None)
        choices = [f"{r.customer_unique_id}  [{r.segment}]" for _, r in df.iterrows()]
        return gr.update(choices=choices, value=None)
    except Exception as e:
        return gr.update(choices=[f"Error: {e}"], value=None)


def initial_load_search():
    """Called on dashboard load — populate dropdown with first 100 customers."""
    return do_search("*", "All")


def load_from_selection(selection: str):
    """Load full profile from a search-result selection (strips the [segment] suffix)."""
    if not selection or selection.startswith("(") or selection.startswith("Error"):
        return "Select a customer from the search results.", None, build_radar_portfolio(), {}, error_figure("Select a customer")
    cid = selection.split("  [")[0].strip()
    return load_customer_profile(cid)


# ── Dashboard UI ──────────────────────────────────────────────
with gr.Blocks(analytics_enabled=False, title="Customer 360") as dashboard:

    page_header(
        "Customer 360 + Next Best Action",
        subtitle="Individual profiles · Portfolio analytics · Personalised actions",
        icon="👤",
    )

    with gr.Tabs():

        # ════════════════════════════════════════════════════════
        # TAB 1 — All Customers
        # ════════════════════════════════════════════════════════
        with gr.Tab("📊 All Customers"):

            # ── KPI strip ──────────────────────────────────────
            kpi_html = kpi_row([
                {"label": "Total Customers",  "value": "—",    "color": "orange"},
                {"label": "Active (90d)",     "value": "—",    "color": "green"},
                {"label": "At-Risk (180d+)",  "value": "—",    "color": "red"},
                {"label": "Avg Review Score", "value": "—",    "color": "gold"},
                {"label": "Total Revenue",    "value": "R$ —", "color": "orange"},
            ])

            # ── Journey flow (top chart) ────────────────────────
            section_title("Journey Flow — Segment · Payment · Delivery · Review", accent="gold")
            portfolio_journey_chart = gr.Plot()

            # ── Bubble + RFM side by side ───────────────────────
            with gr.Row():
                with gr.Column(scale=1):
                    section_title("Segment Landscape — Recency × Monetary Value", accent="gold")
                    portfolio_bubble = gr.Plot()
                with gr.Column(scale=1):
                    section_title("RFM Segmentation", accent="gold")
                    rfm_chart = gr.Plot()

            # ── Revenue trend full width ────────────────────────
            section_title("Monthly Revenue & Customer Trend", accent="orange")
            revenue_chart = gr.Plot()

            # ── Management NBA ──────────────────────────────────
            gr.HTML("<hr style='border-color:rgba(255,68,68,0.25);margin:16px 0 4px'>")
            section_title("Next Best Action — Management Playbook", accent="red")
            with gr.Row():
                portfolio_segment_dd = gr.Dropdown(
                    label="Select Segment",
                    choices=["— Select Segment —", "Champions", "Loyal Customers",
                             "Recent Customers", "Potential Loyalists", "At Risk", "Lost"],
                    value="— Select Segment —",
                    scale=2,
                )
                portfolio_nba_btn = gr.Button("⚡ Generate", variant="secondary", scale=1)
            portfolio_nba_output = gr.HTML(
                "<p style='color:rgba(255,200,100,0.4);font-size:13px;"
                "padding:12px 0'>Select a segment to see C-suite action recommendations.</p>"
            )

        # ════════════════════════════════════════════════════════
        # TAB 2 — By Customer
        # ════════════════════════════════════════════════════════
        with gr.Tab("👤 By Customer"):

            # ── Lookup (left) + Radar (right) ──────────────────
            with gr.Row():
                with gr.Column(scale=1, min_width=300):
                    section_title("Customer Lookup", accent="green")
                    customer_id_input = gr.Textbox(
                        label="Customer ID (supports * wildcard)",
                        placeholder="e.g. 0703cdfb* or leave blank for all…",
                    )
                    segment_filter = gr.Dropdown(
                        label="Segment Filter",
                        choices=_SEGMENTS,
                        value="All",
                    )
                    with gr.Row():
                        search_btn = gr.Button("🔍 Search",  variant="secondary", scale=1)
                        lookup_btn = gr.Button("👤 Look Up", variant="primary",   scale=1)
                    search_results = gr.Dropdown(
                        label="Results — select to load profile",
                        choices=[],
                        interactive=True,
                    )
                    gr.HTML("<hr style='border-color:rgba(255,140,0,0.2);margin:10px 0'>")
                    profile_md = gr.Markdown(
                        "Search or enter a customer ID to view their 360° profile."
                    )

                with gr.Column(scale=2):
                    section_title("Customer 360° Radar", accent="gold")
                    radar_chart = gr.Plot(value=build_radar_portfolio())

            profile_state = gr.State({})

            # ── Next Best Action — full width ───────────────────
            gr.HTML("<hr style='border-color:rgba(255,68,68,0.25);margin:16px 0 4px'>")
            section_title("Next Best Action — Personalised Offer", accent="red")
            with gr.Row():
                nba_btn = gr.Button("⚡ Generate NBA", variant="secondary", scale=1)
            nba_output = gr.HTML(
                "<p style='color:rgba(255,200,100,0.4);font-size:13px;"
                "padding:12px 0'>Look up a customer first.</p>"
            )

            # ── Sankey Journey ──────────────────────────────────
            gr.HTML("<hr style='border-color:rgba(255,140,0,0.2);margin:16px 0 4px'>")
            section_title("Journey Flow — Category · Payment · Delivery · Review", accent="gold")
            journey_chart = gr.Plot(show_label=False)

            # ── Order History ───────────────────────────────────
            gr.HTML("<hr style='border-color:rgba(255,140,0,0.2);margin:16px 0 4px'>")
            section_title("Order History", accent="orange")
            order_table = gr.DataFrame(
                label=None, interactive=False, elem_classes=["compact-table"]
            )

    # ── Wire up events ─────────────────────────────────────────
    dashboard.load(fn=load_kpi_summary,       outputs=kpi_html)
    dashboard.load(fn=initial_load_search,    outputs=search_results)
    dashboard.load(fn=load_portfolio_journey, outputs=portfolio_journey_chart)
    dashboard.load(fn=load_portfolio_bubble,  outputs=portfolio_bubble)
    dashboard.load(fn=load_rfm_chart,         outputs=rfm_chart)
    dashboard.load(fn=load_revenue_trend,     outputs=revenue_chart)

    _lookup_outputs = [profile_md, order_table, radar_chart, profile_state, journey_chart]
    search_btn.click(fn=do_search, inputs=[customer_id_input, segment_filter], outputs=search_results)
    search_results.select(fn=load_from_selection, inputs=search_results, outputs=_lookup_outputs)
    lookup_btn.click(fn=load_customer_profile, inputs=customer_id_input, outputs=_lookup_outputs)
    nba_btn.click(fn=get_nba, inputs=[customer_id_input, profile_state], outputs=nba_output)
    portfolio_nba_btn.click(fn=get_portfolio_nba, inputs=portfolio_segment_dd, outputs=portfolio_nba_output)
    portfolio_segment_dd.change(fn=get_portfolio_nba, inputs=portfolio_segment_dd, outputs=portfolio_nba_output)


if __name__ == "__main__":
    dashboard.launch(server_port=7862, show_error=True, theme=olist_theme, css=CUSTOM_CSS, head=FONT_HEAD)
