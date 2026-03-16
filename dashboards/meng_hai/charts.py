"""
dashboards/meng_hai/charts.py
─────────────────────────────
Chart builder functions for Payment Analytics dashboard.
"""

import plotly.express as px
import plotly.graph_objects as go
from copy import deepcopy

from shared.theme import COLORS, PLOTLY_LAYOUT
from shared.components import kpi_card, error_figure
from shared.utils import dev_config_path, make_bq_client_getter

_get_client = make_bq_client_getter(dev_config_path("meng_hai"))

_TYPE_COLORS = [COLORS["orange"], COLORS["gold"], COLORS["green"], COLORS["red"]]


def _fmt(value, prefix="", suffix=""):
    if value is None:
        return "—"
    if value >= 1_000_000:
        return f"{prefix}{value / 1_000_000:.1f}M{suffix}"
    if value >= 1_000:
        return f"{prefix}{value / 1_000:.1f}K{suffix}"
    return f"{prefix}{value:,.0f}{suffix}"


def _layout(**overrides):
    layout = deepcopy(PLOTLY_LAYOUT)
    layout.update(overrides)
    return layout


# ── Existing charts ──────────────────────────────────────────


def load_kpis():
    client, cfg, err = _get_client()
    if err:
        return f'<div style="color:{COLORS["red"]}">GCP not configured — see quick-setup.md</div>'
    from dashboards.meng_hai.queries import get_payment_summary
    try:
        df = get_payment_summary(client, cfg)
        total_rev = df["total_revenue"].sum()
        total_orders = df["orders"].sum()
        aov = total_rev / total_orders if total_orders else 0
        top_type = df.iloc[0]["payment_type"] if len(df) else "—"

        cards = "".join([
            kpi_card("Total Revenue", _fmt(total_rev, prefix="R$ "), color="orange"),
            kpi_card("Total Orders", _fmt(total_orders), color="gold"),
            kpi_card("Avg Order Value", _fmt(aov, prefix="R$ "), color="green"),
            kpi_card("Top Payment Type", top_type.replace("_", " ").title(), color="orange"),
        ])
        return f'<div style="display:flex;gap:12px;flex-wrap:wrap">{cards}</div>'
    except Exception as e:
        return f'<div style="color:{COLORS["red"]}">Error loading KPIs: {e}</div>'


def load_revenue_by_type():
    client, cfg, err = _get_client()
    if err:
        return error_figure("GCP not configured")
    from dashboards.meng_hai.queries import get_payment_summary
    try:
        df = get_payment_summary(client, cfg)
        fig = px.pie(
            df, values="total_revenue", names="payment_type",
            hole=0.45, title="Revenue by Payment Type",
            color_discrete_sequence=_TYPE_COLORS,
        )
        fig.update_traces(textinfo="percent+label", textfont_size=11)
        fig.update_layout(**_layout(height=380, showlegend=False))
        return fig
    except Exception as e:
        return error_figure(f"Error: {e}")


def load_monthly_trend():
    client, cfg, err = _get_client()
    if err:
        return error_figure("GCP not configured")
    from dashboards.meng_hai.queries import get_monthly_revenue_by_type
    try:
        df = get_monthly_revenue_by_type(client, cfg)
        fig = px.area(
            df, x="month", y="revenue", color="payment_type",
            title="Monthly Revenue by Payment Type",
            labels={"month": "Month", "revenue": "Revenue (R$)",
                    "payment_type": "Payment Type"},
            color_discrete_sequence=_TYPE_COLORS,
        )
        fig.update_layout(**_layout(height=380))
        return fig
    except Exception as e:
        return error_figure(f"Error: {e}")


def load_instalment_dist():
    client, cfg, err = _get_client()
    if err:
        return error_figure("GCP not configured")
    from dashboards.meng_hai.queries import get_instalment_distribution
    try:
        df = get_instalment_distribution(client, cfg)
        fig = px.bar(
            df, x="instalments", y="orders",
            title="Credit Card Instalment Distribution",
            labels={"instalments": "Instalments", "orders": "Orders"},
            color_discrete_sequence=[COLORS["green"]],
        )
        fig.update_layout(**_layout(height=380))
        return fig
    except Exception as e:
        return error_figure(f"Error: {e}")


def load_cancellation_trend():
    client, cfg, err = _get_client()
    if err:
        return error_figure("GCP not configured")
    from dashboards.meng_hai.queries import get_cancellation_rate
    try:
        df = get_cancellation_rate(client, cfg)
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df["month"], y=df["cancel_rate_pct"],
            mode="lines+markers",
            line=dict(color=COLORS["red"], width=2),
            fill="tozeroy",
            fillcolor="rgba(255,68,68,0.1)",
            name="Cancellation Rate (%)",
            marker=dict(size=5),
        ))
        fig.update_layout(**_layout(
            title="Monthly Cancellation Rate",
            height=380,
            yaxis=dict(
                gridcolor="rgba(255,140,0,0.08)",
                linecolor="rgba(255,140,0,0.2)",
                zerolinecolor="rgba(255,140,0,0.15)",
                tickfont=dict(color="rgba(255,140,0,0.6)"),
                ticksuffix="%",
            ),
        ))
        return fig
    except Exception as e:
        return error_figure(f"Error: {e}")


# ── New mart-based charts ────────────────────────────────────


def load_payment_overview():
    client, cfg, err = _get_client()
    if err:
        return error_figure("GCP not configured")
    from dashboards.meng_hai.queries import get_payment_method_overview
    try:
        df = get_payment_method_overview(client, cfg)
        fig = px.bar(
            df, y="payment_type", x="total_revenue",
            orientation="h",
            title="Payment Method Overview",
            labels={"payment_type": "Payment Type", "total_revenue": "Revenue (R$)"},
            color="payment_type",
            color_discrete_sequence=_TYPE_COLORS,
            text="payment_count",
        )
        fig.update_traces(texttemplate="%{text:,} payments", textposition="outside")
        fig.update_layout(**_layout(
            height=380, showlegend=False,
            yaxis=dict(categoryorder="total ascending"),
        ))
        return fig
    except Exception as e:
        return error_figure(f"Error: {e}")


def load_payment_by_geo():
    client, cfg, err = _get_client()
    if err:
        return error_figure("GCP not configured")
    from dashboards.meng_hai.queries import get_payment_by_geo
    try:
        df = get_payment_by_geo(client, cfg)
        fig = px.bar(
            df, x="location", y="total_revenue", color="payment_type",
            title="Payment by Customer State (Top 10)",
            labels={"location": "State", "total_revenue": "Revenue (R$)",
                    "payment_type": "Payment Type"},
            color_discrete_sequence=_TYPE_COLORS,
            barmode="stack",
        )
        fig.update_layout(**_layout(height=380))
        return fig
    except Exception as e:
        return error_figure(f"Error: {e}")


def load_payment_by_product():
    client, cfg, err = _get_client()
    if err:
        return error_figure("GCP not configured")
    from dashboards.meng_hai.queries import get_payment_by_product
    try:
        df = get_payment_by_product(client, cfg)
        fig = px.bar(
            df, x="category", y="total_revenue", color="payment_type",
            title="Payment by Product Category (Top 10)",
            labels={"category": "Category", "total_revenue": "Revenue (R$)",
                    "payment_type": "Payment Type"},
            color_discrete_sequence=_TYPE_COLORS,
            barmode="stack",
        )
        fig.update_layout(**_layout(height=380, xaxis_tickangle=-35))
        return fig
    except Exception as e:
        return error_figure(f"Error: {e}")


def load_payment_by_price_band():
    client, cfg, err = _get_client()
    if err:
        return error_figure("GCP not configured")
    from dashboards.meng_hai.queries import get_payment_by_price_band
    try:
        df = get_payment_by_price_band(client, cfg)
        band_order = ["0-50", "50-100", "100-200", "200-500", "500-1000", "1000+"]
        fig = px.bar(
            df, x="price_band", y="total_revenue", color="payment_type",
            title="Payment by Price Band",
            labels={"price_band": "Price Band (R$)", "total_revenue": "Revenue (R$)",
                    "payment_type": "Payment Type"},
            color_discrete_sequence=_TYPE_COLORS,
            barmode="group",
            category_orders={"price_band": band_order},
        )
        fig.update_layout(**_layout(height=380))
        return fig
    except Exception as e:
        return error_figure(f"Error: {e}")
