"""
dashboards/ben/charts.py
────────────────────────
Chart builder functions for Product Analytics dashboard.
"""

import plotly.express as px
import plotly.graph_objects as go
from copy import deepcopy
import pandas as pd
import numpy as np

from shared.theme import COLORS, PLOTLY_LAYOUT, OLIST_COLORSCALE
from shared.components import kpi_card, error_figure
from shared.utils import dev_config_path, make_bq_client_getter

_get_client = make_bq_client_getter(dev_config_path("ben"))


def _fmt(value, prefix="", suffix=""):
    """Format numeric values with human-readable suffixes (K, M)."""
    if value is None:
        return "—"
    if value >= 1_000_000:
        return f"{prefix}{value / 1_000_000:.1f}M{suffix}"
    if value >= 1_000:
        return f"{prefix}{value / 1_000:.1f}K{suffix}"
    return f"{prefix}{value:,.0f}{suffix}"


def _layout(**overrides):
    """Build Plotly layout by merging overrides with theme defaults."""
    layout = deepcopy(PLOTLY_LAYOUT)
    layout.update(overrides)
    return layout


# ── Summary KPI Row ──────────────────────────────────────────


def load_kpis():
    """
    Load top-level KPI metrics: Revenue, Orders, Unique Products, Avg Review Score.
    Returns: HTML string with 4 KPI cards
    """
    client, cfg, err = _get_client()
    if err:
        return f'<div style="color:{COLORS["red"]}">GCP not configured — see quick-setup.md</div>'

    from dashboards.ben.queries import get_kpi_summary

    try:
        df = get_kpi_summary(client, cfg)
        if df.empty:
            return f'<div style="color:{COLORS["red"]}">No data available</div>'

        row = df.iloc[0]
        total_rev = row["total_revenue"]
        total_orders = row["total_orders"]
        unique_products = row["unique_products"]
        avg_review = row["avg_review_score"]

        cards = "".join([
            kpi_card("Total Revenue", _fmt(total_rev, prefix="R$ "), color="orange"),
            kpi_card("Total Orders", _fmt(total_orders), color="gold"),
            kpi_card("Unique Products", _fmt(unique_products), color="green"),
            kpi_card("Avg Review Score", f"{avg_review:.1f} ⭐", color="orange"),
        ])
        return f'<div style="display:flex;gap:12px;flex-wrap:wrap">{cards}</div>'
    except Exception as e:
        return f'<div style="color:{COLORS["red"]}">Error loading KPIs: {str(e)[:100]}</div>'


# ── Top Categories Bar Chart ────────────────────────────────


def load_top_categories_bar():
    """
    Load horizontal bar chart of top 15 product categories by revenue.
    Color by average review score (gradient).
    """
    client, cfg, err = _get_client()
    if err:
        return error_figure("GCP not configured")

    from dashboards.ben.queries import get_top_categories

    try:
        df = get_top_categories(client, cfg, limit=15)
        if df.empty:
            return error_figure("No data available")

        # Sort by revenue descending for better visualization
        df = df.sort_values("revenue", ascending=True)

        fig = px.bar(
            df,
            y="category",
            x="revenue",
            orientation="h",
            title="Top 15 Categories by Revenue",
            labels={"category": "Category", "revenue": "Revenue (R$)"},
            color="avg_review_score",
            color_continuous_scale=OLIST_COLORSCALE,
            hover_data={
                "orders": True,
                "avg_order_value": ":.2f",
                "avg_review_score": ":.2f",
            },
        )
        fig.update_traces(
            hovertemplate="<b>%{y}</b><br>Revenue: R$ %{x:,.0f}<br>Orders: %{customdata[0]}<br>Avg Order Value: R$ %{customdata[1]:.2f}<br>Avg Review: %{customdata[2]:.2f}<extra></extra>"
        )
        fig.update_layout(**_layout(
            height=450,
            yaxis=dict(categoryorder="total ascending"),
            coloraxis_colorbar=dict(
                title="Avg Review<br>Score",
                tickfont=dict(size=10),
                thickness=15,
                len=0.7,
            ),
        ))
        return fig
    except Exception as e:
        return error_figure(f"Error: {str(e)[:100]}")


# ── Top Products Table ───────────────────────────────────────


def load_top_products_table():
    """
    Load top 20 products by revenue as a pandas DataFrame.
    Returns: DataFrame (Gradio Table expects df, not figure)
    """
    client, cfg, err = _get_client()
    if err:
        # Return empty DataFrame with proper structure on error
        return pd.DataFrame(columns=[
            "product_id", "category", "product_weight_g", "orders", "revenue"
        ])

    from dashboards.ben.queries import get_top_products

    try:
        df = get_top_products(client, cfg, limit=20)
        if df.empty:
            return df

        # Format display columns
        df_display = df.copy()
        df_display["revenue"] = df_display["revenue"].apply(lambda x: f"R$ {x:,.2f}")
        df_display["product_weight_g"] = df_display["product_weight_g"].apply(
            lambda x: f"{x:,.0f}g" if pd.notna(x) else "—"
        )
        df_display = df_display.rename(columns={
            "product_id": "Product ID",
            "category": "Category",
            "product_weight_g": "Weight",
            "orders": "Orders",
            "revenue": "Revenue",
        })
        return df_display[["Product ID", "Category", "Weight", "Orders", "Revenue"]]
    except Exception as e:
        return pd.DataFrame(columns=[
            "product_id", "category", "product_weight_g", "orders", "revenue"
        ])


# ── Category Revenue vs Review Score Bubble Chart ──────────


def load_category_bubble_chart():
    """
    Scatter bubble chart: X=Average Review Score, Y=Total Revenue.
    Bubble size = Order Volume.
    Color = Order Volume (gradient from red to green).
    """
    client, cfg, err = _get_client()
    if err:
        return error_figure("GCP not configured")

    from dashboards.ben.queries import get_category_revenue_vs_reviews

    try:
        df = get_category_revenue_vs_reviews(client, cfg)
        if df.empty:
            return error_figure("No data available")

        fig = px.scatter(
            df,
            x="avg_review_score",
            y="total_revenue",
            size="order_volume",
            color="order_volume",
            hover_name="category",
            hover_data={
                "avg_review_score": ":.2f",
                "total_revenue": ":.2f",
                "order_volume": True,
            },
            title="Category Revenue vs Review Score",
            labels={
                "avg_review_score": "Average Review Score",
                "total_revenue": "Total Revenue (R$)",
                "order_volume": "Order Volume",
            },
            color_continuous_scale=OLIST_COLORSCALE,
            size_max=50,
        )
        fig.update_traces(
            marker=dict(opacity=0.7, line=dict(width=1, color="rgba(255,140,0,0.3)")),
            hovertemplate="<b>%{hovertext}</b><br>Avg Review: %{x:.2f}<br>Revenue: R$ %{y:,.0f}<br>Orders: %{customdata[2]}<extra></extra>"
        )
        fig.update_layout(**_layout(
            height=500,
            xaxis=dict(
                title="Average Review Score",
                gridcolor="rgba(255,140,0,0.08)",
                range=[0, 5.5],
            ),
            yaxis=dict(
                title="Total Revenue (R$)",
                gridcolor="rgba(255,140,0,0.08)",
            ),
            coloraxis_colorbar=dict(
                title="Order<br>Volume",
                tickfont=dict(size=10),
                thickness=15,
                len=0.7,
            ),
        ))
        return fig
    except Exception as e:
        return error_figure(f"Error: {str(e)[:100]}")


# ── Monthly Trend for Selected Category ──────────────────────


def load_monthly_trend_stacked(category: str = ""):
    """
    Monthly order and revenue trend for a selected product category.
    Shows dual traces: orders (left axis) and revenue (right axis).

    Args:
        category (str): Product category name. If empty, returns error figure.
    """
    client, cfg, err = _get_client()
    if err:
        return error_figure("GCP not configured")

    if not category:
        return error_figure("Please select a category")

    from dashboards.ben.queries import get_monthly_category_trend

    try:
        df = get_monthly_category_trend(client, cfg, category)
        if df.empty:
            return error_figure(f"No data for category: {category}")

        fig = go.Figure()

        # Add orders as bar chart (left axis)
        fig.add_trace(go.Bar(
            x=df["month"],
            y=df["orders"],
            name="Orders",
            marker_color=COLORS["orange"],
            yaxis="y1",
            hovertemplate="<b>%{x}</b><br>Orders: %{y:,}<extra></extra>",
        ))

        # Add revenue as line chart (right axis)
        fig.add_trace(go.Scatter(
            x=df["month"],
            y=df["revenue"],
            name="Revenue",
            line=dict(color=COLORS["green"], width=3),
            mode="lines+markers",
            yaxis="y2",
            marker=dict(size=6),
            hovertemplate="<b>%{x}</b><br>Revenue: R$ %{y:,.2f}<extra></extra>",
        ))

        fig.update_layout(**_layout(
            title=f"Monthly Trend: {category}",
            height=400,
            hovermode="x unified",
            yaxis=dict(
                title="Orders",
                titlefont=dict(color=COLORS["orange"]),
                tickfont=dict(color=COLORS["orange"]),
                gridcolor="rgba(255,140,0,0.08)",
            ),
            yaxis2=dict(
                title="Revenue (R$)",
                titlefont=dict(color=COLORS["green"]),
                tickfont=dict(color=COLORS["green"]),
                anchor="x",
                overlaying="y",
                side="right",
            ),
            xaxis=dict(
                title="Month",
                gridcolor="rgba(255,140,0,0.08)",
            ),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1,
            ),
        ))
        return fig
    except Exception as e:
        return error_figure(f"Error: {str(e)[:100]}")


# ── Category Performance Heatmap ─────────────────────────────


def load_category_heatmap():
    """
    2x2 heatmap matrix showing category performance.
    X axis: Review Score (Low ← → High)
    Y axis: Revenue (Low ← → High)
    Cell values: Order Volume count
    """
    client, cfg, err = _get_client()
    if err:
        return error_figure("GCP not configured")

    from dashboards.ben.queries import get_category_revenue_vs_reviews

    try:
        df = get_category_revenue_vs_reviews(client, cfg)
        if df.empty:
            return error_figure("No data available")

        # Create quartile bins for review score and revenue
        df["review_quartile"] = pd.qcut(
            df["avg_review_score"],
            q=4,
            labels=["Very Low (0.0-2.5)", "Low (2.5-3.5)", "High (3.5-4.5)", "Very High (4.5-5.0)"],
            duplicates="drop"
        )
        df["revenue_quartile"] = pd.qcut(
            df["total_revenue"],
            q=4,
            labels=["Low", "Medium", "High", "Very High"],
            duplicates="drop"
        )

        # Create pivot table for heatmap
        heatmap_data = df.groupby(
            ["revenue_quartile", "review_quartile"], as_index=False
        )["order_volume"].sum()

        # Pivot for matrix format
        matrix = heatmap_data.pivot_table(
            index="revenue_quartile",
            columns="review_quartile",
            values="order_volume",
            fill_value=0
        )

        fig = px.imshow(
            matrix,
            labels=dict(x="Review Score Quartile", y="Revenue Quartile", color="Order Volume"),
            title="Category Performance Matrix",
            color_continuous_scale=OLIST_COLORSCALE,
            text_auto=True,
            aspect="auto",
        )
        fig.update_traces(
            text=matrix.values.astype(int),
            texttemplate="%{text:,}",
            textfont=dict(size=12, color="white"),
            hovertemplate="Revenue: %{y}<br>Review Score: %{x}<br>Orders: %{z:,}<extra></extra>"
        )
        fig.update_layout(**_layout(
            height=380,
            coloraxis_colorbar=dict(
                title="Order<br>Volume",
                tickfont=dict(size=10),
                thickness=15,
                len=0.7,
            ),
            xaxis=dict(title="Average Review Score (Quartile)"),
            yaxis=dict(title="Total Revenue (Quartile)"),
        ))
        return fig
    except Exception as e:
        return error_figure(f"Error: {str(e)[:100]}")
