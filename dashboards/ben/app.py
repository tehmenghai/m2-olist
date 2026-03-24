"""
dashboards/ben/app.py — Product Analytics
───────────────────────────────────────────
Owner: Ben
Standalone: python dashboards/ben/app.py
Exports: dashboard (gr.Blocks)

Product Analytics dashboard with:
- Top categories by revenue & reviews
- Product performance & demand signals
- Category trends & quality metrics
- Stars schema: Fact_Orders, Dim_Products, Dim_Reviews, Dim_Customers, Dim_Sellers
"""

import gradio as gr

from shared.theme import olist_theme, CUSTOM_CSS, COLORS, FONT_HEAD
from shared.components import page_header, section_title, alert_box, kpi_row, freshness_badge

from dashboards.ben.charts import (
    load_kpis,
    load_top_categories_bar,
    load_top_products_table,
    load_category_bubble_chart,
    load_category_heatmap,
    load_monthly_trend_stacked,
)

from dashboards.ben.queries import get_category_list


with gr.Blocks(
    title="📦 Product Analytics — Ben",
    analytics_enabled=False,
    theme=olist_theme,
    css=CUSTOM_CSS,
    head=FONT_HEAD
) as dashboard:

    page_header(
        "Product Analytics",
        subtitle="Category performance · Top products · Review quality · Demand signals",
        icon="📦",
    )

    # ── KPI Row ────────────────────────────────────────────────
    with gr.Group():
        kpi_html = gr.HTML()

    dashboard.load(fn=load_kpis, outputs=kpi_html)

    # ── Top Categories ────────────────────────────────────────
    with gr.Group():
        section_title("Category Performance", accent="orange")
        with gr.Row():
            with gr.Column(scale=2):
                categories_chart = gr.Plot(label="Top 15 Categories by Revenue")
                dashboard.load(fn=load_top_categories_bar, outputs=categories_chart)

            with gr.Column(scale=1):
                bubble_chart = gr.Plot(label="Revenue vs Review Score")
                dashboard.load(fn=load_category_bubble_chart, outputs=bubble_chart)

    # ── Monthly Trend with Category Filter ─────────────────────
    with gr.Group():
        section_title("Category Trends", accent="gold")

        # Dropdown for category selection
        category_dropdown = gr.Dropdown(
            label="Select Category",
            choices=["All Categories"],  # Will be populated dynamically
            value="All Categories"
        )

        trend_chart = gr.Plot(label="Monthly Revenue & Orders")

        def update_trend(category):
            if category == "All Categories":
                return gr.Plot.update(value=None)
            return load_monthly_trend_stacked(category)

        category_dropdown.change(fn=update_trend, inputs=category_dropdown, outputs=trend_chart)

        # Populate categories on load
        def populate_categories():
            from shared.utils import make_bq_client_getter, dev_config_path
            _get_client = make_bq_client_getter(dev_config_path("ben"))
            client, cfg, err = _get_client()
            if err or client is None:
                return ["All Categories"]
            try:
                df = get_category_list(client, cfg)
                cats = ["All Categories"] + df["category"].tolist()
                return gr.Dropdown.update(choices=cats)
            except:
                return ["All Categories"]

        dashboard.load(fn=populate_categories, outputs=category_dropdown)

    # ── Top Products ────────────────────────────────────────────
    with gr.Group():
        section_title("Product Performance", accent="green")
        products_table = gr.Dataframe(
            label="Top 20 Products by Revenue",
            interactive=False,
            wrap=True
        )
        dashboard.load(fn=load_top_products_table, outputs=products_table)

    # ── Category Performance Matrix ──────────────────────────────
    with gr.Group():
        section_title("Category Opportunity Matrix", accent="orange")
        heatmap_chart = gr.Plot(label="Revenue vs Quality Distribution")
        dashboard.load(fn=load_category_heatmap, outputs=heatmap_chart)

    # ── Info Box ─────────────────────────────────────────────────
    with gr.Group():
        alert_box(
            "💡 Tip: Hover over charts for details. Use the category filter to explore monthly trends.",
            level="info"
        )


if __name__ == "__main__":
    dashboard.launch(
        server_name="0.0.0.0",
        server_port=7865,
        show_error=True,
        theme=olist_theme,
        css=CUSTOM_CSS,
        head=FONT_HEAD
    )
