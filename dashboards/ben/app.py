"""
dashboards/ben/app.py — Product Analytics
───────────────────────────────────────────
Owner: Ben
Standalone: python dashboards/ben/app.py
Exports: dashboard (gr.Blocks)

TODO: Build your dashboard here.
      See dashboards/lik_hong/app.py as a reference implementation.
      Queries go in dashboards/ben/queries.py.
"""

import gradio as gr

from shared.theme import olist_theme, CUSTOM_CSS, COLORS, FONT_HEAD
from shared.components import page_header, section_title, alert_box

with gr.Blocks(analytics_enabled=False) as dashboard:

    page_header(
        "Product Analytics",
        subtitle="Category performance · Top products · Review quality · Demand signals",
        icon="📦",
    )

    alert_box(
        "This is your dashboard — implement it in this file and dashboards/ben/queries.py.",
        level="info",
    )

    section_title("Gold Tables You Need", accent="gold")
    gr.HTML(f"""
    <div class="olist-card" style="font-family:'Space Mono',monospace;
                                   font-size:0.85rem;color:{COLORS['text_secondary']};
                                   line-height:1.9">
        <span style="color:{COLORS['orange']};font-weight:600">Fact_Orders</span>
        &nbsp;— order facts incl. product_id, category, payment_value, …<br>
        <span style="color:{COLORS['orange']};font-weight:600">Dim_Products</span>
        &nbsp;— product dimensions: category (PT + EN), weight, dimensions<br>
        <span style="color:{COLORS['orange']};font-weight:600">Dim_Reviews</span>
        &nbsp;— review_score joined to orders for per-product quality metrics<br><br>
        <span style="color:{COLORS['text_muted']}">
        Note: join the category translation CSV in your dbt Silver model so<br>
        &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;product_category_name_english is populated in Fact_Orders.<br>
        Reference: pipelines/ben/batch/README.md · queries stub: queries.py
        </span>
    </div>
    """)

    section_title("Suggested Charts", accent="orange")
    gr.HTML(f"""
    <div class="olist-card" style="color:{COLORS['text_secondary']};font-size:0.875rem;
                                   line-height:1.8">
        <b style="color:{COLORS['text_primary']}">Chart ideas to get started:</b><br>
        · Top 15 categories by revenue (horizontal bar)<br>
        · Category review score vs volume (scatter bubble)<br>
        · Monthly revenue trend for a selected category (line + area)<br>
        · Top 20 products by revenue (table)<br>
        · Category performance matrix (heatmap: revenue × review score)
    </div>
    """)


if __name__ == "__main__":
    dashboard.launch(server_port=7865, show_error=True, theme=olist_theme, css=CUSTOM_CSS, head=FONT_HEAD)
