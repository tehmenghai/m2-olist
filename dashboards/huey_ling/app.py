"""
dashboards/huey_ling/app.py — Seller Performance
──────────────────────────────────────────────────
Owner: Huey Ling
Standalone: python dashboards/huey_ling/app.py
Exports: dashboard (gr.Blocks)

TODO: Build your dashboard here.
      See dashboards/lik_hong/app.py as a reference implementation.
      Queries go in dashboards/huey_ling/queries.py.
"""

import gradio as gr

from shared.theme import olist_theme, CUSTOM_CSS, COLORS, FONT_HEAD
from shared.components import page_header, section_title, alert_box

with gr.Blocks(analytics_enabled=False) as dashboard:

    page_header(
        "Seller Performance",
        subtitle="Leaderboard · Delivery latency · Ratings · At-risk sellers",
        icon="🏪",
    )

    alert_box(
        "This is your dashboard — implement it in this file and dashboards/huey_ling/queries.py.",
        level="info",
    )

    section_title("Gold Tables You Need", accent="gold")
    gr.HTML(f"""
    <div class="olist-card" style="font-family:'Space Mono',monospace;
                                   font-size:0.85rem;color:{COLORS['text_secondary']};
                                   line-height:1.9">
        <span style="color:{COLORS['orange']};font-weight:600">Dim_Sellers</span>
        &nbsp;— seller_id, seller_state, seller_city<br>
        <span style="color:{COLORS['orange']};font-weight:600">Fact_Orders</span>
        &nbsp;— order facts incl. seller_id, delivery timestamps, payment_value<br>
        <span style="color:{COLORS['orange']};font-weight:600">Dim_Reviews</span>
        &nbsp;— review_score, review dates — join to orders for per-seller rating<br><br>
        <span style="color:{COLORS['text_muted']}">
        Reference: pipelines/huey_ling/batch/README.md · queries stub: queries.py
        </span>
    </div>
    """)

    section_title("Suggested Charts", accent="orange")
    gr.HTML(f"""
    <div class="olist-card" style="color:{COLORS['text_secondary']};font-size:0.875rem;
                                   line-height:1.8">
        <b style="color:{COLORS['text_primary']}">Chart ideas to get started:</b><br>
        · Seller leaderboard — top 20 by revenue + review score (table)<br>
        · Delivery delay distribution: actual vs estimated (histogram)<br>
        · Review score heatmap by seller state (imshow)<br>
        · At-risk sellers: avg review &lt; 3 OR late delivery &gt; 30% (flagged table)<br>
        · On-time vs late delivery KPI cards
    </div>
    """)


if __name__ == "__main__":
    dashboard.launch(server_port=7866, show_error=True, theme=olist_theme, css=CUSTOM_CSS, head=FONT_HEAD)
