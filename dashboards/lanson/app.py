"""
dashboards/lanson/app.py — Reviews & Satisfaction
───────────────────────────────────────────────────
Owner: Lanson
Standalone: python dashboards/lanson/app.py
Exports: dashboard (gr.Blocks)

TODO: Build your dashboard here.
      See dashboards/lik_hong/app.py as a reference implementation.
      Queries go in dashboards/lanson/queries.py.
      Sentiment analysis (Cloud Function) is a later phase — Lanson owns it.
"""

import gradio as gr

from shared.theme import olist_theme, CUSTOM_CSS, COLORS, FONT_HEAD
from shared.components import page_header, section_title, alert_box

with gr.Blocks(analytics_enabled=False) as dashboard:

    page_header(
        "Reviews & Satisfaction",
        subtitle="Review scores · Sentiment · CSAT trends · Low-score alerts",
        icon="⭐",
    )

    alert_box(
        "This is your dashboard — implement it in this file and dashboards/lanson/queries.py.",
        level="info",
    )

    alert_box(
        "Sentiment analysis (Cloud Function) is planned for a later phase.",
        level="warn",
    )

    section_title("Gold Tables You Need", accent="gold")
    gr.HTML(f"""
    <div class="olist-card" style="font-family:'Space Mono',monospace;
                                   font-size:0.85rem;color:{COLORS['text_secondary']};
                                   line-height:1.9">
        <span style="color:{COLORS['orange']};font-weight:600">Fact_Orders</span>
        &nbsp;— order-level facts (status, timestamps, delivery dates, …)<br>
        <span style="color:{COLORS['orange']};font-weight:600">Dim_Reviews</span>
        &nbsp;— review_score, review_comment_message, review dates<br>
        <span style="color:{COLORS['orange']};font-weight:600">Dim_Customers</span>
        &nbsp;— customer state and city for regional breakdowns<br><br>
        <span style="color:{COLORS['text_muted']}">
        Reference: pipelines/lanson/batch/README.md · queries stub: queries.py
        </span>
    </div>
    """)

    section_title("Suggested Charts", accent="orange")
    gr.HTML(f"""
    <div class="olist-card" style="color:{COLORS['text_secondary']};font-size:0.875rem;
                                   line-height:1.8">
        <b style="color:{COLORS['text_primary']}">Chart ideas to get started:</b><br>
        · Review score distribution 1–5 (bar)<br>
        · Average score trend over time (line)<br>
        · Delivery delay vs review score (bar — shows correlation)<br>
        · Low-score orders table (&lt; 2 stars, needs attention)<br>
        · Proportion of reviews with text comments (KPI)
    </div>
    """)


if __name__ == "__main__":
    dashboard.launch(server_port=7864, show_error=True, theme=olist_theme, css=CUSTOM_CSS, head=FONT_HEAD)
