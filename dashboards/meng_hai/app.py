"""
dashboards/meng_hai/app.py — Payment Analytics
────────────────────────────────────────────────
Owner: Meng Hai
Standalone: python dashboards/meng_hai/app.py
Exports: dashboard (gr.Blocks)

TODO: Build your dashboard here.
      See dashboards/lik_hong/app.py as a reference implementation.
      Queries go in dashboards/meng_hai/queries.py.
"""

import gradio as gr

from shared.theme import olist_theme, CUSTOM_CSS, COLORS, FONT_HEAD
from shared.components import page_header, section_title, alert_box

with gr.Blocks(analytics_enabled=False) as dashboard:

    page_header(
        "Payment Analytics",
        subtitle="Payment methods · Instalment trends · AOV · Cancellation rates",
        icon="💳",
    )

    alert_box(
        "This is your dashboard — implement it in this file and dashboards/meng_hai/queries.py.",
        level="info",
    )

    section_title("Gold Tables You Need", accent="gold")
    gr.HTML(f"""
    <div class="olist-card" style="font-family:'Space Mono',monospace;
                                   font-size:0.85rem;color:{COLORS['text_secondary']};
                                   line-height:1.9">
        <span style="color:{COLORS['orange']};font-weight:600">Fact_Orders</span>
        &nbsp;— order-level facts (status, timestamps, payment_value, category, …)<br>
        <span style="color:{COLORS['orange']};font-weight:600">Dim_Payments</span>
        &nbsp;— payment_type, payment_installments, payment_value per order<br><br>
        <span style="color:{COLORS['text_muted']}">
        Reference: pipelines/meng_hai/batch/README.md · queries stub: queries.py
        </span>
    </div>
    """)

    section_title("Suggested Charts", accent="orange")
    gr.HTML(f"""
    <div class="olist-card" style="color:{COLORS['text_secondary']};font-size:0.875rem;
                                   line-height:1.8">
        <b style="color:{COLORS['text_primary']}">Chart ideas to get started:</b><br>
        · Revenue by payment type (bar or donut)<br>
        · Instalment distribution for credit cards (bar)<br>
        · Monthly revenue trend by payment type (stacked area)<br>
        · Average Order Value by payment method (bar)<br>
        · Monthly cancellation rate (line)
    </div>
    """)


if __name__ == "__main__":
    dashboard.launch(server_port=7863, show_error=True, theme=olist_theme, css=CUSTOM_CSS, head=FONT_HEAD)
