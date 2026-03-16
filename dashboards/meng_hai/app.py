"""
dashboards/meng_hai/app.py — Payment Analytics
────────────────────────────────────────────────
Owner: Meng Hai
Standalone: python dashboards/meng_hai/app.py
Exports: dashboard (gr.Blocks)
"""

import gradio as gr

from shared.theme import olist_theme, CUSTOM_CSS, FONT_HEAD
from shared.components import page_header, section_title
from dashboards.meng_hai.charts import (
    load_kpis,
    load_revenue_by_type,
    load_monthly_trend,
    load_instalment_dist,
    load_cancellation_trend,
    load_payment_overview,
    load_payment_by_geo,
    load_payment_by_product,
    load_payment_by_price_band,
)

# ── Dashboard UI ──────────────────────────────────────────────

with gr.Blocks(analytics_enabled=False) as dashboard:

    page_header(
        "Payment Analytics",
        subtitle="Payment methods · Instalment trends · AOV · Cancellation rates",
        icon="💳",
    )

    # KPI row
    kpi_html = gr.HTML()

    # Row 1: Revenue donut + Monthly trend
    with gr.Row():
        with gr.Column(scale=1):
            section_title("Revenue by Payment Type", accent="orange")
            revenue_type_chart = gr.Plot()
        with gr.Column(scale=2):
            section_title("Monthly Revenue Trend", accent="gold")
            monthly_chart = gr.Plot()

    # Row 2: Instalment dist + Cancellation rate
    with gr.Row():
        with gr.Column(scale=1):
            section_title("Credit Card Instalments", accent="green")
            instalment_chart = gr.Plot()
        with gr.Column(scale=1):
            section_title("Cancellation Rate", accent="red")
            cancel_chart = gr.Plot()

    # Row 3: Payment overview + Geography
    with gr.Row():
        with gr.Column(scale=1):
            section_title("Payment Method Overview", accent="orange")
            overview_chart = gr.Plot()
        with gr.Column(scale=1):
            section_title("Payment by Customer State", accent="gold")
            geo_chart = gr.Plot()

    # Row 4: Product category + Price band
    with gr.Row():
        with gr.Column(scale=1):
            section_title("Payment by Product Category", accent="green")
            product_chart = gr.Plot()
        with gr.Column(scale=1):
            section_title("Payment by Price Band", accent="orange")
            price_band_chart = gr.Plot()

    # Wire up loaders
    dashboard.load(fn=load_kpis, outputs=kpi_html)
    dashboard.load(fn=load_revenue_by_type, outputs=revenue_type_chart)
    dashboard.load(fn=load_monthly_trend, outputs=monthly_chart)
    dashboard.load(fn=load_instalment_dist, outputs=instalment_chart)
    dashboard.load(fn=load_cancellation_trend, outputs=cancel_chart)
    dashboard.load(fn=load_payment_overview, outputs=overview_chart)
    dashboard.load(fn=load_payment_by_geo, outputs=geo_chart)
    dashboard.load(fn=load_payment_by_product, outputs=product_chart)
    dashboard.load(fn=load_payment_by_price_band, outputs=price_band_chart)


if __name__ == "__main__":
    dashboard.launch(server_port=7863, show_error=True, theme=olist_theme, css=CUSTOM_CSS, head=FONT_HEAD)
