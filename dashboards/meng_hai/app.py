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
    load_filter_options,
    load_kpis,
    load_revenue_by_type,
    load_monthly_trend,
    load_instalment_dist,
    load_cancellation_trend,
    load_payment_overview,
    load_payment_by_geo,
    load_payment_by_product,
    load_payment_by_price_band,
    load_geo_bubble_map,
)


def _refresh_all(start_month, end_month, payment_types):
    """Single callback that refreshes KPIs + all 9 charts."""
    sm = start_month or None
    em = end_month or None
    pt = payment_types if payment_types else None
    return (
        load_kpis(sm, em, pt),
        load_revenue_by_type(sm, em, pt),
        load_payment_overview(sm, em, pt),
        load_monthly_trend(sm, em, pt),
        load_cancellation_trend(sm, em, pt),
        load_instalment_dist(sm, em, pt),
        load_payment_by_product(sm, em, pt),
        load_payment_by_price_band(sm, em, pt),
        load_payment_by_geo(sm, em, pt),
        load_geo_bubble_map(sm, em, pt),
    )


def _init_filters():
    """Populate filter dropdowns on page load."""
    months, types = load_filter_options()
    start = gr.update(choices=months, value=months[0] if months else None)
    end = gr.update(choices=months, value=months[-1] if months else None)
    type_cbg = gr.update(choices=types, value=types)
    return start, end, type_cbg


# ── Dashboard UI ──────────────────────────────────────────────

with gr.Blocks(analytics_enabled=False) as dashboard:

    page_header(
        "Payment Analytics",
        subtitle="Payment methods · Instalment trends · AOV · Cancellation rates",
        icon="💳",
    )

    # KPI row
    kpi_html = gr.HTML()

    # Filter row
    with gr.Row():
        start_dd = gr.Dropdown(label="Start Month", scale=1, interactive=True)
        end_dd = gr.Dropdown(label="End Month", scale=1, interactive=True)
        type_cbg = gr.CheckboxGroup(label="Payment Types", scale=2, interactive=True)
        apply_btn = gr.Button("Apply Filters", variant="primary", scale=0,
                              min_width=140)

    # Tabs
    with gr.Tabs():
        with gr.TabItem("Overview"):
            with gr.Row():
                with gr.Column(scale=1):
                    section_title("Revenue by Payment Type", accent="orange")
                    revenue_type_chart = gr.Plot()
                with gr.Column(scale=1):
                    section_title("Payment Method Overview", accent="orange")
                    overview_chart = gr.Plot()
            with gr.Row():
                with gr.Column(scale=2):
                    section_title("Monthly Revenue Trend", accent="gold")
                    monthly_chart = gr.Plot()
                with gr.Column(scale=1):
                    section_title("Cancellation Rate", accent="red")
                    cancel_chart = gr.Plot()

        with gr.TabItem("Breakdowns"):
            with gr.Row():
                with gr.Column():
                    section_title("Credit Card Instalments", accent="green")
                    instalment_chart = gr.Plot()
            with gr.Row():
                with gr.Column(scale=1):
                    section_title("Payment by Product Category", accent="green")
                    product_chart = gr.Plot()
                with gr.Column(scale=1):
                    section_title("Payment by Price Band", accent="orange")
                    price_band_chart = gr.Plot()

        with gr.TabItem("Geography"):
            with gr.Row():
                with gr.Column():
                    section_title("Payment by Customer State", accent="gold")
                    geo_chart = gr.Plot()
            with gr.Row():
                with gr.Column():
                    section_title("Payment Geography — Bubble Map", accent="gold")
                    geo_bubble_chart = gr.Plot()

    # All chart outputs in callback order
    _all_outputs = [
        kpi_html,
        revenue_type_chart, overview_chart,
        monthly_chart, cancel_chart,
        instalment_chart, product_chart, price_band_chart,
        geo_chart, geo_bubble_chart,
    ]

    # Apply button triggers full refresh
    apply_btn.click(
        fn=_refresh_all,
        inputs=[start_dd, end_dd, type_cbg],
        outputs=_all_outputs,
    )

    # On page load: populate filters, then load all charts with defaults
    dashboard.load(fn=_init_filters, outputs=[start_dd, end_dd, type_cbg])
    dashboard.load(
        fn=lambda: _refresh_all(None, None, None),
        outputs=_all_outputs,
    )


if __name__ == "__main__":
    dashboard.launch(
        server_port=7863, show_error=True,
        theme=olist_theme, css=CUSTOM_CSS, head=FONT_HEAD,
    )
