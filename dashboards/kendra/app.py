"""
dashboards/kendra/app.py — Geography Analytics
────────────────────────────────────────────────
Owner: Kendra
Standalone: python dashboards/kendra/app.py
Exports: dashboard (gr.Blocks)

TODO: Build your dashboard here.
      See dashboards/lik_hong/app.py as a reference implementation.
      Queries go in dashboards/kendra/queries.py.
"""

import gradio as gr

from shared.theme import olist_theme, CUSTOM_CSS, COLORS, FONT_HEAD
from shared.components import page_header, section_title, alert_box

with gr.Blocks(analytics_enabled=False) as dashboard:

    page_header(
        "Geography Analytics",
        subtitle="Customer density · Delivery times · Underserved regions · Location map",
        icon="🗺",
    )

    alert_box(
        "This is your dashboard — implement it in this file and dashboards/kendra/queries.py.",
        level="info",
    )

    section_title("Gold Tables You Need", accent="gold")
    gr.HTML(f"""
    <div class="olist-card" style="font-family:'Space Mono',monospace;
                                   font-size:0.85rem;color:{COLORS['text_secondary']};
                                   line-height:1.9">
        <span style="color:{COLORS['orange']};font-weight:600">Dim_Customers</span>
        &nbsp;— customer_state, customer_city, customer_zip_code_prefix<br>
        <span style="color:{COLORS['orange']};font-weight:600">Dim_Sellers</span>
        &nbsp;— seller_state, seller_city — for customer/seller ratio analysis<br>
        <span style="color:{COLORS['orange']};font-weight:600">Dim_Geolocation</span>
        &nbsp;— lat/lng per zip code prefix (deduplicate by prefix in dbt)<br>
        <span style="color:{COLORS['orange']};font-weight:600">Fact_Orders</span>
        &nbsp;— delivery timestamps for regional delivery time analysis<br><br>
        <span style="color:{COLORS['text_muted']}">
        Note: Dim_Geolocation has ~1M rows — deduplicate by zip prefix in dbt Silver.<br>
        &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Choropleth uses Brazil GeoJSON (internet required for first load).<br>
        Reference: pipelines/kendra/batch/README.md · queries stub: queries.py
        </span>
    </div>
    """)

    section_title("Suggested Charts", accent="orange")
    gr.HTML(f"""
    <div class="olist-card" style="color:{COLORS['text_secondary']};font-size:0.875rem;
                                   line-height:1.8">
        <b style="color:{COLORS['text_primary']}">Chart ideas to get started:</b><br>
        · Customer density choropleth by Brazilian state (px.choropleth)<br>
        · Average delivery days by customer state (horizontal bar)<br>
        · Customer location scatter map — sample 3k points (px.scatter_mapbox)<br>
        · Underserved regions table: high customer / low seller ratio per state<br>
        · States covered + fastest/slowest delivery KPI cards
    </div>
    """)


if __name__ == "__main__":
    dashboard.launch(server_port=7867, show_error=True, theme=olist_theme, css=CUSTOM_CSS, head=FONT_HEAD)
