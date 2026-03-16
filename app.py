"""
app.py — Olist Data Product Main Application
─────────────────────────────────────────────
Mounts all 6 developer dashboards + home + admin as Gradio Tabs.
Run:  python app.py
      make run
      ./launch.sh
"""

import os
import gradio as gr
from shared.theme import olist_theme, CUSTOM_CSS, FONT_HEAD

# ── Import each developer's dashboard ────────────────────────
from dashboards.home.app      import dashboard as home_dashboard, _CSS as _HOME_CSS
from dashboards.admin.app     import dashboard as admin_dashboard, _ADMIN_CSS
from dashboards.lik_hong.app  import dashboard as likhong_dashboard
from dashboards.meng_hai.app  import dashboard as menghai_dashboard
from dashboards.lanson.app    import dashboard as lanson_dashboard
from dashboards.ben.app       import dashboard as ben_dashboard
from dashboards.huey_ling.app import dashboard as hueying_dashboard
from dashboards.kendra.app    import dashboard as kendra_dashboard

_ALL_CSS = CUSTOM_CSS + _HOME_CSS + _ADMIN_CSS

# ── Assemble main app ─────────────────────────────────────────
with gr.Blocks(
    title="Olist Data Product",
    analytics_enabled=False,
    theme=olist_theme,
    css=_ALL_CSS,
) as app:

    with gr.Tabs() as tabs:

        with gr.Tab("🏠 Home"):
            home_dashboard.render()

        with gr.Tab("👤 Customer 360"):
            likhong_dashboard.render()

        with gr.Tab("💳 Payment"):
            menghai_dashboard.render()

        with gr.Tab("⭐ Reviews"):
            lanson_dashboard.render()

        with gr.Tab("📦 Products"):
            ben_dashboard.render()

        with gr.Tab("🏪 Sellers"):
            hueying_dashboard.render()

        with gr.Tab("🗺 Geography"):
            kendra_dashboard.render()

        with gr.Tab("⚙️ Admin"):
            admin_dashboard.render()


# ── Launch ───────────────────────────────────────────────────
if __name__ == "__main__":
    port  = int(os.getenv("GRADIO_SERVER_PORT", 7860))
    share = os.getenv("GRADIO_SHARE", "false").lower() == "true"

    app.launch(
        server_name="0.0.0.0",
        server_port=port,
        share=share,
        show_error=True,
        favicon_path=None,
        head=FONT_HEAD,
    )
