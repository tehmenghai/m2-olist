"""
dashboards/meng_hai/queries.py
────────────────────────────────
BigQuery queries for Payment Analytics dashboard.
Each function returns a pandas DataFrame.
"""

from shared.utils import run_query, qualified_table


def get_payment_summary(client, cfg):
    """
    Total revenue, order count, AOV, and avg instalments by payment type.
    Excludes canceled/unavailable orders.
    """
    pay = qualified_table(cfg, "Dim_Payments")
    fact = qualified_table(cfg, "Fact_Orders")
    sql = f"""
    SELECT
        p.payment_type,
        COUNT(DISTINCT p.order_id)          AS orders,
        ROUND(SUM(p.payment_value), 2)      AS total_revenue,
        ROUND(AVG(p.payment_value), 2)      AS avg_order_value,
        ROUND(AVG(p.payment_installments), 1) AS avg_instalments
    FROM {pay} p
    JOIN {fact} f USING (order_id)
    WHERE f.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY 1
    ORDER BY total_revenue DESC
    """
    return run_query(client, sql)


def get_monthly_revenue_by_type(client, cfg):
    """Monthly revenue split by payment type."""
    pay = qualified_table(cfg, "Dim_Payments")
    fact = qualified_table(cfg, "Fact_Orders")
    sql = f"""
    SELECT
        FORMAT_DATE('%Y-%m', DATE(f.order_purchase_timestamp)) AS month,
        p.payment_type,
        ROUND(SUM(p.payment_value), 2) AS revenue
    FROM {pay} p
    JOIN {fact} f USING (order_id)
    WHERE f.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY 1, 2
    ORDER BY 1, 2
    """
    return run_query(client, sql)


def get_instalment_distribution(client, cfg):
    """Distribution of credit-card orders by number of instalments."""
    pay = qualified_table(cfg, "Dim_Payments")
    sql = f"""
    SELECT
        payment_installments AS instalments,
        COUNT(*) AS orders
    FROM {pay}
    WHERE payment_type = 'credit_card'
    GROUP BY 1
    ORDER BY 1
    """
    return run_query(client, sql)


def get_cancellation_rate(client, cfg):
    """Monthly cancellation rate."""
    fact = qualified_table(cfg, "Fact_Orders")
    sql = f"""
    SELECT
        FORMAT_DATE('%Y-%m', DATE(order_purchase_timestamp)) AS month,
        COUNTIF(order_status = 'canceled') AS canceled,
        COUNT(*) AS total,
        ROUND(SAFE_DIVIDE(COUNTIF(order_status = 'canceled'), COUNT(*)) * 100, 2)
            AS cancel_rate_pct
    FROM {fact}
    GROUP BY 1
    ORDER BY 1
    """
    return run_query(client, sql)


# ── New mart-based queries ───────────────────────────────────


def get_payment_method_overview(client, cfg):
    """Summary metrics per payment type from the payment-level mart."""
    mart = qualified_table(cfg, "Mart_Payment_Analytics")
    sql = f"""
    SELECT
        payment_type,
        COUNT(*)                              AS payment_count,
        COUNT(DISTINCT order_id)              AS order_count,
        ROUND(SUM(payment_value), 2)          AS total_revenue,
        ROUND(AVG(payment_value), 2)          AS avg_value,
        ROUND(AVG(payment_installments), 1)   AS avg_installments
    FROM {mart}
    GROUP BY 1
    ORDER BY total_revenue DESC
    """
    return run_query(client, sql)


def get_payment_by_geo(client, cfg):
    """Payment breakdown by customer state (top 10 by revenue)."""
    mart = qualified_table(cfg, "Mart_Payment_Analytics")
    sql = f"""
    WITH state_totals AS (
        SELECT
            customer_state,
            SUM(payment_value) AS state_revenue
        FROM {mart}
        GROUP BY 1
        ORDER BY state_revenue DESC
        LIMIT 10
    )
    SELECT
        m.customer_state                          AS location,
        m.payment_type,
        COUNT(DISTINCT m.order_id)                AS order_count,
        ROUND(SUM(m.payment_value), 2)            AS total_revenue,
        ROUND(SUM(m.payment_value) * 100.0
              / SUM(SUM(m.payment_value)) OVER (PARTITION BY m.customer_state), 1)
                                                  AS pct_of_location,
        st.state_revenue
    FROM {mart} m
    JOIN state_totals st ON m.customer_state = st.customer_state
    GROUP BY 1, 2, st.state_revenue
    ORDER BY st.state_revenue DESC, total_revenue DESC
    """
    return run_query(client, sql)


def get_payment_by_product(client, cfg):
    """Payment breakdown by product category (top 10 by revenue)."""
    mart = qualified_table(cfg, "Mart_Payment_Analytics")
    sql = f"""
    WITH cat_totals AS (
        SELECT
            product_category_english,
            SUM(payment_value) AS cat_revenue
        FROM {mart}
        GROUP BY 1
        ORDER BY cat_revenue DESC
        LIMIT 10
    )
    SELECT
        m.product_category_english                AS category,
        m.payment_type,
        COUNT(DISTINCT m.order_id)                AS order_count,
        ROUND(SUM(m.payment_value), 2)            AS total_revenue,
        ROUND(SUM(m.payment_value) * 100.0
              / SUM(SUM(m.payment_value)) OVER (PARTITION BY m.product_category_english), 1)
                                                  AS pct_of_category,
        ct.cat_revenue
    FROM {mart} m
    JOIN cat_totals ct ON m.product_category_english = ct.product_category_english
    GROUP BY 1, 2, ct.cat_revenue
    ORDER BY ct.cat_revenue DESC, total_revenue DESC
    """
    return run_query(client, sql)


def get_payment_by_price_band(client, cfg):
    """Payment breakdown by price band."""
    mart = qualified_table(cfg, "Mart_Payment_Analytics")
    sql = f"""
    SELECT
        price_band,
        price_band_order,
        payment_type,
        COUNT(*)                              AS payment_count,
        ROUND(SUM(payment_value), 2)          AS total_revenue,
        ROUND(SUM(payment_value) * 100.0
              / SUM(SUM(payment_value)) OVER (PARTITION BY price_band), 1)
                                              AS pct_of_band
    FROM {mart}
    GROUP BY 1, 2, 3
    ORDER BY price_band_order, total_revenue DESC
    """
    return run_query(client, sql)
