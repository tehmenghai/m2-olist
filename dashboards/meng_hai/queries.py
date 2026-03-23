"""
dashboards/meng_hai/queries.py
────────────────────────────────
BigQuery queries for Payment Analytics dashboard.
Each function returns a pandas DataFrame.
"""

from google.cloud import bigquery
from shared.utils import run_query, qualified_table


# ── Filter helpers ──────────────────────────────────────────


def _month_type_clauses(
    prefix: str,
    month_col: str,
    type_col: str | None,
    start_month: str | None,
    end_month: str | None,
    payment_types: list[str] | None,
) -> tuple[str, list]:
    """Build WHERE fragments and BQ params for month/type filters."""
    clauses = []
    params = []
    if start_month:
        clauses.append(f"{prefix}{month_col} >= @start_month")
        params.append(bigquery.ScalarQueryParameter("start_month", "STRING", start_month))
    if end_month:
        clauses.append(f"{prefix}{month_col} <= @end_month")
        params.append(bigquery.ScalarQueryParameter("end_month", "STRING", end_month))
    if payment_types and type_col:
        clauses.append(f"{prefix}{type_col} IN UNNEST(@payment_types)")
        params.append(bigquery.ArrayQueryParameter("payment_types", "STRING", payment_types))
    return (" AND " + " AND ".join(clauses) if clauses else ""), params


def get_filter_options(client, cfg):
    """Return distinct months and payment types for filter dropdowns."""
    mart = qualified_table(cfg, "Mart_Payment_Analytics")
    sql = f"""
    SELECT DISTINCT order_month AS month FROM {mart} ORDER BY 1
    """
    months_df = run_query(client, sql)

    sql2 = f"""
    SELECT DISTINCT payment_type FROM {mart} ORDER BY 1
    """
    types_df = run_query(client, sql2)

    months = months_df["month"].tolist() if len(months_df) else []
    types = types_df["payment_type"].tolist() if len(types_df) else []
    return months, types


# ── Queries (all accept optional filter params) ─────────────


def get_payment_summary(client, cfg, start_month=None, end_month=None, payment_types=None):
    """Total revenue, order count, AOV, and avg instalments by payment type."""
    pay = qualified_table(cfg, "Dim_Payments")
    fact = qualified_table(cfg, "Fact_Orders")
    filt, params = _month_type_clauses(
        "p.", "payment_type", "payment_type",  # Dim_Payments has no month; filter via fact
        None, None, payment_types,
    )
    # Month filter on fact table
    month_clauses = []
    month_params = []
    if start_month:
        month_clauses.append("FORMAT_DATE('%Y-%m', DATE(f.order_purchase_timestamp)) >= @start_month")
        month_params.append(bigquery.ScalarQueryParameter("start_month", "STRING", start_month))
    if end_month:
        month_clauses.append("FORMAT_DATE('%Y-%m', DATE(f.order_purchase_timestamp)) <= @end_month")
        month_params.append(bigquery.ScalarQueryParameter("end_month", "STRING", end_month))
    if payment_types:
        month_clauses.append("p.payment_type IN UNNEST(@payment_types)")
        month_params.append(bigquery.ArrayQueryParameter("payment_types", "STRING", payment_types))

    extra_where = (" AND " + " AND ".join(month_clauses)) if month_clauses else ""
    sql = f"""
    SELECT
        p.payment_type,
        COUNT(DISTINCT p.order_id)          AS orders,
        ROUND(SUM(p.payment_value), 2)      AS total_revenue,
        ROUND(AVG(p.payment_value), 2)      AS avg_order_value,
        ROUND(AVG(p.payment_installments), 1) AS avg_instalments
    FROM {pay} p
    JOIN {fact} f USING (order_id)
    WHERE f.order_status NOT IN ('canceled', 'unavailable'){extra_where}
    GROUP BY 1
    ORDER BY total_revenue DESC
    """
    return run_query(client, sql, month_params)


def get_monthly_revenue_by_type(client, cfg, start_month=None, end_month=None, payment_types=None):
    """Monthly revenue split by payment type."""
    pay = qualified_table(cfg, "Dim_Payments")
    fact = qualified_table(cfg, "Fact_Orders")
    month_clauses = []
    params = []
    if start_month:
        month_clauses.append("FORMAT_DATE('%Y-%m', DATE(f.order_purchase_timestamp)) >= @start_month")
        params.append(bigquery.ScalarQueryParameter("start_month", "STRING", start_month))
    if end_month:
        month_clauses.append("FORMAT_DATE('%Y-%m', DATE(f.order_purchase_timestamp)) <= @end_month")
        params.append(bigquery.ScalarQueryParameter("end_month", "STRING", end_month))
    if payment_types:
        month_clauses.append("p.payment_type IN UNNEST(@payment_types)")
        params.append(bigquery.ArrayQueryParameter("payment_types", "STRING", payment_types))
    extra_where = (" AND " + " AND ".join(month_clauses)) if month_clauses else ""
    sql = f"""
    SELECT
        FORMAT_DATE('%Y-%m', DATE(f.order_purchase_timestamp)) AS month,
        p.payment_type,
        ROUND(SUM(p.payment_value), 2) AS revenue
    FROM {pay} p
    JOIN {fact} f USING (order_id)
    WHERE f.order_status NOT IN ('canceled', 'unavailable'){extra_where}
    GROUP BY 1, 2
    ORDER BY 1, 2
    """
    return run_query(client, sql, params)


def get_instalment_distribution(client, cfg, start_month=None, end_month=None, payment_types=None):
    """Distribution of credit-card orders by number of instalments."""
    mart = qualified_table(cfg, "Mart_Payment_Analytics")
    filt, params = _month_type_clauses("", "order_month", "payment_type",
                                        start_month, end_month, None)
    sql = f"""
    SELECT
        payment_installments AS instalments,
        COUNT(*) AS orders
    FROM {mart}
    WHERE payment_type = 'credit_card'{filt}
    GROUP BY 1
    ORDER BY 1
    """
    return run_query(client, sql, params)


def get_cancellation_rate(client, cfg, start_month=None, end_month=None, payment_types=None):
    """Monthly cancellation rate."""
    fact = qualified_table(cfg, "Fact_Orders")
    month_clauses = []
    params = []
    if start_month:
        month_clauses.append("FORMAT_DATE('%Y-%m', DATE(order_purchase_timestamp)) >= @start_month")
        params.append(bigquery.ScalarQueryParameter("start_month", "STRING", start_month))
    if end_month:
        month_clauses.append("FORMAT_DATE('%Y-%m', DATE(order_purchase_timestamp)) <= @end_month")
        params.append(bigquery.ScalarQueryParameter("end_month", "STRING", end_month))
    extra_where = (" WHERE " + " AND ".join(month_clauses)) if month_clauses else ""
    sql = f"""
    SELECT
        FORMAT_DATE('%Y-%m', DATE(order_purchase_timestamp)) AS month,
        COUNTIF(order_status = 'canceled') AS canceled,
        COUNT(*) AS total,
        ROUND(SAFE_DIVIDE(COUNTIF(order_status = 'canceled'), COUNT(*)) * 100, 2)
            AS cancel_rate_pct
    FROM {fact}
    {extra_where}
    GROUP BY 1
    ORDER BY 1
    """
    return run_query(client, sql, params)


# ── Mart-based queries ──────────────────────────────────────


def _mart_filter(start_month=None, end_month=None, payment_types=None):
    """Build WHERE fragments for mart queries (all share order_month + payment_type)."""
    return _month_type_clauses("", "order_month", "payment_type",
                               start_month, end_month, payment_types)


def get_payment_method_overview(client, cfg, start_month=None, end_month=None, payment_types=None):
    """Summary metrics per payment type from the payment-level mart."""
    mart = qualified_table(cfg, "Mart_Payment_Analytics")
    filt, params = _mart_filter(start_month, end_month, payment_types)
    where = f"WHERE 1=1{filt}" if filt else ""
    sql = f"""
    SELECT
        payment_type,
        COUNT(*)                              AS payment_count,
        COUNT(DISTINCT order_id)              AS order_count,
        ROUND(SUM(payment_value), 2)          AS total_revenue,
        ROUND(AVG(payment_value), 2)          AS avg_value,
        ROUND(AVG(payment_installments), 1)   AS avg_installments
    FROM {mart}
    {where}
    GROUP BY 1
    ORDER BY total_revenue DESC
    """
    return run_query(client, sql, params)


def get_payment_by_geo(client, cfg, start_month=None, end_month=None, payment_types=None):
    """Payment breakdown by customer state (top 10 by revenue)."""
    mart = qualified_table(cfg, "Mart_Payment_Analytics")
    filt, params = _mart_filter(start_month, end_month, payment_types)
    where = f"WHERE 1=1{filt}" if filt else ""
    sql = f"""
    WITH state_totals AS (
        SELECT
            customer_state,
            SUM(payment_value) AS state_revenue
        FROM {mart}
        {where}
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
    {"WHERE 1=1" + filt if filt else ""}
    GROUP BY 1, 2, st.state_revenue
    ORDER BY st.state_revenue DESC, total_revenue DESC
    """
    return run_query(client, sql, params)


def get_payment_by_product(client, cfg, start_month=None, end_month=None, payment_types=None):
    """Payment breakdown by product category (top 10 by revenue)."""
    mart = qualified_table(cfg, "Mart_Payment_Analytics")
    filt, params = _mart_filter(start_month, end_month, payment_types)
    where = f"WHERE 1=1{filt}" if filt else ""
    sql = f"""
    WITH cat_totals AS (
        SELECT
            product_category_english,
            SUM(payment_value) AS cat_revenue
        FROM {mart}
        {where}
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
    {"WHERE 1=1" + filt if filt else ""}
    GROUP BY 1, 2, ct.cat_revenue
    ORDER BY ct.cat_revenue DESC, total_revenue DESC
    """
    return run_query(client, sql, params)


def get_payment_by_price_band(client, cfg, start_month=None, end_month=None, payment_types=None):
    """Payment breakdown by price band."""
    mart = qualified_table(cfg, "Mart_Payment_Analytics")
    filt, params = _mart_filter(start_month, end_month, payment_types)
    where = f"WHERE 1=1{filt}" if filt else ""
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
    {where}
    GROUP BY 1, 2, 3
    ORDER BY price_band_order, total_revenue DESC
    """
    return run_query(client, sql, params)


def get_geo_bubble_map(client, cfg, start_month=None, end_month=None, payment_types=None):
    """Payment volume/revenue by state with centroid coordinates for geo map."""
    mart = qualified_table(cfg, "Mart_Payment_Analytics")
    silver = f"`{cfg['project_id']}.olist_silver.stg_geolocation`"
    filt, params = _mart_filter(start_month, end_month, payment_types)
    where = f"WHERE 1=1{filt}" if filt else ""
    sql = f"""
    WITH state_coords AS (
        SELECT state, AVG(latitude) AS lat, AVG(longitude) AS lng
        FROM {silver}
        GROUP BY 1
    )
    SELECT
        m.customer_state,
        m.payment_type,
        COUNT(*)                          AS payment_count,
        ROUND(SUM(m.payment_value), 2)    AS total_revenue,
        sc.lat,
        sc.lng
    FROM {mart} m
    JOIN state_coords sc ON m.customer_state = sc.state
    {"WHERE 1=1" + filt if filt else ""}
    GROUP BY 1, 2, sc.lat, sc.lng
    ORDER BY total_revenue DESC
    """
    return run_query(client, sql, params)
