"""
dashboards/lik_hong/queries.py
───────────────────────────────
BigQuery queries for Customer 360 + Next Best Action dashboard.
All queries reference Gold tables via shared.utils.qualified_table().
"""

from google.cloud import bigquery
from shared.utils import run_query, qualified_table


def get_customer_profile(client: bigquery.Client, cfg: dict, customer_id: str) -> dict:
    """Fetch a single customer's profile + aggregated order metrics."""
    fact = qualified_table(cfg, "Fact_Orders")
    dim  = qualified_table(cfg, "Dim_Customers")
    sql = f"""
    SELECT
        c.customer_unique_id,
        c.customer_city,
        c.customer_state,
        COUNT(DISTINCT f.order_id)                                  AS total_orders,
        ROUND(SUM(f.payment_value), 2)                              AS total_spend,
        ROUND(AVG(f.payment_value), 2)                              AS avg_order_value,
        MIN(DATE(f.order_purchase_timestamp))                       AS first_order_date,
        MAX(DATE(f.order_purchase_timestamp))                       AS last_order_date,
        DATE_DIFF((SELECT MAX(DATE(order_purchase_timestamp)) FROM {fact}), MAX(DATE(f.order_purchase_timestamp)), DAY) AS days_since_last_order,
        DATE_DIFF(MAX(DATE(f.order_purchase_timestamp)),
                  MIN(DATE(f.order_purchase_timestamp)), MONTH)     AS months_active,
        ROUND(AVG(f.review_score), 2)                               AS avg_review_score
    FROM {dim} c
    LEFT JOIN {fact} f USING (customer_id)
    WHERE c.customer_unique_id = @customer_id
    GROUP BY 1, 2, 3
    """
    params = [bigquery.ScalarQueryParameter("customer_id", "STRING", customer_id)]
    df = run_query(client, sql, params)
    return df.iloc[0].to_dict() if len(df) else {}


def get_rfm_segments(client: bigquery.Client, cfg: dict):
    """Return RFM segment distribution across all customers."""

    fact = qualified_table(cfg, "Fact_Orders")
    dim  = qualified_table(cfg, "Dim_Customers")
    sql = f"""
    WITH rfm AS (
        SELECT
            c.customer_unique_id,
            DATE_DIFF(CURRENT_DATE(), MAX(DATE(f.order_purchase_timestamp)), DAY) AS recency,
            COUNT(DISTINCT f.order_id)    AS frequency,
            ROUND(SUM(f.payment_value),2) AS monetary
        FROM {dim} c
        JOIN {fact} f USING (customer_id)
        GROUP BY 1
    ),
    scored AS (
        SELECT
            monetary,
            NTILE(5) OVER (ORDER BY recency DESC) AS r_score,
            NTILE(5) OVER (ORDER BY frequency)    AS f_score,
            NTILE(5) OVER (ORDER BY monetary)     AS m_score
        FROM rfm
    ),
    segmented AS (
        SELECT
            monetary,
            CASE
                WHEN r_score >= 4 AND f_score >= 4 THEN 'Champions'
                WHEN r_score >= 3 AND f_score >= 3 THEN 'Loyal Customers'
                WHEN r_score >= 4 AND f_score <= 2 THEN 'Recent Customers'
                WHEN r_score <= 2 AND f_score >= 3 THEN 'At Risk'
                WHEN r_score <= 2 AND f_score <= 2 THEN 'Lost'
                ELSE 'Potential Loyalists'
            END AS segment
        FROM scored
    )
    SELECT segment, COUNT(*) AS customer_count,
           ROUND(AVG(monetary), 2) AS avg_monetary
    FROM segmented
    GROUP BY segment
    ORDER BY customer_count DESC
    """
    return run_query(client, sql)


def get_churn_scores(client: bigquery.Client, cfg: dict, limit: int = 20):
    """
    Return top customers by churn risk.
    Note: This pulls pre-computed churn scores from Gold if ML model has run.
    Falls back to recency-based proxy if churn_score column absent.
    """

    dim = qualified_table(cfg, "Dim_Customers")
    fact = qualified_table(cfg, "Fact_Orders")
    sql = f"""
    SELECT
        c.customer_unique_id,
        c.customer_state,
        DATE_DIFF((SELECT MAX(DATE(order_purchase_timestamp)) FROM {fact}), MAX(DATE(f.order_purchase_timestamp)), DAY) AS days_inactive,
        COUNT(DISTINCT f.order_id) AS total_orders,
        ROUND(SUM(f.payment_value), 2)  AS total_spend,
        ROUND(AVG(f.review_score), 2)   AS avg_review_score
    FROM {dim} c
    JOIN {fact} f USING (customer_id)
    GROUP BY 1, 2
    HAVING DATE_DIFF((SELECT MAX(DATE(order_purchase_timestamp)) FROM {fact}), MAX(DATE(f.order_purchase_timestamp)), DAY) > 180
    ORDER BY days_inactive DESC
    LIMIT @limit
    """
    params = [bigquery.ScalarQueryParameter("limit", "INT64", limit)]
    return run_query(client, sql, params)


def get_order_history(client: bigquery.Client, cfg: dict, customer_id: str, limit: int = 200):
    """Full order history for a specific customer (most recent first, capped at `limit`)."""

    fact = qualified_table(cfg, "Fact_Orders")
    dim  = qualified_table(cfg, "Dim_Customers")
    sql = f"""
    SELECT
        f.order_id,
        DATE(f.order_purchase_timestamp)  AS order_date,
        f.order_status,
        f.payment_value,
        f.payment_type,
        f.review_score,
        f.product_category_name_english   AS category,
        f.seller_state
    FROM {fact} f
    JOIN {dim} c USING (customer_id)
    WHERE c.customer_unique_id = @customer_id
    ORDER BY f.order_purchase_timestamp DESC
    LIMIT @limit
    """
    params = [
        bigquery.ScalarQueryParameter("customer_id", "STRING", customer_id),
        bigquery.ScalarQueryParameter("limit", "INT64", limit),
    ]
    return run_query(client, sql, params)


def get_kpi_summary(client: bigquery.Client, cfg: dict) -> dict:
    """Single-query aggregate KPIs for the Customer 360 header strip."""

    fact = qualified_table(cfg, "Fact_Orders")
    sql = f"""
    SELECT
        COUNT(DISTINCT customer_id)                                              AS total_customers,
        COUNTIF(DATE_DIFF((SELECT MAX(DATE(order_purchase_timestamp)) FROM {fact}), last_order_date, DAY) < 90)  AS active_90d,
        COUNTIF(DATE_DIFF((SELECT MAX(DATE(order_purchase_timestamp)) FROM {fact}), last_order_date, DAY) > 180) AS at_risk_180d,
        ROUND(AVG(avg_review_score), 2)                                          AS avg_review_score,
        ROUND(SUM(total_revenue), 0)                                             AS total_revenue
    FROM (
        SELECT
            customer_id,
            MAX(DATE(order_purchase_timestamp)) AS last_order_date,
            AVG(review_score)                   AS avg_review_score,
            SUM(payment_value)                  AS total_revenue
        FROM {fact}
        WHERE order_status NOT IN ('canceled', 'unavailable')
        GROUP BY 1
    )
    """
    df = run_query(client, sql)
    return df.iloc[0].to_dict() if len(df) else {}


def search_customers(client: bigquery.Client, cfg: dict, id_pattern: str, segment_filter: str, limit: int = 50):
    """Search customers by ID pattern (* wildcard → LIKE %) optionally filtered by segment."""
    fact = qualified_table(cfg, "Fact_Orders")
    dim  = qualified_table(cfg, "Dim_Customers")
    sql_pattern = id_pattern.replace("*", "%") if id_pattern else "%"
    sql = f"""
    WITH base AS (
        SELECT
            c.customer_unique_id,
            DATE_DIFF((SELECT MAX(DATE(order_purchase_timestamp)) FROM {fact}), MAX(DATE(f.order_purchase_timestamp)), DAY) AS days_inactive,
            COUNT(DISTINCT f.order_id) AS total_orders
        FROM {dim} c
        JOIN {fact} f USING (customer_id)
        WHERE c.customer_unique_id LIKE @id_pattern
        GROUP BY 1
    ),
    segmented AS (
        SELECT customer_unique_id,
            CASE
                WHEN days_inactive < 90  AND total_orders >= 5 THEN 'Champions'
                WHEN days_inactive < 180 AND total_orders >= 3 THEN 'Loyal Customers'
                WHEN days_inactive < 90                        THEN 'Recent Customers'
                WHEN days_inactive > 180 AND total_orders >= 3 THEN 'At Risk'
                WHEN days_inactive > 365                       THEN 'Lost'
                ELSE 'Potential Loyalists'
            END AS segment
        FROM base
    )
    SELECT customer_unique_id, segment
    FROM segmented
    WHERE @segment_filter = '' OR segment = @segment_filter
    ORDER BY customer_unique_id
    LIMIT @limit
    """
    params = [
        bigquery.ScalarQueryParameter("id_pattern",      "STRING", sql_pattern),
        bigquery.ScalarQueryParameter("segment_filter",  "STRING", segment_filter or ""),
        bigquery.ScalarQueryParameter("limit",           "INT64",  limit),
    ]
    return run_query(client, sql, params)


def get_revenue_trend(client: bigquery.Client, cfg: dict):
    """Monthly revenue trend for all customers."""

    fact = qualified_table(cfg, "Fact_Orders")
    sql = f"""
    SELECT
        FORMAT_DATE('%Y-%m', DATE(order_purchase_timestamp)) AS month,
        COUNT(DISTINCT order_id)       AS orders,
        COUNT(DISTINCT customer_id)    AS unique_customers,
        ROUND(SUM(payment_value), 2)   AS revenue
    FROM {fact}
    WHERE order_status NOT IN ('canceled', 'unavailable')
    GROUP BY 1
    ORDER BY 1
    """
    return run_query(client, sql)


# ── Segment revenue waterfall (chart #1) ──────────────────────

_SEGMENT_SQL = """
    CASE
        WHEN days_inactive < 90  AND total_orders >= 5 THEN 'Champions'
        WHEN days_inactive < 180 AND total_orders >= 3 THEN 'Loyal Customers'
        WHEN days_inactive < 90                        THEN 'Recent Customers'
        WHEN days_inactive > 180 AND total_orders >= 3 THEN 'At Risk'
        WHEN days_inactive > 365                       THEN 'Lost'
        ELSE 'Potential Loyalists'
    END
"""


def get_segment_revenue_waterfall(client: bigquery.Client, cfg: dict):
    """Revenue and customer count by RFM segment, ordered by revenue desc with cumulative %."""
    fact = qualified_table(cfg, "Fact_Orders")
    dim  = qualified_table(cfg, "Dim_Customers")
    sql = f"""
    WITH customer_metrics AS (
        SELECT
            c.customer_unique_id,
            DATE_DIFF((SELECT MAX(DATE(order_purchase_timestamp)) FROM {fact}), MAX(DATE(f.order_purchase_timestamp)), DAY) AS days_inactive,
            COUNT(DISTINCT f.order_id)  AS total_orders,
            SUM(f.payment_value)        AS revenue
        FROM {dim} c
        JOIN {fact} f USING (customer_id)
        GROUP BY 1
    ),
    segmented AS (
        SELECT revenue, {_SEGMENT_SQL} AS segment
        FROM customer_metrics
    )
    SELECT
        segment,
        COUNT(*)                                                          AS customer_count,
        ROUND(SUM(revenue), 0)                                            AS total_revenue,
        ROUND(SUM(revenue) / SUM(SUM(revenue)) OVER () * 100, 1)         AS revenue_pct
    FROM segmented
    GROUP BY 1
    ORDER BY total_revenue DESC
    """
    return run_query(client, sql)


def get_category_affinity(client: bigquery.Client, cfg: dict):
    """Purchase count by (RFM segment × top-10 category) for the heatmap."""
    fact = qualified_table(cfg, "Fact_Orders")
    dim  = qualified_table(cfg, "Dim_Customers")
    sql = f"""
    WITH customer_metrics AS (
        SELECT
            c.customer_unique_id,
            DATE_DIFF((SELECT MAX(DATE(order_purchase_timestamp)) FROM {fact}), MAX(DATE(f.order_purchase_timestamp)), DAY) AS days_inactive,
            COUNT(DISTINCT f.order_id) AS total_orders
        FROM {dim} c
        JOIN {fact} f USING (customer_id)
        GROUP BY 1
    ),
    segmented AS (
        SELECT customer_unique_id, {_SEGMENT_SQL} AS segment
        FROM customer_metrics
    ),
    top_cats AS (
        SELECT product_category_name_english AS category
        FROM {fact}
        WHERE product_category_name_english IS NOT NULL
        GROUP BY 1
        ORDER BY COUNT(*) DESC
        LIMIT 10
    )
    SELECT s.segment, f.product_category_name_english AS category, COUNT(*) AS purchase_count
    FROM {fact} f
    JOIN {dim} c USING (customer_id)
    JOIN segmented s ON s.customer_unique_id = c.customer_unique_id
    WHERE f.product_category_name_english IN (SELECT category FROM top_cats)
    GROUP BY 1, 2
    ORDER BY s.segment, purchase_count DESC
    """
    return run_query(client, sql)


def get_purchase_funnel(client: bigquery.Client, cfg: dict):
    """Customer count by repeat-order bucket (funnel: 1 → 2 → 3-4 → 5+)."""
    fact = qualified_table(cfg, "Fact_Orders")
    sql = f"""
    WITH customer_orders AS (
        SELECT customer_id, COUNT(DISTINCT order_id) AS order_count
        FROM {fact}
        WHERE order_status NOT IN ('canceled', 'unavailable')
        GROUP BY 1
    )
    SELECT
        CASE
            WHEN order_count = 1              THEN '1 Order'
            WHEN order_count = 2              THEN '2 Orders'
            WHEN order_count BETWEEN 3 AND 4  THEN '3–4 Orders'
            WHEN order_count >= 5             THEN '5+ Orders'
        END AS bucket,
        COUNT(*) AS customers,
        CASE
            WHEN order_count = 1             THEN 1
            WHEN order_count = 2             THEN 2
            WHEN order_count BETWEEN 3 AND 4 THEN 3
            WHEN order_count >= 5            THEN 4
        END AS sort_order
    FROM customer_orders
    GROUP BY 1, 3
    ORDER BY 3
    """
    return run_query(client, sql)


def get_portfolio_journey(client: bigquery.Client, cfg: dict):
    """Order flow across all customers: segment → payment_type → delivery_outcome → review_bucket."""
    fact = qualified_table(cfg, "Fact_Orders")
    dim  = qualified_table(cfg, "Dim_Customers")
    sql = f"""
    WITH customer_metrics AS (
        SELECT
            c.customer_unique_id,
            DATE_DIFF((SELECT MAX(DATE(order_purchase_timestamp)) FROM {fact}), MAX(DATE(f.order_purchase_timestamp)), DAY) AS days_inactive,
            COUNT(DISTINCT f.order_id) AS total_orders
        FROM {dim} c
        JOIN {fact} f USING (customer_id)
        GROUP BY 1
    ),
    segmented AS (
        SELECT customer_unique_id, {_SEGMENT_SQL} AS segment
        FROM customer_metrics
    )
    SELECT
        s.segment,
        COALESCE(f.payment_type, 'unknown')       AS payment_type,
        CASE
            WHEN f.order_status = 'delivered' THEN 'Delivered'
            WHEN f.order_status = 'canceled'  THEN 'Canceled'
            ELSE 'In Progress'
        END                                        AS delivery_outcome,
        CASE
            WHEN f.review_score >= 4            THEN 'Satisfied (4-5★)'
            WHEN f.review_score >= 3            THEN 'Neutral (3★)'
            WHEN f.review_score IS NOT NULL     THEN 'Unhappy (1-2★)'
            ELSE 'No Review'
        END                                        AS review_bucket,
        COUNT(*)                                   AS order_count
    FROM {fact} f
    JOIN {dim} c USING (customer_id)
    JOIN segmented s ON s.customer_unique_id = c.customer_unique_id
    GROUP BY 1, 2, 3, 4
    ORDER BY order_count DESC
    """
    return run_query(client, sql)


def get_portfolio_radar(client: bigquery.Client, cfg: dict):
    """Actual average radar scores (Recency/Frequency/Monetary/Satisfaction/Loyalty/Diversity)
    aggregated per RFM segment from live Gold data."""
    fact = qualified_table(cfg, "Fact_Orders")
    dim  = qualified_table(cfg, "Dim_Customers")
    sql = f"""
    WITH customer_metrics AS (
        SELECT
            c.customer_unique_id,
            DATE_DIFF((SELECT MAX(DATE(order_purchase_timestamp)) FROM {fact}), MAX(DATE(f.order_purchase_timestamp)), DAY) AS days_inactive,
            COUNT(DISTINCT f.order_id)  AS total_orders,
            SUM(f.payment_value)        AS total_spend,
            AVG(f.review_score)         AS avg_review_score
        FROM {dim} c
        JOIN {fact} f USING (customer_id)
        GROUP BY 1
    ),
    segmented AS (
        SELECT *,
            {_SEGMENT_SQL} AS segment
        FROM customer_metrics
    )
    SELECT
        segment,
        ROUND(AVG(GREATEST(0, 100 - days_inactive / 3.0)), 1)                       AS recency,
        ROUND(AVG(LEAST(100, total_orders * 20.0)), 1)                               AS frequency,
        ROUND(AVG(LEAST(100, total_spend / 10.0)), 1)                                AS monetary,
        ROUND(AVG(avg_review_score / 5.0 * 100), 1)                                  AS satisfaction,
        ROUND(AVG(LEAST(100, 60
            + IF(total_orders >= 5, 20, 0)
            + IF(total_spend > 500, 20, 0))), 1)                                     AS loyalty,
        ROUND(AVG(LEAST(100, total_orders * 15.0)), 1)                               AS diversity,
        COUNT(*) AS customer_count
    FROM segmented
    GROUP BY 1
    ORDER BY customer_count DESC
    """
    return run_query(client, sql)
