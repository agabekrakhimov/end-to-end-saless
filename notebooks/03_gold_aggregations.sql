-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold Layer — Business-Ready KPI & Metric Tables
-- MAGIC
-- MAGIC All tables in this layer are **batch** (not streaming).
-- MAGIC They are rebuilt on every pipeline run and are the direct source
-- MAGIC for Power BI DirectQuery / Import mode connections.
-- MAGIC
-- MAGIC Tables produced:
-- MAGIC | Table | Description |
-- MAGIC |---|---|
-- MAGIC | `gold_revenue_summary` | Daily/monthly revenue, GP, quota attainment |
-- MAGIC | `gold_regional_performance` | Revenue & margin by region + segment |
-- MAGIC | `gold_product_performance` | Revenue, volume, margin by product/category |
-- MAGIC | `gold_sales_rep_scorecard` | Rep-level pipeline, win rate, avg deal size |
-- MAGIC | `gold_funnel_conversion` | Lead→Opp→Closed funnel by channel/campaign |
-- MAGIC | `gold_campaign_roi` | Marketing ROI, CPA, revenue attributed |
-- MAGIC | `gold_cohort_retention` | Customer cohort LTV & churn signals |
-- MAGIC | `gold_revenue_leakage` | Discount + margin anomalies by rep/product |
-- MAGIC | `gold_exec_summary` | Single-row daily KPI snapshot for exec dashboard |

-- COMMAND ----------
-- MAGIC %python
-- MAGIC # Ensure gold schema exists
-- MAGIC spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

-- COMMAND ----------
-- =========================================================================
-- 1. REVENUE SUMMARY — daily & monthly roll-up
-- =========================================================================
CREATE OR REPLACE TABLE gold.gold_revenue_summary
USING DELTA
TBLPROPERTIES ("quality" = "gold", "delta.enableChangeDataFeed" = "true")
AS
WITH daily AS (
    SELECT
        order_date,
        order_year,
        order_month,
        order_quarter,
        region,
        channel,
        COUNT(*)                                         AS transaction_count,
        SUM(revenue)                                     AS gross_revenue,
        SUM(discounted_revenue)                          AS net_revenue,
        SUM(gross_profit)                                AS gross_profit,
        AVG(gross_margin)                                AS avg_gross_margin,
        SUM(quantity)                                    AS units_sold,
        AVG(revenue)                                     AS avg_deal_size,
        SUM(discount_pct * revenue)                      AS total_discount_given,
        SUM(revenue_leakage)                             AS revenue_leakage
    FROM silver.silver_transactions_enriched
    WHERE order_date IS NOT NULL
    GROUP BY order_date, order_year, order_month, order_quarter, region, channel
),
monthly_quota AS (
    -- Placeholder: replace with actual quota table when available
    SELECT
        order_year,
        order_month,
        SUM(gross_revenue) * 1.10 AS monthly_quota   -- example: 10% above prior period
    FROM daily
    GROUP BY order_year, order_month
)
SELECT
    d.*,
    mq.monthly_quota,
    ROUND(d.gross_revenue / NULLIF(mq.monthly_quota, 0) * 100, 1)  AS quota_attainment_pct,
    -- Month-over-month growth (requires window)
    LAG(SUM(d.gross_revenue)) OVER (
        PARTITION BY d.region, d.channel
        ORDER BY d.order_year, d.order_month
    ) AS prev_month_revenue,
    ROUND(
        (SUM(d.gross_revenue) - LAG(SUM(d.gross_revenue)) OVER (
            PARTITION BY d.region, d.channel
            ORDER BY d.order_year, d.order_month
        )) / NULLIF(LAG(SUM(d.gross_revenue)) OVER (
            PARTITION BY d.region, d.channel
            ORDER BY d.order_year, d.order_month
        ), 0) * 100, 2
    ) AS mom_growth_pct
FROM daily d
LEFT JOIN monthly_quota mq
    ON d.order_year = mq.order_year AND d.order_month = mq.order_month;

-- COMMAND ----------
-- =========================================================================
-- 2. REGIONAL PERFORMANCE
-- =========================================================================
CREATE OR REPLACE TABLE gold.gold_regional_performance
USING DELTA
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
    te.region,
    te.state,
    te.segment,
    te.order_year,
    te.order_month,
    te.order_quarter,
    COUNT(DISTINCT te.customer_id)                       AS unique_customers,
    COUNT(*)                                             AS deal_count,
    SUM(te.revenue)                                      AS gross_revenue,
    SUM(te.discounted_revenue)                           AS net_revenue,
    SUM(te.gross_profit)                                 AS gross_profit,
    AVG(te.gross_margin)                                 AS avg_margin,
    AVG(te.days_to_close)                                AS avg_days_to_close,
    AVG(te.revenue)                                      AS avg_deal_size,
    SUM(te.revenue_leakage)                              AS revenue_leakage,
    -- Segment mix
    SUM(CASE WHEN te.segment = 'Enterprise'  THEN te.revenue ELSE 0 END) AS enterprise_revenue,
    SUM(CASE WHEN te.segment = 'Mid-Market'  THEN te.revenue ELSE 0 END) AS mid_market_revenue,
    SUM(CASE WHEN te.segment = 'SMB'         THEN te.revenue ELSE 0 END) AS smb_revenue,
    -- Channel mix
    SUM(CASE WHEN te.channel = 'Direct Sales' THEN te.revenue ELSE 0 END) AS direct_revenue,
    SUM(CASE WHEN te.channel = 'Partner'      THEN te.revenue ELSE 0 END) AS partner_revenue,
    SUM(CASE WHEN te.channel = 'Web Direct'   THEN te.revenue ELSE 0 END) AS web_revenue
FROM silver.silver_transactions_enriched te
GROUP BY
    te.region, te.state, te.segment,
    te.order_year, te.order_month, te.order_quarter;

-- COMMAND ----------
-- =========================================================================
-- 3. PRODUCT PERFORMANCE
-- =========================================================================
CREATE OR REPLACE TABLE gold.gold_product_performance
USING DELTA
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
    te.product_id,
    te.product_name,
    te.category,
    te.sub_category,
    te.order_year,
    te.order_month,
    SUM(te.quantity)                                     AS units_sold,
    SUM(te.revenue)                                      AS gross_revenue,
    SUM(te.discounted_revenue)                           AS net_revenue,
    SUM(te.gross_profit)                                 AS gross_profit,
    AVG(te.gross_margin)                                 AS avg_margin,
    AVG(te.unit_price)                                   AS avg_selling_price,
    COUNT(DISTINCT te.customer_id)                       AS unique_buyers,
    COUNT(*)                                             AS transaction_count,
    -- Discount pressure
    AVG(te.discount_pct)                                 AS avg_discount_pct,
    SUM(CASE WHEN te.discount_pct > 0.20 THEN 1 ELSE 0 END) AS high_discount_deals,
    SUM(te.revenue_leakage)                              AS revenue_leakage,
    -- Rank within category
    RANK() OVER (
        PARTITION BY te.category, te.order_year, te.order_month
        ORDER BY SUM(te.revenue) DESC
    ) AS revenue_rank_in_category
FROM silver.silver_transactions_enriched te
GROUP BY
    te.product_id, te.product_name, te.category, te.sub_category,
    te.order_year, te.order_month;

-- COMMAND ----------
-- =========================================================================
-- 4. SALES REP SCORECARD
-- =========================================================================
CREATE OR REPLACE TABLE gold.gold_sales_rep_scorecard
USING DELTA
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
    te.sales_rep_id,
    te.order_year,
    te.order_month,
    te.region,
    COUNT(*)                                             AS deals_closed,
    SUM(te.revenue)                                      AS total_revenue,
    AVG(te.revenue)                                      AS avg_deal_size,
    SUM(te.gross_profit)                                 AS total_gross_profit,
    AVG(te.gross_margin)                                 AS avg_margin,
    AVG(te.days_to_close)                                AS avg_days_to_close,
    AVG(te.discount_pct)                                 AS avg_discount_given,
    SUM(te.revenue_leakage)                              AS leakage_generated,
    COUNT(DISTINCT te.customer_id)                       AS unique_customers,
    -- Win rate (deals with positive revenue / total leads — joined below)
    COUNT(*) * 1.0 / NULLIF(
        (SELECT COUNT(*) FROM silver.silver_leads l
         WHERE l.sales_rep_id = te.sales_rep_id
           AND YEAR(l.created_date) = te.order_year
           AND MONTH(l.created_date) = te.order_month), 0
    ) AS win_rate,
    RANK() OVER (
        PARTITION BY te.order_year, te.order_month
        ORDER BY SUM(te.revenue) DESC
    ) AS revenue_rank
FROM silver.silver_transactions_enriched te
GROUP BY te.sales_rep_id, te.order_year, te.order_month, te.region;

-- COMMAND ----------
-- =========================================================================
-- 5. FUNNEL CONVERSION (Lead → Qualified → Converted)
-- =========================================================================
CREATE OR REPLACE TABLE gold.gold_funnel_conversion
USING DELTA
TBLPROPERTIES ("quality" = "gold")
AS
WITH funnel AS (
    SELECT
        l.lead_source,
        c.channel                                        AS campaign_channel,
        c.region,
        YEAR(l.created_date)                             AS cohort_year,
        MONTH(l.created_date)                            AS cohort_month,
        COUNT(*)                                         AS total_leads,
        SUM(CASE WHEN l.status IN ('Qualified','Converted') THEN 1 ELSE 0 END) AS qualified_leads,
        SUM(CASE WHEN l.status = 'Converted'  THEN 1 ELSE 0 END) AS converted_leads,
        SUM(CASE WHEN l.status = 'Lost'       THEN 1 ELSE 0 END) AS lost_leads,
        SUM(l.deal_value)                                AS total_pipeline_value,
        SUM(CASE WHEN l.is_converted THEN l.deal_value ELSE 0 END) AS won_value,
        AVG(CASE WHEN l.is_converted THEN l.conversion_lag_days ELSE NULL END) AS avg_days_to_convert
    FROM silver.silver_leads l
    LEFT JOIN silver.silver_campaigns c ON l.campaign_id = c.campaign_id
    GROUP BY
        l.lead_source, c.channel, c.region,
        YEAR(l.created_date), MONTH(l.created_date)
)
SELECT
    *,
    ROUND(qualified_leads * 100.0 / NULLIF(total_leads, 0),     2) AS lead_to_qualified_pct,
    ROUND(converted_leads * 100.0 / NULLIF(qualified_leads, 0), 2) AS qualified_to_close_pct,
    ROUND(converted_leads * 100.0 / NULLIF(total_leads, 0),     2) AS overall_conversion_pct
FROM funnel;

-- COMMAND ----------
-- =========================================================================
-- 6. CAMPAIGN ROI
-- =========================================================================
CREATE OR REPLACE TABLE gold.gold_campaign_roi
USING DELTA
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
    c.campaign_id,
    c.campaign_name,
    c.channel,
    c.region,
    c.start_date,
    c.end_date,
    c.budget,
    c.spend,
    c.impressions,
    c.clicks,
    c.conversions,
    c.ctr,
    c.conversion_rate,
    c.cpa,
    c.budget_utilisation,
    -- Attributed revenue (leads that converted from this campaign)
    COALESCE(attr.attributed_revenue, 0)                 AS attributed_revenue,
    ROUND(COALESCE(attr.attributed_revenue, 0) / NULLIF(c.spend, 0), 2) AS revenue_per_dollar_spent,
    ROUND((COALESCE(attr.attributed_revenue, 0) - c.spend) / NULLIF(c.spend, 0) * 100, 1) AS roi_pct,
    attr.converted_leads
FROM silver.silver_campaigns c
LEFT JOIN (
    SELECT
        campaign_id,
        COUNT(*)        AS converted_leads,
        SUM(deal_value) AS attributed_revenue
    FROM silver.silver_leads
    WHERE is_converted = TRUE
    GROUP BY campaign_id
) attr ON c.campaign_id = attr.campaign_id;

-- COMMAND ----------
-- =========================================================================
-- 7. COHORT RETENTION (monthly customer cohorts)
-- =========================================================================
CREATE OR REPLACE TABLE gold.gold_cohort_retention
USING DELTA
TBLPROPERTIES ("quality" = "gold")
AS
WITH first_purchase AS (
    SELECT
        customer_id,
        MIN(order_date)                    AS first_order_date,
        DATE_TRUNC('month', MIN(order_date)) AS cohort_month
    FROM silver.silver_sales_transactions
    GROUP BY customer_id
),
monthly_activity AS (
    SELECT
        t.customer_id,
        DATE_TRUNC('month', t.order_date)  AS activity_month,
        SUM(t.revenue)                     AS monthly_revenue,
        COUNT(*)                           AS purchase_count
    FROM silver.silver_sales_transactions t
    GROUP BY t.customer_id, DATE_TRUNC('month', t.order_date)
)
SELECT
    fp.cohort_month,
    ma.activity_month,
    MONTHS_BETWEEN(ma.activity_month, fp.cohort_month) AS cohort_age_months,
    COUNT(DISTINCT ma.customer_id)          AS active_customers,
    COUNT(DISTINCT fp.customer_id)          AS cohort_size,
    ROUND(COUNT(DISTINCT ma.customer_id) * 100.0 / NULLIF(COUNT(DISTINCT fp.customer_id), 0), 1) AS retention_rate,
    SUM(ma.monthly_revenue)                AS cohort_revenue,
    AVG(ma.monthly_revenue)                AS avg_revenue_per_customer
FROM first_purchase fp
LEFT JOIN monthly_activity ma ON fp.customer_id = ma.customer_id
WHERE ma.activity_month >= fp.cohort_month
GROUP BY fp.cohort_month, ma.activity_month, MONTHS_BETWEEN(ma.activity_month, fp.cohort_month)
ORDER BY fp.cohort_month, cohort_age_months;

-- COMMAND ----------
-- =========================================================================
-- 8. REVENUE LEAKAGE DETAIL
-- =========================================================================
CREATE OR REPLACE TABLE gold.gold_revenue_leakage
USING DELTA
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
    te.transaction_id,
    te.order_date,
    te.order_year,
    te.order_month,
    te.customer_id,
    te.company_name,
    te.segment,
    te.region,
    te.sales_rep_id,
    te.product_id,
    te.product_name,
    te.category,
    te.revenue,
    te.discount_pct,
    te.discounted_revenue,
    te.gross_margin,
    te.revenue_leakage,
    -- Leakage severity classification
    CASE
        WHEN te.revenue_leakage > 10000 THEN 'Critical'
        WHEN te.revenue_leakage > 5000  THEN 'High'
        WHEN te.revenue_leakage > 1000  THEN 'Medium'
        ELSE 'Low'
    END AS leakage_severity,
    -- Context flags
    CASE WHEN te.discount_pct > 0.20              THEN TRUE ELSE FALSE END AS excessive_discount,
    CASE WHEN te.gross_margin < 0.10              THEN TRUE ELSE FALSE END AS low_margin_deal,
    CASE WHEN te.days_to_close > 90               THEN TRUE ELSE FALSE END AS long_sales_cycle
FROM silver.silver_transactions_enriched te
WHERE te.revenue_leakage > 0
ORDER BY te.revenue_leakage DESC;

-- COMMAND ----------
-- =========================================================================
-- 9. EXECUTIVE SUMMARY SNAPSHOT (one row per day — latest state)
-- =========================================================================
CREATE OR REPLACE TABLE gold.gold_exec_summary
USING DELTA
TBLPROPERTIES ("quality" = "gold")
AS
WITH latest AS (
    SELECT MAX(order_date) AS snapshot_date FROM silver.silver_sales_transactions
),
mtd AS (
    SELECT
        SUM(revenue)                AS mtd_revenue,
        SUM(gross_profit)           AS mtd_gross_profit,
        AVG(gross_margin)           AS mtd_avg_margin,
        COUNT(*)                    AS mtd_deals,
        COUNT(DISTINCT customer_id) AS mtd_unique_customers
    FROM silver.silver_sales_transactions t
    CROSS JOIN latest l
    WHERE t.order_date BETWEEN DATE_TRUNC('month', l.snapshot_date) AND l.snapshot_date
),
ytd AS (
    SELECT
        SUM(revenue)    AS ytd_revenue,
        SUM(gross_profit) AS ytd_gross_profit,
        COUNT(*)        AS ytd_deals
    FROM silver.silver_sales_transactions t
    CROSS JOIN latest l
    WHERE YEAR(t.order_date) = YEAR(l.snapshot_date) AND t.order_date <= l.snapshot_date
),
leakage AS (
    SELECT COALESCE(SUM(revenue_leakage), 0) AS total_leakage_ytd
    FROM gold.gold_revenue_leakage
    CROSS JOIN latest l
    WHERE order_year = YEAR(l.snapshot_date)
),
pipeline AS (
    SELECT COALESCE(SUM(deal_value), 0) AS open_pipeline
    FROM silver.silver_leads
    WHERE status IN ('New', 'Qualified')
)
SELECT
    l.snapshot_date,
    mtd.mtd_revenue,
    mtd.mtd_gross_profit,
    ROUND(mtd.mtd_avg_margin * 100, 1) AS mtd_margin_pct,
    mtd.mtd_deals,
    mtd.mtd_unique_customers,
    ytd.ytd_revenue,
    ytd.ytd_gross_profit,
    ytd.ytd_deals,
    lk.total_leakage_ytd,
    pl.open_pipeline,
    ROUND(lk.total_leakage_ytd / NULLIF(ytd.ytd_revenue, 0) * 100, 2) AS leakage_as_pct_of_revenue
FROM latest l
CROSS JOIN mtd
CROSS JOIN ytd
CROSS JOIN leakage lk
CROSS JOIN pipeline pl;
