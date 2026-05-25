# Power BI Dashboard — Setup Guide

## Connection Setup

1. Open `sales_dashboard.pbix` in Power BI Desktop (v2.125+)
2. Go to **Home → Transform Data → Data Source Settings**
3. Update the **Databricks SQL Endpoint** connection string:
   ```
   Server:   <workspace>.azuredatabricks.net
   HTTP Path: /sql/1.0/warehouses/<warehouse_id>
   ```
4. Enter your **Personal Access Token** (generate at Databricks → User Settings → Access Tokens)
5. Click **Refresh** — all 12 report pages will load from the Gold layer tables

## Report Pages

| # | Page | Primary Table | Key Visuals |
|---|------|---------------|-------------|
| 1 | Executive Summary | `gold_exec_summary` | KPI cards, MTD vs prior period, quota gauge |
| 2 | Revenue Trend | `gold_revenue_summary` | Line + area chart (daily/monthly toggle), MoM % |
| 3 | Regional Heat Map | `gold_regional_performance` | Filled map, bar chart by state, segment slicer |
| 4 | Sales Funnel | `gold_funnel_conversion` | Funnel chart, conversion %, channel breakdown |
| 5 | Product Performance | `gold_product_performance` | Treemap, scatter (volume vs margin), top-10 table |
| 6 | Rep Scorecard | `gold_sales_rep_scorecard` | Leaderboard table, win-rate bubble chart, rank trend |
| 7 | Campaign ROI | `gold_campaign_roi` | Bar (ROI %), scatter (spend vs revenue), CPA table |
| 8 | Customer Cohorts | `gold_cohort_retention` | Cohort retention matrix heatmap, LTV waterfall |
| 9 | Revenue Leakage | `gold_revenue_leakage` | Sankey-style loss breakdown, rep/product drill-through |
| 10 | Deal Pipeline | `silver_leads` | Kanban-style stage funnel, pipeline velocity |
| 11 | Anomaly Log | `gold.anomaly_alert_log` | Timeline of alerts, severity heatmap |
| 12 | Data Quality | `bronze_data_quality_log` | Pass/fail rates per table, expectation trends |

## Row-Level Security

RLS is configured per regional manager role:

```
[Region] = USERPRINCIPALNAME()
```

To assign roles in Power BI Service:
1. **Dataset → Security → Manage Roles**
2. Add each regional manager's email to their region role
3. Test using **View As → Region = Northeast** etc.

## Scheduled Refresh

Set up in Power BI Service:
- **Dataset → Scheduled Refresh → Daily at 09:00 EST**
- Uses the Databricks gateway connection
- Requires Databricks SQL warehouse to be running (set auto-start on)

## DAX Measures Reference

Key custom measures used across reports:

```dax
-- MTD Revenue
MTD Revenue =
CALCULATE(
    SUM(gold_revenue_summary[gross_revenue]),
    DATESMTD(gold_revenue_summary[order_date])
)

-- YoY Growth %
YoY Growth % =
VAR current = [MTD Revenue]
VAR prior = CALCULATE([MTD Revenue], SAMEPERIODLASTYEAR(gold_revenue_summary[order_date]))
RETURN DIVIDE(current - prior, prior, 0) * 100

-- Gross Margin %
Gross Margin % =
DIVIDE(
    SUM(gold_revenue_summary[gross_profit]),
    SUM(gold_revenue_summary[gross_revenue]),
    0
)

-- Quota Attainment
Quota Attainment % =
AVERAGE(gold_revenue_summary[quota_attainment_pct])

-- Revenue Leakage as % of Revenue
Leakage % =
DIVIDE(
    SUM(gold_revenue_leakage[revenue_leakage]),
    SUM(gold_revenue_summary[gross_revenue]),
    0
) * 100
```
