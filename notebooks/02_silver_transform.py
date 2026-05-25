# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Cleaning, Deduplication & Schema Enforcement
# MAGIC
# MAGIC Reads from Bronze DLT tables and produces validated, conformed Silver tables.
# MAGIC Rules applied at this layer:
# MAGIC - Cast all string dates → proper date/timestamp types
# MAGIC - Standardise categorical values (region, channel, segment)
# MAGIC - Remove exact duplicates; keep latest version of each entity
# MAGIC - Compute derived columns (gross_margin, days_to_close, etc.)
# MAGIC - Enforce referential integrity via DLT expectations

# COMMAND ----------
import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --------------------------------------------------------------------------- #
#  LOOKUP MAPS — used to standardise free-text categoricals                    #
# --------------------------------------------------------------------------- #
REGION_MAP = {
    "ne": "Northeast", "northeast": "Northeast", "north east": "Northeast",
    "se": "Southeast", "southeast": "Southeast", "south east": "Southeast",
    "mw": "Midwest",   "midwest":   "Midwest",   "mid-west":  "Midwest",
    "sw": "Southwest", "southwest": "Southwest", "south west":"Southwest",
    "w":  "West",      "west":      "West",
}

CHANNEL_MAP = {
    "web":      "Web Direct",   "website": "Web Direct",   "online": "Web Direct",
    "direct":   "Direct Sales", "field":   "Direct Sales",
    "partner":  "Partner",      "channel": "Partner",      "reseller": "Partner",
    "event":    "Events",       "conference": "Events",
    "inbound":  "Inbound",      "content":    "Inbound",
    "outbound": "Outbound",     "cold":       "Outbound",
}

SEGMENT_MAP = {
    "smb":        "SMB",        "small":      "SMB",
    "mid":        "Mid-Market", "midmarket":  "Mid-Market", "mid-market": "Mid-Market",
    "enterprise": "Enterprise", "ent":        "Enterprise", "large":      "Enterprise",
}

def _make_map_expr(col_name, mapping: dict):
    """Build a CASE WHEN expression to standardise categoricals."""
    expr = "CASE"
    for k, v in mapping.items():
        expr += f" WHEN lower(trim({col_name})) = '{k}' THEN '{v}'"
    expr += f" ELSE initcap(trim({col_name})) END"
    return F.expr(expr)


# --------------------------------------------------------------------------- #
#  SILVER: SALES TRANSACTIONS                                                   #
# --------------------------------------------------------------------------- #

@dlt.table(
    name="silver_sales_transactions",
    comment="Cleaned, typed, and deduplicated sales transactions",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"},
)
@dlt.expect_or_drop("valid_revenue",     "revenue > 0")
@dlt.expect_or_drop("valid_order_date",  "order_date IS NOT NULL")
@dlt.expect("discount_range",            "discount_pct IS NULL OR (discount_pct >= 0 AND discount_pct <= 1)")
def silver_sales_transactions():
    # Deduplication window: keep the most-recently ingested record per transaction_id
    w_dedup = Window.partitionBy("transaction_id").orderBy(F.col("_ingest_ts").desc())

    return (
        dlt.read_stream("bronze_sales_transactions")
        # --- cast dates ---
        .withColumn("order_date",  F.to_date("order_date",  "yyyy-MM-dd"))
        .withColumn("close_date",  F.to_date("close_date",  "yyyy-MM-dd"))
        # --- derived metrics ---
        .withColumn("gross_profit",   F.col("revenue") - F.col("cost") * F.col("quantity"))
        .withColumn("gross_margin",   F.when(F.col("revenue") > 0,
                                             (F.col("revenue") - F.col("cost") * F.col("quantity")) / F.col("revenue")
                                     ).otherwise(F.lit(None)))
        .withColumn("discounted_revenue", F.col("revenue") * (1 - F.coalesce("discount_pct", F.lit(0))))
        .withColumn("days_to_close",  F.datediff("close_date", "order_date"))
        .withColumn("order_year",     F.year("order_date"))
        .withColumn("order_month",    F.month("order_date"))
        .withColumn("order_quarter",  F.quarter("order_date"))
        # --- standardise categoricals ---
        .withColumn("region",   _make_map_expr("region",  REGION_MAP))
        .withColumn("channel",  _make_map_expr("channel", CHANNEL_MAP))
        .withColumn("currency", F.upper(F.trim("currency")))
        # --- dedup ---
        .withColumn("_row_num", F.row_number().over(w_dedup))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num", "_source_file", "_pipeline_run_id")
    )


# --------------------------------------------------------------------------- #
#  SILVER: CUSTOMERS                                                            #
# --------------------------------------------------------------------------- #

@dlt.table(
    name="silver_customers",
    comment="Cleaned customer master with segment standardisation",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"},
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
def silver_customers():
    w_dedup = Window.partitionBy("customer_id").orderBy(F.col("_ingest_ts").desc())

    return (
        dlt.read_stream("bronze_customers")
        .withColumn("created_date",  F.to_date("created_date", "yyyy-MM-dd"))
        .withColumn("segment",       _make_map_expr("segment", SEGMENT_MAP))
        .withColumn("country",       F.upper(F.trim("country")))
        .withColumn("state",         F.upper(F.trim("state")))
        .withColumn("company_name",  F.trim("company_name"))
        .withColumn("health_band",
            F.when(F.col("health_score") >= 80, "Healthy")
             .when(F.col("health_score") >= 50, "At Risk")
             .otherwise("Churning"))
        .withColumn("_row_num", F.row_number().over(w_dedup))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num", "_source_file", "_pipeline_run_id")
    )


# --------------------------------------------------------------------------- #
#  SILVER: PRODUCTS                                                             #
# --------------------------------------------------------------------------- #

@dlt.table(
    name="silver_products",
    comment="Cleaned product catalog with margin calculations",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dlt.expect("positive_price",           "unit_price IS NULL OR unit_price > 0")
def silver_products():
    w_dedup = Window.partitionBy("product_id").orderBy(F.col("_ingest_ts").desc())

    return (
        dlt.read_stream("bronze_products")
        .withColumn("launch_date",    F.to_date("launch_date", "yyyy-MM-dd"))
        .withColumn("is_active",      F.col("is_active").cast("boolean"))
        .withColumn("unit_margin",    F.col("unit_price") - F.col("unit_cost"))
        .withColumn("margin_pct",
            F.when(F.col("unit_price") > 0,
                   (F.col("unit_price") - F.col("unit_cost")) / F.col("unit_price"))
             .otherwise(F.lit(None)))
        .withColumn("category",       F.trim("category"))
        .withColumn("sub_category",   F.trim("sub_category"))
        .withColumn("_row_num",       F.row_number().over(w_dedup))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num", "_source_file", "_pipeline_run_id")
    )


# --------------------------------------------------------------------------- #
#  SILVER: LEADS                                                                #
# --------------------------------------------------------------------------- #

@dlt.table(
    name="silver_leads",
    comment="Qualified leads with conversion lag and status normalisation",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_lead_id", "lead_id IS NOT NULL")
def silver_leads():
    w_dedup = Window.partitionBy("lead_id").orderBy(F.col("_ingest_ts").desc())

    return (
        dlt.read_stream("bronze_leads")
        .withColumn("created_date",   F.to_date("created_date",   "yyyy-MM-dd"))
        .withColumn("converted_date", F.to_date("converted_date", "yyyy-MM-dd"))
        .withColumn("conversion_lag_days", F.datediff("converted_date", "created_date"))
        .withColumn("is_converted",
            F.when(F.col("status") == "Converted", F.lit(True)).otherwise(F.lit(False)))
        .withColumn("status", F.initcap(F.trim("status")))
        .withColumn("lead_source", F.initcap(F.trim("lead_source")))
        .withColumn("_row_num", F.row_number().over(w_dedup))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num", "_source_file", "_pipeline_run_id")
    )


# --------------------------------------------------------------------------- #
#  SILVER: CAMPAIGNS                                                            #
# --------------------------------------------------------------------------- #

@dlt.table(
    name="silver_campaigns",
    comment="Marketing campaigns with derived efficiency metrics",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_campaign_id", "campaign_id IS NOT NULL")
@dlt.expect("ctr_range", "clicks IS NULL OR impressions IS NULL OR clicks <= impressions")
def silver_campaigns():
    w_dedup = Window.partitionBy("campaign_id").orderBy(F.col("_ingest_ts").desc())

    return (
        dlt.read_stream("bronze_campaigns")
        .withColumn("start_date",  F.to_date("start_date", "yyyy-MM-dd"))
        .withColumn("end_date",    F.to_date("end_date",   "yyyy-MM-dd"))
        .withColumn("duration_days", F.datediff("end_date", "start_date"))
        .withColumn("ctr",
            F.when(F.col("impressions") > 0,
                   F.col("clicks").cast("double") / F.col("impressions"))
             .otherwise(F.lit(None)))
        .withColumn("conversion_rate",
            F.when(F.col("clicks") > 0,
                   F.col("conversions").cast("double") / F.col("clicks"))
             .otherwise(F.lit(None)))
        .withColumn("cpa",
            F.when(F.col("conversions") > 0,
                   F.col("spend") / F.col("conversions"))
             .otherwise(F.lit(None)))   # Cost per acquisition
        .withColumn("roi",
            F.when(F.col("spend") > 0,
                   (F.col("budget") - F.col("spend")) / F.col("spend"))
             .otherwise(F.lit(None)))
        .withColumn("budget_utilisation",
            F.when(F.col("budget") > 0, F.col("spend") / F.col("budget"))
             .otherwise(F.lit(None)))
        .withColumn("channel", _make_map_expr("channel", CHANNEL_MAP))
        .withColumn("region",  _make_map_expr("region",  REGION_MAP))
        .withColumn("_row_num", F.row_number().over(w_dedup))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num", "_source_file", "_pipeline_run_id")
    )


# --------------------------------------------------------------------------- #
#  SILVER: ENRICHED TRANSACTIONS (join with customer + product)                #
# --------------------------------------------------------------------------- #

@dlt.table(
    name="silver_transactions_enriched",
    comment="Sales transactions joined with customer and product dimensions",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"},
)
def silver_transactions_enriched():
    txn  = dlt.read("silver_sales_transactions")
    cust = dlt.read("silver_customers").select(
        "customer_id", "company_name", "segment", "industry", "country", "state", "health_band"
    )
    prod = dlt.read("silver_products").select(
        "product_id", "product_name", "category", "sub_category", "margin_pct"
    )

    return (
        txn
        .join(cust, on="customer_id", how="left")
        .join(prod, on="product_id",  how="left")
        .withColumn("revenue_leakage",
            # Flag deals with discount > 20% AND below-average margin
            F.when(
                (F.col("discount_pct") > 0.20) & (F.col("gross_margin") < 0.15),
                F.col("discounted_revenue") * F.col("discount_pct")
            ).otherwise(F.lit(0.0)))
    )
