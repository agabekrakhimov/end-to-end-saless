# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw Data Ingestion via Delta Live Tables
# MAGIC Medallion Architecture: **Bronze (Raw) → Silver (Clean) → Gold (Aggregated)**
# MAGIC
# MAGIC This notebook defines DLT streaming tables that land raw data from:
# MAGIC - CRM exports (sales transactions, leads, customers)
# MAGIC - ERP exports (products, costs)
# MAGIC - Marketing platform API exports (campaigns)

# COMMAND ----------
import dlt
from pyspark.sql.functions import (
    current_timestamp, input_file_name, col, lit,
    to_date, to_timestamp, regexp_replace, trim
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, DateType, TimestampType
)

# --------------------------------------------------------------------------- #
#  CONFIG — override via pipeline parameters in Databricks Jobs UI             #
# --------------------------------------------------------------------------- #
RAW_BASE    = spark.conf.get("raw.base.path",    "/mnt/raw")
SCHEMA_BASE = spark.conf.get("schema.base.path", "/mnt/bronze/schema")

# --------------------------------------------------------------------------- #
#  SCHEMA DEFINITIONS                                                           #
# --------------------------------------------------------------------------- #

SALES_SCHEMA = StructType([
    StructField("transaction_id",  StringType(),  False),
    StructField("customer_id",     StringType(),  False),
    StructField("product_id",      StringType(),  False),
    StructField("sales_rep_id",    StringType(),  True),
    StructField("order_date",      StringType(),  True),   # cast later
    StructField("close_date",      StringType(),  True),
    StructField("revenue",         DoubleType(),  True),
    StructField("quantity",        IntegerType(), True),
    StructField("discount_pct",    DoubleType(),  True),
    StructField("cost",            DoubleType(),  True),
    StructField("region",          StringType(),  True),
    StructField("channel",         StringType(),  True),   # web/direct/partner
    StructField("deal_stage",      StringType(),  True),
    StructField("currency",        StringType(),  True),
])

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id",     StringType(),  False),
    StructField("company_name",    StringType(),  True),
    StructField("industry",        StringType(),  True),
    StructField("segment",         StringType(),  True),   # SMB/Mid/Enterprise
    StructField("country",         StringType(),  True),
    StructField("state",           StringType(),  True),
    StructField("city",            StringType(),  True),
    StructField("created_date",    StringType(),  True),
    StructField("account_owner",   StringType(),  True),
    StructField("health_score",    DoubleType(),  True),   # 0-100
    StructField("total_ltv",       DoubleType(),  True),
])

PRODUCT_SCHEMA = StructType([
    StructField("product_id",      StringType(),  False),
    StructField("product_name",    StringType(),  True),
    StructField("category",        StringType(),  True),
    StructField("sub_category",    StringType(),  True),
    StructField("unit_price",      DoubleType(),  True),
    StructField("unit_cost",       DoubleType(),  True),
    StructField("launch_date",     StringType(),  True),
    StructField("is_active",       StringType(),  True),
])

LEAD_SCHEMA = StructType([
    StructField("lead_id",         StringType(),  False),
    StructField("campaign_id",     StringType(),  True),
    StructField("customer_id",     StringType(),  True),
    StructField("lead_source",     StringType(),  True),
    StructField("created_date",    StringType(),  True),
    StructField("status",          StringType(),  True),  # New/Qualified/Converted/Lost
    StructField("converted_date",  StringType(),  True),
    StructField("deal_value",      DoubleType(),  True),
    StructField("sales_rep_id",    StringType(),  True),
])

CAMPAIGN_SCHEMA = StructType([
    StructField("campaign_id",     StringType(),  False),
    StructField("campaign_name",   StringType(),  True),
    StructField("channel",         StringType(),  True),  # Email/Paid/Social/Event
    StructField("start_date",      StringType(),  True),
    StructField("end_date",        StringType(),  True),
    StructField("budget",          DoubleType(),  True),
    StructField("spend",           DoubleType(),  True),
    StructField("impressions",     IntegerType(), True),
    StructField("clicks",          IntegerType(), True),
    StructField("conversions",     IntegerType(), True),
    StructField("region",          StringType(),  True),
])

# --------------------------------------------------------------------------- #
#  HELPER: standard audit columns                                               #
# --------------------------------------------------------------------------- #
def add_audit_cols(df):
    return (
        df
        .withColumn("_ingest_ts",      current_timestamp())
        .withColumn("_source_file",    input_file_name())
        .withColumn("_pipeline_run_id", lit(spark.conf.get("pipelines.id", "local")))
    )

def cloud_files_stream(path, schema, fmt="csv"):
    opts = {
        "cloudFiles.format":         fmt,
        "cloudFiles.schemaLocation": f"{SCHEMA_BASE}/{path.split('/')[-1]}",
        "header":                    "true",
        "inferSchema":               "false",
        "multiLine":                 "true",
    }
    return (
        spark.readStream
        .format("cloudFiles")
        .schema(schema)
        .options(**opts)
        .load(f"{RAW_BASE}/{path}/")
    )

# --------------------------------------------------------------------------- #
#  DLT TABLE DEFINITIONS                                                        #
# --------------------------------------------------------------------------- #

@dlt.table(
    name="bronze_sales_transactions",
    comment="Raw sales/CRM transaction records — no transformations applied",
    table_properties={
        "quality":            "bronze",
        "pipelines.autoOptimize.managed": "true",
    },
)
@dlt.expect("has_transaction_id", "transaction_id IS NOT NULL")
@dlt.expect("positive_revenue",   "revenue IS NULL OR revenue >= 0")
def bronze_sales_transactions():
    return add_audit_cols(
        cloud_files_stream("sales_transactions", SALES_SCHEMA)
    )


@dlt.table(
    name="bronze_customers",
    comment="Raw customer/account master data from CRM",
    table_properties={"quality": "bronze"},
)
@dlt.expect("has_customer_id", "customer_id IS NOT NULL")
def bronze_customers():
    return add_audit_cols(
        cloud_files_stream("customers", CUSTOMER_SCHEMA)
    )


@dlt.table(
    name="bronze_products",
    comment="Raw product catalog from ERP",
    table_properties={"quality": "bronze"},
)
@dlt.expect("has_product_id", "product_id IS NOT NULL")
def bronze_products():
    return add_audit_cols(
        cloud_files_stream("products", PRODUCT_SCHEMA)
    )


@dlt.table(
    name="bronze_leads",
    comment="Raw marketing leads before qualification",
    table_properties={"quality": "bronze"},
)
@dlt.expect("has_lead_id", "lead_id IS NOT NULL")
def bronze_leads():
    return add_audit_cols(
        cloud_files_stream("leads", LEAD_SCHEMA)
    )


@dlt.table(
    name="bronze_campaigns",
    comment="Raw campaign spend & performance data from marketing platforms",
    table_properties={"quality": "bronze"},
)
@dlt.expect("has_campaign_id", "campaign_id IS NOT NULL")
@dlt.expect("spend_within_budget", "spend IS NULL OR budget IS NULL OR spend <= budget * 1.15")
def bronze_campaigns():
    return add_audit_cols(
        cloud_files_stream("campaigns", CAMPAIGN_SCHEMA)
    )


# --------------------------------------------------------------------------- #
#  DLT QUALITY DASHBOARD TABLE                                                  #
# --------------------------------------------------------------------------- #

@dlt.table(
    name="bronze_data_quality_log",
    comment="Aggregated DLT expectation results for monitoring",
)
def bronze_data_quality_log():
    """
    Query event_log() to surface expectation pass/fail counts.
    Run this query AFTER the pipeline has executed at least once.
    """
    return spark.sql("""
        SELECT
            timestamp,
            details:flow_progress.name                               AS table_name,
            details:flow_progress.data_quality.dropped_records       AS dropped_records,
            details:flow_progress.data_quality.expectations[0].name  AS expectation_name,
            details:flow_progress.data_quality.expectations[0].passed_records AS passed,
            details:flow_progress.data_quality.expectations[0].failed_records AS failed
        FROM event_log(TABLE(bronze_sales_transactions))
        WHERE event_type = 'flow_progress'
        ORDER BY timestamp DESC
        LIMIT 500
    """)
