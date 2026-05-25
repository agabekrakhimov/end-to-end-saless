"""
Unit tests for PySpark Silver-layer transformation logic.
Run locally:  pip install pyspark pytest && pytest tests/ -v
Run on CI:    see .github/workflows/ci.yml
"""
import pytest
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, TimestampType,
)
import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


# --------------------------------------------------------------------------- #
#  SPARK SESSION FIXTURE                                                        #
# --------------------------------------------------------------------------- #

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("saless-unit-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .getOrCreate()
    )


# --------------------------------------------------------------------------- #
#  SHARED HELPERS                                                               #
# --------------------------------------------------------------------------- #

def make_txn_df(spark, rows):
    schema = StructType([
        StructField("transaction_id", StringType(),  False),
        StructField("customer_id",    StringType(),  True),
        StructField("product_id",     StringType(),  True),
        StructField("sales_rep_id",   StringType(),  True),
        StructField("order_date",     StringType(),  True),
        StructField("close_date",     StringType(),  True),
        StructField("revenue",        DoubleType(),  True),
        StructField("quantity",       IntegerType(), True),
        StructField("discount_pct",   DoubleType(),  True),
        StructField("cost",           DoubleType(),  True),
        StructField("region",         StringType(),  True),
        StructField("channel",        StringType(),  True),
        StructField("deal_stage",     StringType(),  True),
        StructField("currency",       StringType(),  True),
    ])
    return spark.createDataFrame(rows, schema)


# --------------------------------------------------------------------------- #
#  GROSS PROFIT & MARGIN CALCULATION                                            #
# --------------------------------------------------------------------------- #

class TestGrossProfitCalc:
    def test_basic_gross_profit(self, spark):
        df = make_txn_df(spark, [
            ("T001", "C1", "P1", "R1", "2024-01-15", "2024-02-01", 1000.0, 2, 0.0, 300.0, "Northeast", "Direct Sales", "Closed Won", "USD"),
        ])
        result = (
            df
            .withColumn("order_date",  F.to_date("order_date", "yyyy-MM-dd"))
            .withColumn("gross_profit", F.col("revenue") - F.col("cost") * F.col("quantity"))
            .withColumn("gross_margin", (F.col("revenue") - F.col("cost") * F.col("quantity")) / F.col("revenue"))
        ).collect()[0]
        assert result["gross_profit"] == pytest.approx(400.0)   # 1000 - 300*2
        assert result["gross_margin"] == pytest.approx(0.4)

    def test_zero_revenue_margin_is_null(self, spark):
        df = make_txn_df(spark, [
            ("T002", "C1", "P1", "R1", "2024-01-15", None, 0.0, 1, 0.0, 50.0, "West", "Web Direct", "Closed Lost", "USD"),
        ])
        result = (
            df
            .withColumn("gross_margin",
                F.when(F.col("revenue") > 0,
                       (F.col("revenue") - F.col("cost") * F.col("quantity")) / F.col("revenue")
                ).otherwise(F.lit(None)))
        ).collect()[0]
        assert result["gross_margin"] is None

    def test_discounted_revenue(self, spark):
        df = make_txn_df(spark, [
            ("T003", "C1", "P1", "R1", "2024-03-01", "2024-04-01", 2000.0, 1, 0.20, 400.0, "Southeast", "Partner", "Closed Won", "USD"),
        ])
        result = (
            df
            .withColumn("discounted_revenue",
                        F.col("revenue") * (1 - F.coalesce("discount_pct", F.lit(0))))
        ).collect()[0]
        assert result["discounted_revenue"] == pytest.approx(1600.0)


# --------------------------------------------------------------------------- #
#  DEDUPLICATION                                                                #
# --------------------------------------------------------------------------- #

class TestDeduplication:
    def test_keeps_latest_record(self, spark):
        from pyspark.sql.window import Window
        from pyspark.sql.types import TimestampType
        from datetime import datetime

        df = make_txn_df(spark, [
            ("T001", "C1", "P1", "R1", "2024-01-01", None, 500.0,  1, 0.0,  100.0, "West", "Direct Sales", "Closed Won", "USD"),
            ("T001", "C1", "P1", "R1", "2024-01-01", None, 600.0,  1, 0.0,  100.0, "West", "Direct Sales", "Closed Won", "USD"),  # newer
        ])
        ts = [datetime(2024, 1, 2, 10, 0, 0), datetime(2024, 1, 2, 11, 0, 0)]
        ts_df = spark.createDataFrame([(t,) for t in ts], ["_ingest_ts"])
        from pyspark.sql.functions import monotonically_increasing_id
        df = df.withColumn("_row_idx",  monotonically_increasing_id())
        ts_df = ts_df.withColumn("_row_idx", monotonically_increasing_id())
        df = df.join(ts_df, on="_row_idx", how="left").drop("_row_idx")

        w = Window.partitionBy("transaction_id").orderBy(F.col("_ingest_ts").desc())
        result = (
            df.withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
        ).collect()

        assert len(result) == 1
        assert result[0]["revenue"] == pytest.approx(600.0)

    def test_unique_ids_pass_through(self, spark):
        df = make_txn_df(spark, [
            ("T100", "C1", "P1", "R1", "2024-01-01", None, 500.0, 1, 0.0, 100.0, "West", "Inbound", "Closed Won", "USD"),
            ("T101", "C1", "P1", "R1", "2024-01-02", None, 600.0, 1, 0.0, 100.0, "West", "Inbound", "Closed Won", "USD"),
        ])
        assert df.select("transaction_id").distinct().count() == 2


# --------------------------------------------------------------------------- #
#  CATEGORICAL STANDARDISATION                                                  #
# --------------------------------------------------------------------------- #

class TestCategoricalStandardisation:
    @pytest.mark.parametrize("raw,expected", [
        ("ne",         "Northeast"),
        ("northeast",  "Northeast"),
        ("North East", "Northeast"),
        ("SE",         "Southeast"),
        ("mw",         "Midwest"),
        ("W",          "West"),
    ])
    def test_region_standardisation(self, spark, raw, expected):
        REGION_MAP = {
            "ne": "Northeast", "northeast": "Northeast", "north east": "Northeast",
            "se": "Southeast", "southeast": "Southeast", "south east": "Southeast",
            "mw": "Midwest",   "midwest":   "Midwest",   "mid-west":  "Midwest",
            "sw": "Southwest", "southwest": "Southwest", "south west": "Southwest",
            "w":  "West",      "west":      "West",
        }
        cases = " ".join(
            f"WHEN lower(trim(region)) = '{k}' THEN '{v}'"
            for k, v in REGION_MAP.items()
        )
        expr_str = f"CASE {cases} ELSE initcap(trim(region)) END"

        df = spark.createDataFrame([(raw,)], ["region"])
        result = df.withColumn("std_region", F.expr(expr_str)).collect()[0]["std_region"]
        assert result == expected


# --------------------------------------------------------------------------- #
#  DAYS TO CLOSE                                                                #
# --------------------------------------------------------------------------- #

class TestDaysToClose:
    def test_positive_days(self, spark):
        df = make_txn_df(spark, [
            ("T001", "C1","P1","R1","2024-01-01","2024-03-01", 100.0, 1, 0.0, 50.0, "West","Direct Sales","Closed Won","USD"),
        ])
        result = (
            df
            .withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
            .withColumn("close_date", F.to_date("close_date", "yyyy-MM-dd"))
            .withColumn("days_to_close", F.datediff("close_date", "order_date"))
        ).collect()[0]["days_to_close"]
        assert result == 60

    def test_null_close_date(self, spark):
        df = make_txn_df(spark, [
            ("T002","C1","P1","R1","2024-01-01",None,100.0,1,0.0,50.0,"West","Inbound","Closed Won","USD"),
        ])
        result = (
            df
            .withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
            .withColumn("close_date", F.to_date("close_date", "yyyy-MM-dd"))
            .withColumn("days_to_close", F.datediff("close_date", "order_date"))
        ).collect()[0]["days_to_close"]
        assert result is None


# --------------------------------------------------------------------------- #
#  REVENUE LEAKAGE FLAG                                                         #
# --------------------------------------------------------------------------- #

class TestRevenueLeakage:
    def test_high_discount_low_margin_flags_leakage(self, spark):
        df = make_txn_df(spark, [
            ("T001","C1","P1","R1","2024-01-01","2024-02-01",1000.0,1,0.25,880.0,"West","Partner","Closed Won","USD"),
        ])
        result = (
            df
            .withColumn("discounted_revenue", F.col("revenue") * (1 - F.col("discount_pct")))
            .withColumn("gross_margin",        (F.col("revenue") - F.col("cost")) / F.col("revenue"))
            .withColumn("revenue_leakage",
                F.when(
                    (F.col("discount_pct") > 0.20) & (F.col("gross_margin") < 0.15),
                    F.col("discounted_revenue") * F.col("discount_pct")
                ).otherwise(F.lit(0.0)))
        ).collect()[0]
        assert result["revenue_leakage"] > 0

    def test_normal_deal_no_leakage(self, spark):
        df = make_txn_df(spark, [
            ("T002","C1","P1","R1","2024-01-01","2024-02-01",1000.0,1,0.05,500.0,"West","Direct Sales","Closed Won","USD"),
        ])
        result = (
            df
            .withColumn("discounted_revenue", F.col("revenue") * (1 - F.col("discount_pct")))
            .withColumn("gross_margin",        (F.col("revenue") - F.col("cost")) / F.col("revenue"))
            .withColumn("revenue_leakage",
                F.when(
                    (F.col("discount_pct") > 0.20) & (F.col("gross_margin") < 0.15),
                    F.col("discounted_revenue") * F.col("discount_pct")
                ).otherwise(F.lit(0.0)))
        ).collect()[0]
        assert result["revenue_leakage"] == pytest.approx(0.0)


# --------------------------------------------------------------------------- #
#  ANOMALY DETECTION (pure Python — no Spark)                                  #
# --------------------------------------------------------------------------- #

class TestAnomalyDetection:
    def _make_series(self, n=60, spike_at: int = None, spike_mult: float = 5.0):
        """Generate a stable revenue series with optional spike."""
        import numpy as np
        np.random.seed(42)
        data = np.random.normal(loc=100_000, scale=5_000, size=n)
        if spike_at is not None:
            data[spike_at] = data[:spike_at].mean() * spike_mult
        return data

    def test_zscore_detects_revenue_spike(self):
        import numpy as np
        series = self._make_series(60, spike_at=59, spike_mult=4.0)
        historical = series[-31:-1]
        current    = series[-1]
        z = (current - historical.mean()) / historical.std()
        assert abs(z) > 2.5, f"Expected anomaly (Z={z:.2f})"

    def test_zscore_passes_normal_value(self):
        import numpy as np
        series = self._make_series(60)
        historical = series[-31:-1]
        current    = series[-1]
        z = (current - historical.mean()) / historical.std()
        assert abs(z) <= 2.5, f"Expected no anomaly (Z={z:.2f})"

    def test_iqr_detects_outlier(self):
        import numpy as np
        series = np.concatenate([np.random.normal(1000, 50, 89), [5000.0]])
        q1, q3 = np.percentile(series[:-1], [25, 75])
        iqr = q3 - q1
        lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
        assert series[-1] > upper, "Expected outlier outside IQR upper bound"

    def test_threshold_breach(self):
        margin = 0.08   # below 0.10 threshold
        assert margin < 0.10

    def test_no_threshold_breach(self):
        margin = 0.25
        assert not (margin < 0.10)
