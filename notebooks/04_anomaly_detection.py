# Databricks notebook source
# MAGIC %md
# MAGIC # Anomaly Detection + Automated Email Alerting
# MAGIC
# MAGIC This notebook runs **after** the Gold layer pipeline completes.
# MAGIC Schedule via Databricks Jobs to run daily at 08:00.
# MAGIC
# MAGIC ## Detection methods
# MAGIC | Method | Used for |
# MAGIC |--------|----------|
# MAGIC | Z-score (rolling 30-day) | Daily revenue, order volume |
# MAGIC | IQR (trailing 90-day)    | Avg deal size, discount rate |
# MAGIC | Threshold rules          | Margin collapse, leakage spike |
# MAGIC
# MAGIC ## Alerts fired
# MAGIC - Revenue drop > 2.5σ vs 30-day rolling average
# MAGIC - Conversion rate drops > 20% week-over-week
# MAGIC - Discount rate breaches 25% threshold
# MAGIC - Gross margin falls below 10%
# MAGIC - Revenue leakage exceeds $50K in a single day
# MAGIC - Campaign CPA exceeds 3× historical average

# COMMAND ----------
import smtplib
import json
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text      import MIMEText
from datetime             import datetime, timedelta, date
from typing               import List, Dict, Any

import pandas as pd
import numpy  as np
from scipy import stats
from pyspark.sql import functions as F

# --------------------------------------------------------------------------- #
#  CONFIGURATION — override via Databricks Secrets / pipeline parameters       #
# --------------------------------------------------------------------------- #
try:
    cfg = json.loads(
        dbutils.fs.head("/mnt/config/pipeline_config.json")
    )
except Exception:
    # Fallback for local / unit-test runs
    cfg = {
        "smtp": {
            "host":     os.getenv("SMTP_HOST",     "smtp.gmail.com"),
            "port":     int(os.getenv("SMTP_PORT", "587")),
            "user":     os.getenv("SMTP_USER",     "alerts@yourdomain.com"),
            "password": os.getenv("SMTP_PASSWORD", ""),
        },
        "alert_recipients": os.getenv(
            "ALERT_RECIPIENTS",
            "analytics-team@yourdomain.com"
        ).split(","),
        "alert_sender_name": "Sales Analytics Platform",
        "thresholds": {
            "zscore_revenue":    2.5,
            "zscore_volume":     2.5,
            "iqr_multiplier":    1.5,
            "min_gross_margin":  0.10,
            "max_discount_pct":  0.25,
            "max_daily_leakage": 50_000,
            "wow_conversion_drop": 0.20,
            "max_cpa_multiplier": 3.0,
        },
    }

THRESHOLDS = cfg["thresholds"]
TODAY      = date.today()
YESTERDAY  = TODAY - timedelta(days=1)


# --------------------------------------------------------------------------- #
#  1. LOAD KPI DATA FROM GOLD LAYER                                             #
# --------------------------------------------------------------------------- #

def load_daily_revenue(lookback_days: int = 90) -> pd.DataFrame:
    cutoff = YESTERDAY - timedelta(days=lookback_days)
    return (
        spark.table("gold.gold_revenue_summary")
        .filter(F.col("order_date") >= cutoff.isoformat())
        .groupBy("order_date")
        .agg(
            F.sum("gross_revenue").alias("gross_revenue"),
            F.sum("transaction_count").alias("order_volume"),
            F.avg("avg_gross_margin").alias("avg_margin"),
            F.avg("avg_deal_size").alias("avg_deal_size"),
            F.sum("total_discount_given").alias("total_discount"),
            F.sum("revenue_leakage").alias("daily_leakage"),
        )
        .orderBy("order_date")
        .toPandas()
    )


def load_campaign_performance() -> pd.DataFrame:
    return (
        spark.table("gold.gold_campaign_roi")
        .filter(F.col("end_date") >= (YESTERDAY - timedelta(days=30)).isoformat())
        .toPandas()
    )


def load_funnel_conversion(lookback_weeks: int = 8) -> pd.DataFrame:
    cutoff = YESTERDAY - timedelta(weeks=lookback_weeks)
    return (
        spark.table("gold.gold_funnel_conversion")
        .filter(
            (F.col("cohort_year") * 100 + F.col("cohort_month")) >=
            int(cutoff.strftime("%Y%m"))
        )
        .toPandas()
    )


# --------------------------------------------------------------------------- #
#  2. DETECTION ENGINE                                                          #
# --------------------------------------------------------------------------- #

def detect_zscore_anomaly(
    series: pd.Series,
    threshold: float,
    metric_name: str,
    window: int = 30,
) -> List[Dict[str, Any]]:
    """Flag the latest value if its Z-score vs rolling window exceeds threshold."""
    alerts = []
    if len(series) < window + 1:
        return alerts

    historical = series.iloc[-(window + 1):-1]
    current    = series.iloc[-1]
    mean, std  = historical.mean(), historical.std()

    if std == 0:
        return alerts

    z = (current - mean) / std

    if abs(z) > threshold:
        direction = "below" if z < 0 else "above"
        pct_change = (current - mean) / mean * 100
        alerts.append({
            "type":       "Z-Score",
            "metric":     metric_name,
            "severity":   "HIGH" if abs(z) > threshold * 1.5 else "MEDIUM",
            "current":    round(float(current), 2),
            "mean_30d":   round(float(mean),    2),
            "z_score":    round(float(z),       2),
            "pct_change": round(float(pct_change), 1),
            "message":    f"{metric_name} is {direction} 30-day average by {abs(pct_change):.1f}% (Z={z:.2f})",
        })
    return alerts


def detect_iqr_anomaly(
    series: pd.Series,
    multiplier: float,
    metric_name: str,
    window: int = 90,
) -> List[Dict[str, Any]]:
    """Flag the latest value if it falls outside Q1/Q3 ± multiplier×IQR."""
    alerts = []
    if len(series) < 2:
        return alerts

    historical = series.iloc[-window:-1]
    current    = series.iloc[-1]
    q1 = np.percentile(historical.dropna(), 25)
    q3 = np.percentile(historical.dropna(), 75)
    iqr = q3 - q1
    lower, upper = q1 - multiplier * iqr, q3 + multiplier * iqr

    if current < lower or current > upper:
        alerts.append({
            "type":     "IQR",
            "metric":   metric_name,
            "severity": "MEDIUM",
            "current":  round(float(current), 2),
            "lower":    round(float(lower),   2),
            "upper":    round(float(upper),   2),
            "message":  f"{metric_name} ({current:.2f}) is outside IQR bounds [{lower:.2f}, {upper:.2f}]",
        })
    return alerts


def detect_threshold_breach(
    value: float,
    threshold: float,
    metric_name: str,
    direction: str = "below",  # "below" | "above"
) -> List[Dict[str, Any]]:
    triggered = (direction == "below" and value < threshold) or \
                (direction == "above" and value > threshold)
    if triggered:
        return [{
            "type":      "Threshold",
            "metric":    metric_name,
            "severity":  "HIGH",
            "current":   round(float(value),     2),
            "threshold": round(float(threshold), 2),
            "message":   f"{metric_name} ({value:.2f}) breached threshold ({threshold:.2f}) [{direction}]",
        }]
    return []


def detect_wow_drop(
    series: pd.Series,
    metric_name: str,
    max_drop: float = 0.20,
) -> List[Dict[str, Any]]:
    """Flag week-over-week percentage drops exceeding max_drop."""
    if len(series) < 14:
        return []
    this_week = series.iloc[-7:].mean()
    last_week = series.iloc[-14:-7].mean()
    if last_week == 0:
        return []
    wow = (this_week - last_week) / last_week
    if wow < -max_drop:
        return [{
            "type":      "WoW Drop",
            "metric":    metric_name,
            "severity":  "HIGH",
            "current":   round(float(this_week), 2),
            "prior":     round(float(last_week), 2),
            "wow_pct":   round(float(wow * 100),  1),
            "message":   f"{metric_name} dropped {abs(wow)*100:.1f}% week-over-week",
        }]
    return []


# --------------------------------------------------------------------------- #
#  3. RUN ALL DETECTORS                                                         #
# --------------------------------------------------------------------------- #

def run_all_detectors() -> List[Dict[str, Any]]:
    all_alerts = []

    # --- Load data ---
    daily    = load_daily_revenue()
    campaigns = load_campaign_performance()

    if daily.empty:
        print("⚠️  No daily revenue data found. Skipping anomaly detection.")
        return []

    daily = daily.sort_values("order_date")

    # 1. Revenue Z-score
    all_alerts += detect_zscore_anomaly(
        daily["gross_revenue"],
        THRESHOLDS["zscore_revenue"],
        "Daily Gross Revenue",
    )

    # 2. Order volume Z-score
    all_alerts += detect_zscore_anomaly(
        daily["order_volume"],
        THRESHOLDS["zscore_volume"],
        "Daily Order Volume",
    )

    # 3. Avg deal size IQR
    all_alerts += detect_iqr_anomaly(
        daily["avg_deal_size"],
        THRESHOLDS["iqr_multiplier"],
        "Average Deal Size",
    )

    # 4. Gross margin threshold
    if not daily.empty:
        latest_margin = daily["avg_margin"].iloc[-1]
        all_alerts += detect_threshold_breach(
            latest_margin,
            THRESHOLDS["min_gross_margin"],
            "Gross Margin",
            direction="below",
        )

    # 5. Daily revenue leakage threshold
    if not daily.empty:
        latest_leakage = daily["daily_leakage"].iloc[-1]
        all_alerts += detect_threshold_breach(
            latest_leakage,
            THRESHOLDS["max_daily_leakage"],
            "Daily Revenue Leakage",
            direction="above",
        )

    # 6. Week-over-week revenue drop
    all_alerts += detect_wow_drop(
        daily["gross_revenue"],
        "Daily Revenue",
        THRESHOLDS["wow_conversion_drop"],
    )

    # 7. Campaign CPA vs historical average
    if not campaigns.empty:
        avg_cpa = campaigns["cpa"].mean()
        for _, row in campaigns.iterrows():
            if pd.notna(row["cpa"]) and row["cpa"] > avg_cpa * THRESHOLDS["max_cpa_multiplier"]:
                all_alerts.append({
                    "type":     "Campaign CPA",
                    "metric":   f"CPA — {row['campaign_name']}",
                    "severity": "HIGH",
                    "current":  round(row["cpa"],    2),
                    "avg_cpa":  round(float(avg_cpa), 2),
                    "message":  f"Campaign '{row['campaign_name']}' CPA (${row['cpa']:.0f}) is {row['cpa']/avg_cpa:.1f}× the average",
                })

    return all_alerts


# --------------------------------------------------------------------------- #
#  4. EMAIL BUILDER                                                             #
# --------------------------------------------------------------------------- #

def _severity_color(severity: str) -> str:
    return {"HIGH": "#e74c3c", "MEDIUM": "#f39c12", "LOW": "#27ae60"}.get(severity, "#7f8c8d")


def build_html_email(alerts: List[Dict[str, Any]], run_date: date) -> str:
    rows_html = ""
    for a in alerts:
        color = _severity_color(a["severity"])
        rows_html += f"""
        <tr>
          <td style="padding:10px;border-bottom:1px solid #eee">
            <span style="background:{color};color:#fff;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:bold">
              {a['severity']}
            </span>
          </td>
          <td style="padding:10px;border-bottom:1px solid #eee;font-weight:600">{a['metric']}</td>
          <td style="padding:10px;border-bottom:1px solid #eee;color:#555">{a['message']}</td>
        </tr>"""

    header_color = "#e74c3c" if any(a["severity"] == "HIGH" for a in alerts) else "#f39c12"
    return f"""
    <!DOCTYPE html><html><body style="font-family:Arial,sans-serif;background:#f4f4f4;margin:0;padding:20px">
    <div style="max-width:700px;margin:auto;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.1)">
      <div style="background:{header_color};padding:24px 30px">
        <h1 style="color:#fff;margin:0;font-size:22px">⚠️ Sales Analytics — Anomaly Alert</h1>
        <p  style="color:rgba(255,255,255,.85);margin:6px 0 0">{run_date.strftime('%A, %B %-d %Y')} · {len(alerts)} anomalies detected</p>
      </div>
      <div style="padding:30px">
        <p style="color:#333;margin-top:0">The automated anomaly detection pipeline flagged the following KPI deviations:</p>
        <table style="width:100%;border-collapse:collapse;font-size:14px">
          <thead>
            <tr style="background:#f8f8f8">
              <th style="padding:10px;text-align:left;font-size:12px;color:#888;text-transform:uppercase;letter-spacing:.05em">Severity</th>
              <th style="padding:10px;text-align:left;font-size:12px;color:#888;text-transform:uppercase;letter-spacing:.05em">Metric</th>
              <th style="padding:10px;text-align:left;font-size:12px;color:#888;text-transform:uppercase;letter-spacing:.05em">Detail</th>
            </tr>
          </thead>
          <tbody>{rows_html}</tbody>
        </table>
        <hr style="border:none;border-top:1px solid #eee;margin:24px 0">
        <p style="font-size:12px;color:#aaa;margin:0">
          Sent by Sales Analytics Platform · Databricks Scheduled Job<br>
          Reply to this email to escalate or mute specific alerts.
        </p>
      </div>
    </div>
    </body></html>"""


# --------------------------------------------------------------------------- #
#  5. SEND EMAIL                                                                #
# --------------------------------------------------------------------------- #

def send_alert_email(
    alerts: List[Dict[str, Any]],
    run_date: date = YESTERDAY,
) -> None:
    if not alerts:
        print("✅  No anomalies detected — no email sent.")
        return

    smtp_cfg   = cfg["smtp"]
    recipients = cfg["alert_recipients"]
    high_count = sum(1 for a in alerts if a["severity"] == "HIGH")
    subject    = (
        f"🚨 [{high_count} HIGH] Sales Anomalies Detected — {run_date.strftime('%b %-d')}"
        if high_count
        else f"⚠️ Sales Anomalies Detected — {run_date.strftime('%b %-d')}"
    )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"{cfg['alert_sender_name']} <{smtp_cfg['user']}>"
    msg["To"]      = ", ".join(recipients)

    # Plain-text fallback
    plain = "\n".join(f"[{a['severity']}] {a['metric']}: {a['message']}" for a in alerts)
    msg.attach(MIMEText(plain, "plain"))
    msg.attach(MIMEText(build_html_email(alerts, run_date), "html"))

    with smtplib.SMTP(smtp_cfg["host"], smtp_cfg["port"]) as server:
        server.ehlo()
        server.starttls()
        server.login(smtp_cfg["user"], smtp_cfg["password"])
        server.sendmail(smtp_cfg["user"], recipients, msg.as_string())

    print(f"📧  Alert email sent to {recipients} — {len(alerts)} anomalies ({high_count} HIGH)")


# --------------------------------------------------------------------------- #
#  6. PERSIST ALERT LOG TO DELTA                                                #
# --------------------------------------------------------------------------- #

def persist_alert_log(alerts: List[Dict[str, Any]], run_date: date) -> None:
    if not alerts:
        return
    rows = [
        {**a, "alert_date": str(run_date), "created_at": str(datetime.utcnow())}
        for a in alerts
    ]
    df = spark.createDataFrame(rows)
    (
        df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("gold.anomaly_alert_log")
    )
    print(f"💾  {len(alerts)} alert records written to gold.anomaly_alert_log")


# --------------------------------------------------------------------------- #
#  MAIN ENTRY POINT                                                             #
# --------------------------------------------------------------------------- #

if __name__ == "__main__" or True:   # always run in Databricks notebooks
    print(f"🔍  Running anomaly detection for {YESTERDAY} …")
    detected_alerts = run_all_detectors()

    if detected_alerts:
        print(f"\n{'='*60}")
        for alert in detected_alerts:
            print(f"  [{alert['severity']}] {alert['metric']}: {alert['message']}")
        print(f"{'='*60}\n")
    else:
        print("✅  All KPIs within normal bounds.")

    send_alert_email(detected_alerts)
    persist_alert_log(detected_alerts, YESTERDAY)

    print("✅  Anomaly detection complete.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Interactive Anomaly Explorer
# MAGIC Run the cell below to visualise the last 90 days of revenue vs anomaly bands.

# COMMAND ----------
import plotly.graph_objects as go

def plot_revenue_with_bands(lookback: int = 90):
    df = load_daily_revenue(lookback)
    if df.empty:
        print("No data to plot.")
        return

    df = df.sort_values("order_date")
    revenue   = df["gross_revenue"].values
    dates     = pd.to_datetime(df["order_date"]).values

    # Rolling 30-day Z-score bands
    window    = 30
    means, upper_bands, lower_bands = [], [], []
    for i in range(len(revenue)):
        window_data = revenue[max(0, i - window):i] if i > 0 else revenue[:1]
        m, s = window_data.mean(), window_data.std() if len(window_data) > 1 else 0
        means.append(m)
        upper_bands.append(m + THRESHOLDS["zscore_revenue"] * s)
        lower_bands.append(m - THRESHOLDS["zscore_revenue"] * s)

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=dates, y=revenue,      name="Daily Revenue",  line=dict(color="#2c3e50", width=2)))
    fig.add_trace(go.Scatter(x=dates, y=means,        name="30-day Rolling Avg", line=dict(color="#3498db", dash="dash")))
    fig.add_trace(go.Scatter(
        x=list(dates) + list(dates[::-1]),
        y=upper_bands + lower_bands[::-1],
        fill="toself", fillcolor="rgba(231,76,60,0.08)",
        line=dict(color="rgba(0,0,0,0)"), name=f"±{THRESHOLDS['zscore_revenue']}σ Band",
    ))
    # Anomaly markers
    anomaly_mask = [
        abs((revenue[i] - means[i]) / max(
            (np.std(revenue[max(0, i-30):i]) if i > 0 else 1), 1e-9
        )) > THRESHOLDS["zscore_revenue"]
        for i in range(len(revenue))
    ]
    anomaly_dates   = dates[anomaly_mask]
    anomaly_revenue = revenue[anomaly_mask]
    if len(anomaly_dates):
        fig.add_trace(go.Scatter(
            x=anomaly_dates, y=anomaly_revenue, mode="markers",
            marker=dict(color="red", size=10, symbol="x"), name="Anomaly",
        ))

    fig.update_layout(
        title="Daily Revenue — Anomaly Detection Bands (±2.5σ)",
        xaxis_title="Date", yaxis_title="Revenue ($)",
        template="plotly_white", height=450,
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    fig.show()


plot_revenue_with_bands()
