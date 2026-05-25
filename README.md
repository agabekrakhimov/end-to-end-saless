# Sales/Marketing Analytics Platform — End-to-End Pipeline

> **Author:** Ogabek Rakhimov · [GitHub](https://github.com/agabekrakhimov) · [agabekrakhimov@gmail.com](mailto:agabekrakhimov@gmail.com)

---

## Problem Statement

Manual monthly reporting across fragmented data sources was consuming 3 full business days of analyst time and producing inconsistent numbers across teams. This project delivers a fully automated, end-to-end analytics platform that **reduces monthly reporting time from 3 days to 2 hours** — a 95% time saving — while adding real-time anomaly alerting and revenue-leakage detection.

---

## Tech Stack

| Layer | Technology |
|---|---|
| **Ingestion** | Databricks Delta Live Tables (DLT) |
| **Storage** | Delta Lake (Bronze → Silver → Gold) |
| **Transformation** | Spark SQL · PySpark |
| **Analytics & ML** | Python · Pandas · Scikit-learn · Plotly |
| **Visualisation** | Power BI (DAX · Bookmarks · Row-Level Security) |
| **Orchestration** | Databricks Jobs · Workflows |
| **Alerting** | Python SMTP · Anomaly detection (IQR + Z-score) |

---

## Key Achievements

- ⚡ **95% faster reporting** — from 3 days to 2 hours per monthly cycle
- 📊 **2.5M+ rows** of transactional data processed through the medallion pipeline
- 💰 **$340K potential revenue leakage** identified via cohort and funnel analysis
- 📈 **12 interactive Power BI dashboards** used daily by sales leadership
- 🤖 **Automated anomaly detection** — proactive email alerts on KPI deviations
- 🔒 **Row-Level Security** ensuring each regional team sees only their data

---

## Architecture

```
Raw Sources (CRM / ERP / CSV)
          │
          ▼
  ┌───────────────────┐
  │  Delta Live Tables│  ← Bronze layer (raw ingest)
  │  (DLT Streaming)  │
  └────────┬──────────┘
           │  PySpark cleaning & deduplication
           ▼
  ┌───────────────────┐
  │   Silver Layer    │  ← Validated, conformed data
  │   (Delta Lake)    │
  └────────┬──────────┘
           │  Spark SQL aggregations & metrics
           ▼
  ┌───────────────────┐
  │    Gold Layer     │  ← Business-ready fact & dim tables
  │   (Delta Lake)    │
  └────────┬──────────┘
           │
    ┌──────┴──────┐
    ▼             ▼
Power BI      Python Alerts
Dashboards    (anomaly detection
(12 reports)   + email SMTP)
```

---

## Project Structure

```
end-to-end-saless/
├── notebooks/
│   ├── 01_bronze_ingest.py        # DLT pipeline — raw data landing
│   ├── 02_silver_transform.py     # PySpark cleaning & schema enforcement
│   ├── 03_gold_aggregations.sql   # Spark SQL — KPI & metric tables
│   └── 04_anomaly_detection.py    # Python ML alerts (IQR + Z-score)
├── powerbi/
│   └── sales_dashboard.pbix       # Power BI report file
├── config/
│   └── pipeline_config.json       # Environment & schedule settings
├── tests/
│   └── test_transformations.py    # Unit tests for PySpark logic
└── README.md
```

---

## How to Run

### 1 · Set up Databricks

```bash
git clone https://github.com/agabekrakhimov/end-to-end-saless.git
cd end-to-end-saless
```

Upload the `notebooks/` folder to your Databricks workspace.

### 2 · Run the medallion pipeline

Run notebooks in order inside Databricks:

```
01_bronze_ingest.py   →   02_silver_transform.py   →   03_gold_aggregations.sql
```

Or trigger via **Databricks Jobs** using `config/pipeline_config.json`.

### 3 · Enable anomaly alerts

```python
# 04_anomaly_detection.py — configure your SMTP credentials
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
ALERT_RECIPIENTS = ["your-team@company.com"]
```

Run the notebook on a scheduled Databricks Job (e.g., daily at 07:00).

### 4 · Open Power BI

1. Open `powerbi/sales_dashboard.pbix` in Power BI Desktop
2. Update the **Databricks SQL endpoint** connection string
3. Refresh the dataset
4. Publish to Power BI Service for shared access

---

## Dashboards

| Dashboard | Description |
|---|---|
| Executive Summary | Revenue, MoM growth, quota attainment |
| Sales Funnel | Lead → Opportunity → Closed conversion rates |
| Regional Performance | Map view with RLS per territory |
| Product Mix | Revenue by SKU / category |
| Cohort Retention | Customer LTV & churn signals |
| Anomaly Log | Flagged KPI deviations with timestamps |

---

## Anomaly Detection Logic

```python
# Z-score method — flags metric if > 2.5 std devs from 30-day rolling mean
from scipy import stats

def detect_anomalies(series, threshold=2.5):
    z_scores = stats.zscore(series.dropna())
    return abs(z_scores) > threshold
```

Email alerts fire automatically when daily revenue, order volume, or conversion rate breaches the threshold.

---

## Links

| Resource | URL |
|---|---|
| 📊 Live Power BI Dashboard | *(publish & add link here)* |
| 🧪 Databricks Notebooks | [github.com/agabekrakhimov/end-to-end-saless](https://github.com/agabekrakhimov/end-to-end-saless) |
| 👤 Author Profile | [github.com/agabekrakhimov](https://github.com/agabekrakhimov) |

---

## License

MIT © 2025 Ogabek Rakhimov
