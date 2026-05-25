"""
Generate realistic sample CSV data for local / CI testing.
Usage:  python data/generate_sample_data.py --rows 50000 --out data/sample/
"""
import argparse
import os
import random
from datetime import date, timedelta
from uuid import uuid4

import pandas as pd
import numpy as np

# --------------------------------------------------------------------------- #
#  CONFIG                                                                       #
# --------------------------------------------------------------------------- #
SEED = 42
random.seed(SEED)
np.random.seed(SEED)

REGIONS   = ["Northeast", "Southeast", "Midwest", "Southwest", "West"]
CHANNELS  = ["Direct Sales", "Partner", "Web Direct", "Inbound", "Outbound", "Events"]
SEGMENTS  = ["SMB", "Mid-Market", "Enterprise"]
INDUSTRIES = ["Technology", "Finance", "Healthcare", "Manufacturing", "Retail", "Energy"]
DEAL_STAGES = ["Closed Won", "Closed Lost"]
LEAD_STATUSES = ["New", "Qualified", "Converted", "Lost"]
LEAD_SOURCES  = ["Organic Search", "Paid Search", "Email", "Referral", "Social", "Event", "Cold Outreach"]
CAMP_CHANNELS = ["Email", "Paid Search", "Social Media", "Events", "Webinar"]

CATEGORIES = {
    "Analytics Software": ["BI Dashboard", "Data Pipeline", "ML Platform"],
    "CRM Tools":          ["Sales CRM", "Marketing Hub", "Support Suite"],
    "Infrastructure":     ["Cloud Storage", "Compute Credits", "Network Security"],
    "Professional Services": ["Implementation", "Training", "Consulting"],
}

START_DATE = date(2024, 1, 1)
END_DATE   = date(2025, 12, 31)


def rand_date(start=START_DATE, end=END_DATE) -> str:
    delta = (end - start).days
    return (start + timedelta(days=random.randint(0, delta))).isoformat()


def rand_id(prefix: str = "") -> str:
    return f"{prefix}{uuid4().hex[:10].upper()}"


# --------------------------------------------------------------------------- #
#  GENERATORS                                                                   #
# --------------------------------------------------------------------------- #

def make_products(n=50) -> pd.DataFrame:
    rows = []
    for cat, subs in CATEGORIES.items():
        for sub in subs:
            for tier in ["Starter", "Professional", "Enterprise"]:
                base_price = {"Starter": 500, "Professional": 2000, "Enterprise": 8000}[tier]
                unit_price = round(base_price * np.random.uniform(0.8, 1.4), 2)
                unit_cost  = round(unit_price * np.random.uniform(0.25, 0.55), 2)
                rows.append({
                    "product_id":   rand_id("PROD-"),
                    "product_name": f"{sub} — {tier}",
                    "category":     cat,
                    "sub_category": sub,
                    "unit_price":   unit_price,
                    "unit_cost":    unit_cost,
                    "launch_date":  rand_date(date(2022, 1, 1), date(2024, 1, 1)),
                    "is_active":    random.choice(["true", "true", "true", "false"]),
                })
    return pd.DataFrame(rows).head(n)


def make_customers(n=500) -> pd.DataFrame:
    rows = []
    for _ in range(n):
        seg = random.choice(SEGMENTS)
        ltv = {"SMB": 5_000, "Mid-Market": 40_000, "Enterprise": 200_000}[seg]
        rows.append({
            "customer_id":   rand_id("CUST-"),
            "company_name":  f"{''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=3))} Corp",
            "industry":      random.choice(INDUSTRIES),
            "segment":       seg,
            "country":       "US",
            "state":         random.choice(["NY","CA","TX","FL","IL","WA","MA","GA","CO","OH"]),
            "city":          random.choice(["New York","San Francisco","Chicago","Austin","Seattle"]),
            "created_date":  rand_date(date(2020, 1, 1), date(2024, 1, 1)),
            "account_owner": rand_id("REP-"),
            "health_score":  round(np.random.beta(5, 2) * 100, 1),
            "total_ltv":     round(ltv * np.random.uniform(0.5, 2.0), 2),
        })
    return pd.DataFrame(rows)


def make_sales_transactions(customers: pd.DataFrame, products: pd.DataFrame, n=2_500_000) -> pd.DataFrame:
    cust_ids = customers["customer_id"].tolist()
    rep_ids  = [rand_id("REP-") for _ in range(50)]
    prod_rows = products[["product_id", "unit_price", "unit_cost"]].to_dict("records")

    rows = []
    for _ in range(n):
        prod   = random.choice(prod_rows)
        qty    = np.random.randint(1, 20)
        disc   = round(random.choice([0]*6 + [0.05, 0.10, 0.15, 0.20, 0.25, 0.30]), 2)
        rev    = round(prod["unit_price"] * qty * (1 - disc), 2)
        cost   = round(prod["unit_cost"]  * qty, 2)
        odate  = rand_date()
        cdate_dt = date.fromisoformat(odate) + timedelta(days=random.randint(1, 120))
        cdate  = min(cdate_dt, END_DATE).isoformat()
        rows.append({
            "transaction_id": rand_id("TXN-"),
            "customer_id":    random.choice(cust_ids),
            "product_id":     prod["product_id"],
            "sales_rep_id":   random.choice(rep_ids),
            "order_date":     odate,
            "close_date":     cdate,
            "revenue":        rev,
            "quantity":       qty,
            "discount_pct":   disc,
            "cost":           cost,
            "region":         random.choice(REGIONS),
            "channel":        random.choice(CHANNELS),
            "deal_stage":     random.choices(DEAL_STAGES, weights=[75, 25])[0],
            "currency":       "USD",
        })
        if len(rows) % 100_000 == 0:
            print(f"  ... {len(rows):,} transactions generated")

    return pd.DataFrame(rows)


def make_campaigns(n=100) -> pd.DataFrame:
    rows = []
    for _ in range(n):
        budget  = round(random.choice([5_000, 10_000, 25_000, 50_000, 100_000]) * np.random.uniform(0.8, 1.2), 2)
        spend   = round(budget * np.random.uniform(0.60, 1.10), 2)
        impr    = random.randint(10_000, 500_000)
        clicks  = int(impr * np.random.uniform(0.01, 0.08))
        convs   = int(clicks * np.random.uniform(0.02, 0.15))
        sdate   = rand_date(date(2024, 1, 1), date(2025, 6, 1))
        edate   = (date.fromisoformat(sdate) + timedelta(days=random.randint(14, 90))).isoformat()
        rows.append({
            "campaign_id":   rand_id("CAMP-"),
            "campaign_name": f"{random.choice(['Q1','Q2','Q3','Q4'])} {random.choice(['Launch','Nurture','ABM','Retarget','Event'])} {random.randint(2024,2025)}",
            "channel":       random.choice(CAMP_CHANNELS),
            "start_date":    sdate,
            "end_date":      edate,
            "budget":        budget,
            "spend":         spend,
            "impressions":   impr,
            "clicks":        clicks,
            "conversions":   convs,
            "region":        random.choice(REGIONS),
        })
    return pd.DataFrame(rows)


def make_leads(customers: pd.DataFrame, campaigns: pd.DataFrame, n=25_000) -> pd.DataFrame:
    cust_ids = customers["customer_id"].tolist()
    camp_ids = campaigns["campaign_id"].tolist()
    rep_ids  = [rand_id("REP-") for _ in range(50)]

    rows = []
    for _ in range(n):
        status   = random.choices(LEAD_STATUSES, weights=[20, 35, 30, 15])[0]
        cdate    = rand_date()
        convdate = None
        if status == "Converted":
            convdate = (date.fromisoformat(cdate) + timedelta(days=random.randint(7, 90))).isoformat()
        rows.append({
            "lead_id":        rand_id("LEAD-"),
            "campaign_id":    random.choice(camp_ids),
            "customer_id":    random.choice(cust_ids + [None] * 10),
            "lead_source":    random.choice(LEAD_SOURCES),
            "created_date":   cdate,
            "status":         status,
            "converted_date": convdate,
            "deal_value":     round(random.choice([5_000,15_000,50_000,120_000,300_000]) * np.random.uniform(0.5,1.5), 2),
            "sales_rep_id":   random.choice(rep_ids),
        })
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
#  MAIN                                                                         #
# --------------------------------------------------------------------------- #

def main():
    parser = argparse.ArgumentParser(description="Generate sample sales analytics data")
    parser.add_argument("--rows", type=int,  default=50_000,  help="Number of transactions (default 50k; use 2500000 for full scale)")
    parser.add_argument("--out",  type=str,  default="data/sample", help="Output directory")
    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)
    print(f"📦 Generating sample data → {args.out}/")

    print("  products …")
    products = make_products(50)
    products.to_csv(f"{args.out}/products.csv", index=False)

    print("  customers …")
    customers = make_customers(500)
    customers.to_csv(f"{args.out}/customers.csv", index=False)

    print("  campaigns …")
    campaigns = make_campaigns(100)
    campaigns.to_csv(f"{args.out}/campaigns.csv", index=False)

    print("  leads …")
    leads = make_leads(customers, campaigns, 25_000)
    leads.to_csv(f"{args.out}/leads.csv", index=False)

    print(f"  sales_transactions ({args.rows:,} rows) …")
    txns = make_sales_transactions(customers, products, args.rows)
    txns.to_csv(f"{args.out}/sales_transactions.csv", index=False)

    print(f"\n✅ Done!")
    print(f"   products          : {len(products):>8,} rows")
    print(f"   customers         : {len(customers):>8,} rows")
    print(f"   campaigns         : {len(campaigns):>8,} rows")
    print(f"   leads             : {len(leads):>8,} rows")
    print(f"   sales_transactions: {len(txns):>8,} rows")
    total_rev = txns["revenue"].sum()
    print(f"\n   Total simulated revenue : ${total_rev:>14,.0f}")


if __name__ == "__main__":
    main()
