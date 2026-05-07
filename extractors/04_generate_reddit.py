"""
extractors/04_generate_reddit.py
Simulates Reddit Ads API response format.
Output: data/raw/reddit_ads.csv
"""
import os
import numpy as np
import pandas as pd
from datetime import date, timedelta

np.random.seed(42)

START_DATE = date(2024, 1, 1)
END_DATE   = date(2024, 12, 31)
CURRENCY   = "USD"

CAMPAIGNS = [
    {"campaign_id": "RDT_C001", "campaign_name": "Reddit — Tech Community"},
    {"campaign_id": "RDT_C002", "campaign_name": "Reddit — Finance Community"},
]

AD_GROUPS = {
    "RDT_C001": [
        ("RDT_AG001", "RDT_CR001", "r/technology Interest", "FEED"),
        ("RDT_AG002", "RDT_CR002", "r/gadgets Interest",    "CONVERSATION"),
    ],
    "RDT_C002": [
        ("RDT_AG003", "RDT_CR003", "r/personalfinance",   "FEED"),
        ("RDT_AG004", "RDT_CR004", "r/investing Interest", "FEED"),
    ],
}

rows = []
dates = [START_DATE + timedelta(days=d) for d in range((END_DATE - START_DATE).days + 1)]

for camp in CAMPAIGNS:
    for ag_id, cr_id, ag_name, placement in AD_GROUPS[camp["campaign_id"]]:
        for dt in dates:
            seasonal = 1.0 + 0.25 * np.sin((dt.timetuple().tm_yday / 365) * 2 * np.pi - np.pi)
            spend = round(np.random.uniform(40, 600) * seasonal, 2)  # Reddit spend is lower

            ecpm        = np.random.uniform(5, 25)     # effective CPM
            impressions = int(spend / ecpm * 1000)
            ctr         = np.random.uniform(0.003, 0.025)
            clicks      = int(impressions * ctr)
            ecpc        = round(spend / clicks, 4) if clicks > 0 else 0  # effective CPC
            cvr         = np.random.uniform(0.005, 0.07)
            conversions = round(clicks * cvr, 2)
            aov         = np.random.uniform(30, 150)
            roas        = round((conversions * aov) / spend, 2) if spend > 0 else 0

            rows.append({
                "date":             dt.isoformat(),
                "campaign_id":      camp["campaign_id"],
                "campaign_name":    camp["campaign_name"],
                "ad_group_id":      ag_id,
                "ad_group_name":    ag_name,
                "creative_id":      cr_id,
                "creative_name":    ag_name + " — Static",
                "placement":        placement,       # FEED or CONVERSATION
                "spend":            spend,
                "impressions":      impressions,
                "clicks":           clicks,
                "ctr":              round(ctr, 4),
                "ecpm":             round(ecpm, 2),  # Reddit: ecpm not cpm
                "ecpc":             ecpc,            # Reddit: ecpc not cpc
                "conversions":      conversions,
                "conversion_rate":  round(cvr, 4),
                "roas":             roas,
                "currency":         CURRENCY,
            })

DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data/raw")
os.makedirs(DATA_DIR, exist_ok=True)
df = pd.DataFrame(rows)
out = os.path.join(DATA_DIR, "reddit_ads.csv")
df.to_csv(out, index=False)

print(f"✅ Reddit Ads: {len(df):,} rows saved to {out}")
print(f"   Date range: {df['date'].min()} → {df['date'].max()}")
print(f"   Campaigns:  {df['campaign_id'].nunique()}")
print(f"   Total spend: ${df['spend'].sum():,.2f}")