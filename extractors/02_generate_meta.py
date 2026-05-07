"""
extractors/02_generate_meta.py
Simulates Meta Ads (Facebook/Instagram) API response format.
Output: data/raw/meta_ads.csv
"""
import os
import numpy as np
import pandas as pd
from faker import Faker
from datetime import date, timedelta

fake = Faker()
np.random.seed(42)

START_DATE = date(2024, 1, 1)
END_DATE   = date(2024, 12, 31)
CURRENCY   = "USD"

CAMPAIGNS = [
    {"campaign_id": "META_C001", "campaign_name": "Prospecting — Lookalike"},
    {"campaign_id": "META_C002", "campaign_name": "Retargeting — Cart Abandon"},
    {"campaign_id": "META_C003", "campaign_name": "Brand Awareness — Video"},
]

ADSETS = {
    "META_C001": [
        ("META_AS001", "META_AD001", "LAL 1pct Purchasers", "facebook",  "feed"),
        ("META_AS002", "META_AD002", "LAL 2pct Purchasers", "instagram", "feed"),
        ("META_AS003", "META_AD003", "LAL 5pct Interest",   "instagram", "story"),
    ],
    "META_C002": [
        ("META_AS004", "META_AD004", "Cart Abandon 3d",  "facebook",  "feed"),
        ("META_AS005", "META_AD005", "Cart Abandon 7d",  "facebook",  "right_column"),
    ],
    "META_C003": [
        ("META_AS006", "META_AD006", "Awareness Broad", "instagram", "reels"),
    ],
}

rows = []
dates = [START_DATE + timedelta(days=d) for d in range((END_DATE - START_DATE).days + 1)]

for camp in CAMPAIGNS:
    for as_id, ad_id, as_name, publisher, placement in ADSETS[camp["campaign_id"]]:
        for dt in dates:
            seasonal = 1.0 + 0.5 * np.sin((dt.timetuple().tm_yday / 365) * 2 * np.pi - np.pi)
            spend = round(np.random.uniform(100, 1500) * seasonal, 2)  # Meta: dollars, not micros

            cpm         = np.random.uniform(10, 45)
            impressions = int(spend / cpm * 1000)
            ctr         = np.random.uniform(0.005, 0.04)
            clicks      = int(impressions * ctr)
            unique_clicks = int(clicks * np.random.uniform(0.7, 0.9))
            cvr         = np.random.uniform(0.01, 0.10)
            actions     = round(clicks * cvr, 2)    # Meta calls conversions "actions"
            aov         = np.random.uniform(35, 180)
            purchase_roas = round((actions * aov) / spend, 2) if spend > 0 else 0

            rows.append({
                "date_start":          dt.isoformat(),     # Meta naming quirk
                "date_stop":           dt.isoformat(),     # same — daily grain
                "campaign_id":         camp["campaign_id"],
                "campaign_name":       camp["campaign_name"],
                "adset_id":            as_id,              # "adset" not "ad_group"
                "adset_name":          as_name,
                "ad_id":               ad_id,
                "ad_name":             as_name + " — Creative",
                "publisher_platform":  publisher,          # facebook or instagram
                "placement":           placement,
                "spend":               spend,
                "impressions":         impressions,
                "clicks":              clicks,
                "unique_clicks":       unique_clicks,
                "ctr":                 round(ctr, 4),
                "cpm":                 round(cpm, 2),
                "cpc":                 round(spend / clicks, 4) if clicks > 0 else 0,
                "actions":             actions,            # Meta's name for conversions
                "conversion_rate":     round(cvr, 4),
                "purchase_roas":       purchase_roas,
                "currency":            CURRENCY,
            })

DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data/raw")
os.makedirs(DATA_DIR, exist_ok=True)
df = pd.DataFrame(rows)
out = os.path.join(DATA_DIR, "meta_ads.csv")
df.to_csv(out, index=False)

print(f"✅ Meta Ads: {len(df):,} rows saved to {out}")
print(f"   Date range: {df['date_start'].min()} → {df['date_start'].max()}")
print(f"   Campaigns:  {df['campaign_id'].nunique()}")
print(f"   Total spend: ${df['spend'].sum():,.2f}")