"""
extractors/01_generate_google.py
Simulates Google Ads API response format (campaign-day grain).
Output: data/raw/google_ads.csv
"""
import os
import numpy as np
import pandas as pd
from faker import Faker
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv("../.env")
fake = Faker()
np.random.seed(42)

# ── Config ────────────────────────────────────────────────────────────────────
START_DATE = date(2024, 1, 1)
END_DATE   = date(2024, 12, 31)
PLATFORM   = "google"
CURRENCY   = "USD"

CAMPAIGNS = [
    {"campaign_id": "GGL_C001", "campaign_name": "Brand Search",     "network_type": "SEARCH"},
    {"campaign_id": "GGL_C002", "campaign_name": "Competitor Search", "network_type": "SEARCH"},
    {"campaign_id": "GGL_C003", "campaign_name": "Display Retarget",  "network_type": "DISPLAY"},
    {"campaign_id": "GGL_C004", "campaign_name": "YouTube Views",     "network_type": "VIDEO"},
]

AD_GROUPS = {
    "GGL_C001": [("GGL_AG001", "GGL_CR001", "Brand Core",   "Save More Today",   "Official Store"),
                 ("GGL_AG002", "GGL_CR002", "Brand Plus",   "Limited Offer",     "Shop Now")],
    "GGL_C002": [("GGL_AG003", "GGL_CR003", "Competitor A", "Better Than Rival", "Try Free")],
    "GGL_C003": [("GGL_AG004", "GGL_CR004", "Retarget All", "Come Back",         "Get 15% Off")],
    "GGL_C004": [("GGL_AG005", "GGL_CR005", "Awareness",    "See What's New",    "Watch Now")],
}

# ── Generate rows ─────────────────────────────────────────────────────────────
rows = []
dates = [START_DATE + timedelta(days=d) for d in range((END_DATE - START_DATE).days + 1)]

for camp in CAMPAIGNS:
    for ag_id, cr_id, ag_name, headline, description in AD_GROUPS[camp["campaign_id"]]:
        # Seasonal multiplier (Q4 spend spike)
        for dt in dates:
            seasonal = 1.0 + 0.4 * np.sin((dt.timetuple().tm_yday / 365) * 2 * np.pi - np.pi)
            base_spend = np.random.uniform(200, 2000) * seasonal

            cpm        = np.random.uniform(8, 40)        # cost per 1000 impressions
            impressions = int(base_spend / cpm * 1000)
            ctr        = np.random.uniform(0.005, 0.05)  # 0.5% to 5%
            clicks     = int(impressions * ctr)
            cvr        = np.random.uniform(0.01, 0.12)   # 1% to 12% conversion rate
            conversions = round(clicks * cvr, 2)
            aov        = np.random.uniform(35, 180)      # average order value
            roas       = round((conversions * aov) / base_spend, 2) if base_spend > 0 else 0

            rows.append({
                "date":               dt.isoformat(),
                "campaign_id":        camp["campaign_id"],
                "campaign_name":      camp["campaign_name"],
                "ad_group_id":        ag_id,
                "ad_group_name":      ag_name,
                "creative_id":        cr_id,
                "headline_1":         headline,
                "description_1":      description,
                "network_type":       camp["network_type"],
                "cost_micros":        int(base_spend * 1_000_000),   # Google quirk
                "impressions":        impressions,
                "clicks":             clicks,
                "ctr":                round(ctr, 4),
                "avg_cpc":            round(base_spend / clicks, 4) if clicks > 0 else 0,
                "cpm":                round(cpm, 2),
                "conversions":        conversions,
                "conversion_rate":    round(cvr, 4),
                "roas":               roas,
                "currency_code":      CURRENCY,
            })

# ── Save ──────────────────────────────────────────────────────────────────────
DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data/raw")
os.makedirs(DATA_DIR, exist_ok=True)
df = pd.DataFrame(rows)
out = os.path.join(DATA_DIR, "google_ads.csv")
df.to_csv(out, index=False)

print(f"✅ Google Ads: {len(df):,} rows saved to {out}")
print(f"   Date range: {df['date'].min()} → {df['date'].max()}")
print(f"   Campaigns:  {df['campaign_id'].nunique()}")
print(f"   Total cost_micros: {df['cost_micros'].sum():,}")
print(f"   Equivalent spend:  ${df['cost_micros'].sum() / 1_000_000:,.2f}")