"""
extractors/03_generate_tiktok.py
Simulates TikTok Ads API response format.
Output: data/raw/tiktok_ads.csv
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
    {"campaign_id": "TTK_C001", "campaign_name": "TikTok Awareness — Gen Z"},
    {"campaign_id": "TTK_C002", "campaign_name": "TikTok Conversion — DPA"},
]

ADGROUPS = {
    "TTK_C001": [
        ("TTK_AG001", "TTK_AD001", "18-24 Interest", "TIKTOK", "Swipe Up For More"),
        ("TTK_AG002", "TTK_AD002", "25-34 Broad",    "PANGLE", "Shop the Look"),
    ],
    "TTK_C002": [
        ("TTK_AG003", "TTK_AD003", "Retarget Viewers", "TIKTOK", "Get Yours Now"),
        ("TTK_AG004", "TTK_AD004", "Lookalike Buyers",  "TIKTOK", "Limited Time"),
    ],
}

rows = []
dates = [START_DATE + timedelta(days=d) for d in range((END_DATE - START_DATE).days + 1)]

for camp in CAMPAIGNS:
    for ag_id, ad_id, ag_name, placement_type, cta in ADGROUPS[camp["campaign_id"]]:
        for dt in dates:
            seasonal = 1.0 + 0.3 * np.sin((dt.timetuple().tm_yday / 365) * 2 * np.pi - np.pi)
            spend = round(np.random.uniform(80, 1200) * seasonal, 2)

            cpm         = np.random.uniform(6, 30)
            impressions = int(spend / cpm * 1000)
            ctr         = np.random.uniform(0.01, 0.08)   # TikTok CTR tends higher
            clicks      = int(impressions * ctr)
            video_views = int(impressions * np.random.uniform(0.4, 0.85))  # TikTok-specific
            cvr         = np.random.uniform(0.005, 0.08)
            conversions = round(clicks * cvr, 2)
            aov         = np.random.uniform(30, 160)
            roas        = round((conversions * aov) / spend, 2) if spend > 0 else 0

            rows.append({
                "stat_time_day":   dt.isoformat(),     # TikTok date naming
                "campaign_id":     camp["campaign_id"],
                "campaign_name":   camp["campaign_name"],
                "adgroup_id":      ag_id,              # "adgroup" (no underscore) in TikTok API
                "adgroup_name":    ag_name,
                "ad_id":           ad_id,
                "ad_text":         ag_name + " — " + cta,
                "call_to_action":  cta,
                "placement_type":  placement_type,     # TIKTOK or PANGLE
                "spend":           spend,
                "impressions":     impressions,
                "clicks":          clicks,
                "video_views":     video_views,        # TikTok-only metric
                "ctr":             round(ctr, 4),
                "cpm":             round(cpm, 2),
                "cpc":             round(spend / clicks, 4) if clicks > 0 else 0,
                "conversions":     conversions,
                "conversion_rate": round(cvr, 4),
                "real_time_roas":  roas,
                "currency":        CURRENCY,
            })

DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data/raw")
os.makedirs(DATA_DIR, exist_ok=True)
df = pd.DataFrame(rows)
out = os.path.join(DATA_DIR, "tiktok_ads.csv")
df.to_csv(out, index=False)

print(f"✅ TikTok Ads: {len(df):,} rows saved to {out}")
print(f"   Date range: {df['stat_time_day'].min()} → {df['stat_time_day'].max()}")
print(f"   Campaigns:  {df['campaign_id'].nunique()}")
print(f"   Total spend: ${df['spend'].sum():,.2f}")