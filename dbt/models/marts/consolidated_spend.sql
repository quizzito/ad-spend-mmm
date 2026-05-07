-- dbt/models/marts/consolidated_spend.sql
-- Unified ad spend table: all 4 platforms, standardized schema.
-- Materialized as a physical TABLE (not a view) — queryable by external tools.

with google as (
    select * from {{ ref('stg_google') }}
),

meta as (
    select * from {{ ref('stg_meta') }}
),

tiktok as (
    select * from {{ ref('stg_tiktok') }}
),

reddit as (
    select * from {{ ref('stg_reddit') }}
),

unioned as (
    select * from google
    union all
    select * from meta
    union all
    select * from tiktok
    union all
    select * from reddit
),

final as (
    select
        -- identifiers
        platform,
        date,
        campaign_id,
        campaign_name,
        ad_group_id,
        ad_group_name,
        creative_id,
        creative_name,
        placement,
        currency,

        -- spend
        spend_usd,

        -- volume
        impressions,
        clicks,
        conversions,

        -- rates (guard against divide-by-zero upstream)
        coalesce(ctr, 0)               as ctr,
        coalesce(cpm, 0)               as cpm,
        coalesce(cpc, 0)               as cpc,
        coalesce(conversion_rate, 0)   as conversion_rate,
        coalesce(roas, 0)              as roas,

        -- derived metrics (computed here for MMM convenience)
        round(spend_usd / nullif(impressions, 0) * 1000, 4)    as effective_cpm,
        round(conversions / nullif(clicks, 0), 4)              as effective_cvr,
        round(spend_usd / nullif(conversions, 0), 4)           as cost_per_conversion

    from unioned
)

select * from final
order by date, platform, campaign_id