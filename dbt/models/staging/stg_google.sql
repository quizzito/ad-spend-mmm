with source as (
    select * from {{ source('raw', 'google_ads_raw') }}
),

standardized as (
    select
        'google'                                        as platform,
        date::date                                      as date,
        campaign_id,
        campaign_name,
        ad_group_id,
        ad_group_name,
        creative_id,
        headline_1                                      as creative_name,
        network_type                                    as placement,
        round(cost_micros::numeric / 1000000.0, 2)     as spend_usd,
        impressions::integer                            as impressions,
        clicks::integer                                 as clicks,
        conversions::numeric                            as conversions,
        ctr::numeric                                    as ctr,
        cpm::numeric                                    as cpm,
        avg_cpc::numeric                                as cpc,
        conversion_rate::numeric                        as conversion_rate,
        roas::numeric                                   as roas,
        currency_code                                   as currency
    from source
)

select * from standardized