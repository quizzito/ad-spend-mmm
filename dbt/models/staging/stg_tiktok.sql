with source as (
    select * from {{ source('raw', 'tiktok_ads_raw') }}
),

standardized as (
    select
        'tiktok'                                        as platform,
        stat_time_day::date                             as date,
        campaign_id,
        campaign_name,
        adgroup_id                                      as ad_group_id,
        adgroup_name                                    as ad_group_name,
        ad_id                                           as creative_id,
        ad_text                                         as creative_name,
        placement_type                                  as placement,
        spend::numeric                                  as spend_usd,
        impressions::integer                            as impressions,
        clicks::integer                                 as clicks,
        conversions::numeric                            as conversions,
        ctr::numeric                                    as ctr,
        cpm::numeric                                    as cpm,
        cpc::numeric                                    as cpc,
        conversion_rate::numeric                        as conversion_rate,
        real_time_roas::numeric                         as roas,
        currency
    from source
)

select * from standardized