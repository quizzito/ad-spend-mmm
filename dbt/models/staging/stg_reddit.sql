with source as (
    select * from {{ source('raw', 'reddit_ads_raw') }}
),

standardized as (
    select
        'reddit'                                        as platform,
        date::date                                      as date,
        campaign_id,
        campaign_name,
        ad_group_id,
        ad_group_name,
        creative_id,
        creative_name,
        placement,
        spend::numeric                                  as spend_usd,
        impressions::integer                            as impressions,
        clicks::integer                                 as clicks,
        conversions::numeric                            as conversions,
        ctr::numeric                                    as ctr,
        ecpm::numeric                                   as cpm,
        ecpc::numeric                                   as cpc,
        conversion_rate::numeric                        as conversion_rate,
        roas::numeric                                   as roas,
        currency
    from source
)

select * from standardized