with source as (
    select * from {{ source('raw', 'meta_ads_raw') }}
),

standardized as (
    select
        'meta'                                          as platform,
        date_start::date                                as date,
        campaign_id,
        campaign_name,
        adset_id                                        as ad_group_id,
        adset_name                                      as ad_group_name,
        ad_id                                           as creative_id,
        ad_name                                         as creative_name,
        publisher_platform || '_' || placement          as placement,
        spend::numeric                                  as spend_usd,
        impressions::integer                            as impressions,
        clicks::integer                                 as clicks,
        actions::numeric                                as conversions,
        ctr::numeric                                    as ctr,
        cpm::numeric                                    as cpm,
        cpc::numeric                                    as cpc,
        conversion_rate::numeric                        as conversion_rate,
        purchase_roas::numeric                          as roas,
        currency
    from source
)

select * from standardized