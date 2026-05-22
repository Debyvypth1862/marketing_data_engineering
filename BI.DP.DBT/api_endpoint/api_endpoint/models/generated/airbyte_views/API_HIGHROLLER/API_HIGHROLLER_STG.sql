{{ config(
    materialized = "table",
    database = "EXP",
    schema = "PUBLIC",
    alias = "API_HIGHROLLER"
) }}
-- STG: Add hash for change detection
with base as (
    select
        {{ dbt_utils.surrogate_key([
            "date",
            "signup_date",
            "ftd_date",
            "country",
            "publisher_name",
            "advertiser_id",
            "advertiser_name",
            "camp_name",
            "brand_name",
            "clickid",
            "click_cnt",
            "signup_cnt",
            "ftd_cnt",
            "deposit_amt",
            "ftd_amt",
            "net_revenue_amt"
        ]) }} as _AIRBYTE_API_HIGHROLLER_HASHID,
        {{ current_timestamp() }} as _AIRBYTE_EMITTED_AT,
        *
    from {{ ref('API_HIGHROLLER_AB1') }}
)
select * from base