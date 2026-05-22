{{ config(
    materialized = "view",
    database = env_var('INTM_DATABASE', 'INTM'),
    schema = "API_ENDPOINT"
) }}
-- STG: Add hash for change detection
with base as (
    select
        {{ dbt_utils.surrogate_key([
            "PUBLISHER",
            "COUNTRY",
            "ADGROUPID",
            "ADACCOUNTID",
            "BRC_CAMPAIGNID",
            "BRC_CAMPAIGNNAME",
            "CAMPAIGNID",
            "PL_CAMPAIGNID",
            "CLICK_DATE",
            "CONVERSION_TIME",
            "IPADDRESS",
            "GCLID",
            "POST_3RD_PARTY_CLICKID",
            "CONVERSION_NAME",
            "CONVERSION_CURRENCY",
            "CONVERSION_VALUE"
        ]) }} as _AIRBYTE_API_GCLID_REPORT_HASHID,
        {{ current_timestamp() }} as _AIRBYTE_EMITTED_AT,
        *
    from {{ ref('API_GCLID_REPORT_AB1') }}
)
select * from base
