{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}


-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('BRT', '_AIRBYTE_RAW_GOOGLE_ADS_CAMPAIGN_MARKETING_SITE') }}
select
    {{ json_extract_scalar('_airbyte_data', ['google_ads_campaign_id'], ['google_ads_campaign_id']) }} as GOOGLE_ADS_CAMPAIGN_ID,
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as ID,
    {{ json_extract_scalar('_airbyte_data', ['marketing_site_id'], ['marketing_site_id']) }} as MARKETING_SITE_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRT', '_AIRBYTE_RAW_GOOGLE_ADS_CAMPAIGN_MARKETING_SITE') }} as table_alias
-- GOOGLE_ADS_CAMPAIGN_MARKETING_SITE
where 1 = 1

