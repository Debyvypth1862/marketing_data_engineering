{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}


-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('BRT', '_AIRBYTE_RAW_GOOGLE_ADS_CAMPAIGNS') }}
select
    {{ json_extract_scalar('_airbyte_data', ['notes'], ['notes']) }} as NOTES,
    {{ json_extract_scalar('_airbyte_data', ['budget_timing'], ['budget_timing']) }} as BUDGET_TIMING,
    {{ json_extract_scalar('_airbyte_data', ['campaign_type'], ['campaign_type']) }} as CAMPAIGN_TYPE,
    {{ json_extract_scalar('_airbyte_data', ['created_at'], ['created_at']) }} as CREATED_AT,
    {{ json_extract_scalar('_airbyte_data', ['created_by'], ['created_by']) }} as CREATED_BY,
    {{ json_extract_scalar('_airbyte_data', ['deleted_at'], ['deleted_at']) }} as DELETED_AT,
    {{ json_extract_scalar('_airbyte_data', ['currency_code'], ['currency_code']) }} as CURRENCY_CODE,
    {{ json_extract_scalar('_airbyte_data', ['google_ads_account_id'], ['google_ads_account_id']) }} as GOOGLE_ADS_ACCOUNT_ID,
    {{ json_extract_scalar('_airbyte_data', ['updated_at'], ['updated_at']) }} as UPDATED_AT,
    {{ json_extract_scalar('_airbyte_data', ['name'], ['name']) }} as NAME,
    {{ json_extract_scalar('_airbyte_data', ['updated_by'], ['updated_by']) }} as UPDATED_BY,
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as ID,
    {{ json_extract_scalar('_airbyte_data', ['campaign_id'], ['campaign_id']) }} as CAMPAIGN_ID,
    {{ json_extract_scalar('_airbyte_data', ['data_collection_method'], ['data_collection_method']) }} as DATA_COLLECTION_METHOD,
    {{ json_extract_scalar('_airbyte_data', ['status'], ['status']) }} as STATUS,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRT', '_AIRBYTE_RAW_GOOGLE_ADS_CAMPAIGNS') }} as table_alias
-- GOOGLE_ADS_CAMPAIGNS
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

