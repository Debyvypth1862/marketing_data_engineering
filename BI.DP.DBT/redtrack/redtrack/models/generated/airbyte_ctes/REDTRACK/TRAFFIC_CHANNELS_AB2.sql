{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "REDTRACK",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to try_cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('TRAFFIC_CHANNELS_AB1') }}
select

    try_cast(ALIAS as {{ dbt_utils.type_string() }}) as ALIAS,
    try_cast(CAMPAIGN_COUNT as {{ dbt_utils.type_string() }}) as CAMPAIGN_COUNT,
    try_cast(COST_ID as {{ dbt_utils.type_string() }}) as COST_ID,
    try_cast(COST_LEVEL as {{ dbt_utils.type_string() }}) as COST_LEVEL,
    try_cast(COST_MODELS as {{ dbt_utils.type_string() }}) as COST_MODELS,
    try_cast(CREATED_AT as {{ dbt_utils.type_string() }}) as CREATED_AT,
    try_cast(CURRENCY as {{ dbt_utils.type_string() }}) as CURRENCY,
    try_cast(ENABLE_DIRECT_TRAFFIC as {{ dbt_utils.type_string() }}) as ENABLE_DIRECT_TRAFFIC,
    try_cast(ENABLE_IMPRESSIONS as {{ dbt_utils.type_string() }}) as ENABLE_IMPRESSIONS,
    try_cast(ENABLE_PARALLEL_TRACKING as {{ dbt_utils.type_string() }}) as ENABLE_PARALLEL_TRACKING,
    try_cast(EXTERNAL_ID as {{ dbt_utils.type_string() }}) as EXTERNAL_ID,
    try_cast(EXTERNAL_ID_ALIAS as {{ dbt_utils.type_string() }}) as EXTERNAL_ID_ALIAS,
    try_cast(FORMATS as {{ dbt_utils.type_string() }}) as FORMATS,
    try_cast(GOOGLE_ANALYTICS_KEY as {{ dbt_utils.type_string() }}) as GOOGLE_ANALYTICS_KEY,
    try_cast(ID as {{ dbt_utils.type_string() }}) as ID,
    try_cast(IMP_COST_ID as {{ dbt_utils.type_string() }}) as IMP_COST_ID,
    try_cast(IMP_ID as {{ dbt_utils.type_string() }}) as IMP_ID,
    try_cast(INTEGRATION_ID as {{ dbt_utils.type_string() }}) as INTEGRATION_ID,
    try_cast(INTEGRATION_TYPES as {{ dbt_utils.type_string() }}) as INTEGRATION_TYPES,
    try_cast(INTEGRATIONS as {{ dbt_utils.type_string() }}) as INTEGRATIONS,
    try_cast(POSTBACK_PIXEL as {{ dbt_utils.type_string() }}) as POSTBACK_PIXEL,
    try_cast(POSTBACK_URL as {{ dbt_utils.type_string() }}) as POSTBACK_URL,
    try_cast(PRESET_ID as {{ dbt_utils.type_string() }}) as PRESET_ID,
    try_cast(REF_ID as {{ dbt_utils.type_string() }}) as REF_ID,
    try_cast(REF_ID_ALIAS as {{ dbt_utils.type_string() }}) as REF_ID_ALIAS,
    try_cast(SERIAL_NUMBER as {{ dbt_utils.type_string() }}) as SERIAL_NUMBER,
    try_cast(STAT as {{ dbt_utils.type_string() }}) as STAT,
    try_cast(STATUS as {{ dbt_utils.type_string() }}) as STATUS,
    try_cast(SUBS as {{ dbt_utils.type_string() }}) as SUBS,
    try_cast(TITLE as {{ dbt_utils.type_string() }}) as TITLE,
    try_cast(TYPE as {{ dbt_utils.type_string() }}) as TYPE,
    try_cast(UPDATED_AT as {{ dbt_utils.type_string() }}) as UPDATED_AT,
    try_cast(USER_ID as {{ dbt_utils.type_string() }}) as USER_ID,
    _AIRBYTE_AB_ID,
    S3_PATH,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('TRAFFIC_CHANNELS_AB1') }}
-- TRAFFIC_CHANNELS
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

