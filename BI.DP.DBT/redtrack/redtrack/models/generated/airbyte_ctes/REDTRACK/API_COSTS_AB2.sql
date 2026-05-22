{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "REDTRACK",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to try_cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('API_COSTS_AB1') }}
select
    try_cast(CAMPAIGN as {{ dbt_utils.type_string() }}) as CAMPAIGN,
    try_cast(CAMPAIGN_ID as {{ dbt_utils.type_string() }}) as CAMPAIGN_ID,
    try_cast(COUNTRY as {{ dbt_utils.type_string() }}) as COUNTRY,
    try_cast(CREATED_AT as {{ dbt_utils.type_string() }}) as CREATED_AT,
    try_cast(CURRENCY as {{ dbt_utils.type_string() }}) as CURRENCY,
    try_cast(ID as {{ dbt_utils.type_string() }}) as ID,
    try_cast(LEVEL as {{ dbt_utils.type_string() }}) as LEVEL,
    try_cast(PERIOD as {{ dbt_utils.type_string() }}) as PERIOD,
    try_cast(RT_AD_ID as {{ dbt_utils.type_string() }}) as RT_AD_ID,
    try_cast(RT_ADGROUP_ID as {{ dbt_utils.type_string() }}) as RT_ADGROUP_ID,
    try_cast(RT_CAMPAIGN_ID as {{ dbt_utils.type_string() }}) as RT_CAMPAIGN_ID,
    try_cast(RT_PLACEMENT_ID as {{ dbt_utils.type_string() }}) as RT_PLACEMENT_ID,
    try_cast(SOURCE_ALIAS as {{ dbt_utils.type_string() }}) as SOURCE_ALIAS,
    try_cast(SOURCE_COST as {{ dbt_utils.type_string() }}) as SOURCE_COST,
    try_cast(SOURCE_TIMEZONE as {{ dbt_utils.type_string() }}) as SOURCE_TIMEZONE,
    try_cast(TIME_FROM as {{ dbt_utils.type_string() }}) as TIME_FROM,
    try_cast(TIME_TO as {{ dbt_utils.type_string() }}) as TIME_TO,
    try_cast(USER_ID as {{ dbt_utils.type_string() }}) as USER_ID,
    try_cast(USER_TIMEZONE as {{ dbt_utils.type_string() }}) as USER_TIMEZONE,
    _AIRBYTE_AB_ID,
    S3_PATH,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('API_COSTS_AB1') }}
-- API_COSTS
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

