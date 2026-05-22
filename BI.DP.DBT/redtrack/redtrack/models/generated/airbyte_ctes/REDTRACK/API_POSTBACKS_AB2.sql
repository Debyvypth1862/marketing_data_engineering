{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "REDTRACK",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to try_cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('API_POSTBACKS_AB1') }}
select
    try_cast(ALIAS as {{ dbt_utils.type_string() }}) as ALIAS,
    try_cast(ATTEMPTS as {{ dbt_utils.type_string() }}) as ATTEMPTS,
    try_cast(CAMPAIGN as {{ dbt_utils.type_string() }}) as CAMPAIGN,
    try_cast(CAMPAIGN_ID as {{ dbt_utils.type_string() }}) as CAMPAIGN_ID,
    try_cast(CONVERSION_ID as {{ dbt_utils.type_string() }}) as CONVERSION_ID,
    try_cast(CREATED_AT as {{ dbt_utils.type_string() }}) as CREATED_AT,
    try_cast(DESTINATION as {{ dbt_utils.type_string() }}) as DESTINATION,
    try_cast(ERROR as {{ dbt_utils.type_string() }}) as ERROR,
    try_cast(ID as {{ dbt_utils.type_string() }}) as ID,
    try_cast(PREVIOUS_AT as {{ dbt_utils.type_string() }}) as PREVIOUS_AT,
    try_cast(REF_ID as {{ dbt_utils.type_string() }}) as REF_ID,
    try_cast(SOURCE as {{ dbt_utils.type_string() }}) as SOURCE,
    try_cast(SOURCE_ID as {{ dbt_utils.type_string() }}) as SOURCE_ID,
    try_cast(STATUS as {{ dbt_utils.type_string() }}) as STATUS,
    try_cast(TOTAL as {{ dbt_utils.type_string() }}) as TOTAL,
    try_cast(TRACK_ID as {{ dbt_utils.type_string() }}) as TRACK_ID,
    try_cast(TYPE as {{ dbt_utils.type_string() }}) as TYPE,
    try_cast(USER_ID as {{ dbt_utils.type_string() }}) as USER_ID,
    _AIRBYTE_AB_ID,
    S3_PATH,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('API_POSTBACKS_AB1') }}
-- API_POSTBACKS
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

