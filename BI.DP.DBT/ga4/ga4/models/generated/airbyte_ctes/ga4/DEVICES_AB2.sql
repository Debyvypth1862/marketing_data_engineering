{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "GOOGLE_ANALYTICS",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('DEVICES_AB1') }}
select
    try_cast(UUID as {{ dbt_utils.type_string() }}) as UUID,
    try_cast(PROPERTY_ID as {{ dbt_utils.type_string() }}) as PROPERTY_ID,
    try_cast(TO_CHAR(TO_DATE(DATE, 'YYYYMMDD'), 'YYYY-MM-DD') as {{ dbt_utils.type_string() }}) as DATE,
    try_cast(DEVICE_CATEGORY as {{ dbt_utils.type_string() }}) as DEVICE_CATEGORY,
    try_cast(OPERATING_SYSTEM as {{ dbt_utils.type_string() }}) as OPERATING_SYSTEM,
    try_cast(BROWSER as {{ dbt_utils.type_string() }}) as BROWSER,
    try_cast(TOTAL_USERS as {{ dbt_utils.type_string() }}) as TOTAL_USERS,
    try_cast(NEW_USERS as {{ dbt_utils.type_string() }}) as NEW_USERS,
    try_cast(SESSIONS as {{ dbt_utils.type_string() }}) as SESSIONS,
    try_cast(SESSIONS_PERUSER as {{ dbt_utils.type_string() }}) as SESSIONS_PERUSER,
    try_cast(AVERAGE_SESSION_DURATION as {{ dbt_utils.type_string() }}) as AVERAGE_SESSION_DURATION,
    try_cast(SCREEN_PAGE_VIEWS as {{ dbt_utils.type_string() }}) as SCREEN_PAGE_VIEWS,
    try_cast(SCREEN_PAGE_VIEWS_PER_SESSION as {{ dbt_utils.type_string() }}) as SCREEN_PAGE_VIEWS_PER_SESSION,
    try_cast(BOUNCE_RATE as {{ dbt_utils.type_string() }}) as BOUNCE_RATE,    
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('DEVICES_AB1') }}

where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

