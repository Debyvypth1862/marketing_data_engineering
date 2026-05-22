{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "GOOGLE_ANALYTICS",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('PAGES_AB1') }}
select
    try_cast(BOUNCE_RATE as {{ dbt_utils.type_string() }}) as BOUNCE_RATE,
    try_cast(TO_CHAR(TO_DATE(DATE, 'YYYYMMDD'), 'YYYY-MM-DD') as {{ dbt_utils.type_string() }}) as DATE,
    try_cast(HOST_NAME as {{ dbt_utils.type_string() }}) as HOST_NAME,
    try_cast(PAGE_PATH_PLUS_QUERY_STRING as {{ dbt_utils.type_string() }}) as PAGE_PATH_PLUS_QUERY_STRING,
    try_cast(PROPERTY_ID as {{ dbt_utils.type_string() }}) as PROPERTY_ID,
    try_cast(SCREEN_PAGE_VIEWS as {{ dbt_utils.type_string() }}) as SCREEN_PAGE_VIEWS,
    try_cast(UUID as {{ dbt_utils.type_string() }}) as UUID,

    
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('PAGES_AB1') }}

where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

