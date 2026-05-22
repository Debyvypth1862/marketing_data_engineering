{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "GOOGLE_ANALYTICS",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('FOUR_WEEKLY_ACTIVE_USERS_AB1') }}
select
    try_cast(ACTIVE_28_DAY_USERS as {{ dbt_utils.type_string() }}) as ACTIVE_28_DAY_USERS,
    try_cast(PROPERTY_ID as {{ dbt_utils.type_string() }}) as PROPERTY_ID,
    try_cast(UUID as {{ dbt_utils.type_string() }}) as UUID,
    try_cast(TO_CHAR(TO_DATE(DATE, 'YYYYMMDD'), 'YYYY-MM-DD') as {{ dbt_utils.type_string() }}) as DATE,
        
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('FOUR_WEEKLY_ACTIVE_USERS_AB1') }}

where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

