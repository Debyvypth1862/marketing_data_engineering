{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
        try_cast(IMST_ID as {{ dbt_utils.type_float() }}) as IMST_ID,
        try_cast(IMST_NAME as {{ dbt_utils.type_string() }}) as IMST_NAME,
        try_cast(IMST_OUTPUT as {{ dbt_utils.type_string() }}) as IMST_OUTPUT,
        try_cast(IMST_TIMESTAMP as {{ dbt_utils.type_string() }}) as IMST_TIMESTAMP,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('IMPORT_STATUS_AB1') }}
-- IMPORT_STATUS
where 1 = 1
