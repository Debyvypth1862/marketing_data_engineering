{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
        try_cast(STRING_EN as {{ dbt_utils.type_string() }}) as STRING_EN,
        try_cast(STRING_ID as {{ dbt_utils.type_float() }}) as STRING_ID,
        try_cast(STRING_NAME as {{ dbt_utils.type_string() }}) as STRING_NAME,
        try_cast(STRING_PAGE as {{ dbt_utils.type_string() }}) as STRING_PAGE,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,

        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('CLIENTAREA_STRINGS_AB1') }}
-- CLIENTAREA_STRINGS
where 1 = 1