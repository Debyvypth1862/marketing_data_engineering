{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
        try_cast(BLOCK_ID as {{ dbt_utils.type_float() }}) as BLOCK_ID,
        try_cast(BLOCK_IP as {{ dbt_utils.type_string() }}) as BLOCK_IP,
        try_cast(BLOCK_TIMESTAMP as {{ dbt_utils.type_string() }}) as BLOCK_TIMESTAMP,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,

        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('IP_BLOCK_AB1') }}
-- IP_BLOCK
where 1 = 1