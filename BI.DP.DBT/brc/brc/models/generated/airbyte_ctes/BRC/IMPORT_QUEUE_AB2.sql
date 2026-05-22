{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
        try_cast(IMPO_CREATED as {{ dbt_utils.type_string() }}) as IMPO_CREATED,
        try_cast(IMPO_DONE as {{ dbt_utils.type_float() }}) as IMPO_DONE,
        try_cast(IMPO_ENDED_TIME as {{ dbt_utils.type_string() }}) as IMPO_ENDED_TIME,
        try_cast(IMPO_FK_ADVE_STRING as {{ dbt_utils.type_string() }}) as IMPO_FK_ADVE_STRING,
        try_cast(IMPO_FOLDER as {{ dbt_utils.type_string() }}) as IMPO_FOLDER,
        try_cast(IMPO_ID as {{ dbt_utils.type_string() }}) as IMPO_ID,
        try_cast(IMPO_INSTANCE as {{ dbt_utils.type_float() }}) as IMPO_INSTANCE,
        try_cast(IMPO_STARTED as {{ dbt_utils.type_float() }}) as IMPO_STARTED,
        try_cast(IMPO_STARTED_TIME as {{ dbt_utils.type_string() }}) as IMPO_STARTED_TIME,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('IMPORT_QUEUE_AB1') }}
-- IMPORT_QUEUE
where 1 = 1