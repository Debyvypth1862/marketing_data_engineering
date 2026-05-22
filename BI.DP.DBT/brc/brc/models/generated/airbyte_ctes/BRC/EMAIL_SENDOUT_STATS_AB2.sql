{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
        try_cast(EMST_FK_EMAIL as {{ dbt_utils.type_float() }}) as EMST_FK_EMAIL,
        try_cast(EMST_FK_PUBLISHER as {{ dbt_utils.type_float() }}) as EMST_FK_PUBLISHER,
        try_cast(EMST_ID as {{ dbt_utils.type_float() }}) as EMST_ID,
        try_cast(EMST_OPEN_TIME as {{ dbt_utils.type_string() }}) as EMST_OPEN_TIME,
        try_cast(EMST_SEND_STATUS as {{ dbt_utils.type_string() }}) as EMST_SEND_STATUS,
        try_cast(EMST_SEND_TIME as {{ dbt_utils.type_string() }}) as EMST_SEND_TIME,
        try_cast(EMST_TO_EMAIL as {{ dbt_utils.type_string() }}) as EMST_TO_EMAIL,
        try_cast(EMST_UNSUB_TIME as {{ dbt_utils.type_string() }}) as EMST_UNSUB_TIME,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('EMAIL_SENDOUT_STATS_AB1') }}
-- EMAIL_SENDOUT_STATS
where 1 = 1