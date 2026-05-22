
{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
        try_cast(TLOG_ACCOUNT_ID as {{ dbt_utils.type_string() }}) as TLOG_ACCOUNT_ID,
        try_cast(TLOG_CREATED as {{ dbt_utils.type_string() }}) as TLOG_CREATED,
        try_cast(TLOG_CREATED_BY as {{ dbt_utils.type_float() }}) as TLOG_CREATED_BY,
        try_cast(TLOG_DELETED as {{ dbt_utils.type_float() }}) as TLOG_DELETED,
        try_cast(TLOG_DONT_IMPORT as {{ dbt_utils.type_float() }}) as TLOG_DONT_IMPORT,
        try_cast(TLOG_ERROR as {{ dbt_utils.type_string() }}) as TLOG_ERROR,
        try_cast(TLOG_FK_ADVERTISER as {{ dbt_utils.type_float() }}) as TLOG_FK_ADVERTISER,
        try_cast(TLOG_FK_PUBLISHER as {{ dbt_utils.type_float() }}) as TLOG_FK_PUBLISHER,
        try_cast(TLOG_HISTORY as {{ dbt_utils.type_string() }}) as TLOG_HISTORY,
        try_cast(TLOG_ID as {{ dbt_utils.type_float() }}) as TLOG_ID,
        try_cast(TLOG_IMPORT_TIME as {{ dbt_utils.type_string() }}) as TLOG_IMPORT_TIME,
        try_cast(TLOG_NOTE as {{ dbt_utils.type_string() }}) as TLOG_NOTE,
        try_cast(TLOG_PASSWORD as {{ dbt_utils.type_string() }}) as TLOG_PASSWORD,
        try_cast(TLOG_REMOTE_KEY1 as {{ dbt_utils.type_string() }}) as TLOG_REMOTE_KEY1,
        try_cast(TLOG_REMOTE_KEY2 as {{ dbt_utils.type_string() }}) as TLOG_REMOTE_KEY2,
        try_cast(TLOG_REMOTE_KEY3 as {{ dbt_utils.type_string() }}) as TLOG_REMOTE_KEY3,
        try_cast(TLOG_STATUS as {{ dbt_utils.type_float() }}) as TLOG_STATUS,
        try_cast(TLOG_STATUS_DATE as {{ dbt_utils.type_string() }}) as TLOG_STATUS_DATE,
        try_cast(TLOG_SUB as {{ dbt_utils.type_float() }}) as TLOG_SUB,
        try_cast(TLOG_TYPE as {{ dbt_utils.type_string() }}) as TLOG_TYPE,
        try_cast(TLOG_UPDATED as {{ dbt_utils.type_string() }}) as TLOG_UPDATED,
        try_cast(TLOG_UPDATED_BY as {{ dbt_utils.type_float() }}) as TLOG_UPDATED_BY,
        try_cast(TLOG_USERNAME as {{ dbt_utils.type_string() }}) as TLOG_USERNAME,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('TRACKER_LOGINS_AB1') }}
-- TRACKER_LOGINS
where 1 = 1