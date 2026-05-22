{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
        {{ json_extract_scalar('_airbyte_data', ['tlog_account_id'], ['tlog_account_id']) }} as TLOG_ACCOUNT_ID,
        {{ json_extract_scalar('_airbyte_data', ['tlog_created'], ['tlog_created']) }} as TLOG_CREATED,
        {{ json_extract_scalar('_airbyte_data', ['tlog_created_by'], ['tlog_created_by']) }} as TLOG_CREATED_BY,
        {{ json_extract_scalar('_airbyte_data', ['tlog_deleted'], ['tlog_deleted']) }} as TLOG_DELETED,
        {{ json_extract_scalar('_airbyte_data', ['tlog_dont_import'], ['tlog_dont_import']) }} as TLOG_DONT_IMPORT,
        {{ json_extract_scalar('_airbyte_data', ['tlog_error'], ['tlog_error']) }} as TLOG_ERROR,
        {{ json_extract_scalar('_airbyte_data', ['tlog_fk_advertiser'], ['tlog_fk_advertiser']) }} as TLOG_FK_ADVERTISER,
        {{ json_extract_scalar('_airbyte_data', ['tlog_fk_publisher'], ['tlog_fk_publisher']) }} as TLOG_FK_PUBLISHER,
        {{ json_extract_scalar('_airbyte_data', ['tlog_history'], ['tlog_history']) }} as TLOG_HISTORY,
        {{ json_extract_scalar('_airbyte_data', ['tlog_id'], ['tlog_id']) }} as TLOG_ID,
        {{ json_extract_scalar('_airbyte_data', ['tlog_import_time'], ['tlog_import_time']) }} as TLOG_IMPORT_TIME,
        {{ json_extract_scalar('_airbyte_data', ['tlog_note'], ['tlog_note']) }} as TLOG_NOTE,
        {{ json_extract_scalar('_airbyte_data', ['tlog_password'], ['tlog_password']) }} as TLOG_PASSWORD,
        {{ json_extract_scalar('_airbyte_data', ['tlog_remote_key1'], ['tlog_remote_key1']) }} as TLOG_REMOTE_KEY1,
        {{ json_extract_scalar('_airbyte_data', ['tlog_remote_key2'], ['tlog_remote_key2']) }} as TLOG_REMOTE_KEY2,
        {{ json_extract_scalar('_airbyte_data', ['tlog_remote_key3'], ['tlog_remote_key3']) }} as TLOG_REMOTE_KEY3,
        {{ json_extract_scalar('_airbyte_data', ['tlog_status'], ['tlog_status']) }} as TLOG_STATUS,
        {{ json_extract_scalar('_airbyte_data', ['tlog_status_date'], ['tlog_status_date']) }} as TLOG_STATUS_DATE,
        {{ json_extract_scalar('_airbyte_data', ['tlog_sub'], ['tlog_sub']) }} as TLOG_SUB,
        {{ json_extract_scalar('_airbyte_data', ['tlog_type'], ['tlog_type']) }} as TLOG_TYPE,
        {{ json_extract_scalar('_airbyte_data', ['tlog_updated'], ['tlog_updated']) }} as TLOG_UPDATED,
        {{ json_extract_scalar('_airbyte_data', ['tlog_updated_by'], ['tlog_updated_by']) }} as TLOG_UPDATED_BY,
        {{ json_extract_scalar('_airbyte_data', ['tlog_username'], ['tlog_username']) }} as TLOG_USERNAME,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_TRACKER_LOGINS') }} as table_alias
-- TRACKER_LOGINS
where 1 = 1
