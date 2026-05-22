{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
        {{ json_extract_scalar('_airbyte_data', ['emst_fk_email'], ['emst_fk_email']) }} as EMST_FK_EMAIL,
        {{ json_extract_scalar('_airbyte_data', ['emst_fk_publisher'], ['emst_fk_publisher']) }} as EMST_FK_PUBLISHER,
        {{ json_extract_scalar('_airbyte_data', ['emst_id'], ['emst_id']) }} as EMST_ID,
        {{ json_extract_scalar('_airbyte_data', ['emst_open_time'], ['emst_open_time']) }} as EMST_OPEN_TIME,
        {{ json_extract_scalar('_airbyte_data', ['emst_send_status'], ['emst_send_status']) }} as EMST_SEND_STATUS,
        {{ json_extract_scalar('_airbyte_data', ['emst_send_time'], ['emst_send_time']) }} as EMST_SEND_TIME,
        {{ json_extract_scalar('_airbyte_data', ['emst_to_email'], ['emst_to_email']) }} as EMST_TO_EMAIL,
        {{ json_extract_scalar('_airbyte_data', ['emst_unsub_time'], ['emst_unsub_time']) }} as EMST_UNSUB_TIME,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_EMAIL_SENDOUT_STATS') }} as table_alias
-- EMAIL_SENDOUT_STATS
where 1 = 1
