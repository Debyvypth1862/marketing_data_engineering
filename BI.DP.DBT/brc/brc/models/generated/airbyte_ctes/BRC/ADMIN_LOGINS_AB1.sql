{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['logi_fk_admin'], ['logi_fk_admin']) }} as LOGI_FK_ADMIN,
	{{ json_extract_scalar('_airbyte_data', ['logi_id'], ['logi_id']) }} as LOGI_ID,
	{{ json_extract_scalar('_airbyte_data', ['logi_ip'], ['logi_ip']) }} as LOGI_IP,
	{{ json_extract_scalar('_airbyte_data', ['logi_timestamp'], ['logi_timestamp']) }} as LOGI_TIMESTAMP,
	{{ json_extract_scalar('_airbyte_data', ['logi_useragent'], ['logi_useragent']) }} as LOGI_USERAGENT,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_ADMIN_LOGINS') }} as table_alias
-- ADMIN_LOGINS

WHERE 1=1
