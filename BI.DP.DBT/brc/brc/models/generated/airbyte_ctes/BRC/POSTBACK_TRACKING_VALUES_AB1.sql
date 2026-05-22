{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['potv_clickid'], ['potv_clickid']) }} as POTV_CLICKID,
	{{ json_extract_scalar('_airbyte_data', ['potv_date'], ['potv_date']) }} as POTV_DATE,
	{{ json_extract_scalar('_airbyte_data', ['potv_deposit_value'], ['potv_deposit_value']) }} as POTV_DEPOSIT_VALUE,
	{{ json_extract_scalar('_airbyte_data', ['potv_id'], ['potv_id']) }} as POTV_ID,
	{{ json_extract_scalar('_airbyte_data', ['potv_revshare'], ['potv_revshare']) }} as POTV_REVSHARE,
	{{ json_extract_scalar('_airbyte_data', ['potv_timestamp'], ['potv_timestamp']) }} as POTV_TIMESTAMP,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_POSTBACK_TRACKING_VALUES') }} as table_alias
where 1 = 1
