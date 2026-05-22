{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['ster_backup_date'], ['ster_backup_date']) }} as STER_BACKUP_DATE,
	{{ json_extract_scalar('_airbyte_data', ['ster_checked_time'], ['ster_checked_time']) }} as STER_CHECKED_TIME,
	{{ json_extract_scalar('_airbyte_data', ['ster_fk_advertiser'], ['ster_fk_advertiser']) }} as STER_FK_ADVERTISER,
	{{ json_extract_scalar('_airbyte_data', ['ster_fk_publisher'], ['ster_fk_publisher']) }} as STER_FK_PUBLISHER,
	{{ json_extract_scalar('_airbyte_data', ['ster_id'], ['ster_id']) }} as STER_ID,
	{{ json_extract_scalar('_airbyte_data', ['ster_missing_json'], ['ster_missing_json']) }} as STER_MISSING_JSON,
	{{ json_extract_scalar('_airbyte_data', ['ster_month'], ['ster_month']) }} as STER_MONTH,
	{{ json_extract_scalar('_airbyte_data', ['ster_status'], ['ster_status']) }} as STER_STATUS,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_STATS_ERRORS') }} as table_alias
where 1 = 1
