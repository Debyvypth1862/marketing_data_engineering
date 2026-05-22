{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['paym_created'], ['paym_created']) }} as PAYM_CREATED,
	{{ json_extract_scalar('_airbyte_data', ['paym_created_by'], ['paym_created_by']) }} as PAYM_CREATED_BY,
	{{ json_extract_scalar('_airbyte_data', ['paym_fk_publisher'], ['paym_fk_publisher']) }} as PAYM_FK_PUBLISHER,
	{{ json_extract_scalar('_airbyte_data', ['paym_id'], ['paym_id']) }} as PAYM_ID,
	{{ json_extract_scalar('_airbyte_data', ['paym_note'], ['paym_note']) }} as PAYM_NOTE,
	{{ json_extract_scalar('_airbyte_data', ['paym_payment_info'], ['paym_payment_info']) }} as PAYM_PAYMENT_INFO,
	{{ json_extract_scalar('_airbyte_data', ['paym_period_from'], ['paym_period_from']) }} as PAYM_PERIOD_FROM,
	{{ json_extract_scalar('_airbyte_data', ['paym_period_to'], ['paym_period_to']) }} as PAYM_PERIOD_TO,
	{{ json_extract_scalar('_airbyte_data', ['paym_status'], ['paym_status']) }} as PAYM_STATUS,
	{{ json_extract_scalar('_airbyte_data', ['paym_updated'], ['paym_updated']) }} as PAYM_UPDATED,
	{{ json_extract_scalar('_airbyte_data', ['paym_updated_by'], ['paym_updated_by']) }} as PAYM_UPDATED_BY,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_PAYMENTS') }} as table_alias
-- PAYMENTS
where 1 = 1
