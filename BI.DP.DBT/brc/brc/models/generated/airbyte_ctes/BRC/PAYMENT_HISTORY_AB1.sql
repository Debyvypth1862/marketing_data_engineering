{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['pahi_fk_payment'], ['pahi_fk_payment']) }} as PAHI_FK_PAYMENT,
	{{ json_extract_scalar('_airbyte_data', ['pahi_id'], ['pahi_id']) }} as PAHI_ID,
	{{ json_extract_scalar('_airbyte_data', ['pahi_table_payment_details'], ['pahi_table_payment_details']) }} as PAHI_TABLE_PAYMENT_DETAILS,
	{{ json_extract_scalar('_airbyte_data', ['pahi_table_payments'], ['pahi_table_payments']) }} as PAHI_TABLE_PAYMENTS,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_PAYMENT_HISTORY') }} as table_alias
-- PAYMENT_HISTORY
where 1 = 1
