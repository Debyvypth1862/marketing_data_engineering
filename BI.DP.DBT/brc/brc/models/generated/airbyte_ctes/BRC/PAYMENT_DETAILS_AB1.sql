{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['pade_amount'], ['pade_amount']) }} as PADE_AMOUNT,
	{{ json_extract_scalar('_airbyte_data', ['pade_description'], ['pade_description']) }} as PADE_DESCRIPTION,
	{{ json_extract_scalar('_airbyte_data', ['pade_fk_payment'], ['pade_fk_payment']) }} as PADE_FK_PAYMENT,
	{{ json_extract_scalar('_airbyte_data', ['pade_id'], ['pade_id']) }} as PADE_ID,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_PAYMENT_DETAILS') }} as table_alias
-- PAYMENT_DETAILS
where 1 = 1
