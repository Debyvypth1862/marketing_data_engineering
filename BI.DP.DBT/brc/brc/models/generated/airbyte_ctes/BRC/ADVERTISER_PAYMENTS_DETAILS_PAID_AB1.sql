{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['payd_amount'], ['payd_amount']) }} as PAYD_AMOUNT,
	{{ json_extract_scalar('_airbyte_data', ['payd_comment'], ['payd_comment']) }} as PAYD_COMMENT,
	{{ json_extract_scalar('_airbyte_data', ['payd_date'], ['payd_date']) }} as PAYD_DATE,
	{{ json_extract_scalar('_airbyte_data', ['payd_fk_adde_id'], ['payd_fk_adde_id']) }} as PAYD_FK_ADDE_ID,
	{{ json_extract_scalar('_airbyte_data', ['payd_fk_adpa_id'], ['payd_fk_adpa_id']) }} as PAYD_FK_ADPA_ID,
	{{ json_extract_scalar('_airbyte_data', ['payd_id'], ['payd_id']) }} as PAYD_ID,
	{{ json_extract_scalar('_airbyte_data', ['payd_received_by'], ['payd_received_by']) }} as PAYD_RECEIVED_BY,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_ADVERTISER_PAYMENTS_DETAILS_PAID') }} as table_alias
-- ADVERTISER_PAYMENTS_DETAILS_PAID
where 1 = 1
