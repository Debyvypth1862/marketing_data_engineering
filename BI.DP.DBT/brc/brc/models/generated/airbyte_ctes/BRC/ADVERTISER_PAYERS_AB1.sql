{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['payr_address'], ['payr_address']) }} as PAYR_ADDRESS,
	{{ json_extract_scalar('_airbyte_data', ['payr_advertisers_array'], ['payr_advertisers_array']) }} as PAYR_ADVERTISERS_ARRAY,
	{{ json_extract_scalar('_airbyte_data', ['payr_city'], ['payr_city']) }} as PAYR_CITY,
	{{ json_extract_scalar('_airbyte_data', ['payr_contact'], ['payr_contact']) }} as PAYR_CONTACT,
	{{ json_extract_scalar('_airbyte_data', ['payr_country'], ['payr_country']) }} as PAYR_COUNTRY,
	{{ json_extract_scalar('_airbyte_data', ['payr_created'], ['payr_created']) }} as PAYR_CREATED,
	{{ json_extract_scalar('_airbyte_data', ['payr_created_by'], ['payr_created_by']) }} as PAYR_CREATED_BY,
	{{ json_extract_scalar('_airbyte_data', ['payr_id'], ['payr_id']) }} as PAYR_ID,
	{{ json_extract_scalar('_airbyte_data', ['payr_name'], ['payr_name']) }} as PAYR_NAME,
	{{ json_extract_scalar('_airbyte_data', ['payr_note'], ['payr_note']) }} as PAYR_NOTE,
	{{ json_extract_scalar('_airbyte_data', ['payr_payment_agreement'], ['payr_payment_agreement']) }} as PAYR_PAYMENT_AGREEMENT,
	{{ json_extract_scalar('_airbyte_data', ['payr_payment_provider'], ['payr_payment_provider']) }} as PAYR_PAYMENT_PROVIDER,
	{{ json_extract_scalar('_airbyte_data', ['payr_request_date'], ['payr_request_date']) }} as PAYR_REQUEST_DATE,
	{{ json_extract_scalar('_airbyte_data', ['payr_request_payment'], ['payr_request_payment']) }} as PAYR_REQUEST_PAYMENT,
	{{ json_extract_scalar('_airbyte_data', ['payr_updated'], ['payr_updated']) }} as PAYR_UPDATED,
	{{ json_extract_scalar('_airbyte_data', ['payr_updated_by'], ['payr_updated_by']) }} as PAYR_UPDATED_BY,
	{{ json_extract_scalar('_airbyte_data', ['payr_vat'], ['payr_vat']) }} as PAYR_VAT,
	{{ json_extract_scalar('_airbyte_data', ['payr_xero_id'], ['payr_xero_id']) }} as PAYR_XERO_ID,
	{{ json_extract_scalar('_airbyte_data', ['payr_zip'], ['payr_zip']) }} as PAYR_ZIP,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_ADVERTISER_PAYERS') }} as table_alias
-- ADVERTISER_PAYERS
where 1 = 1
