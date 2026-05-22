{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(PAYR_ADDRESS as {{ dbt_utils.type_string() }}) as PAYR_ADDRESS,
	try_cast(PAYR_ADVERTISERS_ARRAY as {{ dbt_utils.type_string() }}) as PAYR_ADVERTISERS_ARRAY,
	try_cast(PAYR_CITY as {{ dbt_utils.type_string() }}) as PAYR_CITY,
	try_cast(PAYR_CONTACT as {{ dbt_utils.type_string() }}) as PAYR_CONTACT,
	try_cast(PAYR_COUNTRY as {{ dbt_utils.type_string() }}) as PAYR_COUNTRY,
	try_cast(PAYR_CREATED as {{ dbt_utils.type_string() }}) as PAYR_CREATED,
	try_cast(PAYR_CREATED_BY as {{ dbt_utils.type_float() }}) as PAYR_CREATED_BY,
	try_cast(PAYR_ID as {{ dbt_utils.type_float() }}) as PAYR_ID,
	try_cast(PAYR_NAME as {{ dbt_utils.type_string() }}) as PAYR_NAME,
	try_cast(PAYR_NOTE as {{ dbt_utils.type_string() }}) as PAYR_NOTE,
	try_cast(PAYR_PAYMENT_AGREEMENT as {{ dbt_utils.type_string() }}) as PAYR_PAYMENT_AGREEMENT,
	try_cast(PAYR_PAYMENT_PROVIDER as {{ dbt_utils.type_string() }}) as PAYR_PAYMENT_PROVIDER,
	try_cast(PAYR_REQUEST_DATE as {{ dbt_utils.type_string() }}) as PAYR_REQUEST_DATE,
	try_cast(PAYR_REQUEST_PAYMENT as {{ dbt_utils.type_float() }}) as PAYR_REQUEST_PAYMENT,
	try_cast(PAYR_UPDATED as {{ dbt_utils.type_string() }}) as PAYR_UPDATED,
	try_cast(PAYR_UPDATED_BY as {{ dbt_utils.type_float() }}) as PAYR_UPDATED_BY,
	try_cast(PAYR_VAT as {{ dbt_utils.type_string() }}) as PAYR_VAT,
	try_cast(PAYR_XERO_ID as {{ dbt_utils.type_string() }}) as PAYR_XERO_ID,
	try_cast(PAYR_ZIP as {{ dbt_utils.type_string() }}) as PAYR_ZIP,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('ADVERTISER_PAYERS_AB1') }}
-- ADVERTISER_PAYERS
where 1 = 1
