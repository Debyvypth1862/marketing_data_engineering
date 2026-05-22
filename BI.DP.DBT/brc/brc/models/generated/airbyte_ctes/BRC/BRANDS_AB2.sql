{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(BRAN_ID as {{ dbt_utils.type_float() }}) as BRAN_ID,
	try_cast(BRAN_NAME as {{ dbt_utils.type_string() }}) as BRAN_NAME,
	try_cast(BRAN_PLATFORM as {{ dbt_utils.type_string() }}) as BRAN_PLATFORM,
	try_cast(BRAN_SLUG as {{ dbt_utils.type_string() }}) as BRAN_SLUG,
	try_cast(BRAN_URL as {{ dbt_utils.type_string() }}) as BRAN_URL,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('BRANDS_AB1') }}
-- BRANDS
where 1 = 1
