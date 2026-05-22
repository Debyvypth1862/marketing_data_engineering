{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['adpa_created'], ['adpa_created']) }} as ADPA_CREATED,
	{{ json_extract_scalar('_airbyte_data', ['adpa_id'], ['adpa_id']) }} as ADPA_ID,
	{{ json_extract_scalar('_airbyte_data', ['adpa_month'], ['adpa_month']) }} as ADPA_MONTH,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_ADVERTISER_PAYMENTS') }} as table_alias
-- ADVERTISER_PAYMENTS
where 1 = 1
