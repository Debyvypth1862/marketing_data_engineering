{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['capr_category'], ['capr_category']) }} as CAPR_CATEGORY,
	{{ json_extract_scalar('_airbyte_data', ['capr_id'], ['capr_id']) }} as CAPR_ID,
	{{ json_extract_scalar('_airbyte_data', ['capr_name'], ['capr_name']) }} as CAPR_NAME,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_CAMPAIGN_PRODUCTS') }} as table_alias
where 1 = 1
