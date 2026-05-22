{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	{{ json_extract_scalar('_airbyte_data', ['podo_domain'], ['podo_domain']) }} as PODO_DOMAIN,
	{{ json_extract_scalar('_airbyte_data', ['podo_id'], ['podo_id']) }} as PODO_ID,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_POSTBACK_DOMAINS') }} as table_alias
-- POSTBACK_DOMAINS
where 1 = 1
