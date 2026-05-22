{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(PODO_DOMAIN as {{ dbt_utils.type_string() }}) as PODO_DOMAIN,
	try_cast(PODO_ID as {{ dbt_utils.type_float() }}) as PODO_ID,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('POSTBACK_DOMAINS_AB1') }}
-- POSTBACK_DOMAINS
where 1 = 1
