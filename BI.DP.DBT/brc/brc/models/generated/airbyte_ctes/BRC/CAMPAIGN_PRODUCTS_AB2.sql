{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(CAPR_CATEGORY as {{ dbt_utils.type_string() }}) as CAPR_CATEGORY,
	try_cast(CAPR_ID as {{ dbt_utils.type_float() }}) as CAPR_ID,
	try_cast(CAPR_NAME as {{ dbt_utils.type_string() }}) as CAPR_NAME,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,

	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('CAMPAIGN_PRODUCTS_AB1') }}
-- CAMPAIGN_PRODUCTS
where 1 = 1