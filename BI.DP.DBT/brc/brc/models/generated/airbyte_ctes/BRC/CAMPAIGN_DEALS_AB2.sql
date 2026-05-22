{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(CADE_CAMP_ARRAY as {{ dbt_utils.type_string() }}) as CADE_CAMP_ARRAY,
	try_cast(CADE_CREATED as {{ dbt_utils.type_string() }}) as CADE_CREATED,
	try_cast(CADE_CREATED_BY as {{ dbt_utils.type_float() }}) as CADE_CREATED_BY,
	try_cast(CADE_DEAL as {{ dbt_utils.type_string() }}) as CADE_DEAL,
	try_cast(CADE_DEAL_TEXT as {{ dbt_utils.type_string() }}) as CADE_DEAL_TEXT,
	try_cast(CADE_DESCRIPTION as {{ dbt_utils.type_string() }}) as CADE_DESCRIPTION,
	try_cast(CADE_FEATURED as {{ dbt_utils.type_float() }}) as CADE_FEATURED,
	try_cast(CADE_FK_ADVERTISER as {{ dbt_utils.type_float() }}) as CADE_FK_ADVERTISER,
	try_cast(CADE_ID as {{ dbt_utils.type_float() }}) as CADE_ID,
	try_cast(CADE_LANG_ARRAY as {{ dbt_utils.type_string() }}) as CADE_LANG_ARRAY,
	try_cast(CADE_LIVE as {{ dbt_utils.type_float() }}) as CADE_LIVE,
	try_cast(CADE_POSITION as {{ dbt_utils.type_float() }}) as CADE_POSITION,
	try_cast(CADE_URL as {{ dbt_utils.type_string() }}) as CADE_URL,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('CAMPAIGN_DEALS_AB1') }}
-- CAMPAIGN_DEALS
where 1 = 1