{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select 
    try_cast(CAMP_CATEGORY as {{ dbt_utils.type_string() }}) as CAMP_CATEGORY,
	try_cast(CAMP_COUNTRY as {{ dbt_utils.type_string() }}) as CAMP_COUNTRY,
	try_cast(CAMP_CPA_IN as {{ dbt_utils.type_float() }}) as CAMP_CPA_IN,
	try_cast(CAMP_CPA_OUT as {{ dbt_utils.type_float() }}) as CAMP_CPA_OUT,
	try_cast(CAMP_CPL_IN as {{ dbt_utils.type_float() }}) as CAMP_CPL_IN,
	try_cast(CAMP_CPL_OUT as {{ dbt_utils.type_float() }}) as CAMP_CPL_OUT,
	try_cast(CAMP_CREATED as {{ dbt_utils.type_string() }}) as CAMP_CREATED,
	try_cast(CAMP_CREATED_BY as {{ dbt_utils.type_float() }}) as CAMP_CREATED_BY,
	try_cast(CAMP_CURRENCY as {{ dbt_utils.type_string() }}) as CAMP_CURRENCY,
	try_cast(CAMP_DEPOSIT_BASELINE as {{ dbt_utils.type_string() }}) as CAMP_DEPOSIT_BASELINE,
	try_cast(CAMP_DESCRIPTION as {{ dbt_utils.type_string() }}) as CAMP_DESCRIPTION,
	try_cast(CAMP_DISPLAY_DEAL as {{ dbt_utils.type_string() }}) as CAMP_DISPLAY_DEAL,
	try_cast(CAMP_ENDED as {{ dbt_utils.type_string() }}) as CAMP_ENDED,
	try_cast(CAMP_FK_ADVERTISER as {{ dbt_utils.type_float() }}) as CAMP_FK_ADVERTISER,
	try_cast(CAMP_FK_BRAND as {{ dbt_utils.type_float() }}) as CAMP_FK_BRAND,
	try_cast(CAMP_HIDDEN as {{ dbt_utils.type_float() }}) as CAMP_HIDDEN,
	try_cast(CAMP_HISTORY as {{ dbt_utils.type_string() }}) as CAMP_HISTORY,
	try_cast(CAMP_HYBRID as {{ dbt_utils.type_float() }}) as CAMP_HYBRID,
	try_cast(CAMP_ID as {{ dbt_utils.type_float() }}) as CAMP_ID,
	try_cast(CAMP_LANGUAGE as {{ dbt_utils.type_string() }}) as CAMP_LANGUAGE,
	try_cast(CAMP_LICENSED as {{ dbt_utils.type_string() }}) as CAMP_LICENSED,
	try_cast(CAMP_NAME as {{ dbt_utils.type_string() }}) as CAMP_NAME,
	try_cast(CAMP_REV_DEAL as {{ dbt_utils.type_float() }}) as CAMP_REV_DEAL,
	try_cast(CAMP_REV_IN as {{ dbt_utils.type_float() }}) as CAMP_REV_IN,
	try_cast(CAMP_REV_OUT as {{ dbt_utils.type_float() }}) as CAMP_REV_OUT,
	try_cast(CAMP_STATUS as {{ dbt_utils.type_string() }}) as CAMP_STATUS,
	try_cast(CAMP_TRAFFIC_SOURCE as {{ dbt_utils.type_string() }}) as CAMP_TRAFFIC_SOURCE,
	try_cast(CAMP_TYPE as {{ dbt_utils.type_string() }}) as CAMP_TYPE,
	try_cast(CAMP_UPDATED as {{ dbt_utils.type_string() }}) as CAMP_UPDATED,
	try_cast(CAMP_UPDATED_BY as {{ dbt_utils.type_float() }}) as CAMP_UPDATED_BY,
	try_cast(CAMP_WAGER_BASELINE as {{ dbt_utils.type_string() }}) as CAMP_WAGER_BASELINE,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('CAMPAIGNS_AB1') }}
-- CAMPAIGNS
where 1 = 1