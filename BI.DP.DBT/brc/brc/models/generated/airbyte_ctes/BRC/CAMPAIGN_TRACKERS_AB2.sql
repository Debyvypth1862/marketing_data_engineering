{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(CAMT_CAMPAIGN_KEY as {{ dbt_utils.type_string() }}) as CAMT_CAMPAIGN_KEY,
	try_cast(CAMT_CHILD as {{ dbt_utils.type_float() }}) as CAMT_CHILD,
	try_cast(CAMT_CHILD_NAME as {{ dbt_utils.type_string() }}) as CAMT_CHILD_NAME,
	try_cast(CAMT_CPA_IN as {{ dbt_utils.type_float() }}) as CAMT_CPA_IN,
	try_cast(CAMT_CPA_OUT as {{ dbt_utils.type_float() }}) as CAMT_CPA_OUT,
	try_cast(CAMT_CPL_IN as {{ dbt_utils.type_float() }}) as CAMT_CPL_IN,
	try_cast(CAMT_CPL_OUT as {{ dbt_utils.type_float() }}) as CAMT_CPL_OUT,
	try_cast(CAMT_CREATED as {{ dbt_utils.type_string() }}) as CAMT_CREATED,
	try_cast(CAMT_CREATED_BY as {{ dbt_utils.type_float() }}) as CAMT_CREATED_BY,
	try_cast(CAMT_DEAL_DATE as {{ dbt_utils.type_string() }}) as CAMT_DEAL_DATE,
	try_cast(CAMT_DISPLAY_DEAL as {{ dbt_utils.type_string() }}) as CAMT_DISPLAY_DEAL,
	try_cast(CAMT_FK_ADVERTISER as {{ dbt_utils.type_float() }}) as CAMT_FK_ADVERTISER,
	try_cast(CAMT_FK_CAMPAIGN as {{ dbt_utils.type_float() }}) as CAMT_FK_CAMPAIGN,
	try_cast(CAMT_FK_LOGIN as {{ dbt_utils.type_float() }}) as CAMT_FK_LOGIN,
	try_cast(CAMT_FK_PUBLISHER as {{ dbt_utils.type_float() }}) as CAMT_FK_PUBLISHER,
	try_cast(CAMT_HIDDEN as {{ dbt_utils.type_float() }}) as CAMT_HIDDEN,
	try_cast(CAMT_HISTORY as {{ dbt_utils.type_string() }}) as CAMT_HISTORY,
	try_cast(CAMT_ID as {{ dbt_utils.type_float() }}) as CAMT_ID,
	try_cast(CAMT_PUBLISHER_LAST_VISIT as {{ dbt_utils.type_string() }}) as CAMT_PUBLISHER_LAST_VISIT,
	try_cast(CAMT_REV_IN as {{ dbt_utils.type_float() }}) as CAMT_REV_IN,
	try_cast(CAMT_REV_OUT as {{ dbt_utils.type_float() }}) as CAMT_REV_OUT,
	try_cast(CAMT_SHOW as {{ dbt_utils.type_float() }}) as CAMT_SHOW,
	try_cast(CAMT_STATUS as {{ dbt_utils.type_string() }}) as CAMT_STATUS,
	try_cast(CAMT_SUBID_ACTIVATED as {{ dbt_utils.type_float() }}) as CAMT_SUBID_ACTIVATED,
	try_cast(CAMT_TYPE as {{ dbt_utils.type_string() }}) as CAMT_TYPE,
	try_cast(CAMT_UPDATED as {{ dbt_utils.type_string() }}) as CAMT_UPDATED,
	try_cast(CAMT_UPDATED_BY as {{ dbt_utils.type_float() }}) as CAMT_UPDATED_BY,
	try_cast(CAMT_URL as {{ dbt_utils.type_string() }}) as CAMT_URL,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('CAMPAIGN_TRACKERS_AB1') }}
-- CAMPAIGN_TRACKERS
where 1 = 1