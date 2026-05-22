{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(CDRE_ACCEPTED_SEEN as {{ dbt_utils.type_float() }}) as CDRE_ACCEPTED_SEEN,
	try_cast(CDRE_FK_CAMP_DEAL as {{ dbt_utils.type_float() }}) as CDRE_FK_CAMP_DEAL,
	try_cast(CDRE_FK_PUBLISHER as {{ dbt_utils.type_float() }}) as CDRE_FK_PUBLISHER,
	try_cast(CDRE_ID as {{ dbt_utils.type_float() }}) as CDRE_ID,
	try_cast(CDRE_NOTE as {{ dbt_utils.type_string() }}) as CDRE_NOTE,
	try_cast(CDRE_REQUEST_DATE as {{ dbt_utils.type_string() }}) as CDRE_REQUEST_DATE,
	try_cast(CDRE_STATUS as {{ dbt_utils.type_string() }}) as CDRE_STATUS,
	try_cast(CDRE_UPDATE_DATE as {{ dbt_utils.type_string() }}) as CDRE_UPDATE_DATE,
	try_cast(CDRE_UPDATED_BY as {{ dbt_utils.type_float() }}) as CDRE_UPDATED_BY,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('CAMPAIGN_DEAL_REQUESTS_AB1') }}
-- CAMPAIGN_DEAL_REQUESTS
where 1 = 1