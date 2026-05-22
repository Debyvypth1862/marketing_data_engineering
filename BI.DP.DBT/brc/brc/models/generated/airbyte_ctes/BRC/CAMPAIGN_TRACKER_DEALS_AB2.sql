{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(CATD_CPA_IN as {{ dbt_utils.type_float() }}) as CATD_CPA_IN,
	try_cast(CATD_CPA_OUT as {{ dbt_utils.type_float() }}) as CATD_CPA_OUT,
	try_cast(CATD_CPL_IN as {{ dbt_utils.type_float() }}) as CATD_CPL_IN,
	try_cast(CATD_CPL_OUT as {{ dbt_utils.type_float() }}) as CATD_CPL_OUT,
	try_cast(CATD_DISPLAY_DEAL as {{ dbt_utils.type_string() }}) as CATD_DISPLAY_DEAL,
	try_cast(CATD_FK_CAMT_ID as {{ dbt_utils.type_float() }}) as CATD_FK_CAMT_ID,
	try_cast(CATD_ID as {{ dbt_utils.type_float() }}) as CATD_ID,
	try_cast(CATD_REV_IN as {{ dbt_utils.type_float() }}) as CATD_REV_IN,
	try_cast(CATD_REV_OUT as {{ dbt_utils.type_float() }}) as CATD_REV_OUT,
	try_cast(CATD_START_MONTH as {{ dbt_utils.type_string() }}) as CATD_START_MONTH,
	try_cast(CATD_UPDATED as {{ dbt_utils.type_string() }}) as CATD_UPDATED,
	try_cast(CATD_UPDATED_BY as {{ dbt_utils.type_float() }}) as CATD_UPDATED_BY,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('CAMPAIGN_TRACKER_DEALS_AB1') }}
-- CAMPAIGN_TRACKER_DEALS
where 1 = 1