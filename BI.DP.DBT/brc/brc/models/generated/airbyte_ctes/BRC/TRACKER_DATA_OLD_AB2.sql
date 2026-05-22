{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(TDAT_CLICKBOT as {{ dbt_utils.type_float() }}) as TDAT_CLICKBOT,
	try_cast(TDAT_CLICKS as {{ dbt_utils.type_float() }}) as TDAT_CLICKS,
	try_cast(TDAT_CPA as {{ dbt_utils.type_float() }}) as TDAT_CPA,
	try_cast(TDAT_DATE as {{ dbt_utils.type_string() }}) as TDAT_DATE,
	try_cast(TDAT_DEPOSIT_VALUE as {{ dbt_utils.type_float() }}) as TDAT_DEPOSIT_VALUE,
	try_cast(TDAT_DEPOSITS as {{ dbt_utils.type_float() }}) as TDAT_DEPOSITS,
	try_cast(TDAT_EXTRA_INCOME as {{ dbt_utils.type_float() }}) as TDAT_EXTRA_INCOME,
	try_cast(TDAT_EXTRA_PAYOUT as {{ dbt_utils.type_float() }}) as TDAT_EXTRA_PAYOUT,
	try_cast(TDAT_FK_CAMPAIGN_KEY as {{ dbt_utils.type_string() }}) as TDAT_FK_CAMPAIGN_KEY,
	try_cast(TDAT_FK_CAMPAIGN_TRACKER as {{ dbt_utils.type_float() }}) as TDAT_FK_CAMPAIGN_TRACKER,
	try_cast(TDAT_FK_CUSTOM as {{ dbt_utils.type_float() }}) as TDAT_FK_CUSTOM,
	try_cast(TDAT_NEW_DEPOSITS as {{ dbt_utils.type_float() }}) as TDAT_NEW_DEPOSITS,
	try_cast(TDAT_POSTBACK as {{ dbt_utils.type_float() }}) as TDAT_POSTBACK,
	try_cast(TDAT_SCRIPT as {{ dbt_utils.type_float() }}) as TDAT_SCRIPT,
	try_cast(TDAT_SIGNUPS as {{ dbt_utils.type_float() }}) as TDAT_SIGNUPS,
	try_cast(TDAT_TOTAL as {{ dbt_utils.type_float() }}) as TDAT_TOTAL,
	try_cast(TDAT_VIEWS as {{ dbt_utils.type_float() }}) as TDAT_VIEWS,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('TRACKER_DATA_OLD_AB1') }}
-- TRACKER_DATA_OLD
where 1 = 1